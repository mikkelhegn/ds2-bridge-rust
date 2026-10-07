use anyhow::{anyhow, Result};
use aws_credential_types::Credentials;
use aws_sdk_s3::config::{BehaviorVersion, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use aws_smithy_async::rt::sleep::SharedAsyncSleep;
use aws_smithy_async::time::SharedTimeSource;
use ds2::extract_logs;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::{WriterProperties, WriterVersion},
};
use spin_sdk::http::body::IncomingBodyExt;
use spin_sdk::http::{IntoResponse, Request};
use spin_sdk::http_service;
use spin_sdk::variables;
use std::io::Cursor;
use http;

use crate::aws::{WasiHttpClient, WasiNow, WasiSleep};

mod aws;
mod ds2;

#[http_service]
async fn handle_ds2_parquet(req: Request) -> Result<impl IntoResponse> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stdout)
        .try_init();

    // Check if it's a POST request
    if req.method() != http::Method::POST {
        return Ok(http::Response::builder()
            .status(405)
            .header("Allow", "POST")
            .body("Method not allowed".to_string())
            .unwrap());
    }

    // Check if path starts with /api/v1/transform/multiple/
    if !req.uri().path().starts_with("/api/v1/transform/multiple") {
        return Ok(http::Response::builder()
            .status(404)
            .body("Path not found".to_string())
            .unwrap());
    }

    // Forward to transform function
    let result = transform(req).await?;
    Ok(http::Response::builder()
        .status(200)
        .body(result)
        .unwrap())
}

async fn transform(req: Request) -> Result<String> {
    let body_bytes = req.into_body().bytes().await?;
    let body_len = body_bytes.len();

    let start = chrono::Utc::now();

    let logs = extract_logs(&body_bytes)?;
    let logs_len = logs.len();

    // Create the Parquet schema
    let schema = ds2::create_schema();

    // Create a buffer to hold the Parquet data
    let mut buffer = Vec::new();
    let cursor = Cursor::new(&mut buffer);

    // Configure the Parquet writer properties
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        // PARQUET_2_0 enables delta encoders (e.g. DeltaBitPackEncoder for ints) that each
        // eagerly allocate a hardcoded 1MB buffer (BitWriter::new(DEFAULT_BIT_WRITER_SIZE)),
        // regardless of any WriterProperties page-size setting. With 61 columns that alone
        // accounted for ~125MB of peak memory on wasm32. PARQUET_1_0's PLAIN/RLE_DICTIONARY
        // encoders grow their buffers lazily instead.
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .build();

    // Create the Parquet writer
    let mut writer = ArrowWriter::try_new(cursor, schema.clone(), Some(props))
        .map_err(|e| anyhow!("Failed to create Parquet writer: {}", e))?;

    // Convert all logs into a single columnar record batch and write it once
    let batch = ds2::logs_to_record_batch(&logs, schema)?;

    writer
        .write(&batch)
        .map_err(|e| anyhow!("Failed to write record batch: {}", e))?;

    // Close the writer and get the buffer
    writer
        .close()
        .map_err(|e| anyhow!("Failed to close Parquet writer: {}", e))?;

    let file_name = format!("log-{}.parquet", start.format("%Y-%m-%d-%H-%M-%S"));

    // Get S3 configuration from Spin variables
    let bucket = variables::get("s3_bucket").await?;
    let region = variables::get("s3_region").await?;
    let endpoint = variables::get("s3_endpoint").await?;
    let access_key_id = variables::get("s3_access_key_id").await?;
    let secret_access_key = variables::get("s3_secret_access_key").await?;

    // Build S3 client with custom WASI implementations
    let credentials = Credentials::new(
        access_key_id,
        secret_access_key,
        None,
        None,
        "spin-variables",
    );

    let s3_config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(credentials)
        .region(Region::new(region))
        .endpoint_url(&endpoint)
        .force_path_style(true)
        .http_client(WasiHttpClient)
        .sleep_impl(SharedAsyncSleep::new(WasiSleep))
        .time_source(SharedTimeSource::new(WasiNow))
        .build();

    let s3_client = S3Client::from_conf(s3_config);

    // Upload to S3
    tracing::info!("Uploading {} bytes to S3 bucket: {}, key: {}", buffer.len(), bucket, file_name);
    let body = ByteStream::from(buffer);

    match s3_client
        .put_object()
        .bucket(&bucket)
        .key(&file_name)
        .content_type("application/vnd.apache.parquet")
        .body(body)
        .send()
        .await
    {
        Ok(_) => tracing::info!("Successfully uploaded to S3"),
        Err(e) => {
            tracing::error!("S3 upload error details: {:?}", e);
            return Err(anyhow!("Failed to upload to S3: {}", e));
        }
    }

    let end = chrono::Utc::now();
    let duration = end - start;
    let duration_ms = duration.num_milliseconds();

    tracing::info!(
        "Processed {} logs (request size: {} bytes) in {} ms, uploaded to S3: {}",
        logs_len, body_len, duration_ms, file_name
    );

    Ok("File uploaded successfully".to_string())
}
