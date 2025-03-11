use anyhow::{anyhow, Result};
use ds2::extract_logs;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::{WriterProperties, WriterVersion},
};
use spin_sdk::{
    http::{IntoResponse, Method, Params, Request, Response, Router},
    http_component,
};
use std::io::Cursor;

mod ds2;

#[http_component]
fn handle_ds2_parquet(req: Request) -> Result<impl IntoResponse> {
    let mut router = Router::new();
    router.post_async("/api/v1/transform/multiple/*", transform);

    Ok(router.handle(req))
}

async fn transform(req: Request, _params: Params) -> Result<impl IntoResponse> {
    let body_len = req.body().len();

    let start = chrono::Utc::now();

    let logs = extract_logs(req)?;
    let logs_len = logs.len();

    // Create the Parquet schema
    let schema = ds2::create_schema();

    // Create a buffer to hold the Parquet data
    let mut buffer = Vec::new();
    let cursor = Cursor::new(&mut buffer);

    // Configure the Parquet writer properties
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();

    // Create the Parquet writer
    let mut writer = ArrowWriter::try_new(cursor, schema.clone(), Some(props))
        .map_err(|e| anyhow!("Failed to create Parquet writer: {}", e))?;

    // Process each log and write to the Parquet file
    for log in logs {
        // Convert the log to a record batch
        let batch = ds2::log_to_record_batch(&log, schema.clone())?;

        // Write the record batch
        writer
            .write(&batch)
            .map_err(|e| anyhow!("Failed to write record batch: {}", e))?;
    }

    // Close the writer and get the buffer
    writer
        .close()
        .map_err(|e| anyhow!("Failed to close Parquet writer: {}", e))?;

    let file_name = format!("log-{}.parquet", start.format("%Y-%m-%d-%H-%M-%S"));
    let request = Request::builder()
        .method(Method::Post)
        .uri(format!(
            "https://s3-uploader.spin.internal/api/v1/upload/{}",
            file_name
        ))
        .body(buffer)
        .build();

    let response: Response = spin_sdk::http::send(request).await?;

    // If uploading to S3 did not return 200, log the error and return 500, so DS2 tries again
    if *response.status() != 200 {
        println!("Response: {:?}", response);
        return Err(anyhow!("Failed to upload to S3"));
    }

    let end = chrono::Utc::now();
    let duration = end - start;
    let duration_ms = duration.num_milliseconds();

    println!(
        "Processed {} logs (request size: {} bytes) in {} ms, file name: {}",
        logs_len, body_len, duration_ms, file_name
    );

    Ok(response)
}
