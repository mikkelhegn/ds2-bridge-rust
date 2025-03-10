use anyhow::{anyhow, Result};
use ds2::Datastream2Log;
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    file::properties::{WriterProperties, WriterVersion},
};
use spin_sdk::{
    http::{IntoResponse, Params, Request, Response, Router},
    http_component,
};
use std::io::Cursor;

mod ds2;

#[http_component]
fn handle_ds2_parquet(req: Request) -> Result<impl IntoResponse> {
    let mut router = Router::new();
    router.post_async("/api/v1/transform/single/*", transform_single);
    router.post_async("/api/v1/transform/multiple/*", transform_multiple);

    Ok(router.handle(req))
}

async fn transform_single(req: Request, _params: Params) -> Result<impl IntoResponse> {
    // Parse the request body as a Datastream2Log
    let body = req.body();
    let log: Datastream2Log = serde_json::from_slice(body)
        .map_err(|e| anyhow!("Failed to parse Datastream2 log: {}", e))?;

    // Create the Parquet schema
    let schema = ds2::create_schema();

    // Convert the log to a record batch
    let batch = ds2::log_to_record_batch(&log, schema.clone())?;

    // Create a buffer to hold the Parquet data
    let mut buffer = Vec::new();
    let cursor = Cursor::new(&mut buffer);

    // Configure the Parquet writer properties
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();

    // Create the Parquet writer
    let mut writer = ArrowWriter::try_new(cursor, schema, Some(props))
        .map_err(|e| anyhow!("Failed to create Parquet writer: {}", e))?;

    // Write the record batch
    writer
        .write(&batch)
        .map_err(|e| anyhow!("Failed to write record batch: {}", e))?;

    // Close the writer and get the buffer
    writer
        .close()
        .map_err(|e| anyhow!("Failed to close Parquet writer: {}", e))?;

    // Return the Parquet data as the response using the builder pattern
    let response = Response::builder()
        .status(200)
        .header("Content-Type", "application/vnd.apache.parquet")
        .header(
            "Content-Disposition",
            "attachment; filename=\"datastream2.parquet\"",
        )
        .body(buffer)
        .build();

    Ok(response)
}

async fn transform_multiple(req: Request, _params: Params) -> Result<impl IntoResponse> {
    // Parse the request body as an array of Datastream2Logs
    let body = req.body();
    let logs: Vec<Datastream2Log> = serde_json::from_slice(body)
        .map_err(|e| anyhow!("Failed to parse Datastream2 logs: {}", e))?;

    if logs.is_empty() {
        return Ok(Response::builder()
            .status(400)
            .header("Content-Type", "text/plain")
            .body("No logs provided")
            .build());
    }

    // Create the Parquet schema
    let schema = ds2::create_schema();

    // Create a buffer to hold the Parquet data
    let mut buffer = Vec::new();
    let cursor = Cursor::new(&mut buffer);

    // Configure the Parquet writer properties
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
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

    // Return the Parquet data as the response
    let response = Response::builder()
        .status(200)
        .header("Content-Type", "application/vnd.apache.parquet")
        .header(
            "Content-Disposition",
            "attachment; filename=\"datastream2_multiple.parquet\"",
        )
        .body(buffer)
        .build();

    Ok(response)
}
