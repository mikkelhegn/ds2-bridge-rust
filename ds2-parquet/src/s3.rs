use aws_credential_types::Credentials;
use aws_sdk_s3::config::{BehaviorVersion, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_smithy_async::rt::sleep::SharedAsyncSleep;
use aws_smithy_async::time::SharedTimeSource;
use spin_sdk::variables;

use crate::aws::{WasiHttpClient, WasiNow, WasiSleep};
use crate::functions::FunctionLog;

// Maximum number of logs to include in a single S3 file
const MAX_LOGS_PER_FILE: usize = 100;
// Number of retry attempts for failed uploads
const MAX_RETRIES: u32 = 3;
// Initial backoff delay in milliseconds
const INITIAL_BACKOFF_MS: u64 = 1000;

pub struct S3 {
    bucket: String,
    prefix: String,
    client: aws_sdk_s3::Client,
}

impl S3 {
    pub async fn from_env() -> Option<Box<dyn crate::destinations::LogDestination>> {
        Self::try_new().await.map(|s3| Box::new(s3) as Box<dyn crate::destinations::LogDestination>)
    }

    async fn try_new() -> Option<Self> {
        let access_key_id = variables::get("s3_access_key_id")
            .await
            .ok()
            .filter(|s| !s.is_empty())?;
        let secret_access_key = variables::get("s3_secret_access_key")
            .await
            .ok()
            .filter(|s| !s.is_empty())?;
        let bucket = variables::get("s3_bucket").await.ok().filter(|s| !s.is_empty())?;
        let endpoint_url = variables::get("s3_endpoint_url")
            .await
            .ok()
            .filter(|s| !s.is_empty())?;
        let region = variables::get("s3_region").await.ok().filter(|s| !s.is_empty())?;
        let prefix = variables::get("s3_prefix").await.ok().filter(|s| !s.is_empty())?;

        tracing::info!(
            "S3 configured: endpoint={}, bucket={}, region={}, prefix={}",
            endpoint_url,
            bucket,
            region,
            prefix
        );

        let credentials = Credentials::new(
            access_key_id,
            secret_access_key,
            None,
            None,
            "spin-variables",
        );

        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(credentials)
            .region(Region::new(region))
            .endpoint_url(endpoint_url)
            .force_path_style(true)
            .http_client(WasiHttpClient)
            .sleep_impl(SharedAsyncSleep::new(WasiSleep))
            .time_source(SharedTimeSource::new(WasiNow))
            .build();

        Some(Self {
            bucket,
            prefix,
            client: aws_sdk_s3::Client::from_conf(config),
        })
    }
}

impl S3 {
    /// Upload a single batch of logs to S3 with retry logic
    async fn upload_with_retry(&self, logs: &[FunctionLog], batch_index: usize) -> anyhow::Result<()> {
        let now = chrono::Utc::now();
        let key = format!(
            "{}/{}/{}-{}.json",
            self.prefix,
            now.format("%Y/%m/%d"),
            now.format("%Y-%m-%dT%H-%M-%S%.3fZ"),
            batch_index,
        );

        let body = serde_json::to_vec(logs)?;

        tracing::info!(
            "S3 upload batch {}: {} logs ({} bytes) to s3://{}/{}",
            batch_index,
            logs.len(),
            body.len(),
            self.bucket,
            key
        );

        let mut last_error = None;

        for attempt in 0..MAX_RETRIES {
            if attempt > 0 {
                let backoff_ms = INITIAL_BACKOFF_MS * 2_u64.pow(attempt - 1);
                tracing::info!(
                    "Retrying S3 upload (attempt {}/{}) after {}ms backoff",
                    attempt + 1,
                    MAX_RETRIES,
                    backoff_ms
                );

                // Simple busy-wait sleep (WASM doesn't have native async sleep)
                let start = std::time::Instant::now();
                while start.elapsed().as_millis() < backoff_ms as u128 {
                    // Small spin to avoid blocking completely
                    async { () }.await;
                }
            }

            let result = self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .content_type("application/json")
                .body(ByteStream::from(body.clone()))
                .send()
                .await;

            match result {
                Ok(_) => {
                    tracing::info!(
                        "Successfully uploaded batch {} to s3://{}/{} (attempt {}/{})",
                        batch_index,
                        self.bucket,
                        key,
                        attempt + 1,
                        MAX_RETRIES
                    );
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(
                        "S3 upload failed for batch {} (attempt {}/{}): {:?}",
                        batch_index,
                        attempt + 1,
                        MAX_RETRIES,
                        e
                    );
                    last_error = Some(e);
                }
            }
        }

        // All retries exhausted
        let error = last_error.unwrap();
        tracing::error!(
            "S3 upload failed for batch {} after {} attempts: {:?}",
            batch_index,
            MAX_RETRIES,
            error
        );
        Err(anyhow::anyhow!(
            "Failed to upload to S3 after {} attempts: {:?}",
            MAX_RETRIES,
            error
        ))
    }
}

#[async_trait::async_trait(?Send)]
impl crate::destinations::LogDestination for S3 {
    fn name(&self) -> &'static str {
        "S3"
    }

    async fn ingest(&self, logs: &[FunctionLog]) -> anyhow::Result<()> {
        if logs.is_empty() {
            return Ok(());
        }

        // Split logs into chunks to avoid large uploads timing out
        let chunks: Vec<&[FunctionLog]> = logs.chunks(MAX_LOGS_PER_FILE).collect();
        let total_chunks = chunks.len();

        tracing::info!(
            "Splitting {} logs into {} batch(es) of max {} logs each",
            logs.len(),
            total_chunks,
            MAX_LOGS_PER_FILE
        );

        let mut successful = 0;
        let mut failed = 0;

        for (index, chunk) in chunks.iter().enumerate() {
            match self.upload_with_retry(chunk, index).await {
                Ok(_) => {
                    successful += 1;
                }
                Err(e) => {
                    failed += 1;
                    tracing::error!("Failed to upload batch {}/{}: {}", index + 1, total_chunks, e);
                    // Continue with other batches instead of failing immediately
                }
            }
        }

        if failed > 0 {
            anyhow::bail!(
                "S3 upload completed with failures: {} successful, {} failed out of {} batches",
                successful,
                failed,
                total_chunks
            );
        }

        tracing::info!(
            "All {} batches uploaded successfully to S3",
            total_chunks
        );

        Ok(())
    }
}
