use anyhow::{anyhow, Result};
use std::sync::Arc;
use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};

// Define a struct that matches the Akamai log structure
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Datastream2Log {
    version: Option<String>,
    is_midgress: Option<String>,
    stream_id: Option<String>,
    cp: Option<String>,
    req_id: Option<String>,
    req_time_sec: Option<String>,
    edge_attempts: Option<String>,
    bytes: Option<String>,
    cli_ip: Option<String>,
    status_code: Option<String>,
    proto: Option<String>,
    req_host: Option<String>,
    req_method: Option<String>,
    req_path: Option<String>,
    req_port: Option<String>,
    rsp_content_len: Option<String>,
    rsp_content_type: Option<String>,
    ua: Option<String>,
    tls_overhead_time_m_sec: Option<String>,
    tls_version: Option<String>,
    obj_size: Option<String>,
    uncompressed_size: Option<String>,
    overhead_bytes: Option<String>,
    total_bytes: Option<String>,
    query_str: Option<String>,
    file_size_bucket: Option<String>,
    brotli_status: Option<String>,
    origin_content_len: Option<String>,
    early_hints: Option<String>,
    edge_ip_binding: Option<String>,
    acc_lang: Option<String>,
    cookie: Option<String>,
    range: Option<String>,
    referer: Option<String>,
    x_forwarded_for: Option<String>,
    max_age_sec: Option<String>,
    req_end_time_m_sec: Option<String>,
    error_code: Option<String>,
    turn_around_time_m_sec: Option<String>,
    transfer_time_m_sec: Option<String>,
    dns_lookup_time_m_sec: Option<String>,
    last_byte: Option<String>,
    asn: Option<String>,
    time_to_first_byte: Option<String>,
    startup_error: Option<String>,
    download_time: Option<String>,
    throughput: Option<String>,
    prefetch_midgress_hits: Option<String>,
    cache_status: Option<String>,
    cacheable: Option<String>,
    breadcrumbs: Option<String>,
    edge_ip: Option<String>,
    country: Option<String>,
    state: Option<String>,
    city: Option<String>,
    server_country: Option<String>,
    billing_region: Option<String>,
    ew_usage_info: Option<String>,
    ew_execution_info: Option<String>,
    content_protection_info: Option<String>,
    custom_field: Option<String>,
    security_rules: Option<String>,
}

// Function to create the Parquet schema
pub fn create_schema() -> SchemaRef {
    let schema = Schema::new(vec![
        Field::new("version", DataType::Int64, true),
        Field::new("isMidgress", DataType::Utf8, true),
        Field::new("streamId", DataType::Utf8, true),
        Field::new("cp", DataType::Utf8, true),
        Field::new("reqId", DataType::Utf8, true),
        Field::new("reqTimeSec", DataType::Utf8, true),
        Field::new("edgeAttempts", DataType::Utf8, true),
        Field::new("bytes", DataType::Utf8, true),
        Field::new("cliIP", DataType::Utf8, true),
        Field::new("statusCode", DataType::Utf8, true),
        Field::new("proto", DataType::Utf8, true),
        Field::new("reqHost", DataType::Utf8, true),
        Field::new("reqMethod", DataType::Utf8, true),
        Field::new("reqPath", DataType::Utf8, true),
        Field::new("reqPort", DataType::Utf8, true),
        Field::new("rspContentLen", DataType::Utf8, true),
        Field::new("rspContentType", DataType::Utf8, true),
        Field::new("UA", DataType::Utf8, true),
        Field::new("tlsOverheadTimeMSec", DataType::Utf8, true),
        Field::new("tlsVersion", DataType::Utf8, true),
        Field::new("objSize", DataType::Utf8, true),
        Field::new("uncompressedSize", DataType::Utf8, true),
        Field::new("overheadBytes", DataType::Utf8, true),
        Field::new("totalBytes", DataType::Utf8, true),
        Field::new("queryStr", DataType::Utf8, true),
        Field::new("fileSizeBucket", DataType::Utf8, true),
        Field::new("brotliStatus", DataType::Utf8, true),
        Field::new("originContentLen", DataType::Utf8, true),
        Field::new("earlyHints", DataType::Utf8, true),
        Field::new("edgeIPBinding", DataType::Utf8, true),
        Field::new("accLang", DataType::Utf8, true),
        Field::new("cookie", DataType::Utf8, true),
        Field::new("range", DataType::Utf8, true),
        Field::new("referer", DataType::Utf8, true),
        Field::new("xForwardedFor", DataType::Utf8, true),
        Field::new("maxAgeSec", DataType::Utf8, true),
        Field::new("reqEndTimeMSec", DataType::Utf8, true),
        Field::new("errorCode", DataType::Utf8, true),
        Field::new("turnAroundTimeMSec", DataType::Utf8, true),
        Field::new("transferTimeMSec", DataType::Utf8, true),
        Field::new("dnsLookupTimeMSec", DataType::Utf8, true),
        Field::new("lastByte", DataType::Utf8, true),
        Field::new("asn", DataType::Utf8, true),
        Field::new("timeToFirstByte", DataType::Utf8, true),
        Field::new("startupError", DataType::Utf8, true),
        Field::new("downloadTime", DataType::Utf8, true),
        Field::new("throughput", DataType::Utf8, true),
        Field::new("prefetchMidgressHits", DataType::Utf8, true),
        Field::new("cacheStatus", DataType::Utf8, true),
        Field::new("cacheable", DataType::Utf8, true),
        Field::new("breadcrumbs", DataType::Utf8, true),
        Field::new("edgeIP", DataType::Utf8, true),
        Field::new("country", DataType::Utf8, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("city", DataType::Utf8, true),
        Field::new("serverCountry", DataType::Utf8, true),
        Field::new("billingRegion", DataType::Utf8, true),
        Field::new("ewUsageInfo", DataType::Utf8, true),
        Field::new("ewExecutionInfo", DataType::Utf8, true),
        Field::new("contentProtectionInfo", DataType::Utf8, true),
        Field::new("customField", DataType::Utf8, true),
        Field::new("securityRules", DataType::Utf8, true),
    ]);
    Arc::new(schema)
}

// Function to convert a batch of logs into a single columnar RecordBatch
pub fn logs_to_record_batch(logs: &[Datastream2Log], schema: SchemaRef) -> Result<RecordBatch> {
    // Create arrays for each field, with one value per log
    let version = Int64Array::from(
        logs.iter()
            .map(|log| log.version.as_ref().and_then(|v| v.parse::<i64>().ok()))
            .collect::<Vec<_>>(),
    );
    let is_midgress =
        StringArray::from(logs.iter().map(|log| log.is_midgress.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let stream_id =
        StringArray::from(logs.iter().map(|log| log.stream_id.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let cp = StringArray::from(logs.iter().map(|log| log.cp.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let req_id =
        StringArray::from(logs.iter().map(|log| log.req_id.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let req_time_sec =
        StringArray::from(logs.iter().map(|log| log.req_time_sec.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let edge_attempts = StringArray::from(
        logs.iter().map(|log| log.edge_attempts.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let bytes =
        StringArray::from(logs.iter().map(|log| log.bytes.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let cli_ip =
        StringArray::from(logs.iter().map(|log| log.cli_ip.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let status_code = StringArray::from(
        logs.iter().map(|log| log.status_code.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let proto =
        StringArray::from(logs.iter().map(|log| log.proto.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let req_host =
        StringArray::from(logs.iter().map(|log| log.req_host.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let req_method = StringArray::from(
        logs.iter().map(|log| log.req_method.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let req_path =
        StringArray::from(logs.iter().map(|log| log.req_path.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let req_port =
        StringArray::from(logs.iter().map(|log| log.req_port.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let rsp_content_len = StringArray::from(
        logs.iter().map(|log| log.rsp_content_len.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let rsp_content_type = StringArray::from(
        logs.iter().map(|log| log.rsp_content_type.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let ua = StringArray::from(logs.iter().map(|log| log.ua.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let tls_overhead_time_m_sec = StringArray::from(
        logs.iter()
            .map(|log| log.tls_overhead_time_m_sec.as_deref().unwrap_or(""))
            .collect::<Vec<_>>(),
    );
    let tls_version = StringArray::from(
        logs.iter().map(|log| log.tls_version.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let obj_size =
        StringArray::from(logs.iter().map(|log| log.obj_size.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let uncompressed_size = StringArray::from(
        logs.iter().map(|log| log.uncompressed_size.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let overhead_bytes = StringArray::from(
        logs.iter().map(|log| log.overhead_bytes.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let total_bytes = StringArray::from(
        logs.iter().map(|log| log.total_bytes.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let query_str = StringArray::from(
        logs.iter().map(|log| log.query_str.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let file_size_bucket = StringArray::from(
        logs.iter().map(|log| log.file_size_bucket.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let brotli_status = StringArray::from(
        logs.iter().map(|log| log.brotli_status.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let origin_content_len = StringArray::from(
        logs.iter().map(|log| log.origin_content_len.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let early_hints = StringArray::from(
        logs.iter().map(|log| log.early_hints.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let edge_ip_binding = StringArray::from(
        logs.iter().map(|log| log.edge_ip_binding.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let acc_lang = StringArray::from(
        logs.iter().map(|log| log.acc_lang.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let cookie =
        StringArray::from(logs.iter().map(|log| log.cookie.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let range =
        StringArray::from(logs.iter().map(|log| log.range.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let referer =
        StringArray::from(logs.iter().map(|log| log.referer.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let x_forwarded_for = StringArray::from(
        logs.iter().map(|log| log.x_forwarded_for.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let max_age_sec = StringArray::from(
        logs.iter().map(|log| log.max_age_sec.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let req_end_time_m_sec = StringArray::from(
        logs.iter().map(|log| log.req_end_time_m_sec.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let error_code = StringArray::from(
        logs.iter().map(|log| log.error_code.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let turn_around_time_m_sec = StringArray::from(
        logs.iter()
            .map(|log| log.turn_around_time_m_sec.as_deref().unwrap_or(""))
            .collect::<Vec<_>>(),
    );
    let transfer_time_m_sec = StringArray::from(
        logs.iter().map(|log| log.transfer_time_m_sec.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let dns_lookup_time_m_sec = StringArray::from(
        logs.iter()
            .map(|log| log.dns_lookup_time_m_sec.as_deref().unwrap_or(""))
            .collect::<Vec<_>>(),
    );
    let last_byte = StringArray::from(
        logs.iter().map(|log| log.last_byte.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let asn = StringArray::from(logs.iter().map(|log| log.asn.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let time_to_first_byte = StringArray::from(
        logs.iter().map(|log| log.time_to_first_byte.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let startup_error = StringArray::from(
        logs.iter().map(|log| log.startup_error.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let download_time = StringArray::from(
        logs.iter().map(|log| log.download_time.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let throughput = StringArray::from(
        logs.iter().map(|log| log.throughput.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let prefetch_midgress_hits = StringArray::from(
        logs.iter()
            .map(|log| log.prefetch_midgress_hits.as_deref().unwrap_or(""))
            .collect::<Vec<_>>(),
    );
    let cache_status = StringArray::from(
        logs.iter().map(|log| log.cache_status.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let cacheable = StringArray::from(
        logs.iter().map(|log| log.cacheable.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let breadcrumbs = StringArray::from(
        logs.iter().map(|log| log.breadcrumbs.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let edge_ip =
        StringArray::from(logs.iter().map(|log| log.edge_ip.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let country =
        StringArray::from(logs.iter().map(|log| log.country.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let state =
        StringArray::from(logs.iter().map(|log| log.state.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let city =
        StringArray::from(logs.iter().map(|log| log.city.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let server_country = StringArray::from(
        logs.iter().map(|log| log.server_country.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let billing_region = StringArray::from(
        logs.iter().map(|log| log.billing_region.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let ew_usage_info = StringArray::from(
        logs.iter().map(|log| log.ew_usage_info.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let ew_execution_info = StringArray::from(
        logs.iter().map(|log| log.ew_execution_info.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let content_protection = StringArray::from(
        logs.iter()
            .map(|log| log.content_protection_info.as_deref().unwrap_or(""))
            .collect::<Vec<_>>(),
    );
    let custom_field = StringArray::from(
        logs.iter().map(|log| log.custom_field.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let security_rules = StringArray::from(
        logs.iter().map(|log| log.security_rules.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );

    // Create the record batch
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(version),
            Arc::new(is_midgress),
            Arc::new(stream_id),
            Arc::new(cp),
            Arc::new(req_id),
            Arc::new(req_time_sec),
            Arc::new(edge_attempts),
            Arc::new(bytes),
            Arc::new(cli_ip),
            Arc::new(status_code),
            Arc::new(proto),
            Arc::new(req_host),
            Arc::new(req_method),
            Arc::new(req_path),
            Arc::new(req_port),
            Arc::new(rsp_content_len),
            Arc::new(rsp_content_type),
            Arc::new(ua),
            Arc::new(tls_overhead_time_m_sec),
            Arc::new(tls_version),
            Arc::new(obj_size),
            Arc::new(uncompressed_size),
            Arc::new(overhead_bytes),
            Arc::new(total_bytes),
            Arc::new(query_str),
            Arc::new(file_size_bucket),
            Arc::new(brotli_status),
            Arc::new(origin_content_len),
            Arc::new(early_hints),
            Arc::new(edge_ip_binding),
            Arc::new(acc_lang),
            Arc::new(cookie),
            Arc::new(range),
            Arc::new(referer),
            Arc::new(x_forwarded_for),
            Arc::new(max_age_sec),
            Arc::new(req_end_time_m_sec),
            Arc::new(error_code),
            Arc::new(turn_around_time_m_sec),
            Arc::new(transfer_time_m_sec),
            Arc::new(dns_lookup_time_m_sec),
            Arc::new(last_byte),
            Arc::new(asn),
            Arc::new(time_to_first_byte),
            Arc::new(startup_error),
            Arc::new(download_time),
            Arc::new(throughput),
            Arc::new(prefetch_midgress_hits),
            Arc::new(cache_status),
            Arc::new(cacheable),
            Arc::new(breadcrumbs),
            Arc::new(edge_ip),
            Arc::new(country),
            Arc::new(state),
            Arc::new(city),
            Arc::new(server_country),
            Arc::new(billing_region),
            Arc::new(ew_usage_info),
            Arc::new(ew_execution_info),
            Arc::new(content_protection),
            Arc::new(custom_field),
            Arc::new(security_rules),
        ],
    )
    .map_err(|e| anyhow!("Failed to create record batch: {}", e))
}

pub fn extract_logs(body_bytes: &[u8]) -> Result<Vec<Datastream2Log>> {
    // The request body from DataStream2 is a new-line-separated list of JSON objects of type Datastream2Log.
    let body = std::str::from_utf8(body_bytes)
        .map_err(|e| anyhow!("Failed to parse request body as UTF-8: {}", e))?;

    let mut logs = Vec::new();

    // Extract all JSON objects from the input
    let json_objects = extract_json_objects(body);

    for (_, json_str) in json_objects.iter().enumerate() {
        // Try to parse as our specific type
        match serde_json::from_str::<Datastream2Log>(json_str) {
            Ok(log) => {
                logs.push(log);
            }
            Err(_e) => {
                // Try with a more lenient approach - parse as Value first
                match serde_json::from_str::<serde_json::Value>(json_str) {
                    Ok(value) => {
                        println!("JSON is valid but doesn't match Datastream2Log structure");
                        // Print the first few keys to help debug
                        if let serde_json::Value::Object(map) = &value {
                            println!("Keys in JSON: {:?}", map.keys().take(5).collect::<Vec<_>>());
                        }
                    }
                    Err(_) => {
                        println!("Not valid JSON");
                    }
                }
            }
        }
    }

    Ok(logs)
}

// Extract all JSON objects from a string.
// This is a pretty naive implementation that tries to handle weird cases of DataStream2 sending
// incomplete JSON objects.
fn extract_json_objects(s: &str) -> Vec<String> {
    let mut objects = Vec::new();
    let mut start_index = None;
    let mut depth = 0;
    let mut in_string = false;
    let mut escape_next = false;

    for (i, c) in s.char_indices() {
        if escape_next {
            escape_next = false;
            continue;
        }

        match c {
            '\\' if in_string => escape_next = true,
            '"' if !escape_next => in_string = !in_string,
            '{' if !in_string => {
                depth += 1;
                if depth == 1 {
                    start_index = Some(i);
                }
            }
            '}' if !in_string => {
                depth -= 1;
                if depth == 0 && start_index.is_some() {
                    let start = start_index.unwrap();
                    objects.push(s[start..=i].to_string());
                    start_index = None;
                }
            }
            _ => {}
        }
    }

    objects
}
