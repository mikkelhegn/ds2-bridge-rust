use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::{
    array::{BooleanArray, Int64Array, StringArray, UInt32Array, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};

// Define a struct that matches the Datastream2 log structure
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Datastream2Log {
    time_iso8601: String,
    time_epoch: i64,
    trace_id: String,
    cp_code: String,
    akamai_resource_id: String,
    edge_request_id: String,
    edge_request_id_lower: String,
    arl: String,
    method: String,
    scheme: String,
    host: String,
    path: String,
    query_string: String,
    client_ip: String,
    client_port: u32,
    server_ip: String,
    server_port: u32,
    status: u32,
    bytes: u64,
    content_length: u64,
    time_to_first_byte: u32,
    request_time_ms: u32,
    edge_response_time_ms: u32,
    edge_start_timestamp: String,
    cachestatus: String,
    tiered_distribution_state: String,
    ssl_protocol: String,
    ssl_cipher: String,
    ssl_connection_time_ms: u32,
    ssl_handshake_time_ms: u32,
    cookie: String,
    set_cookie: String,
    referrer: String,
    user_agent: String,
    request_protocol: String,
    range: String,
    request_id: String,
    x_forwarded_for: String,
    custom_field: String,
    kp_field: String,
    tag: String,
    client_geo_city: String,
    client_geo_country: String,
    client_geo_isp: String,
    client_geo_asn: String,
    server_geo_city: String,
    server_geo_country: String,
    server_geo_isp: String,
    server_geo_asn: String,
    total_udp_packets: u32,
    total_udp_bytes: u32,
    http_lastmile_rtt_ms: u32,
    client_rtt_ms: u32,
    hits_over_ssl: bool,
    error_type: String,
    error_detail: String,
    error_extended_info: String,
}

// Function to create the Parquet schema
pub fn create_schema() -> SchemaRef {
    let schema = Schema::new(vec![
        Field::new("timeIso8601", DataType::Utf8, false),
        Field::new("timeEpoch", DataType::Int64, false),
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("cpCode", DataType::Utf8, false),
        Field::new("akamaiResourceId", DataType::Utf8, false),
        Field::new("EdgeRequestId", DataType::Utf8, false),
        Field::new("edgeRequestId", DataType::Utf8, false),
        Field::new("ARL", DataType::Utf8, false),
        Field::new("method", DataType::Utf8, false),
        Field::new("scheme", DataType::Utf8, false),
        Field::new("host", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("queryString", DataType::Utf8, false),
        Field::new("clientIp", DataType::Utf8, false),
        Field::new("clientPort", DataType::UInt32, false),
        Field::new("serverIp", DataType::Utf8, false),
        Field::new("serverPort", DataType::UInt32, false),
        Field::new("status", DataType::UInt32, false),
        Field::new("bytes", DataType::UInt64, false),
        Field::new("contentLength", DataType::UInt64, false),
        Field::new("timeToFirstByte", DataType::UInt32, false),
        Field::new("requestTimeMs", DataType::UInt32, false),
        Field::new("EdgeResponseTimeMs", DataType::UInt32, false),
        Field::new("EdgeStartTimestamp", DataType::Utf8, false),
        Field::new("cachestatus", DataType::Utf8, false),
        Field::new("TieredDistributionState", DataType::Utf8, false),
        Field::new("sslProtocol", DataType::Utf8, false),
        Field::new("sslCipher", DataType::Utf8, false),
        Field::new("sslConnectionTimeMs", DataType::UInt32, false),
        Field::new("sslHandshakeTimeMs", DataType::UInt32, false),
        Field::new("cookie", DataType::Utf8, false),
        Field::new("setCookie", DataType::Utf8, false),
        Field::new("referrer", DataType::Utf8, false),
        Field::new("userAgent", DataType::Utf8, false),
        Field::new("requestProtocol", DataType::Utf8, false),
        Field::new("range", DataType::Utf8, false),
        Field::new("requestId", DataType::Utf8, false),
        Field::new("xForwardedFor", DataType::Utf8, false),
        Field::new("customField", DataType::Utf8, false),
        Field::new("kpField", DataType::Utf8, false),
        Field::new("tag", DataType::Utf8, false),
        Field::new("clientGeoCity", DataType::Utf8, false),
        Field::new("clientGeoCountry", DataType::Utf8, false),
        Field::new("clientGeoIsp", DataType::Utf8, false),
        Field::new("clientGeoAsn", DataType::Utf8, false),
        Field::new("serverGeoCity", DataType::Utf8, false),
        Field::new("serverGeoCountry", DataType::Utf8, false),
        Field::new("serverGeoIsp", DataType::Utf8, false),
        Field::new("serverGeoAsn", DataType::Utf8, false),
        Field::new("TotalUdpPackets", DataType::UInt32, false),
        Field::new("TotalUdpBytes", DataType::UInt32, false),
        Field::new("httpLastmileRttMs", DataType::UInt32, false),
        Field::new("clientRttMs", DataType::UInt32, false),
        Field::new("hitsOverSsl", DataType::Boolean, false),
        Field::new("errorType", DataType::Utf8, false),
        Field::new("errorDetail", DataType::Utf8, false),
        Field::new("errorExtendedInfo", DataType::Utf8, false),
    ]);
    Arc::new(schema)
}

// Function to convert a Datastream2Log to a RecordBatch
pub fn log_to_record_batch(log: &Datastream2Log, schema: SchemaRef) -> Result<RecordBatch> {
    // Create arrays for each field
    let time_iso8601 = StringArray::from(vec![log.time_iso8601.clone()]);
    let time_epoch = Int64Array::from(vec![log.time_epoch]);
    let trace_id = StringArray::from(vec![log.trace_id.clone()]);
    let cp_code = StringArray::from(vec![log.cp_code.clone()]);
    let akamai_resource_id = StringArray::from(vec![log.akamai_resource_id.clone()]);
    let edge_request_id = StringArray::from(vec![log.edge_request_id.clone()]);
    let edge_request_id_lower = StringArray::from(vec![log.edge_request_id_lower.clone()]);
    let arl = StringArray::from(vec![log.arl.clone()]);
    let method = StringArray::from(vec![log.method.clone()]);
    let scheme = StringArray::from(vec![log.scheme.clone()]);
    let host = StringArray::from(vec![log.host.clone()]);
    let path = StringArray::from(vec![log.path.clone()]);
    let query_string = StringArray::from(vec![log.query_string.clone()]);
    let client_ip = StringArray::from(vec![log.client_ip.clone()]);
    let client_port = UInt32Array::from(vec![log.client_port]);
    let server_ip = StringArray::from(vec![log.server_ip.clone()]);
    let server_port = UInt32Array::from(vec![log.server_port]);
    let status = UInt32Array::from(vec![log.status]);
    let bytes = UInt64Array::from(vec![log.bytes]);
    let content_length = UInt64Array::from(vec![log.content_length]);
    let time_to_first_byte = UInt32Array::from(vec![log.time_to_first_byte]);
    let request_time_ms = UInt32Array::from(vec![log.request_time_ms]);
    let edge_response_time_ms = UInt32Array::from(vec![log.edge_response_time_ms]);
    let edge_start_timestamp = StringArray::from(vec![log.edge_start_timestamp.clone()]);
    let cachestatus = StringArray::from(vec![log.cachestatus.clone()]);
    let tiered_distribution_state = StringArray::from(vec![log.tiered_distribution_state.clone()]);
    let ssl_protocol = StringArray::from(vec![log.ssl_protocol.clone()]);
    let ssl_cipher = StringArray::from(vec![log.ssl_cipher.clone()]);
    let ssl_connection_time_ms = UInt32Array::from(vec![log.ssl_connection_time_ms]);
    let ssl_handshake_time_ms = UInt32Array::from(vec![log.ssl_handshake_time_ms]);
    let cookie = StringArray::from(vec![log.cookie.clone()]);
    let set_cookie = StringArray::from(vec![log.set_cookie.clone()]);
    let referrer = StringArray::from(vec![log.referrer.clone()]);
    let user_agent = StringArray::from(vec![log.user_agent.clone()]);
    let request_protocol = StringArray::from(vec![log.request_protocol.clone()]);
    let range = StringArray::from(vec![log.range.clone()]);
    let request_id = StringArray::from(vec![log.request_id.clone()]);
    let x_forwarded_for = StringArray::from(vec![log.x_forwarded_for.clone()]);
    let custom_field = StringArray::from(vec![log.custom_field.clone()]);
    let kp_field = StringArray::from(vec![log.kp_field.clone()]);
    let tag = StringArray::from(vec![log.tag.clone()]);
    let client_geo_city = StringArray::from(vec![log.client_geo_city.clone()]);
    let client_geo_country = StringArray::from(vec![log.client_geo_country.clone()]);
    let client_geo_isp = StringArray::from(vec![log.client_geo_isp.clone()]);
    let client_geo_asn = StringArray::from(vec![log.client_geo_asn.clone()]);
    let server_geo_city = StringArray::from(vec![log.server_geo_city.clone()]);
    let server_geo_country = StringArray::from(vec![log.server_geo_country.clone()]);
    let server_geo_isp = StringArray::from(vec![log.server_geo_isp.clone()]);
    let server_geo_asn = StringArray::from(vec![log.server_geo_asn.clone()]);
    let total_udp_packets = UInt32Array::from(vec![log.total_udp_packets]);
    let total_udp_bytes = UInt32Array::from(vec![log.total_udp_bytes]);
    let http_lastmile_rtt_ms = UInt32Array::from(vec![log.http_lastmile_rtt_ms]);
    let client_rtt_ms = UInt32Array::from(vec![log.client_rtt_ms]);
    let hits_over_ssl = BooleanArray::from(vec![log.hits_over_ssl]);
    let error_type = StringArray::from(vec![log.error_type.clone()]);
    let error_detail = StringArray::from(vec![log.error_detail.clone()]);
    let error_extended_info = StringArray::from(vec![log.error_extended_info.clone()]);

    // Create the record batch
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(time_iso8601),
            Arc::new(time_epoch),
            Arc::new(trace_id),
            Arc::new(cp_code),
            Arc::new(akamai_resource_id),
            Arc::new(edge_request_id),
            Arc::new(edge_request_id_lower),
            Arc::new(arl),
            Arc::new(method),
            Arc::new(scheme),
            Arc::new(host),
            Arc::new(path),
            Arc::new(query_string),
            Arc::new(client_ip),
            Arc::new(client_port),
            Arc::new(server_ip),
            Arc::new(server_port),
            Arc::new(status),
            Arc::new(bytes),
            Arc::new(content_length),
            Arc::new(time_to_first_byte),
            Arc::new(request_time_ms),
            Arc::new(edge_response_time_ms),
            Arc::new(edge_start_timestamp),
            Arc::new(cachestatus),
            Arc::new(tiered_distribution_state),
            Arc::new(ssl_protocol),
            Arc::new(ssl_cipher),
            Arc::new(ssl_connection_time_ms),
            Arc::new(ssl_handshake_time_ms),
            Arc::new(cookie),
            Arc::new(set_cookie),
            Arc::new(referrer),
            Arc::new(user_agent),
            Arc::new(request_protocol),
            Arc::new(range),
            Arc::new(request_id),
            Arc::new(x_forwarded_for),
            Arc::new(custom_field),
            Arc::new(kp_field),
            Arc::new(tag),
            Arc::new(client_geo_city),
            Arc::new(client_geo_country),
            Arc::new(client_geo_isp),
            Arc::new(client_geo_asn),
            Arc::new(server_geo_city),
            Arc::new(server_geo_country),
            Arc::new(server_geo_isp),
            Arc::new(server_geo_asn),
            Arc::new(total_udp_packets),
            Arc::new(total_udp_bytes),
            Arc::new(http_lastmile_rtt_ms),
            Arc::new(client_rtt_ms),
            Arc::new(hits_over_ssl),
            Arc::new(error_type),
            Arc::new(error_detail),
            Arc::new(error_extended_info),
        ],
    )
    .map_err(|e| anyhow!("Failed to create record batch: {}", e))
}
