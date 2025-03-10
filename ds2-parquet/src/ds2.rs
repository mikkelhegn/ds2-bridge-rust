use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::{
    array::{Array, BooleanArray, Int32Array, Int64Array, StringArray, UInt32Array, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};

// Define a struct that matches the Datastream2 log structure
#[derive(Debug, Serialize, Deserialize)]
pub struct Datastream2Log {
    timeIso8601: String,
    timeEpoch: i64,
    trace_id: String,
    cpCode: String,
    akamaiResourceId: String,
    EdgeRequestId: String,
    edgeRequestId: String,
    ARL: String,
    method: String,
    scheme: String,
    host: String,
    path: String,
    queryString: String,
    clientIp: String,
    clientPort: u32,
    serverIp: String,
    serverPort: u32,
    status: u32,
    bytes: u64,
    contentLength: u64,
    timeToFirstByte: u32,
    requestTimeMs: u32,
    EdgeResponseTimeMs: u32,
    EdgeStartTimestamp: String,
    cachestatus: String,
    TieredDistributionState: String,
    sslProtocol: String,
    sslCipher: String,
    sslConnectionTimeMs: u32,
    sslHandshakeTimeMs: u32,
    cookie: String,
    setCookie: String,
    referrer: String,
    userAgent: String,
    requestProtocol: String,
    range: String,
    requestId: String,
    xForwardedFor: String,
    customField: String,
    kpField: String,
    tag: String,
    clientGeoCity: String,
    clientGeoCountry: String,
    clientGeoIsp: String,
    clientGeoAsn: String,
    serverGeoCity: String,
    serverGeoCountry: String,
    serverGeoIsp: String,
    serverGeoAsn: String,
    TotalUdpPackets: u32,
    TotalUdpBytes: u32,
    httpLastmileRttMs: u32,
    clientRttMs: u32,
    hitsOverSsl: bool,
    errorType: String,
    errorDetail: String,
    errorExtendedInfo: String,
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
    let time_iso8601 = StringArray::from(vec![log.timeIso8601.clone()]);
    let time_epoch = Int64Array::from(vec![log.timeEpoch]);
    let trace_id = StringArray::from(vec![log.trace_id.clone()]);
    let cp_code = StringArray::from(vec![log.cpCode.clone()]);
    let akamai_resource_id = StringArray::from(vec![log.akamaiResourceId.clone()]);
    let edge_request_id = StringArray::from(vec![log.EdgeRequestId.clone()]);
    let edge_request_id_lower = StringArray::from(vec![log.edgeRequestId.clone()]);
    let arl = StringArray::from(vec![log.ARL.clone()]);
    let method = StringArray::from(vec![log.method.clone()]);
    let scheme = StringArray::from(vec![log.scheme.clone()]);
    let host = StringArray::from(vec![log.host.clone()]);
    let path = StringArray::from(vec![log.path.clone()]);
    let query_string = StringArray::from(vec![log.queryString.clone()]);
    let client_ip = StringArray::from(vec![log.clientIp.clone()]);
    let client_port = UInt32Array::from(vec![log.clientPort]);
    let server_ip = StringArray::from(vec![log.serverIp.clone()]);
    let server_port = UInt32Array::from(vec![log.serverPort]);
    let status = UInt32Array::from(vec![log.status]);
    let bytes = UInt64Array::from(vec![log.bytes]);
    let content_length = UInt64Array::from(vec![log.contentLength]);
    let time_to_first_byte = UInt32Array::from(vec![log.timeToFirstByte]);
    let request_time_ms = UInt32Array::from(vec![log.requestTimeMs]);
    let edge_response_time_ms = UInt32Array::from(vec![log.EdgeResponseTimeMs]);
    let edge_start_timestamp = StringArray::from(vec![log.EdgeStartTimestamp.clone()]);
    let cachestatus = StringArray::from(vec![log.cachestatus.clone()]);
    let tiered_distribution_state = StringArray::from(vec![log.TieredDistributionState.clone()]);
    let ssl_protocol = StringArray::from(vec![log.sslProtocol.clone()]);
    let ssl_cipher = StringArray::from(vec![log.sslCipher.clone()]);
    let ssl_connection_time_ms = UInt32Array::from(vec![log.sslConnectionTimeMs]);
    let ssl_handshake_time_ms = UInt32Array::from(vec![log.sslHandshakeTimeMs]);
    let cookie = StringArray::from(vec![log.cookie.clone()]);
    let set_cookie = StringArray::from(vec![log.setCookie.clone()]);
    let referrer = StringArray::from(vec![log.referrer.clone()]);
    let user_agent = StringArray::from(vec![log.userAgent.clone()]);
    let request_protocol = StringArray::from(vec![log.requestProtocol.clone()]);
    let range = StringArray::from(vec![log.range.clone()]);
    let request_id = StringArray::from(vec![log.requestId.clone()]);
    let x_forwarded_for = StringArray::from(vec![log.xForwardedFor.clone()]);
    let custom_field = StringArray::from(vec![log.customField.clone()]);
    let kp_field = StringArray::from(vec![log.kpField.clone()]);
    let tag = StringArray::from(vec![log.tag.clone()]);
    let client_geo_city = StringArray::from(vec![log.clientGeoCity.clone()]);
    let client_geo_country = StringArray::from(vec![log.clientGeoCountry.clone()]);
    let client_geo_isp = StringArray::from(vec![log.clientGeoIsp.clone()]);
    let client_geo_asn = StringArray::from(vec![log.clientGeoAsn.clone()]);
    let server_geo_city = StringArray::from(vec![log.serverGeoCity.clone()]);
    let server_geo_country = StringArray::from(vec![log.serverGeoCountry.clone()]);
    let server_geo_isp = StringArray::from(vec![log.serverGeoIsp.clone()]);
    let server_geo_asn = StringArray::from(vec![log.serverGeoAsn.clone()]);
    let total_udp_packets = UInt32Array::from(vec![log.TotalUdpPackets]);
    let total_udp_bytes = UInt32Array::from(vec![log.TotalUdpBytes]);
    let http_lastmile_rtt_ms = UInt32Array::from(vec![log.httpLastmileRttMs]);
    let client_rtt_ms = UInt32Array::from(vec![log.clientRttMs]);
    let hits_over_ssl = BooleanArray::from(vec![log.hitsOverSsl]);
    let error_type = StringArray::from(vec![log.errorType.clone()]);
    let error_detail = StringArray::from(vec![log.errorDetail.clone()]);
    let error_extended_info = StringArray::from(vec![log.errorExtendedInfo.clone()]);

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
