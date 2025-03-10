import json
import random
import string
import time
import uuid
import sys

from datetime import datetime


def random_ip():
    return ".".join(str(random.randint(0, 255)) for _ in range(4))


def random_port():
    return random.randint(1024, 65535)


def random_str(length=10):
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


def random_trace_id():
    """
    Generate a trace ID in the format of a 32-character hexadecimal string.
    Example: 944c74e67612a1a5b0de5c5364a2828a
    """
    return uuid.uuid4().hex  # Generates a 32-character hex string


def generate_datastream_log():
    """
    Returns a dictionary including all known DataStream2 fields,
    filled with random or placeholder values.
    """
    now_ts = time.time()
    # Generate an approximate ISO8601 string
    iso_time = datetime.utcfromtimestamp(now_ts).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        # Common timestamps
        "timeIso8601": iso_time,
        "timeEpoch": int(now_ts),
        # IDs and references
        "traceId": random_trace_id(),
        "cpCode": str(random.randint(100000, 999999)),
        "akamaiResourceId": random_str(8),
        "edgeRequestId": random_str(12),  # Some logs call it differently
        "edgeRequestIdLower": random_str(12),  # Some logs call it differently
        # Request data
        "arl": f"{random.choice(['GET', 'POST', 'PUT', 'DELETE'])}://{random_str(6)}.example.com/{random_str(4)}",
        "method": random.choice(["GET", "POST", "PUT", "DELETE"]),
        "scheme": random.choice(["http", "https"]),
        "host": f"{random_str(6)}.example.com",
        "path": f"/{random_str(6)}/{random_str(4)}",
        "queryString": f"foo={random_str(4)}&bar={random.randint(1, 999)}",
        "clientIp": random_ip(),
        "clientPort": random_port(),
        "serverIp": random_ip(),
        "serverPort": random_port(),
        # Status & size
        "status": random.choice([200, 301, 302, 404, 500]),
        "bytes": random.randint(100, 100000),
        "contentLength": random.randint(100, 100000),
        # Timing
        "timeToFirstByte": random.randint(1, 500),
        "requestTimeMs": random.randint(1, 2000),
        "edgeResponseTimeMs": random.randint(1, 2000),
        "edgeStartTimestamp": iso_time,
        # Caching
        "cachestatus": random.choice(
            [
                "TCP_HIT",
                "TCP_MISS",
                "TCP_REFRESH_HIT",
                "TCP_REFRESH_MISS",
                "TCP_IMS_MISS",
                "TCP_IMS_HIT",
                "TCP_SWAPFAIL_MISS",
            ]
        ),
        "tieredDistributionState": random.choice(["PARENT_HIT", "PARENT_MISS", "NONE"]),
        # SSL / TLS
        "sslProtocol": random.choice(["TLSv1.2", "TLSv1.3"]),
        "sslCipher": random.choice(
            ["ECDHE-ECDSA-AES128-GCM-SHA256", "ECDHE-RSA-AES256-GCM-SHA384"]
        ),
        "sslConnectionTimeMs": random.randint(1, 50),
        "sslHandshakeTimeMs": random.randint(1, 50),
        # HTTP-specific
        "cookie": f"session_id={random_str(10)}",
        "setCookie": f"session_id={random_str(10)}; Path=/; HttpOnly",
        "referrer": f"https://{random_str(6)}.example.com/",
        "userAgent": "Mozilla/5.0 (Macintosh) AppleWebKit/537.36",
        "requestProtocol": random.choice(["HTTP/1.1", "HTTP/2"]),
        "range": "",
        "requestId": random_str(12),
        "xForwardedFor": random_ip(),
        # Additional fields
        "customField": random_str(10),
        "kpField": random_str(10),
        "tag": random_str(8),
        # Client geography
        "clientGeoCity": random.choice(
            ["New York", "Paris", "London", "Tokyo", "Sydney"]
        ),
        "clientGeoCountry": random.choice(["US", "FR", "UK", "JP", "AU"]),
        "clientGeoIsp": random_str(6),
        "clientGeoAsn": str(random.randint(10000, 99999)),
        # Server geography
        "serverGeoCity": random.choice(["Miami", "Berlin", "Dubai", "Toronto", "Rome"]),
        "serverGeoCountry": random.choice(["US", "DE", "AE", "CA", "IT"]),
        "serverGeoIsp": random_str(6),
        "serverGeoAsn": str(random.randint(10000, 99999)),
        # UDP (rare for HTTP, but in spec)
        "totalUdpPackets": random.randint(0, 100),
        "totalUdpBytes": random.randint(0, 10000),
        # Latency/RTT
        "httpLastmileRttMs": random.randint(0, 500),
        "clientRttMs": random.randint(0, 500),
        "hitsOverSsl": random.choice([True, False]),
        # Error fields (often empty if no error)
        "errorType": random.choice(["", "OriginError", "NetworkError"]),
        "errorDetail": random.choice(["", "ConnectionTimeout", "DNSFailure"]),
        "errorExtendedInfo": random_str(20),
    }


def main():
    # Default to 4MB if no argument is provided
    target_size_mb = 4
    if len(sys.argv) > 1:
        try:
            target_size_mb = float(sys.argv[1])
        except ValueError:
            print(f"Error: Invalid size '{sys.argv[1]}'. Using default 4MB.")

    # Default filename or use second argument if provided
    filename = f"datastream2_{target_size_mb}mb.json"
    if len(sys.argv) > 2:
        filename = sys.argv[2]

    approximate_target_size = int(target_size_mb * 1024 * 1024)
    logs = []
    current_size = 0

    while True:
        logs.append(generate_datastream_log())
        json_data = json.dumps(logs, ensure_ascii=False)
        current_size = len(json_data.encode("utf-8"))

        if current_size >= approximate_target_size:
            break

    with open(filename, "w", encoding="utf-8") as f:
        f.write(json_data)

    print(
        f"Generated {len(logs)} log entries, total {current_size} bytes (~{current_size / 1024 / 1024:.2f} MB)."
    )
    print(f"File saved as '{filename}'.")


def generate_single():
    filename = f"datastream2_single.json"
    data = generate_datastream_log()
    with open(filename, "w", encoding="utf-8") as f:
        f.write(json.dumps(data))

    print(f"File saved as '{filename}'.")


if __name__ == "__main__":
    # main()
    generate_single()
