# DataStream2 Bridge

This repository contains a collection of serverless functions to process DataStream2 logs.

## DataStream2 to Parquet

The `ds2-parquet` component is a function that converts DataStream2 logs to Parquet format.

To use it, you can make a HTTP POST request to the `/api/v1/transform/single` endpoint with a single DataStream2 log in the request body, or to the `/api/v1/transform/multiple` endpoint with an array of DataStream2 logs:

First, either obtain or generate logs in the DataStream2 format.

```bash
$ python3 datastream2_generator.py
# this can be used to generate a single log, or a file with multiple logs
```

Then, build and run the function:

```bash
$ spin build --up
$ curl -X POST http://localhost:3000/api/v1/transform/multiple -d @datastream2_4mb.json --output multiple.parquet
```

The response will be a Parquet file with the DataStream2 logs:

```bash
parquet meta multiple.parquet

File path:  multiple.parquet
Created by: parquet-rs version 54.2.1
message arrow_schema {
  required binary timeIso8601 (STRING);
  required int64 timeEpoch;
  required binary serverGeoCity (STRING);
  ...
  required boolean hitsOverSsl;
  required binary errorType (STRING);
  required binary errorDetail (STRING);
  required binary errorExtendedInfo (STRING);
}

Row group 0:  count: 2524  375.53 B records  start: 4  total(compressed): 925.634 kB total(uncompressed):1.308 MB
--------------------------------------------------------------------------------
                         type      encodings count     avg size   nulls   min / max
timeIso8601              BINARY    S BB_     2524      0.11 B     0       "2025-03-10T12:52:21Z" / "2025-03-10T12:52:40Z"
serverGeoAsn             BINARY    S BB_     2524      7.18 B     0       "10091" / "99929"
TotalUdpPackets          INT32     S BB_     2524      1.07 B     0       "0" / "100"
TotalUdpBytes            INT32     S BB_     2524      5.06 B     0       "2" / "9998"
httpLastmileRttMs        INT32     S BB_     2524      1.95 B     0       "0" / "500"
clientRttMs              INT32     S BB_     2524      1.94 B     0       "0" / "500"
hitsOverSsl              BOOLEAN   S BB      2524      0.15 B     0       "false" / "true"
errorType                BINARY    S BB_     2524      0.29 B     0       "" / "OriginError"
errorDetail              BINARY    S BB_     2524      0.30 B     0       "" / "DNSFailure"
errorExtendedInfo        BINARY    S BB_     2524      23.80 B    0       "02ln4UKgcrlej6mBmbL6" / "zxEz8emOboZHZDz4xO6q"
```
