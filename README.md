# compression-benchmarks

## What's this?

Benchmarks for the Lokole [email data exchange protocol](https://github.com/ascoderu/opwen-cloudserver#data-exchange-format).

## Setup

Install the requirements with `pip install -r requirements.txt`.

Run the tests with `python3 -m benchmarks.tests` and run the linter with `flake8 benchmarks`.

Run the benchmarks via `python3 -u -m benchmarks <emails-zip-url>`.

## Results

| Compressor | Serializer | FileSize | WriteTime | ReadTime |
| ---------- | ---------- | -------- | --------- | -------- |
| (none) | jsonl | 171606.69 kb | 0.5717 s | 0.8538 s |
| (none) | msgpack | 171298.39 kb | 0.5445 s | 0.1920 s |
| (none) | avro | 171293.62 kb | 0.1190 s | 0.1599 s |
| gz | jsonl | 126403.95 kb | 6.3336 s | 1.6911 s |
| gz | msgpack | 126420.68 kb | 5.9751 s | 1.3712 s |
| gz | avro | 126415.92 kb | 5.8574 s | 1.3508 s |
| 3.zs | jsonl | 121602.84 kb | 0.9226 s | 2.7161 s |
| 3.zs | msgpack | 121861.77 kb | 0.5294 s | 1.1620 s |
| 3.zs | avro | ERROR | ERROR | ERROR |
| 22.zs | jsonl | 66823.35 kb | 39.1934 s | 2.7347 s |
| 22.zs | msgpack | 66708.15 kb | 39.6601 s | 1.3876 s |
| 22.zs | avro | ERROR | ERROR | ERROR |
| tar.bz2 | jsonl | 124604.20 kb | 18.9102 s | 10.1713 s |
| tar.bz2 | msgpack | 124544.65 kb | 18.2266 s | 10.0736 s |
| tar.bz2 | avro | 124535.67 kb | 18.5495 s | 10.2544 s |
| tar.xz | jsonl | 112512.32 kb | 72.1213 s | 9.0261 s |
| tar.xz | msgpack | 112502.00 kb | 73.3717 s | 8.0620 s |
| tar.xz | avro | 112501.84 kb | 68.4476 s | 8.0983 s |
