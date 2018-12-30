# compression-benchmarks

## What's this?

Benchmarks for the Lokole [email data exchange protocol](https://github.com/ascoderu/opwen-cloudserver#data-exchange-format).

## Setup

Install the requirements with `pip install -r requirements.txt`.

Run the benchmarks via `python3 -u -m benchmarks <emails-zip-url>`.

## Results

| Compressor | Serializer | FileSize | WriteTime | ReadTime |
| ---------- | ---------- | -------- | --------- | -------- |
| (none) | jsonl | 171606.69 kb | 1.2122 s | 1.8828 s |
| (none) | msgpack | 171298.67 kb | 1.0946 s | 0.1485 s |
| (none) | avro | 171293.62 kb | 0.2137 s | 0.3518 s |
| gz | jsonl | 126402.96 kb | 13.2107 s | 3.6551 s |
| gz | msgpack | 126419.39 kb | 12.9293 s | 3.0305 s |
| gz | avro | 126416.06 kb | 13.5892 s | 3.1989 s |
| 3.zs | jsonl | 121565.40 kb | 2.1617 s | 5.8393 s |
| 3.zs | msgpack | 121848.00 kb | 1.1587 s | 2.4740 s |
| 3.zs | avro | ERROR | ERROR | ERROR |
| 22.zs | jsonl | 66531.39 kb | 45.8304 s | ERROR |
| 22.zs | msgpack | 66636.81 kb | 36.8645 s | 0.5559 s |
| 22.zs | avro | ERROR | ERROR | ERROR |
| tar.bz2 | jsonl | 124605.93 kb | 18.7358 s | 9.9951 s |
| tar.bz2 | msgpack | 124540.57 kb | 17.6111 s | 9.3050 s |
| tar.bz2 | avro | 124531.46 kb | 17.6761 s | 9.3536 s |
| tar.xz | jsonl | 112511.23 kb | 70.6419 s | 8.8058 s |
| tar.xz | msgpack | 112503.52 kb | 71.2753 s | 7.9134 s |
| tar.xz | avro | 112500.46 kb | 69.0527 s | 7.9861 s |
