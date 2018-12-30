# compression-benchmarks

## What's this?

Benchmarks for the Lokole [email data exchange protocol](https://github.com/ascoderu/opwen-cloudserver#data-exchange-format).

## Setup

Install the requirements with `pip install -r requirements.txt`.

Run the benchmarks via `python3 -m benchmarks <emails-zip-url>`.

## Results

| Compression | Serialization | Filesize | Runtime |
| ----------- | ------------- | -------- | ------- |
| (none) | jsonl | 171606.69 kb | 0.5273 s |
| (none) | msgpack | 171298.67 kb | 0.5399 s |
| (none) | avro | 171293.62 kb | 0.1048 s |
| gz | jsonl | 126402.64 kb | 6.0698 s |
| gz | msgpack | 126419.29 kb | 5.8244 s |
| gz | avro | 126414.96 kb | 5.6908 s |
| 3.zs | jsonl | 121544.14 kb | 0.9095 s |
| 3.zs | msgpack | 121875.38 kb | 0.5148 s |
| 3.zs | avro | ERROR | ERROR |
| 22.zs | jsonl | 66752.08 kb | 37.6623 s |
| 22.zs | msgpack | 66617.84 kb | 36.8547 s |
| 22.zs | avro | ERROR | ERROR |
| tar.bz2 | jsonl | 124604.32 kb | 18.7284 s |
| tar.bz2 | msgpack | 124546.63 kb | 17.5692 s |
| tar.bz2 | avro | 124533.31 kb | 17.7366 s |
| tar.xz | jsonl | 112513.34 kb | 70.5483 s |
| tar.xz | msgpack | 112506.42 kb | 70.1478 s |
| tar.xz | avro | 112503.61 kb | 70.7673 s |
