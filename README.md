# compression-benchies

Compression benchmarks for ascoderu

Setup the requirements with `pip install -r requirements.txt`

Run `python3 -m benchmarks <emails-zip-url>`

Results:

| Strategy          | Filesize     | Duration   |
| ----------------- | ------------ | ---------- |
| avro              | 171293.62 kb |  0.4421 s  |
| avro.gz           | 126415.51 kb |  6.3902 s  |
| jsonl             | 171606.69 kb |  0.4845 s  |
| jsonl.gz          | 126402.74 kb |  6.0208 s  |
| jsonl.zs          | 121547.86 kb |  0.9676 s  |
| msgpack           | 171298.67 kb |  0.0907 s  |
| msgpack-header    | 171288.35 kb |  0.0792 s  |
| msgpack-header.gz | 126410.49 kb |  5.7391 s  |
| msgpack-header.zs | 121870.88 kb |  0.9203 s  |
| msgpack.gz        | 126419.45 kb |  5.9043 s  |
| msgpack.zs        | 121930.07 kb |  0.5857 s  |
| tar.bz2           | 124601.20 kb | 18.5556 s  |
| tar.gz            | 126402.65 kb |  8.1514 s  |
| tar.xz            | 112513.03 kb | 71.7144 s  |
