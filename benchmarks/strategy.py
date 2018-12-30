import gzip
import json
import tarfile
from abc import ABC
from tempfile import NamedTemporaryFile
from typing import IO
from typing import Iterable

import fastavro
import msgpack
from zstandard import MAX_COMPRESSION_LEVEL
from zstandard import ZstdCompressor


class _Strategy(ABC):
    @classmethod
    def _open(cls, path: str) -> IO[bytes]:
        raise NotImplementedError


class _Uncompressed(ABC):
    @classmethod
    def _open(cls, path: str) -> IO[bytes]:
        return open(path, 'wb')


class _Gzipped(ABC):
    @classmethod
    def _open(cls, path: str) -> IO[bytes]:
        return gzip.open(path, 'wb')


class _Zstd(ABC):
    _LEVEL = 3

    @classmethod
    def _open(cls, path: str) -> IO[bytes]:
        compressor = ZstdCompressor(level=cls._LEVEL)
        fobj = open(path, 'wb')
        return compressor.stream_writer(fobj)


class _JsonLinesStrategy(_Strategy, ABC):
    @classmethod
    def _to_json(cls, obj: object) -> bytes:
        return json.dumps(obj, separators=(',', ':')).encode('utf-8')

    @classmethod
    def compress(cls, contents: Iterable[dict], outfile: str):
        with cls._open(outfile) as compressed_file:
            for content in contents:
                compressed_file.write(cls._to_json(content))
                compressed_file.write(b'\n')


class JsonLinesStrategy(_Uncompressed, _JsonLinesStrategy):
    EXTENSION = '.jsonl'


class JsonLinesGzipStrategy(_Gzipped, _JsonLinesStrategy):
    EXTENSION = '.jsonl.gz'


class JsonLinesZstdStrategy(_Zstd, _JsonLinesStrategy):
    EXTENSION = '.jsonl.zs'


class JsonLinesZstdMaxStrategy(_Zstd, _JsonLinesStrategy):
    _LEVEL = MAX_COMPRESSION_LEVEL
    EXTENSION = '.jsonl.{}.zs'.format(_LEVEL)


class _TarballStrategy(JsonLinesStrategy, ABC):
    _MEMBER_FILENAME = 'file' + JsonLinesStrategy.EXTENSION
    EXTENSION = ''

    @classmethod
    def compress(cls, contents: Iterable[dict], outfile: str):
        mode = 'w|' + cls.EXTENSION.split('.')[-1]
        with tarfile.open(outfile, mode) as archive:
            with NamedTemporaryFile() as compressed_file:
                JsonLinesStrategy.compress(contents, compressed_file.name)
                compressed_file.seek(0)
                archive.add(compressed_file.name, cls._MEMBER_FILENAME)


class GzTarballStrategy(_TarballStrategy):
    EXTENSION = '.tar.gz'


class Bz2TarballStrategy(_TarballStrategy):
    EXTENSION = '.tar.bz2'


class XzTarballStrategy(_TarballStrategy):
    EXTENSION = '.tar.xz'


class _MsgpackStrategy(_Strategy, ABC):
    @classmethod
    def compress(cls, contents: Iterable[dict], outfile: str):
        with cls._open(outfile) as compressed_file:
            for content in contents:
                msgpack.pack(content, compressed_file)


class MsgpackStrategy(_Uncompressed, _MsgpackStrategy):
    EXTENSION = '.msgpack'


class MsgpackGzipStrategy(_Gzipped, _MsgpackStrategy):
    EXTENSION = '.msgpack.gz'


class MsgpackZstdStrategy(_Zstd, _MsgpackStrategy):
    EXTENSION = '.msgpack.zs'


class _MsgpackHeaderStrategy(_Strategy, ABC):
    @classmethod
    def _write_item(cls, header, item, stream):
        record = [item.get(column) for column in header]
        stream.write(msgpack.packb(record, use_bin_type=True))

    @classmethod
    def compress(cls, contents: Iterable[dict], outfile: str):
        contents = iter(contents)
        with cls._open(outfile) as compressed_file:
            first = next(contents)
            header = sorted(first.keys())
            compressed_file.write(','.join(header).encode('utf-8'))
            compressed_file.write(b'\n')
            cls._write_item(header, first, compressed_file)
            for content in contents:
                cls._write_item(header, content, compressed_file)


class MsgpackHeaderStrategy(_Uncompressed, _MsgpackHeaderStrategy):
    EXTENSION = '.msgpack-header'


class MsgpackHeaderGzipStrategy(_Gzipped, _MsgpackHeaderStrategy):
    EXTENSION = '.msgpack-header.gz'


class MsgpackHeaderZstdStrategy(_Zstd, _MsgpackHeaderStrategy):
    EXTENSION = '.msgpack-header.zs'


class _AvroStrategy(_Strategy, ABC):
    _schema = fastavro.parse_schema({
        "type": "record",
        "name": "Email",
        "fields": [
            {"name": "sent_at",
             "type": ["null", "string"]},
            {"name": "from",
             "type": ["null", "string"]},
            {"name": "subject",
             "type": ["null", "string"]},
            {"name": "body",
             "type": ["null", "string"]},
            {"name": "_uid",
             "type": ["null", "string"]},
            {"name": "to",
             "type": ["null", {"type": "array", "items": "string"}]},
            {"name": "cc",
             "type": ["null", {"type": "array", "items": "string"}]},
            {"name": "bcc",
             "type": ["null", {"type": "array", "items": "string"}]},
            {"name": "attachments",
             "type": ["null", {"type": "array", "items": {
                "type": "record",
                "name": "Attachment",
                "fields": [
                    {"name": "filename", "type": "string"},
                    {"name": "content", "type": "string"},
                ]
             }}]}
        ]
    })

    @classmethod
    def compress(cls, contents: Iterable[dict], outfile: str):
        with cls._open(outfile) as compressed_file:
            fastavro.writer(compressed_file, cls._schema, contents)


class AvroStrategy(_Uncompressed, _AvroStrategy):
    EXTENSION = '.avro'


class AvroGzipStrategy(_Gzipped, _AvroStrategy):
    EXTENSION = '.avro.gz'
