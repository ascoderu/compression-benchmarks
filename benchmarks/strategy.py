import gzip
import json
from abc import ABC
from typing import IO
from typing import Iterable

import msgpack


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


class _JsonLinesStrategy(_Strategy, ABC):
    @classmethod
    def _to_json(cls, obj: object) -> bytes:
        return json.dumps(obj, separators=(',', ':')).encode('utf-8')

    @classmethod
    def compress(cls, contents: Iterable[dict], compressed_filename: str):
        with cls._open(compressed_filename) as compressed_file:
            for content in contents:
                compressed_file.write(cls._to_json(content))
                compressed_file.write(b'\n')


class JsonLinesStrategy(_Uncompressed, _JsonLinesStrategy):
    EXTENSION = '.jsonl'


class JsonLinesGzipStrategy(_Gzipped, _JsonLinesStrategy):
    EXTENSION = '.jsonl.gz'


class _MsgpackStrategy(_Strategy, ABC):
    @classmethod
    def compress(cls, contents: Iterable[dict], compressed_filename: str) -> None:
        with cls._open(compressed_filename) as compressed_file:
            for content in contents:
                msgpack.pack(content, compressed_file)


class MsgpackStrategy(_Uncompressed, _MsgpackStrategy):
    EXTENSION = '.msgpack'


class MsgpackGzipStrategy(_Gzipped, _MsgpackStrategy):
    EXTENSION = '.msgpack.gz'
