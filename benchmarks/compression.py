from abc import ABC
from contextlib import contextmanager
from gzip import open as gzip_open
from tarfile import open as tarfile_open
from tempfile import NamedTemporaryFile
from typing import IO
from typing import Iterable

from zstandard import MAX_COMPRESSION_LEVEL
from zstandard import ZstdCompressor


class _Compression(ABC):
    @property
    def extension(self) -> str:
        raise NotImplementedError

    def open(self, path: str) -> IO[bytes]:
        raise NotImplementedError


class NoCompression(_Compression):
    extension = ''

    @classmethod
    def open(cls, path: str) -> IO[bytes]:
        return open(path, 'wb')


class GzipCompression(_Compression):
    extension = '.gz'

    @classmethod
    def open(cls, path: str) -> IO[bytes]:
        return gzip_open(path, 'wb')


class ZstandardCompression(_Compression):
    def __init__(self, level: int = 3):
        self.level = level

    @property
    def extension(self) -> str:
        return '.{}.zs'.format(self.level)

    def open(self, path: str) -> IO[bytes]:
        compressor = ZstdCompressor(level=self.level)
        fobj = open(path, 'wb')
        return compressor.stream_writer(fobj)


class _TarballCompression(_Compression):
    @property
    def compression(self) -> str:
        raise NotImplementedError

    @property
    def extension(self) -> str:
        return '.tar.{}'.format(self.compression)

    @contextmanager
    def open(self, path: str) -> IO[bytes]:
        mode = 'w|{}'.format(self.compression)
        with tarfile_open(path, mode) as archive:
            with NamedTemporaryFile() as buffer:
                yield buffer
                buffer.seek(0)
                archive.add(buffer.name, 'file')


class Bz2TarballCompression(_TarballCompression):
    @property
    def compression(self) -> str:
        return 'bz2'


class XzTarballCompression(_TarballCompression):
    @property
    def compression(self) -> str:
        return 'xz'


def get_all() -> Iterable[_Compression]:
    return (
        NoCompression(),
        GzipCompression(),
        ZstandardCompression(),
        ZstandardCompression(level=MAX_COMPRESSION_LEVEL),
        Bz2TarballCompression(),
        XzTarballCompression(),
    )
