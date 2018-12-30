from abc import ABC
from contextlib import contextmanager
from gzip import open as gzip_open
from os import remove
from tarfile import open as tarfile_open
from tempfile import NamedTemporaryFile
from typing import IO
from typing import Iterable

from zstandard import MAX_COMPRESSION_LEVEL
from zstandard import ZstdCompressor
from zstandard import ZstdDecompressor


class _Compression(ABC):
    @property
    def extension(self) -> str:
        raise NotImplementedError

    def open_write(self, path: str) -> IO[bytes]:
        raise NotImplementedError

    def open_read(self, path: str) -> IO[bytes]:
        raise NotImplementedError


class NoCompression(_Compression):
    extension = ''

    @classmethod
    def open_write(cls, path: str) -> IO[bytes]:
        return open(path, 'wb')

    @classmethod
    def open_read(cls, path: str) -> IO[bytes]:
        return open(path, 'rb')


class GzipCompression(_Compression):
    extension = '.gz'

    @classmethod
    def open_write(cls, path: str) -> IO[bytes]:
        return gzip_open(path, 'wb')

    @classmethod
    def open_read(cls, path: str) -> IO[bytes]:
        return gzip_open(path, 'rb')


class ZstandardCompression(_Compression):
    def __init__(self, level: int = 3):
        self.level = level

    @property
    def extension(self) -> str:
        return '.{}.zs'.format(self.level)

    def open_write(self, path: str) -> IO[bytes]:
        compressor = ZstdCompressor(level=self.level)
        fobj = open(path, 'wb')
        return compressor.stream_writer(fobj)

    @contextmanager
    def open_read(self, path: str) -> IO[bytes]:
        decompressor = ZstdDecompressor()
        outfobj = NamedTemporaryFile(delete=False)
        try:
            with open(path, 'rb') as infobj:
                decompressor.copy_stream(infobj, outfobj)
            with open(outfobj.name, 'rb') as fobj:
                yield fobj
        finally:
            outfobj.close()
            remove(outfobj.name)


class _TarballCompression(_Compression):
    filename = 'filename'

    @property
    def compression(self) -> str:
        raise NotImplementedError

    @property
    def extension(self) -> str:
        return '.tar.{}'.format(self.compression)

    @contextmanager
    def open_write(self, path: str) -> IO[bytes]:
        mode = 'w|{}'.format(self.compression)
        with tarfile_open(path, mode) as archive:
            with NamedTemporaryFile() as buffer:
                yield buffer
                buffer.seek(0)
                archive.add(buffer.name, self.filename)

    @contextmanager
    def open_read(self, path: str) -> IO[bytes]:
        mode = 'r|{}'.format(self.compression)
        archive = tarfile_open(path, mode)
        try:
            fobj = None
            while True:
                member = archive.next()
                if member is None:
                    break
                if member.name == self.filename:
                    fobj = archive.extractfile(member)
                    break
            if fobj is None:
                raise FileNotFoundError('{} not found in {}'
                                        .format(self.filename, path))
            try:
                yield fobj
            finally:
                fobj.close()
        finally:
            archive.close()


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
