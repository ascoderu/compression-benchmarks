from abc import ABC
from contextlib import contextmanager
from gzip import GzipFile
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

    def write_stream(self, fobj: IO[bytes]) -> IO[bytes]:
        raise NotImplementedError

    def read_stream(self, fobj: IO[bytes]) -> IO[bytes]:
        raise NotImplementedError


class NoCompression(_Compression):
    extension = ''

    @classmethod
    def open_write(cls, path: str) -> IO[bytes]:
        return open(path, 'wb')

    @classmethod
    def open_read(cls, path: str) -> IO[bytes]:
        return open(path, 'rb')

    @contextmanager
    def write_stream(self, fobj: IO[bytes]) -> IO[bytes]:
        try:
            yield fobj
        finally:
            pass

    @contextmanager
    def read_stream(self, fobj: IO[bytes]) -> IO[bytes]:
        try:
            yield fobj
        finally:
            pass


class GzipCompression(_Compression):
    extension = '.gz'

    @classmethod
    def open_write(cls, path: str) -> IO[bytes]:
        return gzip_open(path, 'wb')

    @classmethod
    def open_read(cls, path: str) -> IO[bytes]:
        return gzip_open(path, 'rb')

    @contextmanager
    def write_stream(self, fobj: IO[bytes]) -> IO[bytes]:

        with NamedTemporaryFile() as serialized:
            yield serialized
            serialized.seek(0)
            with GzipFile(mode='wb', fileobj=fobj) as compressed:
                for item in serialized:
                    compressed.write(item)

    @contextmanager
    def read_stream(self, fobj: IO[bytes]) -> IO[bytes]:
        with NamedTemporaryFile() as temp:
            with GzipFile(mode='rb', fileobj=fobj) as decompressed:
                for item in decompressed:
                    temp.write(item)
            temp.seek(0)
            yield temp


class ZstandardCompression(_Compression):
    def __init__(self, level: int = 3):
        self.level = level

    @property
    def extension(self) -> str:
        return '.{}.zs'.format(self.level)

    @contextmanager
    def open_write(self, path: str) -> IO[bytes]:
        compressor = ZstdCompressor(level=self.level)
        fobj = open(path, 'wb')
        try:
            with compressor.stream_writer(fobj) as writer:
                yield writer
        finally:
            fobj.close()

    @contextmanager
    def open_read(self, path: str) -> IO[bytes]:
        decompressor = ZstdDecompressor()
        outfobj = NamedTemporaryFile(delete=False)
        try:
            with open(path, 'rb') as infobj:
                decompressor.copy_stream(infobj, outfobj)
            outfobj.seek(0)
            yield outfobj
        finally:
            outfobj.close()
            remove(outfobj.name)

    @contextmanager
    def write_stream(self, fobj: IO[bytes]) -> IO[bytes]:
        compressor = ZstdCompressor(level=self.level)
        with NamedTemporaryFile() as serialized:
            yield serialized
            serialized.seek(0)
            compressor.copy_stream(serialized, fobj)

    @contextmanager
    def read_stream(self, fobj: IO[bytes]) -> IO[bytes]:
        decompressor = ZstdDecompressor()
        with NamedTemporaryFile() as decompressed:
            decompressor.copy_stream(fobj, decompressed)
            decompressed.seek(0)
            yield decompressed


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

    @contextmanager
    def write_stream(self, fobj: IO[bytes]) -> IO[bytes]:
        mode = 'w|{}'.format(self.compression)
        with NamedTemporaryFile() as serialized:
            yield serialized
            serialized.seek(0)
            with tarfile_open(mode=mode, fileobj=fobj) as compressed:
                compressed.add(serialized.name, self.filename)

    @contextmanager
    def read_stream(self, fobj: IO[bytes]) -> IO[bytes]:
        mode = 'r|{}'.format(self.compression)
        with tarfile_open(mode=mode, fileobj=fobj) as archive:
            extracted = None
            while True:
                member = archive.next()
                if member is None:
                    break
                if member.name == self.filename:
                    extracted = archive.extractfile(member)
                    break
            if extracted is None:
                raise FileNotFoundError('{} not found'
                                        .format(self.filename))
            yield extracted


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
