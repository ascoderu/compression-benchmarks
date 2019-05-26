from abc import ABC
from contextlib import contextmanager
from gzip import GzipFile
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

    def compress(self, fobj: IO[bytes]) -> IO[bytes]:
        raise NotImplementedError

    def decompress(self, fobj: IO[bytes]) -> IO[bytes]:
        raise NotImplementedError


class NoCompression(_Compression):
    extension = ''

    @contextmanager
    def compress(self, fobj: IO[bytes]) -> IO[bytes]:
        yield fobj

    @contextmanager
    def decompress(self, fobj: IO[bytes]) -> IO[bytes]:
        yield fobj


class GzipCompression(_Compression):
    extension = '.gz'

    @contextmanager
    def compress(self, fobj: IO[bytes]) -> IO[bytes]:
        with GzipFile(fileobj=fobj, mode='w') as compressed:
            yield compressed

    @contextmanager
    def decompress(self, fobj: IO[bytes]) -> IO[bytes]:
        with GzipFile(fileobj=fobj, mode='r') as decompressed:
            yield decompressed


class ZstandardCompression(_Compression):
    def __init__(self, level: int = 3):
        self.level = level

    @property
    def extension(self) -> str:
        return '.{}.zs'.format(self.level)

    @contextmanager
    def compress(self, fobj: IO[bytes]) -> IO[bytes]:
        compressor = ZstdCompressor(level=self.level)
        with compressor.stream_writer(fobj) as writer:
            yield writer

    @contextmanager
    def decompress(self, fobj: IO[bytes]) -> IO[bytes]:
        decompressor = ZstdDecompressor()
        outfobj = NamedTemporaryFile(delete=False)
        try:
            decompressor.copy_stream(fobj, outfobj)
            outfobj.seek(0)
            yield outfobj
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
    def compress(self, fobj: IO[bytes]) -> IO[bytes]:
        mode = 'w|{}'.format(self.compression)
        with tarfile_open(fileobj=fobj, mode=mode) as archive:
            with NamedTemporaryFile() as buffer:
                yield buffer
                buffer.seek(0)
                archive.add(buffer.name, self.filename)

    @contextmanager
    def decompress(self, fobj: IO[bytes]) -> IO[bytes]:
        mode = 'r|{}'.format(self.compression)
        archive = tarfile_open(fileobj=fobj, mode=mode)
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
                raise FileNotFoundError('{} not found'.format(self.filename))
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
