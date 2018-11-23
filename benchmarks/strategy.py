import gzip
import json
from os.path import dirname, join

import avro.schema
import bson
import msgpack
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from benchmarks.utils import load_sample_email


def to_json(obj: object) -> str:
    return json.dumps(obj, separators=(',', ':'))


class NoCompressionStrategy:
    EXTENSION = '.json'

    @staticmethod
    def compress(contents: dict, compressed_filename: str) -> None:
        with open(compressed_filename, 'w') as compressed_file:
            compressed_file.write(to_json(contents))

    @staticmethod
    def decompress(compressed_filename: str) -> None:
        with open(compressed_filename, 'r') as compressed_file:
            return json.load(compressed_file)


class GzipStrategy:
    EXTENSION = '.json.gz'

    @staticmethod
    def compress(contents: dict, compressed_filename: str) -> None:
        with gzip.open(compressed_filename, 'wb') as compressed_file:
            compressed_file.write(to_json(contents).encode('utf-8'))

    @staticmethod
    def decompress(compressed_filename: str) -> None:
        return load_sample_email(compressed_filename)


class MsgpackStrategy:
    EXTENSION = '.msgpack'

    @staticmethod
    def compress(contents: dict, compressed_filename: str) -> None:
        with open(compressed_filename, 'wb') as compressed_file:
            compressed_file.write(msgpack.packb(contents))

    @staticmethod
    def decompress(compressed_filename: str) -> dict:
        with open(compressed_filename, 'rb') as compressed_file:
            return msgpack.unpack(compressed_file)


class BsonStrategy:
    EXTENSION = '.bson'

    @staticmethod
    def compress(contents: dict, compressed_filename: str) -> None:
        with open(compressed_filename, 'wb') as compressed_file:
            compressed_file.write(bson.BSON.encode(contents))

    @staticmethod
    def decompress(compressed_filename: str) -> dict:
        with open(compressed_filename, 'rb') as compressed_file:
            return bson.BSON.decode(compressed_file.read())


class AvroStrategy:
    EXTENSION = '.avro'

    @staticmethod
    def compress(contents: dict, compressed_filename: str) -> None:
        with open(join(dirname(__file__), 'email.avsc')) as fobj:
            schema = avro.schema.Parse(fobj.read())

        with open(compressed_filename, 'wb') as compressed_file:
            writer = DataFileWriter(compressed_file, DatumWriter(), schema)
            writer.append(contents)
            writer.close()

    @staticmethod
    def decompress(compressed_filename: str) -> DataFileReader:
        with open(compressed_filename, 'rb') as fobj:
            reader = DataFileReader(fobj, DatumReader())
            return reader
