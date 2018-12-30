import gzip
import json
from os.path import dirname, join

import avro.schema
import msgpack
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from bson import BSON


def to_json(obj: object) -> str:
    return json.dumps(obj, separators=(',', ':'))


class NoCompressionStrategy:
    EXTENSION = '.json'

    @staticmethod
    def compress(contents: dict, compressed_filename: str) -> None:
        with open(compressed_filename, 'w') as compressed_file:
            compressed_file.write(to_json(contents))


class GzipStrategy:
    EXTENSION = '.json.gz'

    @staticmethod
    def compress(contents: dict, compressed_filename: str) -> None:
        with gzip.open(compressed_filename, 'wb') as compressed_file:
            compressed_file.write(to_json(contents).encode('utf-8'))


class MsgpackStrategy:
    EXTENSION = '.msgpack'

    @staticmethod
    def compress(contents: dict, compressed_filename: str) -> None:
        with open(compressed_filename, 'wb') as compressed_file:
            compressed_file.write(msgpack.packb(contents))


class BsonStrategy:
    EXTENSION = '.bson'

    @staticmethod
    def compress(contents: dict, compressed_filename: str) -> None:
        with open(compressed_filename, 'wb') as compressed_file:
            compressed_file.write(BSON.encode(contents))


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
