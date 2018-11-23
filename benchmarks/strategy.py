import json
from json import dumps

import avro.schema
import bson
import msgpack
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from .constants import (
    AVRO_SCHEMA_DIR
)


class OriginalStrategy:
    EXTENSION = '.json'

    @staticmethod
    def compress(contents: dict, compressed_filename: str) -> None:
        with open(compressed_filename, 'w') as compressed_file:
            compressed_file.write(dumps(contents))

    @staticmethod
    def decompress(compressed_filename: str) -> None:
        with open(compressed_filename, 'r') as compressed_file:
            return json.load(compressed_file)


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
        schema = avro.schema.Parse(open(AVRO_SCHEMA_DIR, "rb").read())

        with open(compressed_filename, 'wb') as compressed_file:
            writer = DataFileWriter(compressed_file, DatumWriter(), schema)
            writer.append(contents)
            writer.close()

    @staticmethod
    def decompress(compressed_filename: str) -> DataFileReader:
        reader = DataFileReader(open(compressed_filename, 'rb'), DatumReader())
        return reader
