from abc import ABC
from json import dumps
from typing import IO
from typing import Iterable

from fastavro import parse_schema as avro_parse_schema
from fastavro import writer as avro_writer
from msgpack import dump as msgpack_dump
from msgpack import dumps as msgpack_dumps


class _Serialization(ABC):
    @property
    def extension(self) -> str:
        raise NotImplementedError

    def serialize(self, objs: Iterable[dict], fobj: IO[bytes]):
        raise NotImplementedError


class JsonLinesSerialization(_Serialization):
    extension = '.jsonl'
    encoding = 'utf-8'
    separators = (',', ':')

    @classmethod
    def serialize(cls, objs: Iterable[dict], fobj: IO[bytes]):
        for obj in objs:
            serialized = dumps(obj, separators=cls.separators)
            fobj.write(serialized.encode(cls.encoding))
            fobj.write(b'\n')


class MsgpackSerialization(_Serialization):
    extension = '.msgpack'

    @classmethod
    def serialize(cls, objs: Iterable[dict], fobj: IO[bytes]):
        for obj in objs:
            msgpack_dump(obj, fobj)


class MsgpackHeaderSerialization(_Serialization):
    extension = '.hmsgpack'

    @classmethod
    def _write_item(cls, header, item, stream):
        record = [item.get(column) for column in header]
        stream.write(msgpack_dumps(record, use_bin_type=True))

    @classmethod
    def serialize(cls, objs: Iterable[dict], fobj: IO[bytes]):
        objs = iter(objs)
        first = next(objs)
        header = sorted(first.keys())
        fobj.write(','.join(header).encode('utf-8'))
        fobj.write(b'\n')
        cls._write_item(header, first, fobj)
        for obj in objs:
            cls._write_item(header, obj, fobj)


class AvroSerialization(_Serialization):
    extension = '.avro'

    schema = avro_parse_schema({
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
    def serialize(cls, objs: Iterable[dict], fobj: IO[bytes]):
        avro_writer(fobj, cls.schema, objs)


def get_all() -> Iterable[_Serialization]:
    yield JsonLinesSerialization()
    yield MsgpackSerialization()
    yield AvroSerialization()
