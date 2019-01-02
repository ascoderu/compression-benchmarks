from abc import ABC
from base64 import b64decode
from base64 import b64encode
from copy import deepcopy
from json import dumps
from json import loads
from typing import IO
from typing import Iterable

from bson import BSON
from fastavro import parse_schema as avro_parse_schema
from fastavro import reader as avro_reader
from fastavro import writer as avro_writer
from msgpack import Packer
from msgpack import Unpacker


class _Serialization(ABC):
    @property
    def extension(self) -> str:
        raise NotImplementedError

    def serialize(self, objs: Iterable[dict], fobj: IO[bytes]):
        raise NotImplementedError

    def deserialize(self, fobj: IO[bytes]) -> Iterable[dict]:
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

    @classmethod
    def deserialize(cls, fobj: IO[bytes]) -> Iterable[dict]:
        for line in fobj:
            yield loads(line.decode(cls.encoding))


class BsonLinesSerialization(_Serialization):
    extension = '.bsonl'

    @classmethod
    def serialize(cls, objs: Iterable[dict], fobj: IO[bytes]):
        for obj in objs:
            obj = byteify_attachments(obj)
            fobj.write(BSON.encode(obj))
            fobj.write(b'\n')

    @classmethod
    def deserialize(cls, fobj: IO[bytes]) -> Iterable[dict]:
        for line in fobj:
            # noinspection PyCallByClass,PyTypeChecker
            obj = BSON.decode(line.rstrip(b'\n'))
            # noinspection PyTypeChecker
            obj = unbyteify_attachments(obj)
            yield obj


class MsgpackSerialization(_Serialization):
    extension = '.msgpack'

    @classmethod
    def serialize(cls, objs: Iterable[dict], fobj: IO[bytes]):
        packer = Packer(use_bin_type=True)
        for obj in objs:
            obj = byteify_attachments(obj)
            serialized = packer.pack(obj)
            fobj.write(serialized)

    @classmethod
    def deserialize(cls, fobj: IO[bytes]) -> Iterable[dict]:
        unpacker = Unpacker(fobj, raw=False)
        for obj in unpacker:
            obj = unbyteify_attachments(obj)
            yield obj


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
                    {"name": "content", "type": "bytes"},
                ]
             }}]}
        ]
    })

    @classmethod
    def serialize(cls, objs: Iterable[dict], fobj: IO[bytes]):
        objs = (byteify_attachments(obj) for obj in objs)
        avro_writer(fobj, cls.schema, objs)

    @classmethod
    def deserialize(cls, fobj: IO[bytes]) -> Iterable[dict]:
        for obj in avro_reader(fobj):
            obj = unbyteify_attachments(obj)
            yield {key: value for (key, value) in obj.items()
                   if value is not None}


def byteify_attachments(obj: dict) -> dict:
    if not obj.get('attachments'):
        return obj

    obj = deepcopy(obj)

    for attachment in obj['attachments']:
        content = attachment['content']
        attachment['content'] = b64decode(content)

    return obj


def unbyteify_attachments(obj: dict) -> dict:
    if not obj.get('attachments'):
        return obj

    obj = deepcopy(obj)

    for attachment in obj['attachments']:
        content = attachment['content']
        attachment['content'] = b64encode(content).decode('ascii')

    return obj


def get_all() -> Iterable[_Serialization]:
    return (
        JsonLinesSerialization(),
        BsonLinesSerialization(),
        MsgpackSerialization(),
        AvroSerialization(),
    )
