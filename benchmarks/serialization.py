from abc import ABC
from base64 import b64decode
from base64 import b64encode
from contextlib import closing
from copy import deepcopy
from itertools import groupby
from json import dumps
from json import loads
from operator import itemgetter
from shutil import copyfileobj
from sqlite3 import IntegrityError
from sqlite3 import Row as SqliteRow
from sqlite3 import connect as sqlite_connect
from tempfile import NamedTemporaryFile
from typing import IO
from typing import Iterable

from bson import BSON
from cbor import dump as cbor_dump
from cbor import load as cbor_load
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


class CborSerialization(_Serialization):
    extension = '.cbor'

    @classmethod
    def serialize(cls, objs: Iterable[dict], fobj: IO[bytes]):
        for obj in objs:
            obj = byteify_attachments(obj)
            cbor_dump(obj, fobj)

    @classmethod
    def deserialize(cls, fobj: IO[bytes]) -> Iterable[dict]:
        while True:
            try:
                obj = cbor_load(fobj)
            except EOFError:
                break
            else:
                obj = unbyteify_attachments(obj)
                yield obj


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
            {"name": "read",
             "type": ["null", "boolean"]},
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


class SqliteSerialization(_Serialization):
    extension = '.sqlite'
    separator = chr(30)

    @classmethod
    def serialize(cls, objs: Iterable[dict], fobj: IO[bytes]):
        with NamedTemporaryFile() as db_file:
            with closing(sqlite_connect(db_file.name)) as connection:
                with closing(connection.cursor()) as cursor:
                    cursor.execute('''
                    CREATE TABLE emails (
                        _uid TEXT,
                        read BOOLEAN,
                        sent_at TEXT,
                        "to" TEXT,
                        cc TEXT,
                        bcc TEXT,
                        "from" TEXT,
                        subject TEXT,
                        body TEXT,
                        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL
                    )
                    ''')
                    cursor.execute('''
                    CREATE TABLE contents (
                        content BLOB UNIQUE,
                        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL
                    )
                    ''')
                    cursor.execute('''
                    CREATE TABLE attachments (
                        filename TEXT,
                        content_id INTEGER,
                        cid TEXT,
                        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        FOREIGN KEY (content_id) REFERENCES contents(id)
                    )
                    ''')
                    cursor.execute('''
                    CREATE TABLE emails_to_attachments (
                        email_id INTEGER,
                        attachment_id INTEGER,
                        FOREIGN KEY (email_id) REFERENCES emails(id),
                        FOREIGN KEY (attachment_id) REFERENCES attachments(id)
                    )
                    ''')
                    connection.commit()

                    for obj in objs:
                        obj = byteify_attachments(obj)

                        cursor.execute('''
                        INSERT INTO emails(
                            _uid,
                            read,
                            sent_at,
                            "to",
                            cc,
                            bcc,
                            "from",
                            subject,
                            body
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            obj.get('_uid'),
                            obj.get('read'),
                            obj.get('sent_at'),
                            cls._serialize_list(obj, 'to'),
                            cls._serialize_list(obj, 'cc'),
                            cls._serialize_list(obj, 'bcc'),
                            obj.get('from'),
                            obj.get('subject'),
                            obj.get('body'),
                        ))
                        email_id = cursor.lastrowid

                        for attachment in obj.get('attachments', []):
                            try:
                                cursor.execute('''
                                INSERT INTO contents(
                                    content
                                )
                                VALUES (?)
                                ''', (
                                    attachment.get('content'),
                                ))
                            except IntegrityError:
                                rows = cursor.execute('''
                                    SELECT id
                                    FROM contents
                                    WHERE content = ?
                                ''', (
                                    attachment.get('content'),
                                ))
                                content_id = rows.fetchone()[0]
                            else:
                                content_id = cursor.lastrowid

                            cursor.execute('''
                            INSERT INTO attachments (
                                filename,
                                content_id,
                                cid
                            )
                            VALUES (?, ?, ?)
                            ''', (
                                attachment.get('filename'),
                                content_id,
                                attachment.get('cid'),
                            ))
                            attachment_id = cursor.lastrowid

                            cursor.execute('''
                            INSERT INTO emails_to_attachments(
                                email_id,
                                attachment_id
                            )
                            VALUES (?, ?)
                            ''', (
                                email_id,
                                attachment_id,
                            ))
                        connection.commit()
            db_file.seek(0)
            copyfileobj(db_file, fobj)

    @classmethod
    def deserialize(cls, fobj: IO[bytes]) -> Iterable[dict]:
        with NamedTemporaryFile() as db_file:
            copyfileobj(fobj, db_file)
            db_file.seek(0)
            with closing(sqlite_connect(db_file.name)) as connection:
                connection.row_factory = SqliteRow
                with closing(connection.cursor()) as cursor:
                    joined = cursor.execute('''
                    SELECT emails.id AS __row_order__, *
                    FROM emails
                    LEFT OUTER JOIN emails_to_attachments
                        ON emails.id = emails_to_attachments.email_id
                    LEFT OUTER JOIN attachments
                        ON attachments.id = emails_to_attachments.attachment_id
                    LEFT OUTER JOIN contents
                        ON attachments.content_id = contents.id
                    ORDER BY __row_order__
                    ''')
                    for _, rows in groupby(joined, itemgetter('__row_order__')):
                        rows = list(rows)
                        row = rows[0]
                        obj = {key: row[key] for key in ('_uid', 'read', 'sent_at', 'from', 'subject', 'body')
                               if row[key] is not None}

                        for key in ('to', 'cc', 'bcc'):
                            obj[key] = cls._deserialize_list(row, key)

                        obj['attachments'] = []
                        for row in rows:
                            attachment = {key: row[key] for key in ('filename', 'content', 'cid')
                                          if row[key] is not None}
                            if attachment:
                                obj['attachments'].append(attachment)
                        obj = unbyteify_attachments(obj)
                        yield obj

    @classmethod
    def _serialize_list(cls, obj, key):
        try:
            return cls.separator.join(obj[key])
        except KeyError:
            return None

    @classmethod
    def _deserialize_list(cls, obj, key):
        value = obj[key]
        if not value:
            return []
        return value.split(cls.separator)


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
        CborSerialization(),
        BsonLinesSerialization(),
        MsgpackSerialization(),
        AvroSerialization(),
        SqliteSerialization(),
    )
