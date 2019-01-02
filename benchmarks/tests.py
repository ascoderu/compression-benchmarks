from base64 import b64encode
from io import BytesIO
from os import close
from os import remove
from tempfile import mkstemp
from unittest import TestCase

from benchmarks.compression import get_all as compressors
from benchmarks.serialization import get_all as serializers


class CompressionTests(TestCase):
    def test_roundtrip(self):
        failures = []
        for compressor in compressors():
            expected = b'test content'
            path = self.given_tempfile(compressor)

            with compressor.open_write(path) as compressed:
                compressed.write(expected)

            with compressor.open_read(path) as decompressed:
                actual = decompressed.read()

            failed = actual != expected
            if failed:
                failures.append(compressor.extension)

        if failures:
            self.fail(', '.join(failures))

    def setUp(self):
        self.temp_paths = []

    def tearDown(self):
        for temp_path in self.temp_paths:
            remove(temp_path)

    def given_tempfile(self, compressor):
        fd, temp_path = mkstemp(suffix=compressor.extension)
        close(fd)
        self.temp_paths.append(temp_path)
        return temp_path


class SerializationTests(TestCase):
    def test_roundtrip(self):
        failures = []
        for serializer in serializers():
            expected = [{'to': ['foo@bar'], 'subject': 'baz', 'attachments': [
                {'filename': 'attachment.txt',
                 'content': b64encode(b'foo').decode('ascii')}
            ]}]
            fobj = BytesIO()
            serializer.serialize(expected, fobj)
            fobj.seek(0)
            actual = list(serializer.deserialize(fobj))
            failed = len(actual) != 1 or expected[0] != actual[0]
            if failed:
                failures.append(serializer.extension)

        if failures:
            self.fail(', '.join(failures))


if __name__ == '__main__':
    from unittest import main
    main()
