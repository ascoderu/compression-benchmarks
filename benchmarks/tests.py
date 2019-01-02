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
        for compressor in compressors():
            with self.subTest(compressor=compressor):
                expected = b'test content'
                path = self.given_tempfile(compressor)

                with compressor.open_write(path) as compressed:
                    compressed.write(expected)

                with compressor.open_read(path) as decompressed:
                    actual = decompressed.read()

                self.assertEqual(actual, expected)

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
        for serializer in serializers():
            with self.subTest(serializer=serializer):
                expected = [{
                    'to': ['foo@bar'],
                    'subject': 'baz',
                    'attachments': [{
                        'filename': 'attachment.txt',
                        'content': b64encode(b'foo').decode('ascii')
                    }],
                }]
                fobj = BytesIO()
                serializer.serialize(expected, fobj)
                fobj.seek(0)
                actual = list(serializer.deserialize(fobj))
                self.assertListEqual(actual, expected)


if __name__ == '__main__':
    from unittest import main
    main()
