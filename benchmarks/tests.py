from base64 import b64encode
from io import BytesIO
from os import close
from os import remove
from tempfile import mkstemp
from unittest import TestCase

from benchmarks.compression import get_all as compressors
from benchmarks.encryption import get_all as encryptors
from benchmarks.serialization import get_all as serializers


class TempfilesTestCase(TestCase):
    def setUp(self):
        self.temp_paths = []

    def tearDown(self):
        for temp_path in self.temp_paths:
            remove(temp_path)

    def given_tempfile(self, extension):
        fd, temp_path = mkstemp(suffix=extension)
        close(fd)
        self.temp_paths.append(temp_path)
        return temp_path


class CompressionTests(TestCase):
    def test_roundtrip(self):
        for compressor in compressors():
            with self.subTest(compressor=compressor):
                expected = b'test content'
                path = self.given_tempfile(compressor)

                with open(path, 'wb') as fobj:
                    with compressor.compress(fobj) as compressed:
                        compressed.write(expected)

                with open(path, 'rb') as fobj:
                    with compressor.decompress(fobj) as decompressed:
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


class EncryptionTests(TempfilesTestCase):
    def test_roundtrip(self):
        for encryptor in encryptors():
            if encryptor.extension != 'aes':
                continue
            with self.subTest(encryptor=encryptor):
                expected = b'some bytes'

                path = self.given_tempfile(encryptor.extension)

                with open(path, 'wb') as fobj:
                    with encryptor.encrypt(fobj) as encrypted:
                        encrypted.write(expected)

                with open(path, 'rb') as fobj:
                    self.assertNotEqual(fobj.read(), expected)

                with open(path, 'rb') as fobj:
                    with encryptor.deserialize(fobj) as decrypted:
                        self.assertEqual(decrypted.read(), expected)


if __name__ == '__main__':
    from unittest import main
    main()
