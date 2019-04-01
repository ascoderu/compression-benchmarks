from abc import ABC
from typing import IO
from typing import Iterable
from os import urandom
from contextlib import contextmanager
from tempfile import NamedTemporaryFile
from itertools import tee

from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import CTR
from cryptography.hazmat.primitives.hmac import HMAC
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend


PASSWORD_DERIVE_ITER = 100000


class KeyDerive(object):

    def __init__(self, password, iter, hash, derive, backend, salt):

        self._password = password
        self._iterations = iter
        self._hash = hash
        self._derive = derive
        self._backend = backend
        self._salt = salt

        self._length = 16

    def _generate_key(self):

        kdf = self._derive(
            algorithm=self._hash(),
            length=self._length,
            salt=self._salt,
            iterations=self._iterations,
            backend=self._backend()
        )

        return kdf.derive(self._password)

    def _verify(self, k):

        kdf = self._derive(
            algorithm=self._hash(),
            length=self._length,
            salt=self._salt,
            iterations=self._iterations,
            backend=self._backend()
        )

        kdf.verify(self._password, k)

    def __call__(self):

        key = self._generate_key()
        self._verify(key)
        return key


class Encrypt(object):

    def __init__(self, key, hmac_key,
                 hmac, hash, cipher,
                 algorithm, mode, backend,
                 salt, hmac_salt):

        self._key = key
        self._hmac_key = hmac_key
        self._hmac = hmac
        self._hash = hash
        self._cipher = cipher
        self._algorithm = algorithm
        self._mode = mode
        self._backend = backend
        self._salt = salt
        self._hmac_salt = hmac_salt

        self._iv = urandom(16)

    def __call__(self, data):

        h = self._hmac(
            self._hmac_key,
            self._hash(),
            backend=self._backend()
        )

        ciph = self._cipher(
            self._algorithm(self._key),
            self._mode(self._iv),
            backend=self._backend()
        )

        encryptor = ciph.encryptor()
        aes_params_sent = False

        for raw in data:
            out = encryptor.update(raw)

            if not iv_sent:
                out = self._iv + self._salt + self._hmac_salt + out
                aes_params_sent = True

            h.update(out)

            yield out

        out = encryptor.finalize()
        h.update(out)
        yield out

        signature = h.finalize()
        yield signature


class Decrypt(object):

    def __init__(self, password, hash,
                 derive, hmac, cipher,
                 algorithm, mode, backend):

        self._password = password
        self._hash = hash
        self._derive = derive
        self._hmac = hmac
        self._cipher = cipher
        self._algorithm = algorithm
        self._mode = mode
        self._backend = backend

    def _derive_key(self, salt):

        key_derive = KeyDerive(
            self._password,
            PASSWORD_DERIVE_ITER,
            self._hash,
            self._derive,
            self._backend,
            salt
        )

        return key_derive()

    def _return_chunks(self, data, minsize=48):

        cur, ahead = tee(data)
        ahead_val = next(ahead, None)

        while ahead_val is not None:

            bytes = next(cur, None)
            ahead_val = next(ahead, None)

            while (len(bytes) < minsize) and ahead_val is not None:
                bytes += next(cur, None)
                ahead_val = next(ahead, None)

            if ahead_val is None:
                yield (bytes, None)
            else:
                yield (bytes, True)

    def __call__(self, data):

        decryptor = None
        params_recieve = False

        for enc, nextenc in self._return_chunks(data):
            if not params_recieve:
                params_recieve = True

                iv = enc[:16]
                salt = enc[16:32]
                hmac_salt = enc[32:48]

                key = self._derive_key(salt)
                hmac_key = self._derive_key(hmac_salt)

                h = self._hmac(
                    hmac_key,
                    self._hash(),
                    backend=self._backend()
                )

                ciph = self._cipher(
                    self._algorithm(key),
                    self._mode(iv),
                    backend=self._backend()
                )

                decryptor = ciph.decryptor()
                h.update(iv + salt + hmac_salt)

                enc = enc[48:]

            if nextenc is None:
                signature = enc[-32:]
                enc = enc[:-32]
                h.update(enc)

                h2 = h.copy()
                h.verify(signature)
                h2.finalize()

            else:
                h.update(enc)

            yield decryptor.update(enc)

        yield decryptor.finalize()


class _Encryption(ABC):

    @property
    def extension(self)->str:
        raise NotImplementedError

    @contextmanager
    def encrypt(self, fobj: IO[bytes]) -> IO[bytes]:
        raise NotImplementedError

    @contextmanager
    def deserialize(self, fobj: IO[bytes]) -> IO[bytes]:
        raise NotImplementedError


class AesEncryption(_Encryption):

    extension = 'aes'

    @contextmanager
    def encrypt(self, fobj: IO[bytes]) -> IO[bytes]:
        salt = urandom(16)
        hmac_salt = urandom(16)

        key_deriver = KeyDerive(
            b"client10",
            PASSWORD_DERIVE_ITER,
            SHA256,
            PBKDF2HMAC,
            default_backend,
            salt
        )

        hmac_key_deriver = KeyDerive(
            b"client10",
            PASSWORD_DERIVE_ITER,
            SHA256,
            PBKDF2HMAC,
            default_backend,
            hmac_salt
        )

        key = key_deriver()
        hmac_key = hmac_key_deriver()

        encrypt_stream = Encrypt(
            key,
            hmac_key,
            HMAC,
            SHA256,
            Cipher,
            AES,
            CTR,
            default_backend,
            salt,
            hmac_salt
        )

        try:
            with NamedTemporaryFile() as temp:
                yield temp
                temp.seek(0)
                for item in encrypt_stream(temp):
                    fobj.write(item)

        finally:
            pass

    @contextmanager
    def deserialize(self, fobj: IO[bytes]) -> IO[bytes]:

        decrypt = Decrypt(
            b"client10",
            SHA256,
            PBKDF2HMAC,
            HMAC,
            Cipher,
            AES,
            CTR,
            default_backend
        )

        try:
            with NamedTemporaryFile() as temp:
                for item in decrypt(fobj):
                    temp.write(item)
                yield temp

        finally:
            pass


class NoEncryption(_Encryption):

    extension = ''

    @contextmanager
    def encrypt(self, fobj: IO[bytes]) -> IO[bytes]:
        try:
            yield fobj
        finally:
            pass

    @contextmanager
    def deserialize(self, fobj: IO[bytes]) -> IO[bytes]:
        try:
            yield fobj
        finally:
            pass


def get_all() -> Iterable[_Encryption]:
    return (
        NoEncryption(),
        AesEncryption(),
    )
