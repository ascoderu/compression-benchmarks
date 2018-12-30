from gzip import open as gzip_open
from json import loads
from os import makedirs
from os import remove
from os import stat
from os.path import isdir
from shutil import copyfileobj
from tempfile import NamedTemporaryFile
from zipfile import ZipFile

import requests


def download_to_file(url: str) -> str:
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with NamedTemporaryFile(mode='wb', delete=False) as fobj:
        copyfileobj(response.raw, fobj)

    return fobj.name


def download_sample_emails(emails_zip_url: str, inputs_dir: str) -> None:
    if isdir(inputs_dir):
        return

    emails_zip = download_to_file(emails_zip_url)
    try:
        makedirs(inputs_dir, exist_ok=True)
        ZipFile(emails_zip).extractall(inputs_dir)
    finally:
        remove(emails_zip)


def filesize_kb(path: str) -> float:
    return stat(path).st_size / 1024


def load_sample_email(path: str) -> dict:
    with gzip_open(path, 'r') as fobj:
        raw_sample_email = fobj.read().decode('utf-8')
        return loads(raw_sample_email)
