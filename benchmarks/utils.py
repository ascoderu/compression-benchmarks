from datetime import datetime
from gzip import open as gzip_open
from inspect import getmembers
from inspect import isclass
from json import loads
from os import makedirs
from os import remove
from os import stat
from os.path import isdir
from shutil import copyfileobj
from tempfile import NamedTemporaryFile
from typing import Callable
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


def filesize(path: str) -> str:
    size = stat(path).st_size / 1024
    return '{:.2f} kb'.format(size)


def load_sample_email(path: str) -> dict:
    with gzip_open(path, 'r') as fobj:
        raw_sample_email = fobj.read().decode('utf-8')
        return loads(raw_sample_email)


def timer(callback: Callable[[], None]) -> str:
    start = datetime.now()
    callback()
    end = datetime.now()
    ellapsed = (end - start).total_seconds()
    return '{:.4f} s'.format(ellapsed)


def get_strategies() -> list:
    from benchmarks import strategy

    return sorted([
        (name.replace('Strategy', '').lower(), clazz)
        for name, clazz in getmembers(strategy, isclass)
        if name.endswith('Strategy') and not name.startswith('_')
    ], key=lambda t: t[0])
