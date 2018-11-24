import gzip
import json
import os
from typing import Callable
from datetime import datetime
from inspect import getmembers, isclass
from shutil import copyfileobj
from tempfile import NamedTemporaryFile
from zipfile import ZipFile

import requests

from benchmarks import strategy


def download_to_file(url: str) -> str:
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with NamedTemporaryFile(mode='wb', delete=False) as fobj:
        copyfileobj(response.raw, fobj)

    return fobj.name


def download_sample_emails(emails_zip_url: str, inputs_dir: str) -> None:
    if os.path.isdir(inputs_dir):
        return

    emails_zip = download_to_file(emails_zip_url)
    try:
        os.makedirs(inputs_dir, exist_ok=True)
        ZipFile(emails_zip).extractall(inputs_dir)
    finally:
        os.remove(emails_zip)


def filesize(path: str) -> str:
    size = os.stat(path).st_size / 1024
    return '{:.2f} kb'.format(size)


def load_sample_email(path: str) -> dict:
    with gzip.open(path, 'r') as fobj:
        raw_sample_email = fobj.read().decode('utf-8')
        return json.loads(raw_sample_email)


def timer(callback: Callable[[], None]) -> str:
    start = datetime.now()
    callback()
    end = datetime.now()
    ellapsed = (end - start).total_seconds()
    return '{:.4f}'.format(ellapsed)


def get_strategies() -> list:
    return [(name.replace('Strategy', '').lower(), clazz)
            for name, clazz in getmembers(strategy, isclass)
            if name.endswith('Strategy')]
