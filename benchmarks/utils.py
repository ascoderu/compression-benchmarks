import contextlib
import gzip
import json
import os
from datetime import datetime


def filesize(path):
    size = os.stat(path).st_size / 1024
    return '{:.2f} kb'.format(size)


def load_sample_email(path):
    with gzip.open(path, 'r') as fobj:
        raw_sample_email = fobj.read().decode('utf-8')
        return json.loads(raw_sample_email)


@contextlib.contextmanager
def timer(durations):
    start = datetime.now()
    yield
    end = datetime.now()
    durations.append((end - start).total_seconds())
