import gzip
import json
import os
from datetime import datetime
from inspect import getmembers, isclass

from benchmarks import strategy


def filesize(path):
    size = os.stat(path).st_size / 1024
    return '{:.2f} kb'.format(size)


def load_sample_email(path):
    with gzip.open(path, 'r') as fobj:
        raw_sample_email = fobj.read().decode('utf-8')
        return json.loads(raw_sample_email)


def timer(callback):
    start = datetime.now()
    callback()
    end = datetime.now()
    ellapsed = (end - start).total_seconds()
    return '{:.4f}'.format(ellapsed)


def get_strategies():
    return [(name.replace('Strategy', '').lower(), clazz)
            for name, clazz in getmembers(strategy, isclass)
            if name.endswith('Strategy')]
