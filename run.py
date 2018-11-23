import glob
import os
import sys

import colorama
import tablib

from benchmarks import strategy
from benchmarks.constants import RESULTS_DIR
from benchmarks.utils import filesize, load_sample_email, timer

strategies = [
    strategy.Gzip,
    strategy.Avro,
    strategy.Bson,
    strategy.Msgpack,
    strategy.NoCompression,
]

sample_emails = [(path, load_sample_email(path)) for path in glob.glob('./sample-emails/*')]


for strategy in strategies:
    headers = [strategy.EXTENSION, 'Original Size', 'After Compression']
    dataset = []
    table = tablib.Dataset(headers=headers)
    strategy_dir = os.path.join(RESULTS_DIR, strategy.__name__.lower())
    durations = []

    os.makedirs(strategy_dir, exist_ok=True)

    for original_file, sample_email in sample_emails:
        try:
            compressed_path = os.path.join(strategy_dir, '{}{}'.format(os.path.basename(original_file), strategy.EXTENSION))
            with timer(durations):
                strategy.compress(sample_email, compressed_path)

            table.append(('', filesize(original_file), filesize(compressed_path)))
        except Exception as e:
            print(e, file=sys.stderr)

    print(colorama.Fore.GREEN, table.export('df'))
    print(colorama.Fore.RED + 'Compression for strategy {} took {} seconds\n\n'
          .format(strategy.__name__, sum(durations)))
    print(colorama.Style.RESET_ALL)
