from glob import glob
from os import makedirs
from os.path import join, basename, dirname
from sys import stderr

from colorama import Fore, Style
from tablib import Dataset

from benchmarks import strategy
from benchmarks.utils import filesize, load_sample_email, timer

RESULTS_DIR = 'results'
SAMPLE_EMAILS_DIR = join(dirname(__file__), 'sample-emails')

strategies = [
    strategy.Gzip,
    strategy.Avro,
    strategy.Bson,
    strategy.Msgpack,
    strategy.NoCompression,
]

sample_emails = [(path, load_sample_email(path)) for path in glob(join(SAMPLE_EMAILS_DIR, '*'))]


for strategy in strategies:
    headers = [strategy.EXTENSION, 'Original Size', 'After Compression']
    table = Dataset(headers=headers)
    strategy_dir = join(RESULTS_DIR, strategy.__name__.lower())
    durations = []

    makedirs(strategy_dir, exist_ok=True)

    for original_file, sample_email in sample_emails:
        try:
            compressed_path = join(strategy_dir, '{}{}'.format(basename(original_file), strategy.EXTENSION))
            with timer(durations):
                strategy.compress(sample_email, compressed_path)

            table.append(('', filesize(original_file), filesize(compressed_path)))
        except Exception as e:
            print(e, file=stderr)

    print(Fore.GREEN, table.export('df'))
    print(Fore.RED + 'Compression for strategy {} took {} seconds\n\n'.format(strategy.__name__, sum(durations)))
    print(Style.RESET_ALL)
