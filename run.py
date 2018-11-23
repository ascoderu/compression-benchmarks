from glob import glob
from os import makedirs
from os.path import join, basename, dirname

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
    strategy_dir = join(RESULTS_DIR, strategy.__name__.lower())
    makedirs(strategy_dir, exist_ok=True)

    headers = [strategy.EXTENSION, 'Original', 'Compressed', 'Duration']
    table = Dataset(headers=headers)

    for original_file, sample_email in sample_emails:
        compressed_path = join(strategy_dir, '{}{}'.format(basename(original_file), strategy.EXTENSION))
        try:
            duration = timer(lambda: strategy.compress(sample_email, compressed_path))
        except Exception as e:
            table.append(('', 'ERROR', 'ERROR', 'ERROR'))
        else:
            table.append(('', filesize(original_file), filesize(compressed_path), '{:.4f} s'.format(duration)))

    print(Fore.GREEN, table.export('df'))
    print(Style.RESET_ALL)
