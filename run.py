from argparse import ArgumentParser
from glob import glob
from os import makedirs
from os.path import join, basename, dirname

from colorama import Fore, Style
from tablib import Dataset

from benchmarks.utils import filesize, load_sample_email, timer, get_strategies, download_sample_emails

parser = ArgumentParser()
parser.add_argument('emails_zip_url')
parser.add_argument('--results_dir', default='results')
parser.add_argument('--inputs_dir', default=join(dirname(__file__), 'sample-emails'))
args = parser.parse_args()

download_sample_emails(args.emails_zip_url, args.inputs_dir)

sample_emails = [(path, load_sample_email(path)) for path in glob(join(args.inputs_dir, '*'))]

for strategy_name, strategy in get_strategies():
    strategy_dir = join(args.results_dir, strategy_name)
    makedirs(strategy_dir, exist_ok=True)

    table = Dataset(title=strategy_name, headers=['Original', 'Compressed', 'Duration'])

    for original_file, sample_email in sample_emails:
        compressed_path = join(strategy_dir, basename(original_file) + strategy.EXTENSION)
        try:
            duration = timer(lambda: strategy.compress(sample_email, compressed_path))
        except Exception as e:
            table.append(['ERROR'] * len(table.headers))
        else:
            table.append((filesize(original_file), filesize(compressed_path), duration))

    print(Fore.YELLOW, table.title)
    print(Fore.GREEN, table.export('df'))
    print(Style.RESET_ALL)
