from argparse import ArgumentParser
from glob import glob
from os import makedirs
from os.path import join

from colorama import Fore, Style
from tablib import Dataset

from benchmarks.utils import filesize, load_sample_email, timer, get_strategies, download_sample_emails

parser = ArgumentParser()
parser.add_argument('emails_zip_url')
parser.add_argument('--results_dir', default='results')
parser.add_argument('--inputs_dir', default='sample-emails')
args = parser.parse_args()

download_sample_emails(args.emails_zip_url, args.inputs_dir)
makedirs(args.results_dir, exist_ok=True)

sample_emails = [load_sample_email(path) for path in glob(join(args.inputs_dir, '*'))]

table = Dataset(title='Benchmark', headers=['Strategy', 'Filesize', 'Duration'])

for strategy_name, strategy in get_strategies():
    compressed_path = join(args.results_dir, 'result' + strategy.EXTENSION)
    duration = timer(lambda: strategy.compress(sample_emails, compressed_path))

    table.append((strategy.EXTENSION, filesize(compressed_path), duration))

print(Fore.YELLOW, table.title)
print(Fore.GREEN, table.export('df'))
print(Style.RESET_ALL)
