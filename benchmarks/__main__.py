from argparse import ArgumentParser
from csv import DictWriter, excel_tab
from glob import glob
from os import makedirs
from os.path import join, isfile
from sys import stdout

from benchmarks.utils import filesize, load_sample_email, timer, get_strategies, download_sample_emails

parser = ArgumentParser()
parser.add_argument('emails_zip_url')
parser.add_argument('--results_dir', default='results')
parser.add_argument('--inputs_dir', default='sample-emails')
parser.add_argument('--exclude_attachments', action='store_true')
parser.add_argument('--incremental', action='store_true')
args = parser.parse_args()

download_sample_emails(args.emails_zip_url, args.inputs_dir)
makedirs(args.results_dir, exist_ok=True)

sample_emails = []
for path in glob(join(args.inputs_dir, '*')):
    sample_email = load_sample_email(path)
    if args.exclude_attachments:
        sample_email.pop('attachments', None)
    sample_emails.append(sample_email)

writer = DictWriter(stdout, ('Strategy', 'Filesize', 'Duration'), dialect=excel_tab)
writer.writeheader()

for strategy_name, strategy in get_strategies():
    compressed_path = join(args.results_dir, 'result' + strategy.EXTENSION)
    if args.incremental and isfile(compressed_path):
        continue

    duration = timer(lambda: strategy.compress(iter(sample_emails), compressed_path))

    writer.writerow({
        'Strategy': strategy.EXTENSION.lstrip('.'),
        'Filesize': filesize(compressed_path),
        'Duration': duration,
    })
