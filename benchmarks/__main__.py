from argparse import ArgumentParser
from csv import DictWriter
from csv import excel_tab
from datetime import datetime
from glob import glob
from itertools import product
from os import makedirs
from os.path import isfile
from os.path import join
from sys import stdout

from benchmarks.compression import get_all as all_compressors
from benchmarks.serialization import get_all as all_serializers
from benchmarks.utils import download_sample_emails
from benchmarks.utils import filesize_kb
from benchmarks.utils import load_sample_email

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

writer = DictWriter(
    stdout, ('Compression', 'Serialization', 'Filesize', 'Duration'),
    dialect=excel_tab)

writer.writeheader()

for compressor, serializer in product(all_compressors(), all_serializers()):
    outpath = join(args.results_dir, 'emails{}{}'.format(
        compressor.extension, serializer.extension))

    if args.incremental and isfile(outpath):
        continue

    start = datetime.now()
    try:
        with compressor.open(outpath) as fobj:
            serializer.serialize(iter(sample_emails), fobj)
    except Exception:
        runtime, filesize = 'ERROR', 'ERROR'
    else:
        end = datetime.now()
        runtime = '{:.4f} s'.format((end - start).total_seconds())
        filesize = '{:.2f} kb'.format(filesize_kb(outpath))

    writer.writerow({
        'Compression': compressor.extension.lstrip('.') or '(none)',
        'Serialization': serializer.extension.lstrip('.') or '(none)',
        'Filesize': filesize,
        'Runtime': runtime,
    })
