from argparse import ArgumentParser
from collections import namedtuple
from csv import DictWriter
from csv import excel_tab
from glob import glob
from itertools import product
from os import makedirs
from os.path import isfile
from os.path import join
from sys import stderr
from sys import stdout

from benchmarks.compression import get_all as all_compressors
from benchmarks.serialization import get_all as all_serializers
from benchmarks.utils import download_sample_emails
from benchmarks.utils import filesize_kb
from benchmarks.utils import load_sample_email
from benchmarks.utils import Timer

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

Benchmark = namedtuple('Benchmark', (
    'Compressor',
    'Serializer',
    'FileSize',
    'WriteTime',
    'ReadTime',
))

writer = DictWriter(stdout, Benchmark._fields, dialect=excel_tab)

writer.writeheader()

for compressor, serializer in product(all_compressors(), all_serializers()):
    outpath = join(args.results_dir, 'emails{}{}'.format(
        compressor.extension, serializer.extension))

    if args.incremental and isfile(outpath):
        continue

    try:
        with Timer.timeit() as write_timer:
            with compressor.open_write(outpath) as fobj:
                serializer.serialize(iter(sample_emails), fobj)
    except Exception as ex:
        print(ex, file=stderr)
        write_time = 'ERROR'
        filesize = 'ERROR'
    else:
        write_time = write_timer.seconds()
        filesize = '{:.2f} kb'.format(filesize_kb(outpath))

    try:
        with Timer.timeit() as read_timer:
            with compressor.open_read(outpath) as fobj:
                for _ in serializer.deserialize(fobj):
                    pass
    except Exception as ex:
        print(ex, file=stderr)
        read_time = 'ERROR'
    else:
        read_time = read_timer.seconds()

    writer.writerow(Benchmark(
        Compressor=compressor.extension.lstrip('.') or '(none)',
        Serializer=serializer.extension.lstrip('.') or '(none)',
        FileSize=filesize,
        WriteTime=write_time,
        ReadTime=read_time,
    )._asdict())
