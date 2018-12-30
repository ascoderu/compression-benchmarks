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
from benchmarks.utils import Timer
from benchmarks.utils import download_sample_emails
from benchmarks.utils import filesize_kb
from benchmarks.utils import load_sample_email

Benchmark = namedtuple('Benchmark', (
    'Compressor',
    'Serializer',
    'FileSize',
    'WriteTime',
    'ReadTime',
))


def load_samples(zip_url, inputs_dir, exclude_attachments):
    download_sample_emails(zip_url, inputs_dir)
    sample_emails = []
    for path in glob(join(inputs_dir, '*')):
        sample_email = load_sample_email(path)
        if exclude_attachments:
            sample_email.pop('attachments', None)
        sample_emails.append(sample_email)
    return sample_emails


def run_benchmarks(emails, results_dir, incremental):
    makedirs(results_dir, exist_ok=True)

    for compressor, serializer in product(all_compressors(), all_serializers()):
        outpath = join(results_dir, 'emails{}{}'.format(
            compressor.extension, serializer.extension))

        if incremental and isfile(outpath):
            continue

        try:
            with Timer.timeit() as write_timer:
                with compressor.open_write(outpath) as fobj:
                    serializer.serialize(iter(emails), fobj)
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

        yield Benchmark(
            Compressor=compressor.extension.lstrip('.') or '(none)',
            Serializer=serializer.extension.lstrip('.') or '(none)',
            FileSize=filesize,
            WriteTime=write_time,
            ReadTime=read_time,
        )


def cli():
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument('emails_zip_url')
    parser.add_argument('--results_dir', default='results')
    parser.add_argument('--inputs_dir', default='sample-emails')
    parser.add_argument('--exclude_attachments', action='store_true')
    parser.add_argument('--incremental', action='store_true')
    args = parser.parse_args()

    emails = load_samples(args.emails_zip_url, args.inputs_dir,
                          args.exclude_attachments)

    writer = DictWriter(stdout, Benchmark._fields, dialect=excel_tab)
    writer.writeheader()
    for run in run_benchmarks(emails, args.results_dir, args.incremental):
        writer.writerow(run._asdict())


if __name__ == '__main__':
    cli()
