from collections import namedtuple
from csv import DictWriter
from csv import excel_tab
from glob import glob
from itertools import product
from os import getenv
from os import makedirs
from os.path import isfile
from os.path import join
from sys import stderr
from sys import stdout

from benchmarks.compression import get_all as compressors
from benchmarks.serialization import get_all as serializers
from benchmarks.utils import Timer
from benchmarks.utils import download_sample_emails
from benchmarks.utils import filesize_kb
from benchmarks.utils import load_sample_email
from benchmarks.utils import pretty_extension

Benchmark = namedtuple('Benchmark', (
    'Compressor',
    'Serializer',
    'FilesizeKb',
    'WriteTimeSeconds',
    'ReadTimeSeconds',
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


def print_error(stage, compressor, serializer, ex):
    print('Error during {}-phase in {}+{}: {}'.format(
        stage,
        pretty_extension(compressor.extension),
        pretty_extension(serializer.extension),
        ex,
    ), file=stderr)


def run_benchmarks(emails, results_dir, incremental):
    makedirs(results_dir, exist_ok=True)

    for compressor, serializer in product(compressors(), serializers()):
        outpath = join(results_dir, 'emails{}{}'.format(
            serializer.extension, compressor.extension))

        if incremental and isfile(outpath):
            continue

        try:
            with Timer.timeit() as write_timer:
                with compressor.open_write(outpath) as fobj:
                    serializer.serialize(iter(emails), fobj)
        except Exception as ex:
            print_error('write', compressor, serializer, ex)
            write_time = 'ERROR'
            filesize = 'ERROR'
        else:
            write_time = write_timer.seconds()
            filesize = '{:.2f}'.format(filesize_kb(outpath))

        try:
            with Timer.timeit() as read_timer:
                with compressor.open_read(outpath) as fobj:
                    for _ in serializer.deserialize(fobj):
                        pass
        except Exception as ex:
            print_error('read', compressor, serializer, ex)
            read_time = 'ERROR'
        else:
            read_time = read_timer.seconds()

        yield Benchmark(
            Compressor=pretty_extension(compressor.extension),
            Serializer=pretty_extension(serializer.extension),
            FilesizeKb=filesize,
            WriteTimeSeconds=write_time,
            ReadTimeSeconds=read_time,
        )


def display_benchmarks(results, display_format, buffer=stdout):
    if display_format == 'csv':
        writer = DictWriter(buffer, Benchmark._fields, dialect=excel_tab)
        writer.writeheader()
        for result in results:
            writer.writerow(result._asdict())

    elif display_format == 'html':
        buffer.write('<!doctype html>\n')
        buffer.write('<html>\n')
        buffer.write(' <head>\n')
        buffer.write('  <meta charset="utf-8">\n')
        buffer.write('  <meta name="viewport" content="width=device-width, initial-scale=1">\n')  # noqa: E501
        buffer.write('  <title>Ascoderu compression benchmark results</title>\n')  # noqa: E501
        buffer.write('  <link rel="stylesheet" href="https://unpkg.com/purecss@1.0.0/build/base-min.css">\n')  # noqa: E501
        buffer.write('  <link rel="stylesheet" href="https://unpkg.com/purecss@1.0.0/build/pure-min.css">\n')  # noqa: E501
        buffer.write('  <link rel="stylesheet" href="https://unpkg.com/tablesort@5.1.0/tablesort.css">\n')  # noqa: E501
        buffer.write('  <style>\n')
        buffer.write('   td { text-align: center; }\n')
        buffer.write('   table { margin-bottom: 1em; }\n')
        buffer.write('  </style>\n')
        buffer.write(' </head>\n')
        buffer.write(' <body>\n')
        buffer.write('  <table id="benchmarks" class="pure-table pure-table-horizontal pure-table-striped">\n')  # noqa: E501
        buffer.write('   <thead>\n')
        buffer.write('    <tr>\n')
        for field in Benchmark._fields:
            buffer.write('     <th>{}</th>\n'.format(field))
        buffer.write('    </tr>\n')
        buffer.write('   </thead>\n')
        buffer.write('   <tbody>\n')
        for result in results:
            buffer.write('    <tr>\n')
            for value in result:
                buffer.write('     <td>{}</td>\n'.format(value))
            buffer.write('    </tr>\n')
        buffer.write('   </tbody>\n')
        buffer.write('  </table>\n')
        buffer.write('  <script src="https://unpkg.com/tablesort@5.1.0/dist/tablesort.min.js"></script>\n')  # noqa: E501
        buffer.write('  <script>new Tablesort(document.getElementById("benchmarks"))</script>\n')  # noqa: E501
        if getenv('TRAVIS_COMMIT') and getenv('TRAVIS_REPO_SLUG'):
            buffer.write('  <a class="pure-button" href="https://github.com/{}/tree/{}">View code</a>\n'.format(getenv('TRAVIS_REPO_SLUG'), getenv('TRAVIS_COMMIT')))  # noqa: E501
        buffer.write(' </body>\n')
        buffer.write('</html>\n')

    else:
        raise NotImplementedError(display_format)


def cli():
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument('emails_zip_url')
    parser.add_argument('--results_dir', default='results')
    parser.add_argument('--inputs_dir', default='sample-emails')
    parser.add_argument('--exclude_attachments', action='store_true')
    parser.add_argument('--incremental', action='store_true')
    parser.add_argument('--display_format', default='csv')
    args = parser.parse_args()

    emails = load_samples(args.emails_zip_url, args.inputs_dir,
                          args.exclude_attachments)

    results = run_benchmarks(emails, args.results_dir, args.incremental)

    display_benchmarks(results, args.display_format)


if __name__ == '__main__':
    cli()
