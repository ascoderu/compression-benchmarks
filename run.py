import os
import colorama
import tablib

from timeit import default_timer

from benchmarks.constants import EMAILS_DIR
from benchmarks.utils import (strategy_compress, get_strategy_dir, gunzip_file)
from benchmarks import strategy

strategies = [
    strategy.Avro,
    strategy.Bson,
    strategy.Msgpack,
    strategy.NoCompression,
]

email_files = os.listdir(EMAILS_DIR)

for strategy in strategies:
    headers = [strategy.EXTENSION, 'Original Size', 'After Compression']
    dataset = []
    table = tablib.Dataset(headers=headers)
    strategy_dir = get_strategy_dir(strategy)

    os.makedirs(strategy_dir, exist_ok=True)

    for file in email_files:
        try:
            original_file = os.path.join(EMAILS_DIR, file)

            raw_email_dict = gunzip_file(original_file)
            compressed_name = file + strategy.EXTENSION
            compressed_path = os.path.join(strategy_dir, compressed_name)
            start = default_timer()
            compressed_email = strategy_compress(strategy, raw_email_dict, compressed_path)

            table.append(
                ('', os.stat(original_file).st_size / 1024, os.stat(compressed_path).st_size / 1024),
            )
        except Exception as e:
            continue

    print(colorama.Fore.GREEN, table.export('df'))

    end = default_timer()
    duration = end - start

    print(colorama.Fore.RED + 'Compression for strategy {} took {} seconds\n\n'.format(strategy.EXTENSION.upper(),
                                                                                       duration))
    print(colorama.Style.RESET_ALL)
