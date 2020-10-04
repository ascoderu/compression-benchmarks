# compression-benchmarks

## What's this?

Benchmarks for the Lokole [email data exchange protocol](https://github.com/ascoderu/opwen-cloudserver#data-exchange-format).

> :bulb: Do you know a compression or serialization that could make our data even smaller? Then open a pull request! :octocat:

## Setup

Install the requirements with `pip install -r requirements.txt -r requirements-dev.txt`.

Run the tests with `python3 -m benchmarks.tests` and run the linter with `flake8 benchmarks`.

Run the benchmarks via `python3 -u -m benchmarks <emails-zip-url>`.

## Results

Benchmark results are kept up to date by Github Actions at [ascoderu/compression-benchmarks](https://ascoderu.ca/compression-benchmarks/).
