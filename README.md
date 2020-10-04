# compression-benchmarks

## What's this?

This repository contains serialization, compression and encryption benchmarks for the Lokole [email data exchange protocol](https://github.com/ascoderu/opwen-cloudserver#data-exchange-format) with the aim to find the method of encoding our dataset in the smallest possible size to save bandwidth for users of the system.

> :bulb: Do you know a compression or serialization that could make our data smaller? Then open a pull request! :octocat:

## Setup

Install the requirements with `pip install -r requirements.txt -r requirements-dev.txt`.

Run the tests with `python -m benchmarks.tests` and run the linter with `flake8 benchmarks`.

Run the benchmarks via `python -u -m benchmarks <emails-zip-url>`.

## Results

Benchmark results are kept up to date by [Github Actions](https://github.com/ascoderu/compression-benchmarks/actions?query=workflow%3ACD) at [ascoderu/compression-benchmarks](https://ascoderu.ca/compression-benchmarks/).
