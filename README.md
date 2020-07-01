# ID3C customizations for the Seattle Flu Study

This repository contains:

- a Sqitch project for managing Seattle Flu-specific static data in the ID3C
  database, such as organism assignments for targets, extra site details, and
  bespoke views.

- an ID3C extension which provides additional `id3c` CLI subcommands and ETL
  routines.

- additional support data and code

## Sqitch

Changes in this Sqitch project can rely on changes in the ID3C Sqitch project
by prefixing them with the project name (`seattleflu/schema` currently, but
subject to change after reorganization).

## Tests

Run all tests with:

```sh
pipenv run pytest -v
```

or name an individual test file, for example:

```sh
pipenv run pytest tests/docstrings.py
```
