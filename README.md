# ID3C customizations for the Seattle Flu Study

Currently this repository contains only an additional Sqitch project for
managing Seattle Flu-specific static data in the ID3C database (e.g. organism
assignments for targets, extra site details, bespoke views).

In time, the goal with ID3C is to adopt a core + extensions development model,
and this repository is expected to accumulate additional code and schema
customizations.

## Sqitch

The idea is to use Sqitch to manage database customizations which might
otherwise be managed manually.

Changes in this Sqitch project can rely on changes in the ID3C Sqitch project
by prefixing them with the project name (`seattleflu/schema` currently, but
subject to change after reorganization).


## Dev tools
### Doctests
Run doctests with:
```sh
pytest tests/docstrings.py
```
