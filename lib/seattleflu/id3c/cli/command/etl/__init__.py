"""
Custom ID3C ETL commands.

This module is listed in the entry points configuration of setup.py, which
causes the core id3c.cli module to load this file at CLI runtime.

By in turn loading our own individual commands here, we allow each command
module to register itself via Click's decorators.
"""
from . import (
    redcap_det_swab_n_send
)
