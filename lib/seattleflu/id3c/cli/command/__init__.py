"""
Custom ID3C CLI commands.

This module is listed in the entry points configuration of setup.py, which
causes the core id3c.cli module to load this file at CLI runtime.

By in turn loading our own individual commands here, we allow each command
module to register itself via Click's decorators.
"""
from . import (
    reportable_conditions
)
