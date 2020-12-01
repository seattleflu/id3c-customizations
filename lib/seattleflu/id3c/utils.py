"""
Utilities.
"""
import re
from textwrap import dedent


def unwrap(text: str) -> str:
    """
    Unwraps *text* after dedenting it.
    """
    return re.sub(r"\n+", " ", dedent(text.strip("\n")))
