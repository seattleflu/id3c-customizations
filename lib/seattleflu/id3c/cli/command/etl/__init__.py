"""
Run ETL routines
"""
import logging
import re
from typing import Any, Optional


LOG = logging.getLogger(__name__)


def race(races: Optional[Any]) -> list:
    """
    Given one or more *races*, returns the matching race identifier found in
    Audere survey data.

    Single values may be passed:

    >>> race("OTHER")
    ['other']

    A list of values may also be passed:

    >>> race(["ASIAN", "bLaCk", "white"])
    ['asian', 'blackOrAfricanAmerican', 'white']

    Leading and trailing space is ignored:

    >>> race("   amerind ")
    ['americanIndianOrAlaskaNative']

    Internal runs of whitespace are collapsed during comparison:

    >>> race("Native  Hawaiian or other     pacific islander")
    ['nativeHawaiian']

    but not completely ignored:

    >>> race("whi te")
    Traceback (most recent call last):
        ...
    seattleflu.id3c.cli.command.etl.UnknownRaceError: Unknown race name «whi te»

    ``None`` may be passed for convenience with :meth:`dict.get`.

    >>> race(None)
    [None]

    An :class:`UnknownRaceError` is raised when an unknown value is
    encountered:

    >>> race("foobarbaz")
    Traceback (most recent call last):
        ...
    seattleflu.id3c.cli.command.etl.UnknownRaceError: Unknown race name «foobarbaz»

    >>> race(["white", "nonsense", "other"])
    Traceback (most recent call last):
        ...
    seattleflu.id3c.cli.command.etl.UnknownRaceError: Unknown race name «nonsense»
    """
    if races is None:
        LOG.debug("No race response found.")
        return [None]

    if not isinstance(races, list):
        races = races.split("|")

    # Keys must be lowercase for case-insensitive lookup
    race_map = {
        "americanindianoralaskanative": "americanIndianOrAlaskaNative",
        "american indian or alaska native": "americanIndianOrAlaskaNative",
        "amerind": "americanIndianOrAlaskaNative",
        "native_american": "americanIndianOrAlaskaNative",
        "indian": "americanIndianOrAlaskaNative",

        "asian": "asian",

        "blackorafricanamerican": "blackOrAfricanAmerican",
        "black or african american": "blackOrAfricanAmerican",
        "black": "blackOrAfricanAmerican",

        "nativehawaiian": "nativeHawaiian",
        "native hawaiian or other pacific islander": "nativeHawaiian",
        "native hawaiian or other pacific islande": "nativeHawaiian",
        "hawaiian-pacislander": "nativeHawaiian",
        "nativehi": "nativeHawaiian",
        "native_hawaiian": "nativeHawaiian",

        "white": "white",

        "other": "other",
        "other race": "other",
        "multiple races": "other",

        "refused": None,
        "prefer not to say": None,
        "did not wish to indicate": None,
        "unknown": None,
    }

    assert set(race_map.keys()) == set(map(str.lower, race_map.keys()))

    def standardize_whitespace(string):
        return re.sub(r"\s+", " ", string.strip())

    def standardize_race(race):
        try:
            return race_map[standardize_whitespace(race).lower()]
        except KeyError:
            raise UnknownRaceError(f"Unknown race name «{race}»") from None

    return list(map(standardize_race, races))


class UnknownRaceError(ValueError):
    """
    Raised by :function:`race` if its provided *race_name* is not among the set
    of expected values.
    """
    pass


from . import (
    clinical,
    longitudinal,
    redcap_det_kiosk,
    redcap_det_swab_n_send,
    redcap_det_swab_and_home_flu,
    redcap_det_uw_retrospectives,
)
