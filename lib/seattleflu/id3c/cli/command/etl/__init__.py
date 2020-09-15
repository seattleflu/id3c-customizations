"""
Run ETL routines
"""
import logging
import re
from typing import Any, Optional, List
from textwrap import dedent
from functools import wraps
from id3c.cli.redcap import Record as REDCapRecord, is_complete


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

    Strings with pipe "|" or forward slash "/" delimeters are converted to
    lists:

    >>> race("black|white")
    ['blackOrAfricanAmerican', 'white']

    >>> race("asian/amerind")
    ['asian', 'americanIndianOrAlaskaNative']

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
        # Split on "|" or "/"
        races = re.split(r"\||/", races)

    # Keys must be lowercase for case-insensitive lookup
    race_map = {
        "americanindianoralaskanative": "americanIndianOrAlaskaNative",
        "american indian or alaska native": "americanIndianOrAlaskaNative",
        "amerind": "americanIndianOrAlaskaNative",
        "native_american": "americanIndianOrAlaskaNative",
        "indian": "americanIndianOrAlaskaNative",
        "native": "americanIndianOrAlaskaNative",

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
        "ha_pi": "nativeHawaiian",

        "white": "white",

        "other": "other",
        "other race": "other",
        "multiple races": "other",

        "refused": None,
        "prefer not to say": None,
        "did not wish to indicate": None,
        "unknown": None,
        "dont_say": None,
    }

    assert set(race_map.keys()) == set(map(str.lower, race_map.keys()))

    def standardize_race(race):
        try:
            return race_map[standardize_whitespace(race).lower()]
        except KeyError:
            raise UnknownRaceError(f"Unknown race name «{race}»") from None

    return list(map(standardize_race, races))


def standardize_whitespace(string: str) -> str:
    """
    Removes leading, trailing, and repeat whitespace from a given *string*.
    """
    return re.sub(r"\s+", " ", string.strip())


def first_record_instance(routine):
    """
    A decorator that passes only the first REDCap record from list of
    *redcap_record_instances* to the *routine* and logs a warning about having
    multiple instances of a REDCap record.

    The decorated function should be an ETL routine for REDCap records that
    must accept a dictionary *redcap_record* argument.
    """
    @wraps(routine)
    def decorated(*args, **kwargs):
        record_instances: List[REDCapRecord] = kwargs.pop("redcap_record_instances")
        kwargs["redcap_record"] = record_instances[0]

        if len(record_instances) > 1:
            LOG.warning(dedent(f"""
            Found multiple record instances for record id «{kwargs["redcap_record"].id}».
            Multiple record instances per record id are usually due to
            repeating instruments/longitudinal events in REDCap.
            If this project does not have repeating elements,
            this may be caused by a bug in REDCap."""))

        LOG.debug(f"Only processing the first instance of the REDCap record «{kwargs['redcap_record'].id}»")
        return routine(*args, **kwargs)
    return decorated


def required_instruments(required_instruments: List[str]):
    """
    A decorator that checks the *redcap_record* being processed by the
    ETL routine has completed all *required_instruments*.

    Returns `None` if not all *required_instruments* are completed.

    The decorated function should be an ETL routine for REDCap records that
    must accept a dictionary *redcap_record* argument.
    """
    def decorator(routine):
        @wraps(routine)
        def decorated(*args, **kwargs):
            redcap_record = kwargs["redcap_record"]

            incomplete_instruments = {
                instrument
                    for instrument
                    in required_instruments
                    if not is_complete(instrument, redcap_record)
            }

            if incomplete_instruments:
                LOG.debug(f"The following required instruments «{incomplete_instruments}» are not yet marked complete.")
                return None

            return routine(*args, **kwargs)

        return decorated

    return decorator


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
    redcap_det_asymptomatic_swab_n_send,
    redcap_det_scan,
    redcap_det_uw_reopening
)
