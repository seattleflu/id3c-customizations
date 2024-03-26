"""
Run ETL routines
"""
import logging
import re
from typing import Any, Optional, List
from textwrap import dedent
from functools import wraps
from id3c.cli.redcap import Record as REDCapRecord, is_complete
from .fhir import create_coding

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
        "american indian or native alaskan": "americanIndianOrAlaskaNative",
        "american indian": "americanIndianOrAlaskaNative",
        "american indian and alaska native": "americanIndianOrAlaskaNative",
        "american indian or alaska native": "americanIndianOrAlaskaNative",
        "amerind": "americanIndianOrAlaskaNative",
        "native american": "americanIndianOrAlaskaNative",
        "native_american": "americanIndianOrAlaskaNative",
        "indian": "americanIndianOrAlaskaNative",
        "native": "americanIndianOrAlaskaNative",
        "alaska native": "americanIndianOrAlaskaNative",
        "ai_an": "americanIndianOrAlaskaNative",

        "asian": "asian",

        "blackorafricanamerican": "blackOrAfricanAmerican",
        "black or african american": "blackOrAfricanAmerican",
        "black": "blackOrAfricanAmerican",
        "black or african-american": "blackOrAfricanAmerican",
        "black_aa": "blackOrAfricanAmerican",

        "nativehawaiian": "nativeHawaiian",
        "native hawaiian": "nativeHawaiian",
        "native hawaiian and other pacific islander" : "nativeHawaiian",
        "native hawaiian or other pacific islander": "nativeHawaiian",
        "native hawaiian or other pacific islande": "nativeHawaiian",
        "native hawaiian or pacific islander": "nativeHawaiian",
        "hawaiian-pacislander": "nativeHawaiian",
        "pacific islander": "nativeHawaiian",
        "nativehi": "nativeHawaiian",
        "native_hawaiian": "nativeHawaiian",
        "ha_pi": "nativeHawaiian",
        "nh_opi": "nativeHawaiian",

        "white": "white",
        "white or caucasian": "white",
        "caucasian": "white",

        "other": "other",
        "other race": "other",
        "multiple races": "other",
        "more than one race": "other",
        "another race": "other",

        "refused": None,
        "patient refused": None,
        "prefer not to say": None,
        "did not wish to indicate": None,
        "unknown": None,
        "dont_say": None,
        "declined to answer": None,
        "patient not present": None,
        "unavailable or unknown": None,
        "unable to collect": None,
    }

    assert set(race_map.keys()) == set(map(str.lower, race_map.keys()))

    def standardize_race(race):
        try:
            return race_map[standardize_whitespace(race).lower()]
        except KeyError:
            raise UnknownRaceError(f"Unknown race name «{race}»") from None

    return list(map(standardize_race, races))


def ethnicity(ethnicity: Optional[str]) -> Optional[bool]:
    """
    Returns a standardized boolean value for the given *ethnicity*

    >>> ethnicity("HISPANIC OR LATINO")
    True

    >>> ethnicity("NOT HISPANIC OR LATINO")
    False

    >>> ethnicity("NULL") == None
    True

    A :class:`UnknownEthnicityError` is raised when an unknown value is encountered:

    >>> ethnicity("FOOBARBAZ")
    Traceback (most recent call last):
        ...
    seattleflu.id3c.cli.command.etl.UnknownEthnicityError: Unknown ethnicity value «foobarbaz»
    """

    if isinstance(ethnicity, str):
        ethnicity = standardize_whitespace(ethnicity.lower())

    if ethnicity is None or ethnicity == "":
        return None

    # Leaving this code here to be implemented later. My original approach was to use FHIR
    # coding for ethnicity, which would be preffered, but for consistency with other ETLs
    # I switched to ingesting ethnicity as a boolean. To transition to FHIR codes across all projects will
    # require updating multiple ETLs, shipping views, and re-ingesting data. A card has been added
    # to tackle this at a later date.
    # -drr 2021-12-30

    #hispanic_or_latino = create_coding("http://hl7.org/fhir/v3/Ethnicity", "2135-2", "hispanic or latino")
    #not_hispanic_or_latino = create_coding("http://hl7.org/fhir/v3/Ethnicity", "2186-5", "not hispanic or latino")

    mapper = {
        "hispanic or latino":                 True,           # hispanic_or_latino,
        "hispanic or latino/a or latinx":     True,           # hispanic_or_latino,
        "not hispanic or latino":             False,          # not_hispanic_or_latino,
        "non-hispanic or latino/a or latinx": False,          # not_hispanic_or_latino,
        "unavailable or unknown":             None,
        "unknown to patient":                 None,
        "patient declined to respond":        None,
        "null":                               None,
        "declined to answer":                 None,
        "unable to collect":                  None,
        "prefer not to answer":               None,
        "don't know":                         None,
    }

    if ethnicity not in mapper:
        raise UnknownEthnicityError(f"Unknown ethnicity value «{ethnicity}»")

    return mapper[ethnicity]


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

class UnknownEthnicityError(ValueError):
    """
    Raised by :function: `ethnicity` if it finds an ethnicity
    that is not among a set of mapped values
    """
    pass


from . import (
    clinical,
    longitudinal,
    redcap_det_swab_n_send,
    redcap_det_uw_retrospectives,
    redcap_det_scan,
    redcap_det_uw_reopening,
    redcap_det_fh_airs,
)
