"""
Mapping functions shared by REDCap DET ETLs.
"""
from typing import Optional


def map_sex(sex_response: str) -> Optional[str]:
    """
    Map expected *sex_response* from a REDCap record to FHIR gender codes
    (https://www.hl7.org/fhir/valueset-administrative-gender.html)
    """
    sex_map = {
        'Male': 'male',
        'Female': 'female',
        'Indeterminate/other': 'other',
        'Prefer not to say': 'unknown',
        '': 'unknown'
    }

    if sex_response not in sex_map:
        raise UnknownSexError(f"Unknown sex response «{sex_response}»")

    return sex_map[sex_response]


def map_vaccine(vaccine_response: str) -> Optional[str]:
    """
    Maps a vaccine response to FHIR immunization status codes
    (https://www.hl7.org/fhir/valueset-immunization-status.html)
    """
    vaccine_map = {
        'Yes': 'completed',
        'No': 'not-done',
        'Do not know': None,
        '': None,
    }

    if vaccine_response not in vaccine_map:
        raise UnknownVaccineResponseError(f"Unknown vaccine response «{vaccine_response}»")

    return vaccine_map[vaccine_response]


class UnknownSexError(ValueError):
    """
    Raised by :function: `map_sex` if a provided *sex_response*
    is not among a set of expected values
    """
    pass


class UnknownVaccineResponse(ValueError):
    """
    Raised by :function: `map_vaccine` if a provided
    *vaccine_response* is not among a set of expected values
    """
    pass
