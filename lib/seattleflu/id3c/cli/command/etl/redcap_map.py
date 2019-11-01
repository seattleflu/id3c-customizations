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


class UnknownSexError(ValueError):
    """
    Raised by :function: `map_sex` if a provided *sex_response*
    is not among a set of expected values
    """
    pass
