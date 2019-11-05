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


def map_vaccine(vaccine_response: str) -> Optional[bool]:
    """
    Maps a vaccine response to FHIR immunization status codes
    (https://www.hl7.org/fhir/valueset-immunization-status.html)
    """
    vaccine_map = {
        'Yes': True,
        'No': False,
        'Do not know': None,
        '': None,
    }

    if vaccine_response not in vaccine_map:
        raise UnknownVaccineResponseError(f"Unknown vaccine response «{vaccine_response}»")

    return vaccine_map[vaccine_response]


def map_symptom(symptom_name: str) -> Optional[str]:
    """
    Maps a *symptom_name* to current symptom values in ID3C warehouse.

    There is no official standard for symptoms, we are using the values
    created by Audere from year 1 (2018-2019).
    """
    symptom_map = {
        'Feeling feverish':                     'feelingFeverish',
        'Headache':                             'headaches',
        'Headaches':                             'headaches',
        'Cough':                                'cough',
        'Chills or shivering':                  'chillsOrShivering',
        'Sweats':                               'sweats',
        'Sore throat or itchy/scratchy throat': 'soreThroat',
        'Nausea or vomiting':                   'nauseaOrVomiting',
        'Runny or stuffy nose':                 'runnyOrStuffyNose',
        'Runny / stuffy nose':                 'runnyOrStuffyNose',
        'Feeling more tired than usual':        'fatigue',
        'Muscle or body aches':                 'muscleOrBodyAches',
        'Diarrhea':                             'diarrhea',
        'Ear pain or ear discharge':            'earPainOrDischarge',
        'Rash':                                 'rash',
        'Increased trouble with breathing':     'increasedTroubleBreathing',
        'None of the above':                    None
    }

    if symptom_name not in symptom_map:
        raise UnknownSymptomNameError(f"Unknown symptom name «{symptom_name}»")

    return symptom_map[symptom_name]


class UnknownSexError(ValueError):
    """
    Raised by :function: `map_sex` if a provided *sex_response*
    is not among a set of expected values
    """
    pass


class UnknownVaccineResponseError(ValueError):
    """
    Raised by :function: `map_vaccine` if a provided
    *vaccine_response* is not among a set of expected values
    """
    pass


class UnknownSymptomNameError(ValueError):
    """
    Raised by :function: `map_symptom` if a provided
    *symptom_name* is not among a set of expected values
    """
    pass
