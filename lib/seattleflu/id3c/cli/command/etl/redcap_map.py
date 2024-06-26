"""
Mapping functions shared by REDCap DET ETLs.
"""
from typing import Optional


def map_sex(sex_response: str) -> Optional[str]:
    """
    Map expected *sex_response* from a REDCap record to FHIR administrative gender codes
    (https://www.hl7.org/fhir/valueset-administrative-gender.html)
    """
    sex_map = {
        'male': 'male',
        'm': 'male',
        'female': 'female',
        'f': 'female',
        'indeterminate/other': 'other',
        'other (please specify)': 'other',
        'other': 'other',
        'prefer not to say': 'unknown',
        'dont_say': 'unknown',
        'unknown': 'unknown',
        'u': 'unknown',
        '': 'unknown'
    }

    if sex_response.lower() not in sex_map:
        raise UnknownSexError(f"Unknown sex response «{sex_response}»")

    return sex_map[sex_response.lower()]


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
        'feeling feverish':                     'feelingFeverish',
        'fever':                                'feelingFeverish',
        'headache':                             'headaches',
        'headaches':                            'headaches',
        'cough':                                'cough',
        'chills':                               'chillsOrShivering',
        'chills or shivering':                  'chillsOrShivering',
        'sweats':                               'sweats',
        'throat':                               'soreThroat',
        'sore throat or itchy/scratchy throat': 'soreThroat',
        'nausea':                               'nauseaOrVomiting',
        'nausea or vomiting':                   'nauseaOrVomiting',
        'nose':                                 'runnyOrStuffyNose',
        'runny or stuffy nose':                 'runnyOrStuffyNose',
        'runny / stuffy nose':                  'runnyOrStuffyNose',
        'tired':                                'fatigue',
        'feeling more tired than usual':        'fatigue',
        'ache':                                 'muscleOrBodyAches',
        'muscle or body aches':                 'muscleOrBodyAches',
        'diarrhea':                             'diarrhea',
        'ear':                                  'earPainOrDischarge',
        'ear pain or ear discharge':            'earPainOrDischarge',
        'rash':                                 'rash',
        'breathe':                              'increasedTroubleBreathing',
        'increased trouble with breathing':     'increasedTroubleBreathing',
        'sob':                                  'increasedTroubleBreathing',
        'eye':                                  'eyePain',
        'smell_taste':                          'lossOfSmellOrTaste',
        'other':                                'other',
        'none':                                 'none',
        'none of the above':                    'none',
        'unk':                                  'none',
        'no_answer':                            'none'
    }

    if symptom_name.lower() not in symptom_map:
        raise UnknownSymptomNameError(f"Unknown symptom name «{symptom_name}»")

    return symptom_map[symptom_name.lower()]

def map_chronic_illness(illness_name: str):
    """
    Maps a *chronic_illness* to current values in ID3C warehouse.
    """
    illness_map = {
        'asthma or reactive airway disease':                'asthma',
        'blood disorders (e.g. sickle cell)':               'blood',
        'copd/emphysema':                                   'copd',
        'copd/ emphysema':                                  'copd',
        'chronic bronchitis':                               'bronchitis',
        'cancer':                                           'cancer',
        'diabetes':                                         'diabetes',
        'heart disease (heart failure or heart attack)':    'cvd',
        'immunosuppression (by medication or disease)':     'immunosupression',
        'liver disease':                                    'liver',
        'none of the above':                                'none',
        'none of these conditions':                         'none',
        'do not know':                                      'dont_know',
        'prefer not to say':                                'dont_say'
    }

    if illness_name.lower() not in illness_map:
        raise UnknownIllnessNameError(f"Unknown illness name «{illness_name}»")

    return illness_map[illness_name.lower()]

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

class UnknownIllnessNameError(ValueError):
    """
    Raised by :function: `map_chronic_illness` if a provided
    *illness_name* is not among a set of expected values
    """
    pass
