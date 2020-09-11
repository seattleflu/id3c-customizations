"""
Functions shared by REDCap DET ETL
"""
from typing import Optional


def normalize_net_id(net_id: str=None) -> Optional[str]:
    """
    If netid or UW email provided, return netid@washington.edu

    >>> normalize_net_id('abcd')
    'abcd@washington.edu'

    >>> normalize_net_id('aBcD ')
    'abcd@washington.edu'

    >>> normalize_net_id('abcd@uw.edu')
    'abcd@washington.edu'

    >>> normalize_net_id('aBcD@u.washington.edu')
    'abcd@washington.edu'

    If missing netid or non-uw email provided, it cannot be normalized so return nothing
    >>> normalize_net_id('notuw@gmail.com')
    >>> normalize_net_id('nodomain@')
    >>> normalize_net_id('multiple@at@signs')
    >>> normalize_net_id()
    >>> normalize_net_id('')
    >>> normalize_net_id(' ')
    """

    if not net_id or net_id.isspace():
        return None

    net_id = net_id.strip().lower()

    # if a uw email was entered, drop the domain before normalizing
    if net_id.count('@') == 1:
        net_id, domain = net_id.split('@')
        if domain not in ['u.washington.edu', 'uw.edu']:
            return None
    elif net_id.count('@') > 1:
        return None

    username = f'{net_id}@washington.edu'
    return username
