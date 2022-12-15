from logging import Logger
from typing import List
import logging

kube_logger: Logger = logging.getLogger(__name__)


def clean_dictionary_nulls(d: dict) -> dict:
    """Removes all null values from the dictionary"""
    if d is None:
        return {}
    for k in list(d.keys()):
        if d[k] is None:
            del d[k]

    return d


def unqiue_with_order(lst) -> list:
    """Returns only the unique values while keeping order"""
    ulst = []
    uvals = set()
    for v in lst:
        if v in uvals:
            continue
        uvals.add(v)
        ulst.append(v)
    return ulst


def not_empty_string(val: str):
    """Returns true if the string is not empty (len>0) and not None"""
    return isinstance(val, str) and len(val) > 0


def join_locations_list(*args) -> List[str]:
    lcoations = []
    v = None
    for v in args:
        if v is None or (isinstance(v, str) and len(v.strip()) == ""):
            continue
        if isinstance(v, list):
            lcoations += v
        else:
            lcoations += v.split(",")
    return unqiue_with_order(lcoations)
