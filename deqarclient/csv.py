# -*- coding: utf-8 -*-

import csv
import re

from .errors import DataError

class ListWithHoles(dict):
    """
    create lists from non-continuous indexes as a dict and later compress into a list
    """

    def __setitem__(self, key, item):
        if isinstance(key, int):
            super().__setitem__(key, item)
        else:
            raise DataError("list index must be of type 'int'")

    def squeeze(self):
        return [ self[i] for i in sorted(self.keys()) ]


def flat_to_nested(flatdict):
    """
    converts a "flattened" dict into a nested structure

    (used for CSV column headers with . for sub-objects and [n] for arrays)
    """

    def _add_or_merge(key, next_keys, value, target):
        """
        add value to target dict/list, directly as scalar or recurse to sub-dict if needed
        """
        if key in target:
            if next_keys:
                if isinstance(target[key], dict):
                    _merge_nested(next_keys, value, target[key])
                else:
                    raise DataError(f"field {key} is used as dict but exists with another type already")
            else:
                if isinstance(target[key], dict):
                    raise DataError(f"field {key} is used as scalar but exists as dict already")
                else:
                    target[key] = value
        else:
            if next_keys:
                target[key] = dict()
                _merge_nested(next_keys, value, target[key])
            else:
                target[key] = value

    def _merge_nested(keys, value, target):
        """
        place value into target dict, adding to or creating sub-dicts and lists as needed
        """
        if isinstance(keys[0], str):
            match = re.match(r"\s*(.*)\[(\d+)\]\s*$", keys[0])
        else:
            match = False
        if match:
            key = match[1]
            index = int(match[2])
            if key in target:
                if isinstance(target[key], ListWithHoles):
                    _add_or_merge(index, keys[1:], value, target[key])
                else:
                    raise DataError(f"field {key} used as list but exists with another type already")
            else:
                target[key] = ListWithHoles()
                _add_or_merge(index, keys[1:], value, target[key])
        else:
            _add_or_merge(keys[0], keys[1:], value, target)

    def _squeeze_lists(origin):
        """
        recursively "squeeze" the ListWithHoles objects
        """
        for key in list(origin):
            if isinstance(origin[key], dict):
                _squeeze_lists(origin[key])
                if isinstance(origin[key], ListWithHoles):
                    origin[key] = origin[key].squeeze()

    # iterate over the flat dict and merge each element
    nested = dict()
    for key in flatdict:
        if isinstance(key, str):
            key_stack = key.strip(" \n\r\t\v\f").split(".")
        else:
            key_stack = [ key ]
        if isinstance(flatdict[key], str):
            flatdict[key] = flatdict[key].strip(" \n\r\t\v\f")
        if flatdict[key] not in [ None, '' ]:
            _merge_nested(key_stack, flatdict[key], nested)
    _squeeze_lists(nested)
    return(nested)


class NestedDictReader(csv.DictReader):
    """
    Adapted CSV DictReader that directly converts each row to a nested data structure
    """

    def __next__(self):
        return(flat_to_nested(super().__next__()))

