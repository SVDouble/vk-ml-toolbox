import collections
import itertools
import json
import logging
import multiprocessing
import os
import random

from fetcher import USERS_PATH, GROUPS_PATH


class SingletonType(type):
    def __new__(mcs, name, bases, attrs):
        # Assume the target class is created (i.e. this method to be called) in the main thread.
        cls = super(SingletonType, mcs).__new__(mcs, name, bases, attrs)
        cls.__shared_instance_lock__ = multiprocessing.Lock()
        return cls

    def __call__(cls, *args, **kwargs):
        with cls.__shared_instance_lock__:
            try:
                return cls.__shared_instance__
            except AttributeError:
                cls.__shared_instance__ = super(SingletonType, cls).__call__(*args, **kwargs)
                return cls.__shared_instance__


def deep_merge(*args, add_keys=True):
    """
    Deep merge arbitrary number of dicts
    See: https://gist.github.com/angstwad/bf22d1822c38a92ec0a9#gistcomment-3305932
    """
    assert len(args) >= 2, "deep_merge requires at least two dicts to merge"
    rtn_dct = args[0].copy()
    merge_dicts = args[1:]
    for merge_dct in merge_dicts:
        if add_keys is False:
            merge_dct = {key: merge_dct[key] for key in set(rtn_dct).intersection(set(merge_dct))}
        for k, v in merge_dct.items():
            if not rtn_dct.get(k):
                rtn_dct[k] = v
            elif k in rtn_dct and not isinstance(v, type(rtn_dct[k])):
                raise TypeError(f"Overlapping keys exist with different types: "
                                f"original is {type(rtn_dct[k])}, new value is {type(v)}")
            elif isinstance(rtn_dct[k], dict) and isinstance(merge_dct[k], collections.abc.Mapping):
                rtn_dct[k] = deep_merge(rtn_dct[k], merge_dct[k], add_keys=add_keys)
            elif isinstance(v, list):
                for list_value in v:
                    if list_value not in rtn_dct[k]:
                        rtn_dct[k].append(list_value)
            else:
                rtn_dct[k] = v
    return rtn_dct


def get_path(entity_type):
    if entity_type == 'user':
        return USERS_PATH
    elif entity_type == 'group':
        return GROUPS_PATH
    else:
        raise AttributeError(f'Entity "user" or "group" expected, got "{entity_type}"')


def save(uid: int, entity_type: str, data):
    with (get_path(entity_type) / f'{uid}.json').open('w') as f:
        json.dump(data, f)


def load(uid: int, entity_type: str):
    file = get_path(entity_type) / f'{uid}.json'
    try:
        with file.open('r') as f:
            return json.load(f)
    except json.decoder.JSONDecodeError:
        logging.critical(f'Data of {entity_type} {uid} is damaged, removing file')
        os.remove(file)


def sample(lst, size):
    return lst if len(lst) <= size else random.sample(lst, size)


def flatten(iterable):
    return list(itertools.chain.from_iterable(iterable))
