import collections
import glob
import gzip
import itertools
import json
import logging
import multiprocessing
import os
import random
from pathlib import Path
from typing import List

from tqdm import tqdm

from fetcher import USERS_PATH, GROUPS_PATH, MERGED_PATH
from fetcher.check import check_user, check_group
from fetcher.exceptions import FileDamagedError


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


def get_path(entity_type: str):
    return {'user': USERS_PATH, 'group': GROUPS_PATH, 'bundle': MERGED_PATH}[entity_type]


def get_compressed(entity_type: str):
    return {'user': False, 'group': False, 'bundle': True}[entity_type]


def get_ext(compressed: bool):
    return 'bz' if compressed else 'json'


def get_file(name, entity_type: str, compressed: bool):
    return get_path(entity_type) / f'{name}.{get_ext(compressed)}'


def discover(entity_type: str, compressed=None):
    compressed = compressed or get_compressed(entity_type)
    return set(map(lambda p: int(Path(p).stem), glob.glob(str(get_path(entity_type) / f'*.{get_ext(compressed)}'))))


def save(name, entity_type: str, data, compressed=None):
    compressed = compressed or get_compressed(entity_type)
    file = get_file(name, entity_type, compressed)
    if compressed:
        with gzip.open(file, 'wt', encoding='utf-8', compresslevel=9) as f:
            json.dump(data, f)
    else:
        with file.open('w') as f:
            json.dump(data, f)


def load(name, entity_type: str, compressed=None):
    compressed = compressed or get_compressed(entity_type)
    file = get_file(name, entity_type, compressed)
    try:
        if compressed:
            with gzip.open(file, 'rt', encoding='utf-8') as f:
                return json.load(f)
        else:
            with file.open('r') as f:
                return json.load(f)
    except json.decoder.JSONDecodeError as e:
        os.remove(file)
        raise FileDamagedError(f'Data of {entity_type} {name} is damaged') from e


def check(uid, entity_type):
    try:
        data = load(uid, entity_type)
    except FileDamagedError:
        return False
    if entity_type == 'user':
        return check_user(data)
    else:
        return check_group(data)


def filter_suitable(ids, entity_type, show_progress=False):
    return {uid for uid in (tqdm(ids) if show_progress else ids) if check(uid, entity_type)}


def merge(entity_type, compress=None):
    # check entities and load them
    data = [load(uid, entity_type) for uid in filter_suitable(discover(entity_type), entity_type)]
    save(f'{entity_type}s', 'bundle', data, compress)
    logging.info(f'merger: dumped {len(data)} {entity_type}s')


def sample(lst, size):
    return lst if len(lst) <= size or size == -1 else random.sample(lst, size)


def flatten(iterable) -> List:
    return list(itertools.chain.from_iterable(iterable))
