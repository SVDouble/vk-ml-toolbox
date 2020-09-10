import collections
import glob
import gzip
import itertools
import json
import logging
import math
import multiprocessing
import os
import pickle
import random
from enum import Enum
from functools import reduce, partial
from pathlib import Path
from typing import List

from tqdm.contrib.concurrent import process_map

from fetcher import USERS_PATH, GROUPS_PATH, ML_PATH, BUNDLED_USERS_PATH, BUNDLED_GROUPS_PATH
from fetcher.exceptions import FileDamagedError
from fetcher.transform import check


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


class Modes(Enum):
    JSON, ARCHIVE, PICKLE = range(3)


def get_path(entity_type: str):
    return {
        'user': USERS_PATH,
        'group': GROUPS_PATH,
        'bundle-user': BUNDLED_USERS_PATH,
        'bundle-group': BUNDLED_GROUPS_PATH,
        'pickle-user': ML_PATH,
        'pickle-group': ML_PATH
    }[entity_type]


def get_mode(entity_type: str):
    return {
        'user': Modes.JSON, 'group': Modes.JSON,
        'bundle-user': Modes.ARCHIVE, 'bundle-group': Modes.ARCHIVE,
        'pickle-user': Modes.PICKLE, 'pickle-group': Modes.PICKLE
    }[entity_type]


def get_ext(mode):
    return {Modes.JSON: 'json', Modes.ARCHIVE: 'bz', Modes.PICKLE: 'pickle'}[mode]


def get_file(name, entity_type: str, mode):
    return get_path(entity_type) / f'{name}.{get_ext(mode)}'


def discover(entity_type: str):
    path = str(get_path(entity_type) / f'*.{get_ext(get_mode(entity_type))}')
    return set(map(lambda p: int(Path(p).stem), glob.glob(path)))


def save(name, entity_type: str, obj):
    mode = get_mode(entity_type)
    file = get_file(name, entity_type, mode)
    if mode is Modes.JSON:
        with file.open('w') as f:
            json.dump(obj, f)
    elif mode is Modes.ARCHIVE:
        with gzip.open(file, 'wt', encoding='utf-8', compresslevel=9) as f:
            json.dump(obj, f)
    elif mode is Modes.PICKLE:
        with file.open('wb') as f:
            pickle.dump(obj, f)
    else:
        raise RuntimeError(f'Got unknown mode {mode} ')


def load(name, entity_type: str, raise_exception=True):
    mode = get_mode(entity_type)
    file = get_file(name, entity_type, mode)
    try:
        if mode is Modes.JSON:
            with file.open('r') as f:
                return json.load(f)
        elif mode is Modes.ARCHIVE:
            with gzip.open(file, 'rt', encoding='utf-8') as f:
                return json.load(f)
        elif mode is Modes.PICKLE:
            with file.open('rb') as f:
                return pickle.load(f)
        else:
            raise RuntimeError(f'Got unknown mode {mode} ')
    except json.decoder.JSONDecodeError as e:
        os.remove(file)
        if raise_exception:
            raise FileDamagedError(f'Data of {entity_type} {name} is damaged') from e


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def check_chunk(chunk, entity_type):
    return {uid for uid in chunk if check(load(uid, entity_type, raise_exception=False), entity_type)}


def filter_suitable(ids, entity_type, chunk_size=1000, track=False):
    total = math.ceil(len(ids) / chunk_size)
    id_sets = process_map(partial(check_chunk, entity_type=entity_type),
                          chunks(list(ids), chunk_size), chunksize=1, total=total, disable=not track)
    return reduce(lambda a, b: a | b, id_sets)


def process_chunk(chunk, entity_type, k):
    data = [load(uid, entity_type) for uid in chunk]
    save(k, f'bundle-{entity_type}', data)


def chunkify(entity_type, chunk_size=1000):
    ids = filter_suitable(discover(entity_type), entity_type)
    if len(ids):
        total = math.ceil(len(ids) / chunk_size)
        process_map(process_chunk, chunks(list(ids), chunk_size),
                    itertools.repeat(entity_type), itertools.count(), chunksize=1, total=total)
        logging.info(f'merger: dumped {len(ids)} {entity_type}s, {total} chunks total')
    else:
        logging.warning(f'merger: no suitable {entity_type}s found')


def sample(lst, size):
    return lst if len(lst) <= size or size == -1 else random.sample(lst, size)


def flatten(iterable) -> List:
    return list(itertools.chain.from_iterable(iterable))
