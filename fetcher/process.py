import collections
import glob
import itertools
import json
import random
from functools import partial
from pathlib import Path
from typing import Dict, List

from tqdm.contrib.concurrent import process_map

from fetcher import GROUPS_PATH, USERS_PATH
from fetcher.methods import fetch_group, fetch_user
from fetcher.tokens import tokens


# deep merge of dicts
# https://gist.github.com/angstwad/bf22d1822c38a92ec0a9#gistcomment-3305932
def deep_merge(*args, add_keys=True):
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


def make_sample(consume: str, produce: str, size: int, source: List[int], per_entity: bool = False) -> List[int]:
    def sample_or_gtfo(lst):
        return lst if len(lst) <= size else random.sample(lst, size)

    ids = list()
    if consume == 'user':
        path = USERS_PATH
        key = 'friends' if produce == 'user' else 'groups'
    else:
        path = GROUPS_PATH
        if produce == 'group':
            raise AttributeError('Both consume and produce are groups')
        key = 'members'

    for uid in source:
        with (path / f'{uid}.json').open() as f:
            ids.append(json.load(f)[key])

    # here ids is a list of lists of ints, but we are gonna make it plain
    # first of all throw away all Nones
    ids = filter(lambda x: bool(x), ids)
    flatten = itertools.chain.from_iterable
    return list(flatten(map(sample_or_gtfo, ids))) if per_entity else sample_or_gtfo(list(flatten(ids)))


def run(todo: Dict, methods: Dict):
    # literally extend methods
    for key, method in methods.items():
        extends = method.get('extends')
        if extends:
            methods[key] = deep_merge(methods[extends], method)

    cache = {}

    for key, stage in todo.items():
        entity = stage['entity']

        # get ids
        ids = stage['ids']
        if isinstance(ids, int):
            ids = [ids]
        if isinstance(ids, dict):
            ref = ids['from']
            ids = make_sample(todo[ref]['entity'], entity, ids['count'], cache[ref], ids.get('per_entity', False))
        if not isinstance(ids, list):
            raise RuntimeError('Failed to deduce ids')
        cache[key] = ids

        # prepare tasks
        requests = stage['include']
        for name, request in requests.items():
            method = methods[name]
            param = method['param']
            if param != 'any' and param != entity:
                raise TypeError(f'Method {name} has no support for entity {entity}')
            requests[name] = deep_merge(method, {'request': request if isinstance(request, dict) else dict()})

        # build function fetch
        if entity == 'user':
            path = USERS_PATH
            fetch = fetch_user
        elif entity == 'group':
            path = GROUPS_PATH
            fetch = fetch_group
        else:
            raise AttributeError(f'Entity: "user" or "group" expected, got "{entity}"')
        fetch = partial(fetch, path=path, tokens=tokens, tasks=requests)

        # find out what ids are missing
        cached_ids = set(map(lambda p: int(Path(p).stem), glob.glob(str(path / '*.json'))))
        missing_ids = set(ids) - cached_ids

        # get missing entities
        missing_count = len(missing_ids)
        cached_count = len(ids) - missing_count
        if missing_count != 0:
            print(f'Starting "{key}"\n{cached_count} entities cached, {missing_count} to go')
            process_map(fetch, list(missing_ids))
        else:
            print(f'Skipping "{key}": already cached')
