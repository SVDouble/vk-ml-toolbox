import glob
import json
import logging
from functools import partial
from pathlib import Path
from timeit import default_timer as timer
from typing import Dict, Set

from tqdm.contrib.concurrent import process_map

from fetcher import GROUPS_PATH, USERS_PATH
from fetcher.check import check
from fetcher.methods import fetch
from fetcher.utils import deep_merge, flatten, get_path, sample


def make_sample(consume: str, produce: str, size: int, source: Set[int], per_entity: bool = False) -> Set[int]:
    """Extracts data from previous stage and creates a sample based on ids from it"""
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
            ids.append(json.load(f).get(key))

    # here ids is a list of lists of ints, but we are gonna make it plain
    # first of all throw away all Nones
    ids = filter(lambda x: bool(x), ids)
    ids = flatten(map(partial(sample, size=size), ids)) if per_entity else sample(flatten(ids), size)
    return set(ids)


def run(todo: Dict, methods: Dict):
    """Runs all the tasks"""
    # literally extend methods
    for key, method in methods.items():
        extends = method.get('extends')
        if extends:
            methods[key] = deep_merge(methods[extends], method)

    cache = {}

    for key, stage in todo.items():
        start_time = timer()
        logging.info(f'stage({key}): starting')
        entity_type = stage['type']

        # get ids
        ids = stage['ids']
        if isinstance(ids, int):
            ids = {ids}
        elif isinstance(ids, list):
            ids = set(ids)
        elif isinstance(ids, dict):
            ref = ids['from']
            ids = make_sample(todo[ref]['type'], entity_type, ids['count'], cache[ref], ids.get('per_entity', False))
        if not isinstance(ids, set):
            raise RuntimeError('Failed to deduce ids')
        cache[key] = ids

        # prepare tasks
        requests = stage['include']
        for name, request in requests.items():
            method = methods[name]
            requests[name] = deep_merge(method, {'request': request if isinstance(request, dict) else dict()})

        # find out what ids are missing
        cached_ids = set(map(lambda p: int(Path(p).stem), glob.glob(str(get_path(entity_type) / '*.json'))))
        missing_ids = ids - cached_ids

        # get missing entities
        if missing_ids:
            logging.info(f'fetch({key}): {len(ids) - len(missing_ids)} entities cached, {len(missing_ids)} to go')
            process_map(partial(fetch, entity_type=entity_type, tasks=requests), list(missing_ids), chunksize=5)
        else:
            logging.info(f'fetch({key}): already cached')
        results = [uid for uid in ids if check(uid, entity_type)]
        logging.info(f'check({key}): {len(results)} out of {len(ids)} entities OK')
        logging.info(f'stage({key}): completed in {timer() - start_time:.2f} seconds\n')
    logging.info('All stages completed! Exiting')
