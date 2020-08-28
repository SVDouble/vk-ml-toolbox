import glob
import json
import logging
from functools import partial
from pathlib import Path
from timeit import default_timer as timer
from typing import Dict, List

from tqdm.contrib.concurrent import process_map

from fetcher import GROUPS_PATH, USERS_PATH
from fetcher.check import check
from fetcher.methods import fetch
from fetcher.utils import deep_merge, flatten, get_path, sample


def make_sample(consume: str, produce: str, size: int, source: List[int], per_entity: bool = False) -> List[int]:
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
            ids.append(json.load(f)[key])

    # here ids is a list of lists of ints, but we are gonna make it plain
    # first of all throw away all Nones
    ids = filter(lambda x: bool(x), ids)
    return flatten(map(partial(sample, size=size), ids)) if per_entity else sample(flatten(ids), size)


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
        entity_type = stage['type']

        # get ids
        ids = stage['ids']
        if isinstance(ids, int):
            ids = [ids]
        if isinstance(ids, dict):
            ref = ids['from']
            ids = make_sample(todo[ref]['type'], entity_type, ids['count'], cache[ref], ids.get('per_entity', False))
        if not isinstance(ids, list):
            raise RuntimeError('Failed to deduce ids')
        cache[key] = ids

        # prepare tasks
        requests = stage['include']
        for name, request in requests.items():
            method = methods[name]
            requests[name] = deep_merge(method, {'request': request if isinstance(request, dict) else dict()})

        # find out what ids are missing
        cached_ids = set(map(lambda p: int(Path(p).stem), glob.glob(str(get_path(entity_type) / '*.json'))))
        missing_ids = set(ids) - cached_ids

        # get missing entities
        missing_count = len(missing_ids)
        cached_count = len(ids) - missing_count
        if missing_count != 0:
            logging.info(f'Starting {key}: {cached_count} entities cached, {missing_count} to go')
            process_map(partial(fetch, entity_type=entity_type, tasks=requests), list(missing_ids))
            logging.info(f'{key} completed in {timer() - start_time:.2f} seconds')
        else:
            logging.info(f'Skipping {key} (cached)')
        results = [uid for uid in ids if check(uid, entity_type)]
        logging.info(f'{len(results)} out of {len(ids)} entities OK')
    logging.info('All stages completed! Exiting')
