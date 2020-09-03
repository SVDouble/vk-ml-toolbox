import logging
from functools import partial
from timeit import default_timer as timer
from typing import Dict, Set

import yaml
from tqdm.contrib.concurrent import process_map

from fetcher import PREFIX
from fetcher.methods import fetch
from fetcher.tokens import get_token_manager
from fetcher.utils import deep_merge, flatten, sample, load, discover, merge, filter_suitable


def make_sample(consume: str, produce: str, size: int, source: Set[int], per_entity: bool = False) -> Set[int]:
    """Extracts data from previous stage and creates a sample based on ids from it"""
    if consume == 'user':
        key = 'friends' if produce == 'user' else 'groups'
    else:
        if produce == 'group':
            raise AttributeError('Both consume and produce are groups')
        key = 'members'

    ids = filter(lambda x: bool(x), [load(uid, consume).get(key) for uid in source])
    return set(flatten(map(partial(sample, size=size), ids)) if per_entity else sample(flatten(ids), size))


def init_and_run():
    # load settings and run script
    with open(PREFIX / 'todo.yml', 'r') as todo_yml:
        with open(PREFIX / 'fetcher' / 'methods.yml', 'r') as methods_yml:
            try:
                todo = yaml.safe_load(todo_yml)
                if not todo:
                    raise RuntimeError('Nothing to fetch!')
                methods = yaml.safe_load(methods_yml)
                if not methods:
                    raise RuntimeError('No methods specified!')

                logging.info(f'init: upcoming stages - {list(todo.keys())}')
                logging.info(f'init: methods allowed - {list(methods.keys())}')

                # fetch all entities
                run(todo, methods)

                # dump and compress data
                what = ['user', 'group']
                for entity_type in what:
                    logging.info(f'merger: processing {entity_type}s')
                    merge(entity_type)
                logging.info('merger: all done, exiting')
            except yaml.YAMLError as exc:
                logging.exception('init: failed to load settings')


def run(todo: Dict, methods: Dict):
    """Runs all the tasks"""
    # literally extend methods
    for key, method in methods.items():
        extends = method.get('extends')
        if extends:
            methods[key] = deep_merge(methods[extends], method)

    cache = {}
    token_manager = get_token_manager()

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
        cached_ids = discover(entity_type)
        missing_ids = ids - cached_ids

        # get missing entities
        if missing_ids:
            logging.info(f'fetch({key}): {len(ids) - len(missing_ids)} entities cached, {len(missing_ids)} to go')
            func = partial(fetch, entity_type=entity_type, tasks=requests, token_manager=token_manager)
            process_map(func, list(missing_ids))
        else:
            logging.info(f'fetch({key}): already cached')
        results = filter_suitable(ids, entity_type)
        logging.info(f'check({key}): {len(results)} out of {len(ids)} entities OK')
        logging.info(f'stage({key}): completed in {timer() - start_time:.2f} seconds')
    logging.info('fetcher: all stages completed! Exiting')
