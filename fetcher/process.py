import logging
import os
import sys
import time
from functools import partial
from pathlib import Path
from timeit import default_timer as timer
from typing import Dict, Set

import fasttext as fasttext
import yaml
from tqdm.contrib.concurrent import process_map

from fetcher import PREFIX
from fetcher.exceptions import DamagedEntitiesFoundError
from fetcher.methods import fetch
from fetcher.ml import extract_data
from fetcher.tokens import get_token_manager
from fetcher.utils import deep_merge, flatten, sample, load, discover, chunkify, filter_suitable, save


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
                run_fetcher(todo, methods)

                types = set(map(lambda stage: stage['type'], todo.values()))
                # dump processed data
                run_merger(types)
                # partially create dataset
                run_ml(types)

            except yaml.YAMLError:
                logging.exception('init: failed to load settings')


def run_fetcher(todo: Dict, methods: Dict):
    """Runs all the tasks"""
    # literally extend methods
    for key, method in methods.items():
        extends = method.get('extends')
        if extends:
            methods[key] = deep_merge(methods[extends], method)

    ids_store = {}
    verified_ids_store = {}
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
            ids = make_sample(
                todo[ref]['type'],
                entity_type, ids['count'],
                verified_ids_store[ref] if ids.get('only_verified') else ids_store[ref],
                ids.get('per_entity', False))
        if not isinstance(ids, set):
            raise RuntimeError('Failed to deduce ids')
        ids_store[key] = ids

        # prepare tasks
        requests = stage['include']
        for name, request in requests.items():
            method = methods[name]
            requests[name] = deep_merge(method, {'request': request if isinstance(request, dict) else dict()})

        # find out what ids are missing
        while True:
            try:
                cached_ids = discover(entity_type)
                missing_ids = ids - cached_ids

                # get missing entities
                if missing_ids:
                    logging.info(
                        f'fetch({key}): {len(ids) - len(missing_ids)} entities cached, {len(missing_ids)} to go')
                    func = partial(fetch, entity_type=entity_type, tasks=requests, token_manager=token_manager)
                    process_map(func, list(missing_ids), max_workers=32, chunksize=1)
                else:
                    logging.info(f'fetch({key}): already cached')
                logging.info(f'check({key}): starting')
                time.sleep(0.005)  # prevent progress bar being shown before logging kicks in
                results = filter_suitable(ids, entity_type, show_progress=True)
                verified_ids_store[key] = results
                logging.info(f'check({key}): {len(results)} out of {len(ids)} entities OK')
                if ids - discover(entity_type):
                    raise DamagedEntitiesFoundError(f'check({key}): some entities removed during checking')
                logging.info(f'stage({key}): completed in {timer() - start_time:.2f} seconds')
                break
            except (TypeError, DamagedEntitiesFoundError):
                exc_type, exc_value, exc_traceback = sys.exc_info()
                if exc_type is DamagedEntitiesFoundError:
                    logging.warning(f'stage({key}): checker has found some damages entities')
                else:
                    logging.warning(f'stage({key}): a concurrent error occurred')
                logging.info(f'stage({key}): restarting')
                continue
    logging.info('fetcher: all stages completed! Exiting')


def run_merger(types):
    for entity_type in types:
        logging.info(f'merger: processing {entity_type}s')
        chunkify(entity_type)
    logging.info('merger: all done, exiting')


def run_ml(types):
    # try to load model
    model = None
    path = os.getenv('MODEL_PATH')
    logging.info(f'ml: trying to load model on path "{path}"')
    if path and Path(path).exists():
        model = fasttext.load_model(path)
        logging.info(f'ml: model loaded successfully')
    else:
        logging.warning('ml: error occurred when loading model')

    for entity_type in types:
        logging.info(f'ml: processing {entity_type}s')
        for name, obj in extract_data(entity_type, model=model):
            save(name, f'pickle-{entity_type}', obj)
    logging.info('ml: all done, exiting')
