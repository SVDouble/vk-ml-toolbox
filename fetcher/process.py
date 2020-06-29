import collections
import glob
import itertools
import json
import logging
import random
from functools import partial
from pathlib import Path
from typing import Callable, Dict, List

from tqdm.contrib.concurrent import process_map

from fetcher import GROUPS_PATH, USERS_PATH
from fetcher.methods import fetch_group, fetch_user
from fetcher.tokens import tokens


def get(key: str, agenda: Dict,
        ids: List[int], path: Path = None,
        fetch: Callable = lambda: None) -> None:
    cached_ids = set(map(lambda p: int(Path(p).stem), glob.glob(str(path / '*.json'))))
    missing_ids = set(ids) - cached_ids

    # get missing entities
    fetch = partial(fetch, path=path, tokens=tokens, **agenda)
    message = '{} {} cached, {} to go'.format(len(ids) - len(missing_ids), key, len(missing_ids))
    logging.info(message)
    print(message)
    process_map(fetch, list(missing_ids))


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


def make_sample(key: str, size, source: List[int]) -> List[int]:
    ids = list()
    bind = {
        'users': {'path': USERS_PATH, 'extract': lambda x: x['groups']},
        'groups': {'path': GROUPS_PATH, 'extract': lambda x: x['members']}
    }
    tool = bind[key]
    for uid in source:
        with (tool['path'] / '{}.json'.format(uid)).open() as f:
            ids.append(tool['extract'](json.load(f)))

    # here ids is a list of lists of ints, but this function is gonna make it plain
    # first of all throw away all Nones
    ids = filter(lambda x: bool(x), ids)
    ids = list(itertools.chain.from_iterable(ids))

    return ids if len(ids) <= size else random.sample(ids, size)


# merge params with defaults
def prepare(key: str, tasks: Dict, defaults) -> Dict:
    generic = defaults.get('generic', dict())
    specific = defaults.get(key, dict())
    methods = {**generic, **specific}
    if not methods:
        raise RuntimeError('No methods specified for key "{}"'.format(key))

    # check that all methods are correct
    unsupported = tasks.keys() - methods.keys()
    if len(unsupported) > 0:
        raise RuntimeError("Keys {} aren't supported!".format(unsupported))

    # fix empty methods (instead of being empty dicts they are actually Nones)
    def fix(data):
        return data if type(data) is dict else dict()

    # primarily deep_merge is used to correctly merge 'sample'
    # otherwise {**dict1, **dict2} works just fine
    merged = {name: deep_merge(fix(methods[name]), fix(content)) for name, content in tasks.items()}

    # convert task.fields from array to string
    # otherwise fields won't be processed by vk api
    def convert(task: Dict) -> Dict:
        fields = task.get('fields')
        if fields:
            task['fields'] = ','.join(fields)
        return task

    return {k: convert(v) for k, v in merged.items()}


def run(todo: Dict = None, defaults: Dict = None):
    logging.info('Started new stage!')
    if not todo:
        raise RuntimeError('Nothing to fetch!')
    if not defaults:
        raise RuntimeError('No defaults specified!')
    logging.debug('Running TODO: {}'.format(todo))
    bind = {
        'users': {'path': USERS_PATH, 'fetch': fetch_user},
        'groups': {'path': GROUPS_PATH, 'fetch': fetch_group}
    }
    for key in todo:
        # get path and fetch
        params = bind.get(key)
        if not params:
            raise RuntimeError('Unknown key in todo: %s', key)

        # check settings, set defaults
        tasks = todo[key]
        agenda = prepare(key, tasks, defaults)
        meta = agenda.pop('meta')
        sample = agenda.pop('sample', None)
        ids = meta.pop('ids')

        # fetch data
        get(key, agenda, ids, **params)

        # run next stage
        if sample:
            sample_meta = sample.pop('meta')
            # each job runs separately
            for sample_key in sample:
                task = sample[sample_key]
                task_meta = sample_meta[sample_key]

                def run_task(entities):
                    patch = {'meta': {'ids': make_sample(key, task_meta['size'], entities)}}
                    run({sample_key: deep_merge(task, patch)}, defaults)

                if task_meta['per-entity']:
                    # run new stage per entity
                    for uid in ids:
                        logging.info('Preparing next stage')
                        run_task([uid])
                else:
                    logging.info('Preparing next stage')
                    run_task(ids)
