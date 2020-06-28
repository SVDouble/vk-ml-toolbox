import glob
import logging
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


# def extract(path):
#     def load(eid: int):
#         with (path / '{}.json'.format(eid)).open() as f:
#             return json.load(f)


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
        raise RuntimeError("Keys {} aren't supported yet!".format(unsupported))

    def fix(data):
        return data if type(data) is dict else dict()

    merged = {name: {**fix(methods[name]), **fix(content)} for name, content in tasks.items()}

    # convert task.fields from array to string
    # otherwise fields won't be processed by vk api
    def convert(task: Dict) -> Dict:
        fields = task.get('fields')
        if fields:
            task['fields'] = ','.join(fields)
        return task

    return {k: convert(v) for k, v in merged.items()}


def run(todo: Dict = None, defaults: Dict = None):
    if not todo:
        raise RuntimeError('Nothing to fetch!')
    if not defaults:
        raise RuntimeError('No defaults specified!')
    bind = {
        'users': {'path': USERS_PATH, 'fetch': fetch_user},
        'groups': {'path': GROUPS_PATH, 'fetch': fetch_group}
    }
    for key in todo:
        # get path and fetch
        params = bind.get(key)
        if not params:
            raise RuntimeError('Unknown key in todo: %s', key)

        # check settings
        tasks = todo[key]
        agenda = prepare(key, tasks, defaults)
        meta = agenda.pop('meta', None)
        if not meta:
            raise RuntimeError('No metadata present!')
        ids = meta.get('ids')
        if not ids:
            raise RuntimeError('No ids specified!')

        # fetch data
        get(key, agenda, ids, **params)

        also = agenda.pop('also', None)
        if also:
            logging.info('Multistage run detected!')
