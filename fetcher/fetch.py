from functools import partial
import glob
import itertools
import json
import logging
import os
import random

from fetcher import GROUPS_PATH, USERS_PATH, LOGGER
from fetcher.methods import fetch_group, fetch_user
from fetcher.parallel import parallel_map


def handle_items(ids, upath, fetch, message=''):
    """fetches all items with given ids"""

    def save(path, uid, data):
        with open(path + str(uid) + '.json', 'w') as f:
            json.dump(data, f)

    def load(path, uid):
        with open(path + str(uid) + '.json') as f:
            return json.load(f)

    logger = logging.getLogger(LOGGER)

    fetch = partial(fetch, save=partial(save, upath))

    ids = set(ids)
    cached_ids = set(map(lambda p: int(os.path.splitext(os.path.basename(p))[0]),
                         glob.glob(upath + '*.json')))
    cached_data = [load(upath, uid) for uid in cached_ids]

    uncached_ids = list(ids - cached_ids)
    # uncached_data = [fetch(uid) for uid in uncached_ids]
    logger.warning(message.format(len(uncached_ids)))
    uncached_data = parallel_map(fetch, uncached_ids)
    for (uid, content) in zip(uncached_ids, uncached_data):
        save(upath, uid, content)
    return [item for item in cached_data + uncached_data if item is not None]


def handle_groups(ids):
    return handle_items(ids, GROUPS_PATH, fetch_group, 'Fetch {} groups')


def handle_users(ids):
    return handle_items(ids, USERS_PATH, fetch_user, 'Fetch {} users')


def process(group_id, count):
    # load one group
    group = handle_groups([group_id])[0]

    # get its members
    members_ids = random.sample(group['members'], count)

    # load their data
    members = handle_users(members_ids)

    # get all groups
    all_groups_ids_concatenated = list(map(lambda u: u['groups'] or [], members))
    all_groups_ids = set(itertools.chain.from_iterable(all_groups_ids_concatenated))

    # upload them
    handle_groups(all_groups_ids)
