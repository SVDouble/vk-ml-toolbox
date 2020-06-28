import functools
import itertools
import json
import logging
import math
from pathlib import Path
from typing import Dict, Callable

import vk_api
from requests.exceptions import RequestException


def save(path: Path, uid: int, data):
    with (path / '{}.json'.format(uid)).open('w') as f:
        json.dump(data, f)


def request(tokens, agenda: Dict[str, Dict]):
    # bins all decorated functions with their aliases
    bind = dict()

    def decorator_wrapper(method, alias, fallback=None):
        def decorator(func: Callable):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                token = tokens.get(method)
                if token is None:
                    logging.warning('No tokens found for method ', method)
                    return fallback
                tokens.use(token, method)
                session = vk_api.VkApi(token=token, api_version='5.122')
                try:
                    return func(
                        functools.partial(session.method, method=method),
                        agenda.get(alias), *args, **kwargs
                    )
                except (RequestException, vk_api.VkApiError) as e:
                    # disable tokens for this method, if it doesn't work
                    if e is vk_api.ApiError:
                        if e.code == 29:
                            tokens.report(token, method)
                    return fallback

            bind[alias] = wrapper
            return wrapper

        return decorator

    return decorator_wrapper, bind


def fetch(func: Callable):
    @functools.wraps(func)
    def wrapper(uid, tokens, path, **kwargs):
        # create decorator with specified token factory
        vk_api_request, bind = request(tokens, kwargs)
        # bind all methods
        func(uid, vk_api_request)
        # call methods and fetch data
        raw_data = {key: bind[key]() for key in kwargs.keys() & bind.keys()}
        save(path, uid, raw_data)

    return wrapper


@fetch
def fetch_user(uid, vk_api_request):
    @vk_api_request(method='users.get', alias='user')
    def user(query, values):
        return query(values={**values, 'user_ids': [uid]})[0]

    @vk_api_request(method='users.getSubscriptions', alias='groups')
    def groups(query, values):
        return query(values={**values, 'user_id': uid})['groups']['items']

    @vk_api_request(method='friends.get', alias='friends')
    def friends(query, values):
        return query(values={**values, 'user_id': uid})['items']

    @vk_api_request(method='wall.get', alias='owner_posts')
    def posts(query, values):
        return query(values={**values, 'owner_id': uid})['items']

    @vk_api_request(method='wall.get', alias='other_posts')
    def reposts(query, values):
        return query(values={**values, 'owner_id': uid})['items']


@fetch
def fetch_group(uid, vk_api_request):
    @vk_api_request(method='groups.getById', alias='group')
    def group(query, values):
        return query(values={**values, 'group_id': uid})[0]

    @vk_api_request(method='groups.getMembers', alias='members')
    def members(query, values):
        count = values.pop('count')
        return list(itertools.chain.from_iterable(
            [query(values={**values, 'group_id': uid, 'offset': offset * 1000})['items']
             for offset in range(math.floor(count / 1000))]))

    @vk_api_request(method='wall.get', alias='owner_posts')
    def group_posts(query, values):
        return query(values={**values, 'owner_id': '-' + str(uid)})['items']

    @vk_api_request(method='wall.get', alias='other_posts')
    def member_posts(query, values):
        return query(values={**values, 'owner_id': '-' + str(uid)})['items']
