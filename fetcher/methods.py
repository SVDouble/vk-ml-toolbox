import logging
from math import ceil
from functools import partial
from string import Template
from typing import Dict

import vk_api
from requests.exceptions import RequestException

from fetcher.exceptions import NoTokenError
from fetcher.tokens import tokens
from fetcher.utils import deep_merge, save, flatten, sample


def members(query, values, step=1000):
    """Get all group members"""
    # remove 'count' from values, otherwise the request is incorrect
    count = values.pop('count')
    total = ceil(count / step)
    responses = [query(values={**values, 'offset': offset * step})['items'] for offset in range(total)]
    return sample(flatten(responses), count)


def fetch(uid, entity_type, tasks: Dict[str, Dict]):
    # methods that cannot be fully configured in yaml
    delegates = {'members': members}
    # dictionary with resolved data
    data = dict()

    for key, task in tasks.items():
        method = task['method']
        token = tokens.get(method)
        try:
            session = vk_api.VkApi(token=tokens.get(method), api_version='5.122')
            # fill all required fields of the request like uid and merge them with method defaults
            patch = {k: Template(v).substitute(uid=uid) for k, v in task['bind'][entity_type].items()}
            request = deep_merge(task['request'], patch)
            # check whether the method can be executed directly
            delegate = delegates.get(key)
            if delegate:
                response = delegate(partial(session.method, method=method), request)
            else:
                response = session.method(method, values=request)
                # extract payload from complicated json structure
                for step in task['extract']:
                    response = response[step]
            data[key] = response

        except (RequestException, vk_api.VkApiError, NoTokenError) as e:
            logging.exception(e)
            # disable tokens for this method, if it doesn't work
            if e is vk_api.ApiError:
                if e.code == 29:
                    tokens.report(token, method)
            # TODO: handle other exceptions
            return None

    save(uid, entity_type, data)
