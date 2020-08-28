import logging
import math
from string import Template
from typing import Dict

import vk_api
from requests.exceptions import RequestException

from fetcher.exceptions import NoTokenError
from fetcher.tokens import tokens
from fetcher.utils import deep_merge, save, flatten


def members(query, values):
    """Get all group members"""
    step = 1000
    total = math.floor(values.pop('count') / step)
    return flatten([query(values={**values, 'offset': offset * step})['items'] for offset in range(total)])


def fetch(uid, path, tasks: Dict[str, Dict]):
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
            request = deep_merge(task['request'], {k: Template(v).substitute(uid=uid) for k, v in task['bind'].items()})
            # check whether the method can be executed directly
            delegate = delegates.get(key)
            if delegate:
                response = delegate(session.method, request)
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
            return None

    save(path, uid, data)
