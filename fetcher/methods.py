import logging
import sys
from math import ceil
from string import Template
from typing import Dict

import vk_api
from requests.exceptions import RequestException

from fetcher.exceptions import NoTokenError
from fetcher.tokens import tokens
from fetcher.utils import deep_merge, save, flatten, sample


def members(query, method, values, step=1000):
    """Get all group members"""
    # remove 'count' from values, otherwise the request is incorrect
    count = values.pop('count')
    if count == -1:
        count = query(method='groups.getById', values={
            'group_id': values['group_id'],
            'fields': 'members_count'
        })[0]['members_count']
    total = ceil(count / step)
    return sample(flatten([
        query(method=method, values={**values, 'offset': offset * step})['items'] for offset in range(total)
    ]), count)


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
                response = delegate(session.method, method, request)
            else:
                response = session.method(method, values=request)
                # extract payload from complicated json structure
                for step in task['extract']:
                    response = response[step]
            data[key] = response

        except (RequestException, vk_api.VkApiError, NoTokenError):
            exc_type, exc_value, exc_traceback = sys.exc_info()
            if exc_type is vk_api.ApiError:
                code = exc_value.code
                # access denied [15] / profile is banned [18] / profile is private [30]
                if code in [15, 18, 30]:
                    pass
                # token is exhausted, disable it
                elif code == 29:
                    logging.warning(f'E: got VkApi #29, disabling corresponding token')
                    tokens.report(token, method)
                # print unknown code
                else:
                    logging.warning(f'E: got VkApi #{code} on method {method}')
            else:
                # TODO: handle other exceptions
                logging.warning(f'Unknown exception occurred: {exc_value}')

    save(uid, entity_type, data)
