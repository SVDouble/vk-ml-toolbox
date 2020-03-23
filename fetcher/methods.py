import functools
import logging

import vk_api
from requests.exceptions import RequestException

from fetcher import LOGGER
from fetcher.reshape import reshape_user, reshape_group


def request(factory):
    def decorator_wrapper(method, fallback=None):
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                logger = logging.getLogger(LOGGER)
                token = factory.get(method)
                if token is None:
                    logger.warning('No tokens found for method ', method)
                    return fallback
                factory.use(token, method)
                session = vk_api.VkApi(
                    token=token,
                    api_version='5.103')
                try:
                    return func(
                        functools.partial(session.method, method=method),
                        *args, **kwargs
                    )
                except (RequestException,
                        vk_api.VkApiError) as e:
                    # disable token for this method, if it doesn't work
                    if e is vk_api.ApiError:
                        if e.code == 29:
                            factory.report(token, method)
                    # logger.warning('Request: {}'.format(e))
                    return fallback

            return wrapper

        return decorator
    return decorator_wrapper


def fetch_user(uid, token, save=None):
    vk_api_request = request(token)

    @vk_api_request(method='users.get')
    def user(query, user_id):
        fields = 'sex,verified,bdate,city,country,home_town,education,last_seen,has_photo,photo_50,followers_count,' \
                 'activities,interests,music,movies,tv,books,games,about'
        values = {'user_ids': [user_id], 'fields': fields}
        return query(values=values)[0]

    @vk_api_request(method='users.getSubscriptions')
    def groups(query, user_id):
        values = {'user_id': [user_id]}
        return query(values=values)['groups']['items']

    @vk_api_request(method='friends.get')
    def friends(query, user_id):
        values = {'user_id': user_id, 'order': 'mobile'}
        return query(values=values)['items']

    @vk_api_request(method='wall.get', fallback=[])
    def posts(query, user_id):
        values = {
            'owner_id': user_id,
            'count': 10,
            'filter': 'owner',
            'extended': 1,
            'fields': 'text,comments,likes,reposts',
        }
        return query(values=values)['items']

    @vk_api_request(method='wall.get', fallback=[])
    def reposts(query, user_id):
        values = {
            'owner_id': user_id,
            'count': 10,
            'filter': 'others',
            'extended': 1,
            'fields': 'text,comments,likes,reposts',
        }
        return query(values=values)['items']

    u = user(uid)
    u.update({
        'groups': groups(uid),
        'friends': friends(uid),
        'posts': posts(uid),
        'reposts': reposts(uid),
    })
    data = reshape_user(u)
    if save is not None:
        save(uid, data)
    return data


def fetch_group(uid, token, save=None):
    vk_api_request = request(token)

    @vk_api_request('groups.getById')
    def group(query, group_id):
        fields = 'description,fixed_post,members_count,status,has_photo,photo_50,activity,age_limits,city,country'
        values = {'group_id': group_id, 'fields': fields}
        return query(values=values)[0]

    @vk_api_request('groups.getMembers')
    def members(query, group_id):
        values = {'group_id': group_id}
        return query(values=values)['items']

    @vk_api_request('wall.get')
    def owner_posts(query, group_id):
        values = {
            'owner_id': '-' + str(group_id),
            'count': 5,
            'filter': 'owner',
            'extended': 1,
            'fields': 'text,comments,likes,reposts',
        }
        return query(values=values)['items']

    @vk_api_request('wall.get')
    def member_posts(query, group_id):
        values = {
            'owner_id': '-' + str(group_id),
            'count': 10,
            'filter': 'others',
            'extended': 1,
            'fields': 'text,comments,likes,reposts',
        }
        return query(values=values)['items']

    group = group(uid)
    group.update({
        'members': members(uid),
        'owner_posts': owner_posts(uid),
        'member_posts': member_posts(uid)
    })
    data = reshape_group(group)
    if save is not None:
        save(uid, data)
    return data
