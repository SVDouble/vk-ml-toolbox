import itertools


def get_group_description(group):
    return ' '.join(itertools.chain.from_iterable(group[field].split() for field in ['name', 'description', 'status']))


def check_user(obj):
    try:
        u = obj['user']
        # has a photo
        assert u['has_photo']
        # has at least 10 friends
        assert len(obj['friends']) >= 10
        # at least two meaningful posts
        assert sum(1 for text in {' '.join(post['text'].split()) for post in obj['posts']} if len(text) > 30) >= 2
        # at least five groups
        assert len(obj['groups']) >= 5
        # all fields are accessible
        fields = ['id', 'sex', 'verified', 'city', 'followers_count']
        assert all(f in u for f in fields)
    except (KeyError, AssertionError):
        return False
    return True


def check_group(obj):
    try:
        g = obj['group']
        # is not closed
        assert g['is_closed'] == 0
        # is not deactivated
        assert 'deactivated' not in g
        # sufficient description
        assert len(get_group_description(g)) >= 500
        # at least 50 members
        assert g['members_count'] >= 50
        # has a photo
        assert g['has_photo']
        # all fields are accessible
        fields = ['id', 'members_count', 'activity']
        assert all(f in g for f in fields)
    except (KeyError, AssertionError):
        return False
    return True


def check(obj, entity_type):
    return {'user': check_user, 'group': check_group}[entity_type](obj)


def transform_user(obj):
    u = obj['user']
    u['groups_count'] = len(obj['groups'])
    u['friends_count'] = len(obj['friends'])
    u['posts_count'] = len(obj['posts'])
    u['city'] = u['city']['title']
    return u


def transform_group(obj):
    g = obj['group']
    g['text'] = get_group_description(g)
    return g


def transform(obj, entity_type):
    return {'user': transform_user, 'group': transform_group}[entity_type](obj)
