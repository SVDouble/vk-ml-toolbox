from fetcher.utils import load


def check(uid, entity_type):
    data = load(uid, entity_type)
    if entity_type == 'user':
        return check_user(data)
    else:
        return check_group(data)


def check_user(obj):
    try:
        # has a photo
        assert obj['user']['has_photo']
        # has at least 10 friends
        assert len(obj['friends']) >= 10
        # at least two meaningful posts
        assert sum(1 for x in obj['posts'] if len(x['text'].strip()) > 10) >= 2
        # at least five groups
        assert len(obj['groups']) >= 5
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
        assert len(g['name'] + g['description'] + g['status']) >= 200
        # at least 50 members
        assert len(obj['members']) >= 50
        # has a photo
        assert obj['group']['has_photo']
    except (KeyError, AssertionError):
        return False
    return True
