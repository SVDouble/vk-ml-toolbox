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
        assert sum(len(' '.join(g[field].split())) for field in ['name', 'description', 'status']) >= 500
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
