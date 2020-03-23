def reshape_post(item):
    return {
        'text': item.get('text'),
        'num_comments': item.get('comments', {}).get('count'),
        'num_reposts': item.get('reposts', {}).get('count'),
        'num_likes': item.get('likes', {}).get('count'),
    }


def reshape_user(raw):
    return {
        'id': raw.get('id'),
        'info': {
            'sex': raw.get('sex'),
            'verified': raw.get('verified'),
            'bdate': raw.get('bdate'),
            'city': raw.get('city', {}).get('title'),
            'country': raw.get('country', {}).get('title'),
            'home_town': raw.get('home_town'),
        },
        'photo': {
            'has_photo': raw.get('has_photo'),
            'photo_50': raw.get('photo_50'),
        },
        'social': {
            'education': raw.get('education'),
            'last_seen': raw.get('last_seen'),
            'followers_count': raw.get('followers_count'),
            'activities': raw.get('activities'),
            'interests': raw.get('interests'),
            'music': raw.get('music'),
            'movies': raw.get('movies'),
            'tv': raw.get('tv'),
            'books': raw.get('books'),
            'games': raw.get('games'),
            'about': raw.get('about'),
        },
        'friends': raw.get('friends'),
        'posts': list(map(reshape_post, raw.get('posts'))),
        'reposts': list(map(reshape_post, raw.get('reposts'))),
        'groups': raw.get('groups'),

    }


def reshape_group(raw):
    return {
        'id': raw.get('id'),
        'info': {
            'name': raw.get('name'),
            'screen_name': raw.get('screen_name'),
            'is_closed': raw.get('is_closed'),
            'deactivated': raw.get('deactivated'),
            'type': raw.get('type'),
            'description': raw.get('description'),
            'fixed_post': raw.get('fixed_post'),
            'members_count': raw.get('members_count'),
            'status': raw.get('status'),
        },
        'photo': {
            'has_photo': raw.get('has_photo', False),
            'photo_50': raw.get('photo_50'),
        },
        'optional': {
            'activity': raw.get('activity'),
            'age_limits': raw.get('age_limits'),
            # 'city': raw.get('city'),
            # 'country': raw.get('country'),
        },
        'wall': {
            'owner': list(map(reshape_post, raw.get('owner_posts') or [])),
            'members': list(map(reshape_post, raw.get('member_posts') or [])),
        },
        'members': raw.get('members'),
    }
