import logging
import re
from functools import partial
from typing import List

import numpy as np
import pandas as pd
from tqdm import tqdm

from fetcher.transform import transform as t
from fetcher.utils import discover, flatten, load


def load_and_transform(entity_type, transform: lambda arg: arg) -> List:
    bundle_type = f'bundle-{entity_type}'
    return flatten(map(transform, load(chunk, bundle_type)) for chunk in discover(bundle_type))


def to_df(entity_type):
    entities = load_and_transform(entity_type, partial(t, entity_type=entity_type))
    if len(entities):
        df = pd.json_normalize(entities)
        df.drop_duplicates(subset='id', keep='last', inplace=True)
        df.set_index(keys='id', drop=True, inplace=True)
        logging.info(f'ml: created dataframe with {len(df)} {entity_type}s')
        return df
    else:
        logging.warning(f'ml: no suitable {entity_type}s found')


def get_text_filtering_func(lang='ru'):
    expr = {'ru': r'[А-я]+', 'eng': r'[A-z]+'}[lang]
    return lambda x: ' '.join(re.findall(expr, x.lower()))


def extract_group_data(model):
    # social df
    df = to_df('group')
    if df is None:
        return
    yield 'group_social.pd', df

    if model:
        # posts
        filter_text = get_text_filtering_func()
        embeddings = {}
        for uid, text in tqdm(zip(df.index, df.text), total=len(df)):
            partial_embeddings = [model.get_sentence_vector(filter_text(part)) for part in text.split('\n')]
            assert len(partial_embeddings) >= 3
            embeddings[uid] = np.mean(np.stack(partial_embeddings), axis=0)
        yield 'group2post_embedding.dict', embeddings
        del df, embeddings
    else:
        logging.warning('ml: skipping group2post embedding')


def extract_user_data(model):
    def extract_field(f):
        return dict(load_and_transform('user', lambda obj: (obj['user']['id'], obj[f])))

    # social df
    df = to_df('user')
    if df is None:
        return
    yield 'user_social.pd', df
    del df

    # dicts of groups and friends
    fields = ['groups', 'friends']
    for field in fields:
        entities = extract_field(field)
        if entities:
            yield f'user2{field}.dict', entities
        del entities

    # posts
    if model:
        filter_text = get_text_filtering_func()
        embeddings = {}
        for uid, posts in tqdm(extract_field('posts').items()):
            user_posts = set([p['text'] for p in posts])
            partial_embeddings = []
            for post in user_posts:
                if len(' '.join(post.split())) > 30:
                    partial_embeddings.append(model.get_sentence_vector(filter_text(' '.join(post.split()))))
            assert len(partial_embeddings) >= 2
            embeddings[uid] = np.mean(np.stack(partial_embeddings), axis=0)
        yield 'user2text_embedding.dict', embeddings
    else:
        logging.warning('ml: skipping user2text embedding')


def extract_data(entity_type, model=None):
    return {'user': extract_user_data, 'group': extract_group_data}[entity_type](model)
