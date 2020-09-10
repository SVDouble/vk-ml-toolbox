import logging
from functools import partial

import pandas

from fetcher.transform import transform
from fetcher.utils import discover, flatten, load


def to_df(entity_type):
    bundle_type = f'bundle-{entity_type}'
    entities = flatten(map(partial(transform, entity_type=entity_type), load(chunk, bundle_type))
                       for chunk in discover(bundle_type))
    if len(entities):
        df = pandas.DataFrame(entities)
        df.drop_duplicates(subset='id', keep='last', inplace=True)
        df.set_index(keys='id', drop=True, inplace=True)
        logging.info(f'ml: created dataframe with {len(df)} {entity_type}s')
        return df
    else:
        logging.info(f'ml: no suitable {entity_type}s found')
