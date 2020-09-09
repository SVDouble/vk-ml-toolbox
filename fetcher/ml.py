import pandas

from fetcher.utils import discover, flatten, load


def to_df(entity_type):
    bundle_type = f'bundle-{entity_type}'
    entities = flatten(load(chunk, bundle_type) for chunk in discover(bundle_type))
    df = pandas.DataFrame(entities)
    df.drop_duplicates(subset='id', keep='last', inplace=True)
    df.set_index(keys='id', drop=True, inplace=True)
    return df
