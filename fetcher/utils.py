import collections
import itertools
import json
from pathlib import Path


# deep merge of dicts
# https://gist.github.com/angstwad/bf22d1822c38a92ec0a9#gistcomment-3305932
def deep_merge(*args, add_keys=True):
    assert len(args) >= 2, "deep_merge requires at least two dicts to merge"
    rtn_dct = args[0].copy()
    merge_dicts = args[1:]
    for merge_dct in merge_dicts:
        if add_keys is False:
            merge_dct = {key: merge_dct[key] for key in set(rtn_dct).intersection(set(merge_dct))}
        for k, v in merge_dct.items():
            if not rtn_dct.get(k):
                rtn_dct[k] = v
            elif k in rtn_dct and not isinstance(v, type(rtn_dct[k])):
                raise TypeError(f"Overlapping keys exist with different types: "
                                f"original is {type(rtn_dct[k])}, new value is {type(v)}")
            elif isinstance(rtn_dct[k], dict) and isinstance(merge_dct[k], collections.abc.Mapping):
                rtn_dct[k] = deep_merge(rtn_dct[k], merge_dct[k], add_keys=add_keys)
            elif isinstance(v, list):
                for list_value in v:
                    if list_value not in rtn_dct[k]:
                        rtn_dct[k].append(list_value)
            else:
                rtn_dct[k] = v
    return rtn_dct


def save(path: Path, uid: int, data):
    with (path / f'{uid}.json').open('w') as f:
        json.dump(data, f)


def flatten(iterable):
    return list(itertools.chain.from_iterable(iterable))