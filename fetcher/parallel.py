from multiprocessing import managers

from pathos.multiprocessing import ProcessPool
from tqdm.auto import tqdm

from fetcher import STATS_PATH
from fetcher.tokens import Tokens

# register Tokens class to make it pickable
sync_manager = managers.SyncManager
sync_manager.register('Tokens', Tokens)
manager = sync_manager()
manager.start()
token = manager.Tokens(path=STATS_PATH, freq=10)


def parallel_map(transform, args):
    tokens = [token for _ in range(len(args))]
    pool = ProcessPool()
    res = list(tqdm(pool.uimap(transform, args, tokens), total=len(args)))
    pool.clear()
    return res
