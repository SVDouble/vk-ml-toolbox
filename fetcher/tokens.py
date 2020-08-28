import json
import logging
import random
from collections import defaultdict
from multiprocessing import managers
from pathlib import Path

from fetcher import VK_TOKENS, STATS_PATH
from fetcher.exceptions import NoTokenError
from fetcher.utils import SingletonType

IS_HEALTHY = 'isHealthy'
USE_RATE = 'useRate'


class Tokens(metaclass=SingletonType):
    def __init__(self, path: Path, freq: int) -> None:
        # total number of operations
        self.count = 0
        # how often tokens are dumped
        self.dump_freq = freq
        self.path: Path = path
        self.pull = {token: defaultdict(lambda: {
            USE_RATE: 0,
            IS_HEALTHY: True
        }) for token in set(VK_TOKENS)}

    def get(self, method: str):
        available = list(filter(lambda r: r[1][method][IS_HEALTHY], self.pull.items()))
        if len(available) == 0:
            raise NoTokenError(f'No more tokens available for {method}')

        if self.count % self.dump_freq == 0:
            self.dump()
        self.count += 1

        token = random.choice(available)[0]
        self.use(token, method)
        return token

    def dump(self) -> None:
        with (self.path / 'stats.json').open('w') as f:
            json.dump(self.pull, f)

    def use(self, token: str, method: str) -> None:
        self.pull[token][method][USE_RATE] += 1

    def report(self, token: str, method: str) -> None:
        logging.info(f'Disable token {token} for {method}')
        self.pull[token][method][IS_HEALTHY] = False


# register Tokens class to make it pickable
sync_manager = managers.SyncManager
sync_manager.register('Tokens', Tokens)
manager = sync_manager()
manager.start()
tokens = manager.Tokens(path=STATS_PATH, freq=10)
