import json
import logging
import os
import random
from collections import defaultdict
from multiprocessing import managers
from pathlib import Path

from dotenv import load_dotenv

from fetcher import PREFIX, DATA_PATH
from fetcher.exceptions import NoTokenError
from fetcher.utils import SingletonType

IS_HEALTHY = 'isHealthy'
USE_RATE = 'useRate'


class Tokens(metaclass=SingletonType):
    def __init__(self, tokens, path: Path, freq: int) -> None:
        self.tokens = tokens
        # total number of operations
        self.count = 0
        # how often tokens are dumped
        self.dump_freq = freq
        self.path: Path = path
        self.pull = {token: defaultdict(lambda: {
            USE_RATE: 0,
            IS_HEALTHY: True
        }) for token in set(tokens)}

    def get(self, method: str):
        available = list(filter(lambda r: r[1][method][IS_HEALTHY], self.pull.items()))
        if len(available) == 0:
            message = f'No available tokens for {method}'
            logging.critical(message)
            raise NoTokenError(message)

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

    def report(self, token: str, method: str = None) -> None:
        methods = [method] if method else self.pull[token].keys()
        for method in methods:
            self.pull[token][method][IS_HEALTHY] = False


def load_tokens():
    load_dotenv(dotenv_path=PREFIX / 'local.env')
    try:
        tokens = json.loads(os.getenv('VK_TOKENS'))
        assert len(tokens) > 0, 'No tokens specified!'
        logging.info(f'init: {len(tokens)} tokens available')
        return tokens
    except (TypeError, AssertionError) as exc:
        raise RuntimeError('No tokens found') from exc


def get_token_manager():
    return manager.Tokens(load_tokens(), path=DATA_PATH, freq=10)


# register Tokens class
sync_manager = managers.SyncManager
sync_manager.register('Tokens', Tokens)
manager = sync_manager()
manager.start()
