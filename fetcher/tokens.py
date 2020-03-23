import json
import logging
import multiprocessing
import random
import threading
from collections import defaultdict

from fetcher import VK_TOKENS, LOGGER

lock = threading.Lock()


class SingletonType(type):
    def __new__(mcs, name, bases, attrs):
        # Assume the target class is created (i.e. this method to be called) in the main thread.
        cls = super(SingletonType, mcs).__new__(mcs, name, bases, attrs)
        cls.__shared_instance_lock__ = multiprocessing.Lock()
        return cls

    def __call__(cls, *args, **kwargs):
        with cls.__shared_instance_lock__:
            try:
                return cls.__shared_instance__
            except AttributeError:
                cls.__shared_instance__ = super(SingletonType, cls).__call__(*args, **kwargs)
                return cls.__shared_instance__


class Tokens(metaclass=SingletonType):
    def __init__(self, path, freq):
        self.logger = logging.getLogger(LOGGER)

        # initial value
        self.stats_count = 0

        # how often tokens are dumped
        self.stats_freq = freq

        # path to save dumps
        self.stats_path = path

        self.pool = set(VK_TOKENS)
        self.tokens = {token: {
            'stats': defaultdict(int),
            'health': defaultdict(lambda: True)
        } for token in self.pool}

    def get(self, method):
        available = list(filter(lambda t: t[1]['health'][method], self.tokens.items()))
        if len(available) == 0:
            return None

        if self.stats_count % self.stats_freq == 0:
            self.dump()
        self.stats_count += 1

        return random.choice(available)[0]

    def dump(self):
        with open(self.stats_path + 'stats.json', 'w') as f:
            json.dump(self.tokens, f)

    def use(self, token, method):
        self.tokens[token]['stats'][method] += 1

    def report(self, token, method):
        self.tokens[token]['health'][method] = False
