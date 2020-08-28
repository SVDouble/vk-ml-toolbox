import logging

import yaml

from fetcher import PREFIX
from fetcher.process import run

# load settings and run script
with open(PREFIX / 'todo.yml', 'r') as todo_yml:
    with open(PREFIX / 'fetcher' / 'methods.yml', 'r') as methods_yml:
        try:
            todo = yaml.safe_load(todo_yml)
            if not todo:
                raise RuntimeError('Nothing to fetch!')
            methods = yaml.safe_load(methods_yml)
            if not methods:
                raise RuntimeError('No methods specified!')

            logging.info(f'Stages: {list(todo.keys())}')
            logging.info(f'Methods allowed: {list(methods.keys())}')
            run(todo, methods)
        except yaml.YAMLError as exc:
            logging.exception('Failed to load settings', exc)
