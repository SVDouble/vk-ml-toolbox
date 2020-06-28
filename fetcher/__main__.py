import logging

import yaml

from fetcher import PREFIX
from fetcher.process import run

# load settings and run script
with open(PREFIX / 'settings.yml', 'r') as settings_yml:
    try:
        settings = yaml.safe_load(settings_yml)
        logging.info('Fetcher started!')
        logging.debug('Fetcher will run with the following settings: %s', settings)
    except yaml.YAMLError as exc:
        logging.exception("Couldn't load settings", exc)
    run(**settings)
