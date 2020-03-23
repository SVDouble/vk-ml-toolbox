import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

# init folders to store data
STATS_PATH = '../data/stats/'
GROUPS_PATH = '../data/groups/'
USERS_PATH = '../data/users/'
Path(STATS_PATH).mkdir(parents=True, exist_ok=True)
Path(GROUPS_PATH).mkdir(parents=True, exist_ok=True)
Path(USERS_PATH).mkdir(parents=True, exist_ok=True)

# set up logger
LOGGER = 'fetcher'
logger = logging.getLogger(LOGGER)
logger.setLevel(logging.INFO)

# load tokens
load_dotenv()
VK_TOKENS = json.loads(os.getenv('VK_TOKENS'))
if VK_TOKENS is None:
    e = Exception('VK_TOKEN is not defined, exiting')
    logger.error(e)
    raise e
