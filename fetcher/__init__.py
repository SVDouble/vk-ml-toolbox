import json
import logging
import os
from inspect import cleandoc as trim
from pathlib import Path

import multiprocessing_logging
from dotenv import load_dotenv

PREFIX = Path(__file__).parents[1]

# init folders to store data
STATS_PATH = PREFIX / 'data'
GROUPS_PATH = PREFIX / 'data/raw/groups'
USERS_PATH = PREFIX / 'data/raw/users'
Path(STATS_PATH).mkdir(parents=True, exist_ok=True)
Path(GROUPS_PATH).mkdir(parents=True, exist_ok=True)
Path(USERS_PATH).mkdir(parents=True, exist_ok=True)

# init logger
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S',
    filename=PREFIX / 'data/log.txt',
)
multiprocessing_logging.install_mp_handler()

# load env
load_dotenv(dotenv_path=PREFIX / 'local.env')
try:
    VK_TOKENS = json.loads(os.getenv('VK_TOKENS'))
    assert len(VK_TOKENS) > 0, 'No tokens specified!'
    logging.debug('Loaded %i tokens', len(VK_TOKENS))
except (TypeError, AssertionError) as exc:
    raise RuntimeError(trim("""
    Couldn't load tokens, exiting 
    Make sure you've put tokens into a local.env file and placed it in the project root
    """)) from exc
