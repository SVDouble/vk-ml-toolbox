import logging
import random
from pathlib import Path

import multiprocessing_logging

# set up random
random.seed(239)

# get project root
PREFIX = Path(__file__).parents[1]

# init folders to store data
DATA_PATH = PREFIX / 'data'
STATS_PATH = DATA_PATH
MERGED_PATH = DATA_PATH / 'dataset'
GROUPS_PATH = DATA_PATH / 'groups'
USERS_PATH = DATA_PATH / 'users'
Path(STATS_PATH).mkdir(parents=True, exist_ok=True)
Path(GROUPS_PATH).mkdir(parents=True, exist_ok=True)
Path(USERS_PATH).mkdir(parents=True, exist_ok=True)
Path(MERGED_PATH).mkdir(parents=True, exist_ok=True)

# init logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.FileHandler(PREFIX / 'data/log.txt'),
        logging.StreamHandler()
    ]
)
multiprocessing_logging.install_mp_handler()
