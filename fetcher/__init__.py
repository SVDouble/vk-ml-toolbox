import logging
import random
from pathlib import Path

import multiprocessing_logging

# set up random
random.seed(239)

# get project root
PREFIX = Path(__file__).parents[1]
GROUPS_POSTFIX = 'groups'
USERS_POSTFIX = 'users'

# init folders to store data
DATA_PATH = PREFIX / 'data'
ML_PATH = DATA_PATH / 'ml'
ML_GROUPS_PATH = ML_PATH / GROUPS_POSTFIX
ML_USERS_PATH = ML_PATH / USERS_POSTFIX
RAW_PATH = DATA_PATH / 'raw'
GROUPS_PATH = RAW_PATH / GROUPS_POSTFIX
USERS_PATH = RAW_PATH / USERS_POSTFIX
for path in [ML_GROUPS_PATH, ML_USERS_PATH, GROUPS_PATH, USERS_PATH]:
    Path(path).mkdir(parents=True, exist_ok=True)

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
