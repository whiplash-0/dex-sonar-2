import logging
from os import environ

from src.config.config import CONFIGS_DIR as SOURCE_CONFIGS_DIR, config


CONFIGS_DIR = SOURCE_CONFIGS_DIR

TESTING_MODE = config.getboolean('Bot', 'testing_mode')
PRODUCTION_MODE = not TESTING_MODE

LOGGING_LEVEL = logging.INFO if not config.getboolean('Logging', 'debug_mode') else logging.DEBUG
LOGGING_FORMAT = (
    '%(name)s :: %(levelname)s :: %(message)s'
    if PRODUCTION_MODE else
    '%(asctime)s :: %(name)s :: %(message)s'
)
LOGGING_TIMESTAMP_FORMAT = '%m-%d %H:%M:%S'

USER_ID = int(environ.get('USER_ID'))

BOT_TOKEN = environ.get('BOT_TOKEN' if PRODUCTION_MODE else 'TESTING_BOT_TOKEN')
SILENT_BOT_TOKEN = environ.get('SILENT_BOT_TOKEN' if PRODUCTION_MODE else 'TESTING_SILENT_BOT_TOKEN')
