from os import environ

from src.config.config import CONFIG


TEST_MODE = CONFIG.getboolean('Bot', 'test mode')
PROD_MODE = not TEST_MODE

BOT_TOKEN = environ.get('BOT_TOKEN' if PROD_MODE else 'TESTING_BOT_TOKEN')
SILENT_BOT_TOKEN = environ.get('SILENT_BOT_TOKEN' if PROD_MODE else 'TESTING_SILENT_BOT_TOKEN')

USER_ID = int(environ.get('USER_ID'))

PAIRS = 100 if PROD_MODE else 5
