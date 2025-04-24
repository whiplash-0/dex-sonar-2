import subprocess
from os import environ

from src.config.config import CONFIG
from src.pairs.pair import Contract
from src.utils import utils


TEST_MODE = CONFIG.getboolean('Bot', 'test mode')
PROD_MODE = not TEST_MODE

BOT_TOKEN = environ.get('BOT_TOKEN' if PROD_MODE else 'TEST_BOT_TOKEN')
SILENT_BOT_TOKEN = environ.get('SILENT_BOT_TOKEN' if PROD_MODE else 'TEST_SILENT_BOT_TOKEN')

if not CONFIG.getboolean('Bot', 'cloud'):  # use CLI to fetch URL
    result = subprocess.run(
        ['heroku', 'config:get', 'DATABASE_URL', '-a', CONFIG.get('Heroku', 'app name')],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode == 0: DATABASE_URL = result.stdout.strip()
    else: raise ValueError(f'Error fetching `DATABASE_URL` via CLI: {result.stderr.strip()}')

else:  # otherwise Heroku will add it to environment variables
    DATABASE_URL = environ.get('DATABASE_URL')

DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql+asyncpg://', 1)  # ensure compatibility with asynchronous paradigm

USER_ID = int(environ.get('USER_ID'))

PAIRS_FILTER = (
    lambda pairs: [
        x for x in pairs if
        x.contract is Contract.USDT and
        x.turnover >= CONFIG.getfloat('Pairs', 'min turnover', default=0)
    ]
)

class SpikeDetector:
    THRESHOLD_FUNCTION =(
        utils.create_linear_piecewise_interpolation((1, 0.035), (5, 0.05), (10, 0.06), (30, 0.08))
        if PROD_MODE else
        lambda _: 0
    )
    TURNOVER_MULTIPLIER = utils.create_turnover_based_log_scaling(
        base=1e9,
        low_scale=1.2,
        high_scale=4,
    )
