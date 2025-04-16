from os import environ

from src.config.config import CONFIG
from src.pairs.pair import Contract
from src.utils import utils


TEST_MODE = CONFIG.getboolean('Bot', 'test mode')
PROD_MODE = not TEST_MODE

BOT_TOKEN = environ.get('BOT_TOKEN' if PROD_MODE else 'TEST_BOT_TOKEN')
SILENT_BOT_TOKEN = environ.get('SILENT_BOT_TOKEN' if PROD_MODE else 'TEST_SILENT_BOT_TOKEN')

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
