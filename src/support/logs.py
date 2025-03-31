import logging
from datetime import datetime, tzinfo
from logging import Formatter, LogRecord, StreamHandler, getLogger
from math import floor
from statistics import mean
from zoneinfo import ZoneInfo

import colorama
from colorama import Fore

from src.config import parameters


VERBOSE = floor(mean([logging.INFO, logging.WARNING]))

def verbose(self, msg, *args, **kwargs):
    if self.isEnabledFor(VERBOSE):
        self._log(VERBOSE, msg, args, **kwargs)


class ColoredFormatter(Formatter):
    def format(self, record: LogRecord):
        return {
            logging.DEBUG:    Fore.BLUE,
            VERBOSE:          Fore.MAGENTA,
            logging.INFO:     Fore.BLACK,
            logging.WARNING:  Fore.YELLOW,
            logging.ERROR:    Fore.RED,
            logging.CRITICAL: Fore.RED,
        }[record.levelno] + super().format(record)


def setup_logging(timezone: tzinfo = ZoneInfo('UTC')):
    logging.Formatter.converter = lambda *args: datetime.now(timezone).timetuple()

    colorama.init(autoreset=True)
    logging.addLevelName(VERBOSE, 'VERBOSE')
    logging.Logger.verbose = verbose

    root_logger = getLogger()
    root_logger.setLevel(level=parameters.LOGGING_LEVEL)

    handler = StreamHandler()
    handler.setLevel(parameters.LOGGING_LEVEL)
    handler.setFormatter(ColoredFormatter(parameters.LOGGING_FORMAT, datefmt=parameters.LOGGING_TIMESTAMP_FORMAT))
    root_logger.addHandler(handler)

    getLogger('asyncio').setLevel(logging.WARNING)
    getLogger('httpx').setLevel(logging.WARNING)
    getLogger('httpcore').setLevel(logging.INFO)

    getLogger('telegram').setLevel(logging.WARNING)
    getLogger('matplotlib').setLevel(logging.INFO)
    getLogger('pybit').setLevel(logging.WARNING)
    getLogger('urllib3').setLevel(logging.WARNING)
    getLogger('websocket').setLevel(logging.WARNING)
