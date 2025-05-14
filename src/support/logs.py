import logging
from datetime import tzinfo
from logging import CRITICAL, DEBUG, Formatter, INFO, LogRecord, StreamHandler, WARNING, getLogger
from math import floor
from statistics import mean
from zoneinfo import ZoneInfo

import colorama
from colorama import Fore

from src.utils.time import Timestamp


VERBOSE = floor(mean([
    INFO,
    WARNING,
]))


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


def setup_logging(
        level: str,
        format: str,
        timestamp_format: str,
        timezone: tzinfo = ZoneInfo('UTC'),
):
    logging.Formatter.converter = lambda *args: Timestamp.now(timezone).timetuple()

    colorama.init(autoreset=True)
    logging.addLevelName(VERBOSE, 'VERBOSE')
    logging.Logger.verbose = verbose

    root_logger = getLogger()
    root_logger.setLevel(level=level)

    handler = StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(ColoredFormatter(format, datefmt=timestamp_format))
    root_logger.addHandler(handler)

    for scope, default_level, debug_level in [
        ('telegram',   WARNING,  None    ),
        ('pybit',      WARNING,  None    ),
        ('websocket',  CRITICAL, WARNING ),
        ('asyncio',    WARNING,  None    ),
        ('httpx',      WARNING,  None    ),
        ('httpcore',   INFO,     None    ),
        ('matplotlib', INFO,     None    ),
        ('urllib3',    WARNING,  None    ),
    ]:
        getLogger(scope).setLevel(
            default_level
            if level > DEBUG or debug_level is None else
            debug_level
        )
