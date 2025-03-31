import asyncio
import inspect
import logging
from datetime import timedelta

from dateutil import tz

from src.config import parameters
from src.config.config import config
from src.core.async_infinite_tasks import AsyncInfiniteTasks
from src.core.bot import Bot
from src.core.message import TrendMessage
from src.core.trend_detector import Mode, Trend, TrendDetector
from src.pairs.live_pairs import LivePairs
from src.pairs.pair import Contract, Pair
from src.support import logs
from src.utils import time, utils


logs.setup_logging(timezone=config.get_timezone('Logging', 'timezone'))
logger = logging.getLogger(__name__)


class Application:
    def __init__(self):
        self.bot = Bot(
            token=parameters.BOT_TOKEN,
            token_silent=parameters.SILENT_BOT_TOKEN,
        )
        self.pairs = LivePairs(
            update_frequency=config.get_timedelta_from_seconds('Pairs', 'update_frequency'),
            callback_on_update=self.callback_on_pair_update,
            include_filter=lambda pairs: sorted(
                filter(
                    lambda x: x.contract is Contract.USDT,
                    pairs,
                ),
                key=lambda x: x.turnover,
                reverse=True,
            )[:(
                100
                if parameters.PROD_MODE else
                5
            )],
            mute_list_file_name='mute_list.txt',
        )
        self.trend_detector = TrendDetector(
            max_range=30,
            absolute_change_threshold=utils.create_linear_piecewise_interpolation((1, 0.035), (5, 0.05), (10, 0.06), (30, 0.08)) if parameters.PROD_MODE else lambda _: 0.001,
            turnover_multiplier=utils.create_turnover_based_log_scaling(base=1e9, low_scale=1.2, high_scale=4),
            weak_trend_threshold=0.9,
            cooldown=timedelta(hours=2),
            mode=Mode.UPTREND,
        )
        self.tasks = AsyncInfiniteTasks(
            self.run_loop_updating_status(interval=timedelta(minutes=1)),
            self.run_loop_checking_pairs_connection(interval=timedelta(seconds=10)),
            self.run_loop_trend_detection(),
        )
        self.start = time.get_timestamp()
        self.queue = asyncio.Queue()

    def run(self):
        logger.info('Starting bot')
        logger.info('Pairs: ' + ', '.join([x.pretty_symbol for x in self.pairs]))
        asyncio.run(self.bot.run(self.tasks.run()))
        logger.info('Stopping bot')

    async def run_loop_updating_status(self, interval: timedelta):
        try:
            while True:
                await self.bot.set_description(f'Uptime: {time.format_timedelta(time.get_time_passed_since(self.start))}')
                await asyncio.sleep(interval.total_seconds())

        except asyncio.CancelledError:
            logger.debug(f'Task `{inspect.currentframe().f_code.co_name}` was cancelled'); raise

        finally:
            await self.bot.remove_description()

    async def run_loop_checking_pairs_connection(self, interval: timedelta):
        try:
            while True:
                if not self.pairs.is_connection_alive():
                    logger.error(f'Pair connection was closed. Raising `CancelledError` to end program')
                    raise asyncio.CancelledError()
                await asyncio.sleep(interval.total_seconds())

        except asyncio.CancelledError:
            logger.debug(f'Task `{inspect.currentframe().f_code.co_name}` was cancelled'); raise

    async def run_loop_trend_detection(self):
        try:
            self.pairs.subscribe_to_stream()
            while True:
                await self.callback_on_pair_update_async_part(*(await self.queue.get()))
                logger.debug(f'Trend detection callback executed. Left: {self.queue.qsize()}')

        except asyncio.CancelledError:
            logger.debug(f'Task `{inspect.currentframe().f_code.co_name}` was cancelled'); raise

    def callback_on_pair_update(self, pair: Pair):
        if trend := self.trend_detector.detect(pair):
            logger.info(f'{pair.pretty_symbol}: {trend.change:+.1%}{"" if trend.is_normal else " (weak)"}')
            self.tasks.run_coroutine_threadsafe(self.queue.put((pair, trend)))

    async def callback_on_pair_update_async_part(self, pair: Pair, trend: Trend):
        message = TrendMessage(
            pair,
            trend,
            timezone_=tz.gettz('Europe/Prague')
        )
        await self.bot.send_message(
            user=parameters.USER_ID,
            text=message.get_text(),
            image=message.get_image(),
            silent=self.pairs.is_muted(pair) or trend.is_weak,
        )


if __name__ == '__main__':
    Application().run()
