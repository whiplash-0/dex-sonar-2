import asyncio
import logging
import math
from datetime import timedelta

from dateutil import tz

from dex_sonar import time, utils
from dex_sonar.async_infinite_tasks import AsyncInfiniteTasks
from dex_sonar.bot import Bot
from dex_sonar.config import parameters
from dex_sonar.config.config import config
from dex_sonar.live_pairs import LivePairs
from dex_sonar.logs import setup_logging
from dex_sonar.message import TrendMessage
from dex_sonar.pair import Contract, Pair
from dex_sonar.trend_detector import Trend, TrendDetector


setup_logging()
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
                if parameters.PRODUCTION_MODE else
                5
            )],
            mute_list_file_name='mute_list.txt',
        )
        self.trend_detector = TrendDetector(
            max_range=30,
            absolute_change_threshold=(
                utils.get_monotone_parabola(
                    (1,  0.01 * 2),
                    (10, 0.01 * 3.5),
                    (30, 0.01 * 5),
                )
                if parameters.PRODUCTION_MODE else
                lambda _: 0.01
            ),
            turnover_multiplier=(
                lambda x: 1 / (2 ** (1.5 * (math.log10(x) - math.log10(100_000_000))))
            ),
            weak_trend_threshold=0.9,
            cooldown=timedelta(minutes=30),
        )
        self.tasks = AsyncInfiniteTasks(
            self.run_loop_updating_status(interval=timedelta(minutes=1)),
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
        finally:
            await self.bot.remove_description()

    async def run_loop_trend_detection(self):
        with self.pairs.subscribe_to_stream():
            while True:
                await self.callback_on_pair_update_async_part(*(await self.queue.get()))
                logger.info(f'Callback executed. Left: {self.queue.qsize()}')

    def callback_on_pair_update(self, pair: Pair):
        if trend := self.trend_detector.detect(pair):
            logger.info(f'Detected trend in {pair.pretty_symbol}: {trend.change:+.1%}{"" if trend.is_normal else " (weak)"}')
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
