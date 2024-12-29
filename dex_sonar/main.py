import asyncio
import logging
import math
from datetime import timedelta

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
            )[:100],
        )
        self.trend_detector = TrendDetector(
            max_range=15,
            absolute_change_threshold=utils.get_line(
                (1,  0.01 * 0.5),
                (15, 0.01 * 1),
            ),
            turnover_multiplier=lambda x: 1 + 0.5 * (math.log10(self.pairs['BTCUSDT'].turnover) - math.log10(x)),
        )
        self.tasks = AsyncInfiniteTasks(
            self.run_loop_updating_status(interval=timedelta(minutes=1)),
            self.run_loop_trend_detection(),
        )
        self.detection_cooldown = timedelta(hours=1)
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
                await self.bot.set_description(f'Uptime: {time.format_timedelta(time.get_time_passed_since(self.start), shorten=True)}')
                await asyncio.sleep(interval.total_seconds())
        finally:
            await self.bot.remove_description()

    async def run_loop_trend_detection(self):
        with self.pairs.subscribe_to_stream():
            while True:
                await self.callback_on_pair_update_async_part(*(await self.queue.get()))
                logger.info(f'Callback executed. Queue size: {self.queue.qsize()}')

    def callback_on_pair_update(self, pair: Pair):
        if time.get_time_passed_since(self.trend_detector.get_last_detection_time(pair)) >= self.detection_cooldown:
            if trend := self.trend_detector.detect(pair):
                logger.info(f'Detected trend in {pair.pretty_symbol}: {trend.change:+.1%}')
                self.tasks.run_coroutine_threadsafe(self.queue.put((pair, trend)))

    async def callback_on_pair_update_async_part(self, pair: Pair, trend: Trend):
        message = TrendMessage(pair, trend)
        await self.bot.send_message(
            parameters.USER_ID,
            message.get_text(),
            message.get_image(),
        )


if __name__ == '__main__':
    Application().run()
