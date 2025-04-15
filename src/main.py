import asyncio
import inspect
import logging
import os
from datetime import datetime, timedelta

from src.config import parameters
from src.config.config import CONFIG
from src.core.async_tasks import AsyncTasks
from src.core.bot import Bot
from src.core.message import SpikeMessage
from src.core.spike_detector import Catch, Prefer, Spike, SpikeDetector
from src.pairs import live_pairs
from src.pairs.live_pairs import LivePairs
from src.pairs.pair import Pair
from src.support import logs
from src.utils import time
from src.utils.utils import format_large_number


logs.setup_logging(
    level=logging.INFO if not CONFIG.getboolean('Logging', 'debug') else logging.DEBUG,
    format=CONFIG.get('Logging', 'format'),
    timestamp_format=CONFIG.get('Logging', 'timestamp format'),
    timezone=CONFIG.get_timezone('Logging', 'timezone'),
)
logger = logging.getLogger(__name__)


class Application:
    def __init__(self):
        self.bot = Bot(
            token=parameters.BOT_TOKEN,
            token_silent=parameters.SILENT_BOT_TOKEN,
        )
        self.pairs = LivePairs(
            update_frequency_price=CONFIG.get_timedelta_from_seconds('Pairs', 'update frequency price'),
            update_frequency_instruments_info=CONFIG.get_timedelta_from_seconds('Pairs', 'update frequency instruments info'),
            callback_on_price_update=self._callback_on_price_update,
            pairs_filter=parameters.PAIRS_FILTER,
        )
        self.spike_detector = SpikeDetector(
            max_range=CONFIG.getint('Spike detector', 'max range'),
            threshold_function=parameters.SpikeDetector.THRESHOLD_FUNCTION,
            turnover_multiplier=parameters.SpikeDetector.TURNOVER_MULTIPLIER,
            catch=Catch.UPSPIKES_ONLY,
            prefer=Prefer.MAX_CHANGE,
            cooldown=CONFIG.get_timedelta_from_minutes('Spike detector', 'cooldown'),
        )
        self.tasks = AsyncTasks(
            self.task_update_pairs(),
            self.task_update_bot_status(poll_interval=timedelta(minutes=1)),
            self.task_detect_spikes(),
        )
        self.start = time.get_timestamp()
        self.queue = asyncio.Queue()

    def run(self):
        logger.info('Bot started')
        logger.info(f'Pairs ({len(self.pairs)}, turnover > ${format_large_number(self.pairs.get_sorted_by_turnover()[-1].turnover)}): ' + ', '.join([x.pretty_symbol for x in self.pairs]))
        asyncio.run(self.bot.run(self.tasks.run(blocking=True)))
        logger.info('Bot stopped')

    async def task_update_pairs(self):
        try:
            await self.pairs.start_continuous_updating()

        except live_pairs.WebsocketConnectionLostError:
            logger.error('Websocket connection lost. Stopping bot')
            raise asyncio.CancelledError()

    async def task_update_bot_status(self, poll_interval: timedelta):
        try:
            while True:
                await self.bot.set_description(
                    f'Uptime: {time.format_timedelta(time.get_time_passed_since(self.start))} '
                    f'({datetime.now(CONFIG.get_timezone("Logging", "timezone")).strftime("%H:%M %d-%m")})'
                )
                await asyncio.sleep(poll_interval.total_seconds())

        except asyncio.CancelledError:
            logger.debug(f'Task `{inspect.currentframe().f_code.co_name}` was cancelled'); raise

        finally:
            await self.bot.remove_description()

    async def task_detect_spikes(self):
        try:
            while True:
                await self._callback_on_price_update_async(*(await self.queue.get()))
                logger.debug(f'Spike detection callback executed. Left: {self.queue.qsize()}')

        except asyncio.CancelledError:
            logger.debug(f'Task `{inspect.currentframe().f_code.co_name}` was cancelled'); raise

    def _callback_on_price_update(self, pair: Pair):
        if spike := self.spike_detector.detect(pair):
            logger.info(f'{pair.pretty_symbol}: {spike.change:+.1%}')
            self.tasks.run_coroutine_threadsafe(self.queue.put((pair, spike)))

    async def _callback_on_price_update_async(self, pair: Pair, spike: Spike):
        message = SpikeMessage(
            pair,
            spike,
            timezone=CONFIG.get_timezone('Chart', 'timezone')
        )
        await self.bot.send_message(
            user=parameters.USER_ID,
            text=message.get_text(),
            image=message.get_image(),
        )


if __name__ == '__main__':
    Application().run()
    os._exit(0)  # to avoid pybit thread ending delay
