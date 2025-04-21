import asyncio
import logging
import os
from datetime import datetime, timedelta

from src.config import parameters
from src.config.config import CONFIG
from src.core.async_tasks import AsyncConcurrentTasks, AsyncSequentialTasks
from src.core.custom_bot import CustomBot
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
        self.bot = CustomBot(
            token=parameters.BOT_TOKEN,
            token_silent=parameters.SILENT_BOT_TOKEN,
            whitelist=[parameters.USER_ID],
        )
        self.pairs = LivePairs(
            update_frequency_price=CONFIG.get_timedelta_from_seconds('Pairs', 'update frequency price'),
            polling_interval_update_instruments_info=CONFIG.get_timedelta_from_seconds('Pairs', 'update frequency instruments info'),
            callback_on_price_update=self._callback_on_price_update,
            pairs_filter=parameters.PAIRS_FILTER,
        )
        self.upspike_detector = SpikeDetector(
            max_range=CONFIG.getint('Upspike detector', 'max range'),
            threshold_function=parameters.SpikeDetector.THRESHOLD_FUNCTION,
            turnover_multiplier=parameters.SpikeDetector.TURNOVER_MULTIPLIER,
            catch=Catch.UPSPIKES_ONLY,
            prefer=Prefer.MAX_CHANGE,
            cooldown=CONFIG.get_timedelta_from_minutes('Upspike detector', 'cooldown'),
        )
        self.tasks = AsyncSequentialTasks(
            self.init(),
            self.bot.run(
                AsyncConcurrentTasks(
                    self.task_update_pairs(),
                    self.task_update_bot_status(polling_interval=timedelta(minutes=1)),
                    self.task_call_async_callbacks_from_live_pairs(),
                ).run(blocking=True)
            ),
        )
        self.start_time = time.get_timestamp()
        self.callback_queue = asyncio.Queue()

    def run(self):
        logger.info('Bot started')
        asyncio.run(self.tasks.run())
        logger.info('Bot stopped')

    async def init(self):
        await self.pairs.load_pairs()
        logger.info(f'Pairs ({len(self.pairs)}, turnover > ${format_large_number(self.pairs.get_sorted_by_turnover()[-1].turnover)}): ' + ', '.join([x.base_symbol for x in self.pairs]))

    async def task_update_pairs(self):
        try:
            await self.pairs.start_live_updates()

        except live_pairs.WebsocketConnectionLostError:
            logger.error('Websocket connection lost. Stopping bot')
            raise asyncio.CancelledError()

        finally:
            await self.pairs.stop_live_updates()

    async def task_update_bot_status(self, polling_interval: timedelta):
        try:
            while True:
                await self.bot.set_description(
                    f'Uptime: {time.format_timedelta(time.get_time_passed_since(self.start_time))} '
                    f'({datetime.now(CONFIG.get_timezone("Logging", "timezone")).strftime("%H:%M %d-%m")})'
                )
                await asyncio.sleep(polling_interval.total_seconds())

        finally:
            await self.bot.remove_description()

    def _callback_on_price_update(self, pair: Pair):
        if (upspike := self.upspike_detector.detect(pair)) and abs(pair.funding_rate_per_day) <= CONFIG.get_percent('Upspike detector', 'max funding rate'):
            logger.info(f'{pair.base_symbol + ":":>{pair.BASE_SYMBOL_MAX_LEN + 1}} {upspike.change:+.1%}')
            self.tasks.schedule_coroutine_in_async_thread(self.callback_queue.put((pair, upspike, time.get_monotonic())))

    async def task_call_async_callbacks_from_live_pairs(self):
        while True:
            pair, upspike, start_time = await self.callback_queue.get()
            logger.debug(f'{pair.base_symbol + ":":>{pair.BASE_SYMBOL_MAX_LEN + 1}} Delay before callback: {time.get_monotonic() - start_time:.1f}s'); start_time = time.get_monotonic()
            await self._callback_on_price_update_async(pair, upspike)
            logger.debug(f'{pair.base_symbol + ":":>{pair.BASE_SYMBOL_MAX_LEN + 1}} Callback executed in:  {time.get_monotonic() - start_time:.1f}s. Left: {self.callback_queue.qsize()}')

    async def _callback_on_price_update_async(self, pair: Pair, upspike: Spike):
        message = SpikeMessage(
            pair=pair,
            spike=upspike,
            timezone=CONFIG.get_timezone('Chart', 'timezone')
        )
        await self.bot.send_message(
            user=parameters.USER_ID,
            text=message.get_text(),
            image=message.get_image(),
        )


if __name__ == '__main__':
    try:
        Application().run()
        os._exit(0)  # to avoid pybit thread ending delay
    except Exception as e:
        logger.exception(e)
        os._exit(1)
