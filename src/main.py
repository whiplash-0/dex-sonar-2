import asyncio
import inspect
import logging
from datetime import datetime, timedelta

from src.config import parameters
from src.config.config import CONFIG
from src.core.async_infinite_tasks import AsyncInfiniteTasks
from src.core.bot import Bot
from src.core.message import SpikeMessage
from src.core.spike_detector import Mode, Spike, SpikeDetector
from src.pairs.live_pairs import LivePairs
from src.pairs.pair import Contract, Pair
from src.support import logs
from src.utils import time, utils
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
            update_frequency=CONFIG.get_timedelta_from_seconds('Pairs', 'update frequency'),
            callback_on_update=self.callback_on_pair_update,
            include_filter=(
                lambda pairs: list(filter(
                    lambda x: x.contract is Contract.USDT,
                    pairs.get_sorted_by_turnover(),
                ))[:parameters.PAIRS]
            ),
        )
        self.spike_detector = SpikeDetector(
            mode=Mode.UPSPIKE,
            max_range=30,
            absolute_change_threshold=utils.create_linear_piecewise_interpolation((1, 0.035), (5, 0.05), (10, 0.06), (30, 0.08)) if parameters.PROD_MODE else lambda _: 0.001,
            turnover_multiplier=utils.create_turnover_based_log_scaling(base=1e9, low_scale=1.2, high_scale=4),
            cooldown=timedelta(hours=2),
        )
        self.tasks = AsyncInfiniteTasks(
            self.run_loop_updating_status(interval=timedelta(minutes=1)),
            self.run_loop_checking_pairs_connection(interval=timedelta(seconds=10)),
            self.run_loop_spike_detection(),
        )
        self.start = time.get_timestamp()
        self.queue = asyncio.Queue()

    def run(self):
        logger.info('Starting bot')
        logger.info(f'Pairs (>${format_large_number(self.pairs.get_sorted_by_turnover()[-1].turnover)} by turnover): ' + ', '.join([x.pretty_symbol for x in self.pairs]))
        asyncio.run(self.bot.run(self.tasks.run()))
        logger.info('Stopping bot')

    async def run_loop_updating_status(self, interval: timedelta):
        try:
            while True:
                await self.bot.set_description(
                    f'Uptime: {time.format_timedelta(time.get_time_passed_since(self.start))} '
                    f'({datetime.now(CONFIG.get_timezone("Logging", "timezone")).strftime("%H:%M %d-%m")})'
                )
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

    async def run_loop_spike_detection(self):
        try:
            self.pairs.subscribe_to_stream()
            while True:
                await self.callback_on_pair_update_async_part(*(await self.queue.get()))
                logger.debug(f'Spike detection callback executed. Left: {self.queue.qsize()}')

        except asyncio.CancelledError:
            logger.debug(f'Task `{inspect.currentframe().f_code.co_name}` was cancelled'); raise

    def callback_on_pair_update(self, pair: Pair):
        if spike := self.spike_detector.detect(pair):
            logger.info(f'{pair.pretty_symbol}: {spike.change:+.1%}')
            self.tasks.run_coroutine_threadsafe(self.queue.put((pair, spike)))

    async def callback_on_pair_update_async_part(self, pair: Pair, spike: Spike):
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
