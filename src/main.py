import asyncio
import logging
import os

from src.config import parameters
from src.config.config import CONFIG
from src.contracts import live_contracts
from src.contracts.contract import Contract
from src.contracts.live_contracts import Intervals, LiveContracts
from src.core.async_tasks import AsyncConcurrentTasks, AsyncSequentialTasks
from src.core.custom_bot import CustomBot
from src.core.message import SpikeMessage
from src.core.spike_detector import Catch, Prefer, Spike, SpikeDetector
from src.support import logs
from src.support.upspike_threshold import UpspikeThreshold
from src.utils import time
from src.utils.time import Timedelta, Timestamp
from src.utils.utils import format_large_number


POLLING_INTERVAL_UPDATE_BOT_STATUS = Timedelta(minutes=1)
RETURN_CODE_SUCCESS = 0
RETURN_CODE_FAILURE = 1


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
        self.contracts = LiveContracts(
            intervals=Intervals(
                price_update=CONFIG.get_timedelta_from_seconds('Contracts', 'price update interval'),
                instruments_info_update=CONFIG.get_timedelta_from_seconds('Contracts', 'instruments info update interval'),
            ),
            callback_on_price_update=self._callback_on_price_update,
            should_contract_be_included=parameters.SHOULD_CONTRACT_BE_INCLUDED,
        )
        self.upspike_detector = SpikeDetector(
            max_range=CONFIG.get_int('Upspike detector', 'max range'),
            threshold_function=parameters.UpspikeDetector.THRESHOLD_FUNCTION,
            turnover_multiplier=parameters.UpspikeDetector.TURNOVER_MULTIPLIER,
            catch=Catch.UPSPIKES_ONLY,
            prefer=Prefer.MAX_CHANGE,
            cooldown=CONFIG.get_timedelta_from_minutes('Upspike detector', 'cooldown'),
        )
        self.tasks = AsyncSequentialTasks(
            self.init(),
            self.bot.run(
                AsyncConcurrentTasks(
                    self.task_update_bot_status(polling_interval=POLLING_INTERVAL_UPDATE_BOT_STATUS),
                    self.task_update_contracts(),
                    self.task_handle_callbacks_from_live_contracts(),
                ).run(blocking=True)
            ),
            termination_signal_handler=self.stop,
        )
        self.start_time = time.get_timestamp()
        self.callback_queue = asyncio.Queue()

    def run(self):
        try:
            logger.info(f'Bot is starting')
            asyncio.run(self.tasks.run())
            logger.info(f'Bot stopped. Uptime: {time.format_timedelta(time.get_time_passed_since(self.start_time))}')
            os._exit(RETURN_CODE_SUCCESS)  # to avoid pybit thread ending delay
        except Exception as e:
            logger.exception(e)
            os._exit(RETURN_CODE_FAILURE)

    def stop(self):
        self.tasks.schedule_task_in_async_thread(self.tasks.cancel())

    async def init(self):
        await self.bot.init()
        await self.contracts.init()
        await UpspikeThreshold.init()
        logger.info(f'Contracts ({len(self.contracts)}, turnover > ${format_large_number(self.contracts.get_sorted_by_turnover()[-1].turnover)}): ' + ', '.join(self.contracts.get_base_symbols()))

    async def task_update_contracts(self):
        try:
            await self.contracts.start_live_updates()

        except live_contracts.ConnectionLostError:
            logger.error('Websocket connection lost. Stopping bot')
            raise asyncio.CancelledError()

        finally:
            await self.contracts.stop_live_updates()

    async def task_update_bot_status(self, polling_interval: Timedelta):
        try:
            while True:
                await self.bot.set_description(
                    f'Uptime: {time.format_timedelta(time.get_time_passed_since(self.start_time))} '
                    f'({Timestamp.now(CONFIG.get_timezone("Logging", "timezone")).strftime("%H:%M %d-%m")})'
                )
                await asyncio.sleep(polling_interval.total_seconds())

        finally:
            await self.bot.remove_description()

    def _callback_on_price_update(self, contract: Contract):
        if (
                not contract.is_being_delisted and
                abs(contract.funding_rate_per_day) <= CONFIG.get_percent('Upspike detector', 'max funding rate') and
                (upspike := self.upspike_detector.detect(contract))
        ):
            logger.info(f'{contract.base_symbol + ":":>{contract.BASE_SYMBOL_MAX_LEN + 1}} {upspike.change:+.1%}')
            self.tasks.schedule_task_in_async_thread(self.callback_queue.put((contract, upspike, time.get_monotonic())))

    async def task_handle_callbacks_from_live_contracts(self):
        while True:
            contract, upspike, start_time = await self.callback_queue.get()
            logger.debug(f'{contract.base_symbol + ":":>{contract.BASE_SYMBOL_MAX_LEN + 1}} Delay before callback: {time.get_monotonic() - start_time:.1f}s'); start_time = time.get_monotonic()
            await self._callback_on_price_update_async(contract, upspike)
            logger.debug(f'{contract.base_symbol + ":":>{contract.BASE_SYMBOL_MAX_LEN + 1}} Callback executed in:  {time.get_monotonic() - start_time:.1f}s. Left: {self.callback_queue.qsize()}')

    async def _callback_on_price_update_async(self, contract: Contract, upspike: Spike):
        message = SpikeMessage(
            contract=contract,
            spike=upspike,
            timezone=CONFIG.get_timezone('Chart', 'timezone')
        )
        await self.bot.send_message(
            user=parameters.USER_ID,
            text=message.get_text(),
            image=message.get_image(),
        )



if __name__ == '__main__':
    Application().run()
