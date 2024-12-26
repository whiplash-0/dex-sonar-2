import asyncio
import logging

from dex_sonar import time
from dex_sonar.bot import Bot
from dex_sonar.config import parameters
from dex_sonar.config.config import config
from dex_sonar.live_pairs import LivePairs
from dex_sonar.logs import setup_logging
from dex_sonar.message import TrendMessage
from dex_sonar.pair import Pair
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
            include_filter=lambda pairs: sorted(pairs, key=lambda x: x.turnover, reverse=True)[:1],
        )
        self.trend_detector = TrendDetector(
            max_range=60,
            absolute_change_threshold=lambda range, is_uptrend: 0.01,
        )
        self.event_loop = None
        self.start = time.get_timestamp()

    def run(self):
        self.bot.run(self._run())

    async def _run(self):
        try:
            logger.info('Starting bot')
            logger.info('Pairs: ' + ', '.join([x.pretty_symbol for x in self.pairs]))

            self.pairs.subscribe_to_stream()

            while True:
                await self.update_bot_status()
                await asyncio.sleep(10)

        except asyncio.CancelledError:
            logger.info('Stopping bot')

        finally:
            self.pairs.close_connection()
            await self.clear_bot_status()

    async def update_bot_status(self):
        await self.bot.set_description(f'Uptime: {time.format_timedelta(time.get_time_passed_since(self.start), shorten=True)}')

    async def clear_bot_status(self):
        await self.bot.remove_description()

    def callback_on_pair_update(self, pair: Pair):
        if trend := self.trend_detector.detect(pair): self.bot.run_coroutine_threadsafe(self.callback_on_pair_update_async_part(pair, trend))

    async def callback_on_pair_update_async_part(self, pair: Pair, trend: Trend):
        message = TrendMessage(pair, trend)
        await self.bot.send_message(
            parameters.USER_ID,
            message.get_text(),
            message.get_image(),
        )


if __name__ == '__main__':
    Application().run()
