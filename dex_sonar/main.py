import logging
from asyncio import CancelledError, sleep

from dex_sonar import time
from dex_sonar.bot import Bot
from dex_sonar.config import parameters
from dex_sonar.config.config import config
from dex_sonar.live_pairs import LivePairs
from dex_sonar.logs import setup_logging


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
        self.start = time.get_timestamp()

    def run(self):
        self.bot.run(self._run())

    async def _run(self):
        try:
            self.pairs.subscribe_to_stream()
            while True:
                await self.update_bot_status()
                await sleep(60)

        except CancelledError:
            logger.info(f'Stopping the bot')

        finally:
            self.pairs.close_connection()
            await self.clear_bot_status()

    async def update_bot_status(self):
        await self.bot.set_description(f'Uptime: {time.format_timedelta(time.get_time_passed_since(self.start), shorten=True)}')

    async def clear_bot_status(self):
        await self.bot.remove_description()

    def callback_on_pair_update(self):
        ...


if __name__ == '__main__':
    Application().run()
