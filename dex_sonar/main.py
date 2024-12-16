import logging
from asyncio import CancelledError, sleep

from dex_sonar import time
from dex_sonar.bot import Bot
from dex_sonar.config import BOT_TOKEN, SILENT_BOT_TOKEN
from dex_sonar.live_pairs import LivePairs
from dex_sonar.logs import setup_logging


setup_logging()
logger = logging.getLogger(__name__)


class Application:
    def __init__(self):
        self.bot = Bot(
            token=BOT_TOKEN,
            token_silent=SILENT_BOT_TOKEN,
        )
        self.pairs = LivePairs(
            include_filter=lambda pairs: sorted(pairs, key=lambda x: x.turnover, reverse=True)[:1],
            callback_on_update=self.callback_on_pair_update,
        )
        self.start = time.get_timestamp()

    def run(self):
        self.bot.run(self._run())

    async def _run(self):
        try:
            self.pairs.subscribe_to_stream()
            while True:
                await self.bot.set_description(f'Uptime: {time.format_timedelta(time.get_time_passed_since(self.start), shorten=True)}')
                await sleep(60)

        except CancelledError:
            logger.info(f'Stopping the bot')

        finally:
            self.pairs.close_connection()
            await self.bot.remove_description()

    def callback_on_pair_update(self):
        ...


if __name__ == '__main__':
    Application().run()
