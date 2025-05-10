import logging
from io import BytesIO
from typing import Coroutine, Sequence

from telegram import Bot as TelegramBot, InlineKeyboardMarkup, LinkPreviewOptions
from telegram.constants import ParseMode
from telegram.ext import ApplicationBuilder, BaseHandler, Defaults


DEFAULTS = Defaults(
    parse_mode=ParseMode.MARKDOWN_V2,
    link_preview_options=LinkPreviewOptions(is_disabled=True),
)


logger = logging.getLogger(__name__)


Token = str
UserID = int
Text = str
ImageBuffer = BytesIO


class Bot:
    def __init__(self, token: Token, token_silent: Token):
        self.application = ApplicationBuilder().token(token).defaults(DEFAULTS).build()
        self.application_silent = ApplicationBuilder().token(token_silent).defaults(DEFAULTS).build()
        self.bot: TelegramBot = self.application.bot
        self.bot_silent: TelegramBot = self.application_silent.bot

    async def set_my_commands(self, commands: Sequence[tuple[str, str]]):
        await self.bot.set_my_commands(commands)
        await self.bot_silent.set_my_commands(commands)

    def add_handlers(self, *handlers: BaseHandler, group=None):
        for x in [self.application, self.application_silent]:
            x.add_handlers(handlers, **({'group': group} if group is not None else {}))

    async def run(self, coro: Coroutine):
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling(error_callback=self._handle_telegram_error)

        await self.application_silent.initialize()
        await self.application_silent.start()
        await self.application_silent.updater.start_polling(error_callback=self._handle_telegram_error)

        await coro

        await self.application_silent.updater.stop()
        await self.application_silent.stop()
        await self.application_silent.shutdown()

        await self.application.updater.stop()
        await self.application.stop()
        await self.application.shutdown()

    async def send_message(
            self,
            user: UserID,
            text: Text,
            image: ImageBuffer = None,
            reply_markup: InlineKeyboardMarkup = None,
            silent=False,
    ):
        bot = self.bot if not silent else self.bot_silent

        if image is None:
            await bot.send_message(
                chat_id=user,
                text=text,
                reply_markup=reply_markup,
            )
        else:
            await bot.send_photo(
                chat_id=user,
                photo=image,
                caption=text,
                reply_markup=reply_markup,
            )

    async def set_description(self, description):
        await self.bot.set_my_short_description(description)
        await self.bot_silent.set_my_short_description(description)

    async def remove_description(self):
        await self.bot.set_my_short_description(None)
        await self.bot_silent.set_my_short_description(None)

    @staticmethod
    def _handle_telegram_error(error):
        """
        Suppresses extra verbose output with traceback from telegram logger
        """
        ...
