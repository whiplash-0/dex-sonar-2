import logging
from io import BytesIO
from typing import Coroutine, Iterable

from telegram import Bot as TelegramBot, InlineKeyboardMarkup, LinkPreviewOptions, Update
from telegram.constants import ParseMode
from telegram.ext import ApplicationBuilder, ApplicationHandlerStop, BaseHandler, ContextTypes, Defaults, TypeHandler


logger = logging.getLogger(__name__)


Token = str
UserID = int
Text = str
ImageBuffer = BytesIO


class Bot:
    def __init__(self, token: Token, token_silent: Token, user_whitelist: Iterable[UserID]):
        defaults = Defaults(
            parse_mode=ParseMode.MARKDOWN_V2,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
        self.application = ApplicationBuilder().token(token).defaults(defaults).build()
        self.application_silent = ApplicationBuilder().token(token_silent).defaults(defaults).build()
        self.bot: TelegramBot = self.application.bot
        self.bot_silent: TelegramBot = self.application_silent.bot
        self.whitelist = user_whitelist
        self._init()

    def _init(self):
        self._add_handlers(TypeHandler(Update, self._authorize_access), group=0)  # whitelist

    async def run(self, coro: Coroutine):
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling()

        await self.application_silent.initialize()
        await self.application_silent.start()
        await self.application_silent.updater.start_polling()

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

    def _add_handlers(self, handlers: BaseHandler | Iterable[BaseHandler], group=None):
        if isinstance(handlers, BaseHandler): handlers = [handlers]
        for x in [self.application, self.application_silent]:
            x.add_handlers(handlers, **({'group': group} if group is not None else {}))

    async def _authorize_access(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user

        if user.id not in self.whitelist:
            if update.message:
                update_repr = f'Message: {update.message.text}'
            elif update.my_chat_member:
                update_repr = f'New member status: {update.my_chat_member.new_chat_member.status}'
            else:
                update_repr = f'Update: {update}'

            logger.warning(f'Unauthorized access from: {user.full_name} @{user.username if user.username else ""} #{user.id}. {update_repr}')
            raise ApplicationHandlerStop
