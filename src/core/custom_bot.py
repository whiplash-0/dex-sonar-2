import logging
from typing import Iterable

from telegram import InlineKeyboardButton, ReplyKeyboardMarkup, Update
from telegram.ext import ApplicationHandlerStop, CommandHandler, ContextTypes, TypeHandler

from src.core.bot import Bot, UserID


logger = logging.getLogger(__name__)


class CustomBot(Bot):
    def __init__(self, whitelist: Iterable[UserID], **kwargs):
        super().__init__(**kwargs)
        self.whitelist = whitelist
        self._attach_handlers()

    def _attach_handlers(self):
        self.add_handlers(TypeHandler(Update, self._authorize_access), group=0)  # whitelist
        self.add_handlers(CommandHandler('start', self._start), group=1)

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

    @staticmethod
    async def _start(update: Update, context: ContextTypes.DEFAULT_TYPE):
        markup = ReplyKeyboardMarkup([[InlineKeyboardButton('Menu', callback_data='menu')]], resize_keyboard=True)
        await update.message.reply_text(text='Menu has been pinned to your input area', reply_markup=markup)
