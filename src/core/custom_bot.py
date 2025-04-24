import logging
from enum import Enum, auto
from typing import Iterable

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update, error
from telegram.ext import ApplicationHandlerStop, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, TypeHandler, filters

from src.core.bot import Bot, UserID
from src.support.upspike_threshold import UpspikeThreshold as UpspikeThresholdValue


COMMANDS = [('start', 'Start the bot and get menu')]

START_TEXT = 'Menu has been pinned to your input area'

class UpspikeThreshold:
    NAME = 'Upspike threshold'
    TEXT = 'Adjust the upspike threshold using buttons below:'
    STEP = 0.05
    MIN = 0.5
    MAX = 3


logger = logging.getLogger(__name__)



async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    markup = ReplyKeyboardMarkup([[InlineKeyboardButton(UpspikeThreshold.NAME)]], resize_keyboard=True)
    await update.message.reply_text(text=START_TEXT, reply_markup=markup)



class UpspikeThresholdButton(str, Enum):
    def _generate_next_value_(name, start, count, last_values):
        return f'upspike_threshold_{count}'

    DECREASE = auto()
    VALUE = auto()
    INCREASE = auto()


def create_upspike_threshold_markup():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton('➖', callback_data=UpspikeThresholdButton.DECREASE),
        InlineKeyboardButton(f'{UpspikeThresholdValue.get():.0%}', callback_data=UpspikeThresholdButton.VALUE),
        InlineKeyboardButton('➕', callback_data=UpspikeThresholdButton.INCREASE)
    ]])


async def send_upspike_threshold_adjusting(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(text=UpspikeThreshold.TEXT, reply_markup=create_upspike_threshold_markup())


async def adjust_upspike_threshold(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    match query.data:

        case UpspikeThresholdButton.DECREASE:
            if (value := UpspikeThresholdValue.get() - UpspikeThreshold.STEP) >= UpspikeThreshold.MIN:
                await UpspikeThresholdValue.set(value)

        case UpspikeThresholdButton.INCREASE:
            if (value := UpspikeThresholdValue.get() + UpspikeThreshold.STEP) <= UpspikeThreshold.MAX:
                await UpspikeThresholdValue.set(value)

    try:
        await query.edit_message_text(text=UpspikeThreshold.TEXT, reply_markup=create_upspike_threshold_markup())
    except error.BadRequest as e:  # ignore exception about same content after editing
        if 'specified new message content and reply markup are exactly the same' not in str(e): raise



class CustomBot(Bot):
    def __init__(self, whitelist: Iterable[UserID], **kwargs):
        super().__init__(**kwargs)
        self.whitelist = whitelist
        self._init()

    def _init(self):
        self.add_handlers(
            TypeHandler(Update, self._authorize_access),  # whitelist
            group=0,
        )
        self.add_handlers(
            CommandHandler('start', start),
            MessageHandler(filters.Regex(f'^{UpspikeThreshold.NAME}$'), send_upspike_threshold_adjusting),
            CallbackQueryHandler(adjust_upspike_threshold, pattern='|'.join(UpspikeThresholdButton)),
            group=1,
        )

    async def init(self):
        await self.set_my_commands(COMMANDS)

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
