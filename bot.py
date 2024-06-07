"""Telegram Bot module"""

import asyncio
import json

from aiogram import Bot, Dispatcher, types
from aiogram.filters.command import Command
from dotenv import dotenv_values

from aggregate import get_result

API_KEY: str = dotenv_values().get("BOT_API")  # type: ignore

# Объект бота
bot = Bot(token=API_KEY)

# Диспетчер
dp = Dispatcher()

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Хэндлер на команду /start"""
    await message.answer("Hello!")


@dp.message()
async def user_message_handler(message: types.Message):
    """Принимаем вводимые пользователем данные"""
    input_data: str = message.text.replace("\n", "").replace("\xa0", "")  # type: ignore
    print(message.text)
    try:
        result = get_result(input_data)
        await message.answer(json.dumps(result))
    except ValueError as e:
        await message.answer(str(e))


async def main():
    """Запуск процесса поллинга новых апдейтов"""
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
