import asyncio

from aiogram import Bot


async def main():
    bot = Bot(token="7412940598:AAF8WIQEBHIrAUBkYs93w5l_Ofq5dWlTpI8")
    user = await bot.get_chat(8416915706)
    username = user.username
    print(username)

if __name__ == '__main__':
    asyncio.run(main())
