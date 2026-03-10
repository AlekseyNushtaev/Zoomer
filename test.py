import asyncio

from aiogram import Bot


async def main():
    bot = Bot(token="")
    user = await bot.get_chat(8416915706)
    username = user.username
    print(username)

if __name__ == '__main__':
    asyncio.run(main())
