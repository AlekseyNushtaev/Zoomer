from typing import List
import urllib.parse

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder


def create_kb(width: int,
              *args: str,
              **kwargs: str) -> InlineKeyboardMarkup:
    """
    Создает инлайн-клавиатуру на лету с заданными параметрами.
    """
    # Инициализируем билдер для создания инлайн-клавиатуры
    kb_builder = InlineKeyboardBuilder()
    # Список для хранения созданных кнопок
    buttons: List[InlineKeyboardButton] = []

    # В текущей реализации args не используется, оставлено для будущего расширения
    if args:
        # Здесь может быть добавлена обработка позиционных аргументов
        pass

    # Обрабатываем именованные аргументы (callback_data: text)
    if kwargs:
        for button_data, button_text in kwargs.items():
            # Создаем кнопку с текстом и callback-данными
            buttons.append(InlineKeyboardButton(
                text=button_text,
                callback_data=button_data
            ))

    # Распаковываем список кнопок в билдер, формируя ряды по width кнопок
    kb_builder.row(*buttons, width=width)

    # Возвращаем собранную клавиатуру
    return kb_builder.as_markup()


def check_keyboard():
    # Создаем клавиатуру с инлайн-кнопками
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Подписаться на канал",
                url="https://t.me/zoomerskyvpn"
            )
        ],
        [
            InlineKeyboardButton(
                text="Проверить подписку",
                callback_data="check_channel"
            )
        ]
    ])
    return keyboard


def keyboard_start_bonus():
    keyboard = create_kb(1,
                         free_vpn='🔥 Попробовать бесплатно')
    return keyboard


def keyboard_start():
    keyboard = create_kb(1,
                         buy_vpn='🛒 Купить подписку',
                         connect_vpn='🔗 Подключить VPN',
                         ref='👥 Рефералка',
                         buy_gift='🎁 Подарить подписку',
                         info='💡 Информация')
    return keyboard


def keyboard_tariff_bonus():
    return create_kb(1,
                     r_30='🤝 30 дней - 99 руб',
                     r_90='👌 90 дней - 269 руб',
                     r_180='💪 180 дней - 499 руб',
                     r_white_30='🦾 Включи мобильный - 299 руб',
                     free_vpn='🔥ПОПРОБОВАТЬ 5 дней БЕСПЛАТНО🔥',
                     back_to_main='🔙 Назад'
                     )


def keyboard_tariff():
    return create_kb(1,
                     r_30='🤝 30 дней - 99 руб',
                     r_90='👌 90 дней - 269 руб',
                     r_180='💪 180 дней - 499 руб',
                     r_white_30='🦾 Включи мобильный - 299 руб',
                     back_to_main='🔙 Назад'
                     )


def keyboard_gift_tariff():
    return create_kb(1,
                     gift_r_30='🤝 30 дней - 99 руб',
                     gift_r_90='👌 90 дней - 269 руб',
                     gift_r_180='💪 180 дней - 499 руб',
                     gift_r_white_30='🦾 Включи мобильный - 299 руб',
                     back_to_main='🔙 Назад'
                     )


def keyboard_subscription(sub_url, sub_url_white):
    buttons = []
    if sub_url:
        buttons.append([InlineKeyboardButton(text="📋 Моя подписка", url=sub_url)])
    if sub_url_white:
        buttons.append([InlineKeyboardButton(text="🔥 Включи мобильный", url=sub_url_white)])
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data='back_to_main')])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def keyboard_sub_after_buy(sub_url):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 В личный кабинет", url=sub_url)],
        [InlineKeyboardButton(text="🎁 Подарить подписку", callback_data="start_gift")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data='back_to_main')],
    ])
    return keyboard


def keyboard_payment_cancel():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 Купить подписку", callback_data="buy_vpn")],
        [InlineKeyboardButton(text="🎁 Подарить подписку", callback_data="start_gift")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data='back_to_main')],
    ])
    return keyboard

def keyboard_payment_method(tarif):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 СБП", callback_data=f"sbp_{tarif}")],
        [InlineKeyboardButton(text="⭐️ Telegram Stars", callback_data=f"stars_{tarif}")],
        [InlineKeyboardButton(text="💎 TON", callback_data=f"crypto_ton_{tarif}")],
        [InlineKeyboardButton(text="💵 USDT", callback_data=f"crypto_usdt_{tarif}")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data='back_to_buy_menu')],
    ])
    return keyboard

def keyboard_payment_method_stock(tarif):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 СБП", callback_data=f"sbp_{tarif}")],
        [InlineKeyboardButton(text="⭐️ Telegram Stars", callback_data=f"stars_{tarif}")],
        [InlineKeyboardButton(text="💎 TON", callback_data=f"crypto_ton_{tarif}")],
        [InlineKeyboardButton(text="💵 USDT", callback_data=f"crypto_usdt_{tarif}")],
    ])
    return keyboard


def keyboard_payment_sbp(text, pay_url):
    return InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=text, url=pay_url)]
            ])


def keyboard_payment_stars(stars_amount):
    return InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"Оплатить {stars_amount} ⭐️", pay=True)]
            ])


def ref_keyboard(user_id):
    # Создаем клавиатуру с инлайн-кнопками
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Пригласить друзей🫶",
                url=f"https://t.me/share/url?url=https://t.me/zoomerskyvpn_bot?start=ref{user_id}&text={urllib.parse.quote('Вот ссылка для тебя на топовый ВПН!')}"
            )
        ],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
    ])
    return keyboard
