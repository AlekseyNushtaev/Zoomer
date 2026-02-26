import sqlite3
import urllib.parse
from datetime import datetime

from bot import sql
from botapi_sender import send_message
from config import ADMIN_IDS
from keyboard import create_kb
from logging_config import logger
import asyncio
from aiogram import Router, Bot, F
from aiogram.types import Message, CallbackQuery, ContentType
from aiogram.filters import StateFilter, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


router = Router()


class BroadcastState(StatesGroup):
    waiting_for_message = State()
    waiting_for_parameter = State()
    waiting_for_parameter_value = State()
    confirm_send = State()


def update_broadcast_status(user_id, status):
    conn = sqlite3.connect('sqlite3.db')  # Убедитесь, что путь к базе данных верный
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE users
            SET last_broadcast_status = ?, last_broadcast_date = ?
            WHERE User_id = ?
        """, (status, datetime.now().date(), user_id))
        conn.commit()
    except sqlite3.OperationalError as e:
        logger.info(f"An error occurred: {e}")
    finally:
        conn.close()


@router.message(Command(commands=['broadcast']))
async def broadcast_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id in ADMIN_IDS:  # Проверка прав администратора из базы данных
        back_button_keyboard = create_kb(1, broadcast_cancel='🔙 Назад')
        await message.answer("Отправьте сообщение для рассылки или нажмите '🔙 Назад' для отмены.",
                             reply_markup=back_button_keyboard)
        await state.set_state(BroadcastState.waiting_for_message)
    else:
        await message.answer("Эта команда доступна только администраторам.")


# Обработка сообщения для рассылки
@router.message(BroadcastState.waiting_for_message)
async def broadcast_waiting_for_message(message: Message, state: FSMContext):
    # Проверяем тип контента
    if message.content_type not in [
        ContentType.TEXT,
        ContentType.PHOTO,
        ContentType.VIDEO,
        ContentType.DOCUMENT,
        ContentType.VOICE,
        ContentType.AUDIO,
        ContentType.ANIMATION,
        ContentType.STICKER
    ]:
        await message.answer("Этот тип контента не поддерживается для рассылки.")
        return
    elif message.content_type == ContentType.TEXT:
        mes = await message.answer(message.text, disable_web_page_preview=True)
        await state.update_data(
            broadcast_message_id=mes.message_id,
            broadcast_chat_id=mes.chat.id,
            broadcast_content_type=mes.content_type
        )
    else:
        # Сохраняем сообщение (включая тип контента) в состоянии
        await state.update_data(
            broadcast_message_id=message.message_id,
            broadcast_chat_id=message.chat.id,
            broadcast_content_type=message.content_type
        )

    # Запрашиваем список доступных параметров из базы данных
    parameters = sql.GET_AVAILABLE_PARAMETERS()  # Эта функция должна возвращать список доступных параметров
    parameters_text = "\n".join(parameters)  # Преобразуем список в строку для отображения
    await message.answer(
        f"Теперь выберите параметр из следующего списка:\n{parameters_text}\nИли нажмите '🔙 Назад' для отмены.")

    await state.set_state(BroadcastState.waiting_for_parameter)


# Обработчик для выбора параметра
@router.message(BroadcastState.waiting_for_parameter)
async def process_parameter(message: Message, state: FSMContext):
    selected_parameter = message.text
    available_parameters = sql.GET_AVAILABLE_PARAMETERS()  # Получаем список доступных параметров для проверки

    if selected_parameter not in available_parameters:
        await message.answer("Ошибка: выбранный параметр недействителен. Пожалуйста, выберите из списка.")
        return

    # Сохраняем выбранный параметр в состоянии
    await state.update_data(selected_parameter=selected_parameter)

    await confirm_broadcast(message, state)


async def confirm_broadcast(message: Message, state: FSMContext):
    # Получаем пользователей по выбранному параметру и значению, если это не "Все пользователи"
    data = await state.get_data()
    selected_parameter = data.get('selected_parameter')
    user_ids = []
    if selected_parameter == "all_users":
        user_ids = sql.SELECT_ALL_USERS()  # Получаем всех пользователей
    elif selected_parameter == 'not_connected_subscribe_yes':
        user_ids = sql.SELECT_NOT_CONNECTED_SUBSCRIBE_YES()
    elif selected_parameter == 'not_connected_subscribe_off':
        user_ids = sql.SELECT_NOT_CONNECTED_SUBSCRIBE_OFF()
    elif selected_parameter == 'connected_subscribe_off':
        user_ids = sql.SELECT_CONNECTED_SUBSCRIBE_OFF()
    elif selected_parameter == 'connected_subscribe_yes':
        user_ids = sql.SELECT_CONNECTED_SUBSCRIBE_YES()
    elif selected_parameter == 'not_subscribed':
        user_ids = sql.SELECT_NOT_SUBSCRIBED()

    if not user_ids:
        await message.answer("Нет пользователей, соответствующих выбранному параметру и значению.")
        await state.clear()
        return

    # Создаем клавиатуру для подтверждения
    confirm_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Подтвердить", callback_data="Подтвердить")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="broadcast_cancel")]
    ])

    await message.answer("Подтвердите отправку сообщения выбранным пользователям или нажмите '🔙 Назад' для отмены.\n"
                         f"{selected_parameter} - {len(user_ids)}",
                         reply_markup=confirm_keyboard)
    await state.set_state(BroadcastState.confirm_send)


@router.callback_query(F.data == 'Подтвердить', StateFilter(BroadcastState.confirm_send))
async def broadcast_confirm_send(callback: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    broadcast_message_id = data.get('broadcast_message_id')
    broadcast_chat_id = data.get('broadcast_chat_id')
    broadcast_content_type = data.get('broadcast_content_type')
    selected_parameter = data.get('selected_parameter')

    # Проверка наличия сообщения для отправки
    if not broadcast_message_id or not broadcast_chat_id or not broadcast_content_type:
        await callback.message.edit_text("Ошибка: сообщение не найдено. Отправка прервана.")
        await state.clear()
        return
    user_ids = []
    keyboard_broadcast = None
    # Получаем пользователей по выбранному параметру
    if selected_parameter == "all_users":
        user_ids = sql.SELECT_ALL_USERS()  # Получаем всех пользователей
        keyboard_broadcast = create_kb(1, r_120='🔥 Акция: 120 дней - 269 руб')
    elif selected_parameter == 'not_connected_subscribe_yes':
        user_ids = sql.SELECT_NOT_CONNECTED_SUBSCRIBE_YES()
        keyboard_broadcast = create_kb(1, connect_vpn='🔗 Подключить VPN')
    elif selected_parameter == 'not_connected_subscribe_off':
        user_ids = sql.SELECT_NOT_CONNECTED_SUBSCRIBE_OFF()
        keyboard_broadcast = create_kb(1, buy_vpn='🛒 Купить подписку')
    elif selected_parameter == 'connected_subscribe_off':
        user_ids = sql.SELECT_CONNECTED_SUBSCRIBE_OFF()
        keyboard_broadcast = create_kb(1, r_120='🔥 Акция: 120 дней - 269 руб')
    elif selected_parameter == 'connected_subscribe_yes':
        user_ids = sql.SELECT_CONNECTED_SUBSCRIBE_YES()
        keyboard_broadcast = create_kb(1, r_120='🔥 Акция: 120 дней - 269 руб')
    elif selected_parameter == 'not_subscribed':
        user_ids = sql.SELECT_NOT_SUBSCRIBED()
        keyboard_broadcast = create_kb(1, free_vpn='🔥 Попробовать бесплатно')

    # Проверяем, есть ли пользователи для отправки
    if not user_ids:
        await callback.message.edit_text("Нет пользователей, соответствующих выбранному параметру и значению.")
        await state.clear()
        return
    count = 0
    # Отправляем сообщение пользователям
    user_ids.append(1012882762)
    for user_id in user_ids:
        try:
            await bot.copy_message(
                chat_id=user_id,
                from_chat_id=broadcast_chat_id,
                message_id=broadcast_message_id,
                reply_markup=keyboard_broadcast,
            )
            update_broadcast_status(user_id, 'sent')  # Успешная отправка
            await asyncio.sleep(0.05)
            count += 1
        except Exception as e:
            update_broadcast_status(user_id, 'failed')  # Ошибка отправки
            logger.error(f"Failed to send message to {user_id}: {e}")
            error_text = str(e)
            if "403" in error_text or "blocked by the user" in error_text:
                sql.UPDATE_DELETE(user_id, True)
    logger.success(f"Send broadcast to {count} users")

    await callback.message.edit_text(f"Сообщение успешно отправлено {count} пользователям.")
    await state.clear()


# Обработка отмены рассылки
@router.callback_query(F.data == 'broadcast_cancel')
async def cancel_broadcast(callback: CallbackQuery, state: FSMContext):
    current_state = await state.get_state()
    if current_state in [BroadcastState.confirm_send, BroadcastState.waiting_for_message]:
        await callback.message.edit_text("Рассылка отменена.")
        await state.clear()
    else:
        await callback.answer("Отмена завершена.")





@router.message(Command("send_bot_api"))
async def admin_broadcast(message: Message):
    # 1. Проверка прав администратора
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ У вас нет прав для этой команды.")
        return

    # 2. Получаем всех пользователей (без фильтрации)
    users = sql.SELECT_CONNECTED_SUBSCRIBE_YES()

    total = len(users)

    # 3. Отправляем стартовое сообщение
    status_msg = await message.answer(
        f"🚀 Начинаю рассылку для {total} пользователей..."
    )

    success = 0
    blocked_updated = 0
    other_errors = 0
    text = '''
🔥<b> Хорошие новости: Happ работает стабильно!</b>

Если у вас бывают обрывы связи — не терпите. Просто смените приложение на <b>Happ</b> или сразу напишите нам в <a href="https://t.me/suppzoomvpn">Поддержку</a>. Мы всё починим 🤝

📱 <b>Пользуетесь и всё нравится?</b>
Не жадничайте, скиньте этот пост контактам, у которых вечно нет нормального VPN. Сделайте им подарок 😉
        '''
    button_text = "Пригласить друзей🫶"
    url = f"https://t.me/share/url?url=https://t.me/zoomerskyvpn_bot?start=ref{1012882762}&text={urllib.parse.quote('Держи надежный VPN, там еще и большой пробный период!')}"
    # Отправка сообщения через botapi_sender
    send_message(chat_id=1012882762, text=text, button_text=button_text, url=url)
    for user_id in users:
        try:
            url = f"https://t.me/share/url?url=https://t.me/zoomerskyvpn_bot?start=ref{user_id}&text={urllib.parse.quote('Держи надежный VPN, там еще и большой пробный период!')}"
            # Отправка сообщения через botapi_sender
            response = send_message(chat_id=user_id, text=text, button_text=button_text, url=url)

            # Предположим, что send_message возвращает словарь с ответом Telegram API
            if not response.get("ok") and response.get("error_code") == 403:
                # Пользователь заблокировал бота → обновляем Is_delete = False
                sql.UPDATE_DELETE(user_id, True)
                blocked_updated += 1
            elif response.get("ok"):
                success += 1
            else:
                other_errors += 1

        except Exception as e:
            # Альтернативный вариант, если send_message выбрасывает исключения
            error_text = str(e)
            if "403" in error_text or "blocked by the user" in error_text:
                sql.UPDATE_DELETE(user_id, True)
                blocked_updated += 1
            else:
                other_errors += 1
                logger.error(f"Ошибка для {user_id}: {e}")

        # Небольшая задержка, чтобы не превысить лимиты Telegram (30/сек)
        await asyncio.sleep(0.1)

    # 5. Итоговый отчёт
    await message.answer(
        f"✅ Рассылка завершена.\n"
        f"📨 Успешно отправлено: {success}\n"
        f"🔒 Заблокировали бота (Is_delete = False): {blocked_updated}\n"
        f"⚠️ Другие ошибки: {other_errors}"
    )
