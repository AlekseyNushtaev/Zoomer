import sqlite3
import time
from datetime import datetime

import requests
from sqlalchemy import select, update, insert

from bot import sql, bot, x3
from config import CHANEL_ID, ADMIN_IDS
from keyboard import keyboard_start, keyboard_start_bonus, keyboard_tariff_bonus, keyboard_tariff, \
    keyboard_subscription, ref_keyboard, keyboard_gift_tariff, check_keyboard, create_kb, \
    keyboard_payment_method, keyboard_payment_sbp, keyboard_payment_method_stock
from logging_config import logger
from config_bd.BaseModel import engine, gifts, white_counter
from payments import pay_platega
import asyncio
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, ChatMemberUpdated
from aiogram.filters import ChatMemberUpdatedFilter, KICKED, MEMBER, Command
from lexicon import lexicon, dct_price, dct_desc


router: Router = Router()


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


# Этот хэндлер срабатывает на команду /start
@router.message(Command(commands="start"))
async def process_start_command(message: Message, command: Command):

    user_data = sql.SELECT_ID(message.from_user.id)
    has_paid_subscription = False
    in_chanel = False
    ref_login = ''
    existing = False
    stamp = ''
    ttclid = None

    if user_data:
        has_paid_subscription = user_data[4]
        in_chanel = user_data[7]
        existing = True

    if len(message.text.split(' ')) == 1:
        if user_data:
            logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} нажал старт повторно')
        else:
            logger.success(f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз')

    else:
        if 'ref' in message.text:
            if user_data:
                logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} нажал старт повторно с реферальной ссылкой')
            else:
                logger.success(
                    f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз по реферальной ссылкой')
                ref_login = message.text.split(' ')[1].replace('ref', '')

        elif 'gift_' in message.text:
            logger.info(
                f'Юзер {message.from_user.id} - {message.from_user.username} пытается активировать подарочную подписку')
            gift_id = message.text.split(' ')[1].replace('gift_', '')
            has_paid_subscription = await activate_gift(message, gift_id)
            await asyncio.sleep(2)
            existing = True
        elif 'ttclid_' in message.text:
            if user_data:
                logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} нажал старт повторно с меткой ttclid')
            else:
                logger.success(
                    f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз по метке ttclid')
                stamp = 'YuraTT'
                ttclid = message.text.split(' ')[1].replace('ttclid_', '').replace('_', '.')

                payload = {
                    'event_source': 'web',
                    'event_source_id': 'D5U8OFJC77U9E3ANE170',
                    'data': [
                        {
                            'event': 'Subscribe',
                            'event_time': int(time.time()),
                            'context': {
                                'ad': {
                                    'callback': ttclid
                                }
                            }
                        }
                    ]
                }
                response = requests.post(
                    'https://business-api.tiktok.com/open_api/v1.3/event/track/',
                    json=payload,
                    headers={
                        'Content-Type': 'application/json',
                        'Access-Token': '7a9d82c42eaccd2393b74f31975fb8cc96bbb5d6'
                    },
                    timeout=2
                )

                if response.status_code == 200:
                    logger.success('Пиксель успешно отправлен в TikTok')
                else:
                    logger.error(f'Ошибка TikTok API: статус {response.status_code}, ответ: {response.text}')
        else:
            if user_data:
                logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} нажал старт повторно с меткой')
            else:
                logger.success(
                    f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз по метке')
                stamp = message.text.split(' ')[1]

    if not existing:
        sql.INSERT(message.from_user.id, False, False, ref=ref_login, stamp=stamp)
        logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} добавлен в БД')
        if ttclid:
            sql.UPDATE_TTCLID(message.from_user.id, ttclid)
            logger.info(f'Юзеру {message.from_user.id} - {message.from_user.username} присвоен ttclid')

    if not in_chanel:
        await message.answer(lexicon['to_chanel'], reply_markup=check_keyboard())
        logger.info(
            f'Юзер {message.from_user.id} - {message.from_user.username} перешел на проверку подписки')
        return

    if not has_paid_subscription:
        await message.answer(text=lexicon['start_bonus'],
                             reply_markup=keyboard_start_bonus(),
                             disable_web_page_preview=True)
    else:
        await message.answer(text=lexicon['start'],
                             reply_markup=keyboard_start(),
                             disable_web_page_preview=True)


@router.callback_query(F.data == 'check_channel')
async def check_chanel(callback: CallbackQuery):
    await callback.answer()
    """Проверка подписки на канал"""
    try:
        chat_member = await bot.get_chat_member(
            chat_id=CHANEL_ID,  # Ваш канал ID
            user_id=callback.from_user.id
        )

        if chat_member.status in ["member", "administrator", "creator"]:
            sql.UPDATE_ADMIN(callback.from_user.id)
        user_data = sql.SELECT_ID(callback.from_user.id)
        has_paid_subscription = user_data[4] if user_data else False

        if not has_paid_subscription:
            await callback.message.answer(text=lexicon['start_bonus'],
                                          reply_markup=keyboard_start_bonus(),
                                          disable_web_page_preview=True)
        else:
            await callback.message.answer(text=lexicon['start'],
                                          reply_markup=keyboard_start(),
                                          disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Error checking subscription: {e}")
        await callback.answer('Ошибка проверки подписки. Попробуйте позже.', show_alert=True)


@router.callback_query(F.data == 'buy_vpn')
async def buy_vpn_cb(callback: CallbackQuery):
    await callback.answer()
    user_data = sql.SELECT_ID(callback.from_user.id)
    has_paid_subscription = False

    if user_data is not None and len(user_data) > 4:
        has_paid_subscription = user_data[4]

    result_active = await x3.activ(str(callback.from_user.id))

    if result_active['activ'] == '🔎 - Не подключён' and not has_paid_subscription:
        await callback.message.answer(text=lexicon['buy'],
                                      reply_markup=keyboard_tariff_bonus(),
                                      disable_web_page_preview=True)
    else:
        await callback.message.answer(text=lexicon['buy'],
                                      reply_markup=keyboard_tariff(),
                                      disable_web_page_preview=True)


@router.callback_query(F.data == 'connect_vpn')
async def direct_connect_vpn_cb(callback: CallbackQuery):
    await callback.answer()
    await x3.test_connect()
    user_id = str(callback.from_user.id)
    user_id_white = user_id + '_white'
    sub_url = await x3.sublink(user_id)
    sub_url_white = await x3.sublink(user_id_white)

    if not sub_url and not sub_url_white:
        await callback.message.answer(lexicon['no_sub'])
        return

    await callback.message.answer(
        text=lexicon['to_sub'],
        reply_markup=keyboard_subscription(sub_url, sub_url_white),
        disable_web_page_preview=True
    )


@router.callback_query(F.data.in_({'r_30', 'r_90', 'r_180', 'r_white_30'}))
async def process_payment_method(callback: CallbackQuery):
    await callback.answer()
    if 'white' in callback.data:
        with engine.connect() as conn:
            # Проверяем, есть ли запись в white_counter для этого пользователя
            check_stmt = select(white_counter).where(white_counter.c.user_id == callback.from_user.id)
            result = conn.execute(check_stmt).fetchone()
            if not result:
                # Добавляем запись
                insert_stmt = insert(white_counter).values(user_id=callback.from_user.id)
                conn.execute(insert_stmt)
                conn.commit()
                logger.info(f"✅ Добавлена запись в white_counter для пользователя {callback.from_user.id}")
    tariff = callback.data
    await callback.message.answer('Выберите метод оплаты:', reply_markup=keyboard_payment_method(tariff))


@router.callback_query(F.data.startswith('sbp_'))
async def process_payment_sbp(callback: CallbackQuery):
    await callback.answer()
    gift_flag = False
    white_flag = False
    if 'gift_' in callback.data:
        gift_flag = True
    duration = callback.data.replace('sbp_r_', '').replace('sbp_gift_r_', '')
    desc_key = duration

    rub_amount = dct_price[duration]
    if callback.from_user.id in ADMIN_IDS:
        rub_amount = 1
    user_id = str(callback.from_user.id)

    if 'white' in duration:
        duration = duration.replace('white_', '')
        white_flag = True

    if gift_flag:
        payment_info = await pay_platega.pay_for_gift(
            val=str(rub_amount),
            des=f"Подписка в подарок {dct_desc[desc_key]}",
            user_id=user_id,
            duration=duration,
            white=white_flag,
            payment_method=2,  # 2 = СБП QR
        )
    else:
        payment_info = await pay_platega.pay(
            val=str(rub_amount),
            des=dct_desc[desc_key],
            user_id=user_id,
            duration=duration,
            white=white_flag,
            payment_method=2  # 2 = СБП QR
        )

    if payment_info['status'] == 'pending':
        try:
            text = lexicon['payment_link']
            if white_flag:
                text = lexicon['payment_link_white']
            await callback.message.edit_text(
                text=text,
                reply_markup=keyboard_payment_sbp("💳 Оплатить через СБП", payment_info['url'])
            )
            logger.info(f"Юзер {user_id} создал счет на оплату {'подарка' if gift_flag else ''} {rub_amount} руб")
            
        except Exception as e:
            error_message = f"Ошибка при создании счета: {str(e)}"
            logger.error(error_message)
            await callback.message.answer(lexicon['error_payment'], reply_markup=create_kb(1, back_to_main='🔙 Назад'))


@router.callback_query(F.data == 'free_vpn')
async def free_vpn_cb(callback: CallbackQuery):
    await callback.answer()
    day = 5

    user_data = sql.SELECT_ID(callback.from_user.id)
    has_paid_subscription = False
    if user_data is not None and len(user_data) > 4:
        has_paid_subscription = user_data[4]
    if has_paid_subscription:
        await callback.message.answer(text=lexicon['free_vpn_no'],
                                      reply_markup=keyboard_start())
        return
    # Проверка на наличие данных
    await x3.test_connect()
    logger.info(await x3.addClient(day, str(callback.from_user.id), int(callback.from_user.id)))
    result_active = await x3.activ(str(callback.from_user.id))
    time = result_active['time']

    # Проверка на наличие данных
    if sql.SELECT_ID(callback.from_user.id) is not None:
        sql.UPDATE_PAYNULL(callback.from_user.id)
    else:
        sql.INSERT(callback.from_user.id, True)
    user_id = str(callback.from_user.id)
    sub_url = await x3.sublink(user_id)

    await callback.message.answer(text=lexicon['buy_success'].format(time),
                                  reply_markup=keyboard_subscription(sub_url, None),
                                  disable_web_page_preview=True)


@router.callback_query(F.data == 'info')
async def faq(callback: CallbackQuery):
    await callback.answer()
    user_data = sql.SELECT_ID(callback.from_user.id)
    has_paid_subscription = False
    if user_data is not None and len(user_data) > 4:
        has_paid_subscription = user_data[4]
    if has_paid_subscription:
        await callback.message.answer(
            text=lexicon['start'],
            reply_markup=keyboard_start(),
            disable_web_page_preview=True
        )
    else:
        await callback.message.answer(
            text=lexicon['start_bonus'],
            reply_markup=keyboard_start_bonus(),
            disable_web_page_preview=True
        )


@router.callback_query(F.data == 'ref')
async def referral_program(callback: CallbackQuery):
    await callback.answer()
    count = sql.SELECT_COUNT_REF(int(callback.from_user.id))
    await callback.message.answer(
        text=lexicon['ref_info'].format(count, callback.from_user.id),
        reply_markup=ref_keyboard(callback.from_user.id),
        disable_web_page_preview=True
    )


@router.callback_query(F.data == 'buy_gift')
async def gift_subscription_start(callback: CallbackQuery):
    await callback.answer()
    """Начало процесса подарка подписки"""
    await callback.message.answer(
        lexicon['gift_start'],
        reply_markup=keyboard_gift_tariff()
    )


@router.callback_query(F.data.startswith('gift_'))
async def process_gift_payment_method(callback: CallbackQuery):
    await callback.answer()
    if 'white' in callback.data:
        with engine.connect() as conn:
            # Проверяем, есть ли запись в white_counter для этого пользователя
            check_stmt = select(white_counter).where(white_counter.c.user_id == callback.from_user.id)
            result = conn.execute(check_stmt).fetchone()
            if not result:
                # Добавляем запись
                insert_stmt = insert(white_counter).values(user_id=callback.from_user.id)
                conn.execute(insert_stmt)
                conn.commit()
                logger.info(f"✅ Добавлена запись в white_counter для пользователя {callback.from_user.id}")
    tariff = callback.data
    await callback.message.answer('Выберите метод оплаты подарочной подписки:', reply_markup=keyboard_payment_method(tariff))


async def activate_gift(message: Message, gift_id: str):
    """Активация подарка по gift_id"""

    with engine.connect() as conn:
        # Проверяем существование и статус подарка
        stmt = select(gifts).where(
            gifts.c.gift_id == gift_id,
            gifts.c.flag == False,
            gifts.c.recepient_id == None
        )
        result = conn.execute(stmt).fetchone()

        if not result:
            await message.answer(lexicon['gift_no'])
            logger.warning(f'Ссылка на подарок протухла')
            if sql.SELECT_ID(message.from_user.id) is None:
                sql.INSERT(message.from_user.id, False)
                logger.success(
                    f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз по подарочной ссылке')
            return False

        duration = result.duration
        white_flag = result.white_flag

        # Активируем подарок для текущего пользователя
        stmt = update(gifts).where(gifts.c.gift_id == gift_id).values(
            recepient_id=message.from_user.id,
            flag=True
        )
        conn.execute(stmt)
        conn.commit()

        # Активируем подписку для получателя
        await x3.test_connect()
        user_id = message.from_user.id
        user_id_str = str(message.from_user.id)
        if white_flag:
            user_id_str += '_white'


        # Проверяем существует ли пользователь
        existing_user = await x3.get_user_by_username(user_id_str)

        if existing_user and 'response' in existing_user and existing_user['response']:
            response = await x3.updateClient(duration, user_id_str, user_id)
        else:
            response = await x3.addClient(duration, user_id_str, user_id)

        if response:
            # Получаем информацию о подписке
            result_active = await x3.activ(user_id_str)
            subscription_time = result_active.get('time', '-')

            # Обновляем базу данных
            if sql.SELECT_ID(message.from_user.id) is not None:
                sql.UPDATE_PAYNULL(message.from_user.id)
                logger.info(
                    f'Юзер {message.from_user.id} - {message.from_user.username} получил в подарок подписку, уже был в БД')
            else:
                sql.INSERT(message.from_user.id, True)
                logger.success(
                    f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз и получил подарочную подписку')

            # Отправляем сообщение получателю
            await message.answer(lexicon['gift_yes'].format(duration, subscription_time))
            return True

        else:
            await message.answer("❌ Ошибка при активации подарка. Обратитесь в поддержку.")
            if sql.SELECT_ID(message.from_user.id) is None:
                sql.INSERT(message.from_user.id, False)
            return False


@router.callback_query(F.data == 'back_to_buy_menu')
async def handle_back_to_menu(callback: CallbackQuery):
    """Обработчик для возврата в главное меню из оплаты"""
    await callback.message.answer(text=lexicon['buy'], reply_markup=keyboard_tariff())


@router.callback_query(F.data == 'back_to_main')
async def handle_back_to_menu(callback: CallbackQuery):
    """Обработчик для возврата в главное меню из оплаты"""
    await callback.message.answer(text=lexicon['start'],
                                  reply_markup=keyboard_start(),
                                  disable_web_page_preview=True)


@router.callback_query(F.data == 'back_to_gift_menu')
async def handle_back_to_menu(callback: CallbackQuery):
    """Обработчик для возврата в главное меню из оплаты"""
    await callback.message.edit_text(text=lexicon['gift_start'], reply_markup=keyboard_gift_tariff())


@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=KICKED))
async def user_blocked_bot(event: ChatMemberUpdated):
    sql.UPDATE_DELETE(event.from_user.id, True)
    logger.warning(f'Юзер {event.from_user.id} заблокировал бота')


@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=MEMBER))
async def user_unblocked_bot(event: ChatMemberUpdated):
    sql.UPDATE_DELETE(event.from_user.id, False)
    logger.success(f'Юзер {event.from_user.id} разблокировал бота')


@router.callback_query(F.data == 'r_120')
async def process_payment_method_bonus(callback: CallbackQuery):
    tariff = callback.data
    await callback.message.answer('Выберите метод оплаты акционной подписки:', reply_markup=keyboard_payment_method_stock(tariff))