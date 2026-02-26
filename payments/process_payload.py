import uuid
from datetime import datetime

from sqlalchemy import insert

from bot import x3, sql
from handlers.handlers_user import bot

from config_bd.BaseModel import engine, gifts, payments_stars
from keyboard import create_kb, keyboard_sub_after_buy
from lexicon import lexicon
from logging_config import logger


async def process_confirmed_payment(payload):
    """Обработка подтвержденного платежа"""
    try:
        # Парсим payload
        payload_parts = dict(item.split(':') for item in payload.split(','))
        user_id = int(payload_parts.get('user_id', 0))
        duration = int(payload_parts.get('duration', 0))
        white_flag = payload_parts.get('white', 'False') == 'True'
        is_gift = payload_parts.get('gift', 'False') == 'True'
        method = payload_parts.get('method', '')
        if method in ('sbp, stars'):
            amount = int(payload_parts.get('amount', 0))
        else:
            amount = float(payload_parts.get('amount', 0.0))

        logger.info(
            f"Обработка подтвержденного платежа для user={user_id}, duration={duration}, white={white_flag}, "
            f"gift={is_gift}, method={method}, amount={amount}")
        if method == 'sbp':
            currency = 'руб'
        elif method == 'stars':
            currency = '⭐️'
            try:
                with engine.connect() as conn:
                    stmt = insert(payments_stars).values(
                        user_id=user_id,
                        amount=amount,
                        is_gift=is_gift,
                    )
                    conn.execute(stmt)
                    conn.commit()
                logger.success(f"💰 Платёж Telegram Stars записан: user_id={user_id}, amount={amount}, is_gift={is_gift}")
            except Exception as e:
                logger.error(f"❌ Ошибка записи платежа Telegram Stars: {e}")
        elif method in ('ton', 'usdt'):
            currency = method.upper()
        else:
            currency = ''

        if is_gift:
            # Обработка подарка
            gift_id = str(uuid.uuid4())

            with engine.connect() as conn:
                stmt = insert(gifts).values(
                    gift_id=gift_id,
                    giver_id=user_id,
                    duration=duration,
                    recepient_id=None,
                    white_flag=white_flag,
                    flag=False
                )
                conn.execute(stmt)
                conn.commit()

            logger.info(f"✅ Запись о подарке создана: gift_id={gift_id}")

            # Отправляем сообщение с ссылкой на подарок
            marker = ' с обходом белых листов 🔥🔥🔥' if white_flag else ''
            gift_message = lexicon['payment_gift'].format(duration, marker, gift_id)

            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=gift_message,
                    disable_web_page_preview=True
                )

                # Второе сообщение с инструкцией
                await bot.send_message(
                    chat_id=user_id,
                    text=lexicon['payment_gift_faq'],
                    reply_markup=create_kb(1, back_to_main='🔙 Назад')
                )

                logger.info(f"✅ Сообщения о подарке отправлены пользователю {user_id}")

            except Exception as e:
                logger.error(f"❌ Ошибка отправки сообщения о подарке: {e}")

        else:
            # Обработка обычного платежа
            x3.test_connect()
            user_id_str = str(user_id)
            if white_flag:
                user_id_str += '_white'

            # Проверяем существует ли пользователь
            existing_user = x3.get_user_by_username(user_id_str)

            if existing_user and 'response' in existing_user and existing_user['response']:
                logger.info(f"⏫ Обновляем {user_id_str} на {duration} дней")
                response = x3.updateClient(duration, user_id_str, user_id)
            else:
                logger.info(f"➕ Добавляем {user_id_str} на {duration} дней")
                response = x3.addClient(duration, user_id_str, user_id)

            if not response:
                logger.error(f"❌ Не удалось обновить клиента {user_id_str}")
                return

            # Получаем информацию о подписке
            result_active = x3.activ(user_id_str)
            subscription_time = result_active.get('time', '-')

            # Обновляем дату окончания подписки в БД
            if subscription_time != '-':
                try:
                    subscription_end_date = datetime.strptime(subscription_time, '%d-%m-%Y %H:%M МСК')
                    if white_flag:
                        sql.update_white_subscription_end_date(user_id, subscription_end_date)
                    else:
                        sql.update_subscription_end_date(user_id, subscription_end_date)
                    logger.info(f"✅ Дата подписки обновлена: {subscription_end_date}")
                except ValueError as e:
                    logger.error(f"❌ Ошибка парсинга даты: {e}")

            # Проверка реферальной системы
            try:
                user_data = sql.SELECT_ID(user_id)
                if user_data and len(user_data) > 4:
                    is_pay_null = user_data[4]  # Поле Is_pay_null
                    ref_id_str = user_data[2]  # Поле Ref

                    # Если это первый платеж пользователя и есть реферер
                    if not is_pay_null and ref_id_str:
                        try:
                            ref_id = int(ref_id_str)
                            ref_data = sql.SELECT_ID(ref_id)

                            if ref_data and len(ref_data) > 4:
                                ref_is_pay_null = ref_data[4]

                                # Если реферер уже оплачивал
                                if ref_is_pay_null:
                                    logger.info(f"🎁 Начисляем 7 дней рефереру {ref_id} за приглашение")

                                    # Добавляем 7 дней подписки рефереру
                                    x3.test_connect()
                                    ref_existing = x3.get_user_by_username(str(ref_id))

                                    if ref_existing and 'response' in ref_existing and ref_existing['response']:
                                        x3.updateClient(7, str(ref_id), ref_id)
                                        logger.info(f"✅ Обновлена подписка реферера {ref_id} на 7 дней")

                                    # Обновляем дату подписки реферера в БД
                                    ref_result_active = x3.activ(str(ref_id))
                                    ref_subscription_time = ref_result_active.get('time', '-')

                                    if ref_subscription_time != '-':
                                        try:
                                            ref_subscription_end_date = datetime.strptime(ref_subscription_time,
                                                                                          '%d-%m-%Y %H:%M МСК')
                                            sql.update_subscription_end_date(ref_id, ref_subscription_end_date)
                                            logger.info(f"✅ Дата подписки реферера обновлена")
                                        except ValueError as e:
                                            logger.error(f"❌ Ошибка парсинга даты реферера: {e}")

                                    # Отправляем уведомление рефереру
                                    try:
                                        await bot.send_message(
                                            chat_id=ref_id,
                                            text=lexicon['ref_success'].format(user_id),
                                            reply_markup=create_kb(1, back_to_main='🔙 Назад')
                                        )
                                        logger.info(f"✅ Уведомление отправлено рефереру {ref_id}")
                                    except Exception as e:
                                        logger.error(f"❌ Ошибка отправки уведомления рефереру: {e}")

                        except (ValueError, Exception) as e:
                            logger.error(f"❌ Ошибка при обработке реферальной системы: {e}")
            except Exception as e:
                logger.error(f"❌ Ошибка при проверке реферальной системы: {e}")

            # Обновляем статус оплаты в БД users
            if sql.SELECT_ID(user_id) is not None:
                sql.UPDATE_PAYNULL(user_id)
            else:
                sql.INSERT(user_id, True)

            # Отправляем уведомление пользователю
            try:
                sub_link = x3.sublink(user_id_str)
                marker = 'продлена' if existing_user else 'активирована'
                message_text = lexicon['payment_success'].format(marker, subscription_time, amount, currency, duration, sub_link)

                await bot.send_message(
                    chat_id=user_id,
                    text=message_text,
                    parse_mode='HTML',
                    disable_web_page_preview=True,
                    reply_markup=keyboard_sub_after_buy(sub_link)
                )

                logger.info(f"✅ Уведомление отправлено пользователю {user_id}")

            except Exception as e:
                logger.error(f"❌ Ошибка отправки уведомления: {e}")

    except Exception as e:
        logger.error(f"❌ Ошибка обработки подтвержденного платежа: {e}")