import random
from datetime import datetime, timezone

from sqlalchemy import select

from bot import sql, x3, bot
from config import ADMIN_IDS
from config_bd.models import Users
from keyboard import create_kb
from logging_config import logger
import asyncio
from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import Command

from sheduler.check_connect import check_connect

router = Router()


@router.message(F.video, F.from_user.id.in_(ADMIN_IDS))
async def get_video(message: Message):
    await message.answer(message.video.file_id)


@router.message(Command(commands=['user']))
async def user_info(message: Message):

    # Проверка прав администратора
    if message.from_user.id not in ADMIN_IDS:
        return

    try:
        # Извлекаем аргументы команды
        args = message.text.split()

        if len(args) < 2:
            await message.answer("❌ Использование: /user <telegram_id>\nНапример: /user 123456789")
            return

        user_id = int(args[1].strip())

        # Проверяем, существует ли пользователь в БД
        user_data = await sql.SELECT_ID(user_id)

        if not user_data:
            await message.answer(f"❌ Пользователь с ID {user_id} не найден в базе данных.")
            return
        text = []
        for i in range(len(user_data)):
            if isinstance(user_data[i], datetime):
                item = user_data[i].strftime('%Y-%m-%d %H:%M:%S')
                text.append(item)
            elif user_data[i] is None:
                text.append('None')
            else:
                text.append(str(user_data[i]))
        text = '\n'.join(text)
        await message.answer(text)
    except Exception as e:
        await message.answer(f'Ошибка при формировании сообщения: {str(e)}')


@router.message(Command(commands=['sub']))
async def set_subscription_date(message: Message):
    """Установка subscription_end_date или white_subscription_end_date в БД (только БД, не в панели)"""

    # Проверка прав администратора
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Эта команда доступна только администраторам.")
        return

    try:
        # Извлекаем аргументы команды
        args = message.text.split()

        if len(args) < 3:
            await message.answer(
                "❌ Использование:\n"
                "  /sub <telegram_id> <дата_время>               – обновить обычную подписку\n"
                "  /sub <telegram_id> white <дата_время>         – обновить белую подписку\n"
                "Примеры:\n"
                "  /sub 123456789 2026-02-01 17:14:27\n"
                "  /sub 123456789 white 2026-02-01 17:14:27\n"
                "Формат даты: YYYY-MM-DD HH:MM:SS"
            )
            return

        user_id = int(args[1].strip())

        # Определяем тип обновляемого поля
        if args[2].lower() == 'white':
            field_type = 'white'
            # Дата начинается с третьего аргумента
            date_str = " ".join(args[3:])
        else:
            field_type = 'regular'
            # Дата начинается со второго аргумента
            date_str = " ".join(args[2:])

        # Парсим дату и время
        try:
            date_formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%d.%m.%Y %H:%M:%S",
                "%d.%m.%Y %H:%M"
            ]

            subscription_date = None
            for date_format in date_formats:
                try:
                    subscription_date = datetime.strptime(date_str, date_format)
                    break
                except ValueError:
                    continue

            if subscription_date is None:
                await message.answer(
                    f"❌ Неверный формат даты: {date_str}\n"
                    "Используйте формат: YYYY-MM-DD HH:MM:SS\n"
                    "Пример: 2026-02-01 17:14:27"
                )
                return

        except ValueError as e:
            await message.answer(f"❌ Ошибка парсинга даты: {e}")
            return

        # Проверяем, существует ли пользователь в БД
        user_data = await sql.SELECT_ID(user_id)

        if not user_data:
            await message.answer(f"❌ Пользователь с ID {user_id} не найден в базе данных.")
            return

        # Обновляем соответствующую дату в БД
        try:
            if field_type == 'white':
                await sql.update_white_subscription_end_date(user_id, subscription_date)
                # Получаем обновлённое значение для проверки (white_subscription_end_date — индекс 10)
                updated_date = user_data[10] if len(user_data) > 10 else None
                field_name = "white_subscription_end_date"
            else:
                await sql.update_subscription_end_date(user_id, subscription_date)
                updated_date = await sql.get_subscription_end_date(user_id)
                field_name = "subscription_end_date"

            await message.answer(
                f"✅ Дата подписки ({field_name}) успешно обновлена!\n\n"
                f"👤 Пользователь: {user_id}\n"
                f"📅 Новая дата окончания: {subscription_date.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"📝 Проверка из БД: {updated_date.strftime('%Y-%m-%d %H:%M:%S') if updated_date else 'Ошибка чтения'}\n\n"
                f"⚠️ Внимание: Изменена только дата в БД. Подписка в панели управления (X3) не изменена."
            )

            logger.info(
                f"Администратор {message.from_user.id} изменил {field_name} для пользователя {user_id} на {subscription_date}")
        except Exception as e:
            await message.answer(f"❌ Ошибка при обновлении даты в БД: {str(e)}")
            logger.error(f"Ошибка update_subscription_end_date: {e}")

    except ValueError:
        await message.answer(
            "❌ Неверный формат Telegram ID или даты.\n"
            "Используйте: /sub 123456789 2026-02-01 17:14:27\n"
            "Или: /sub 123456789 white 2026-02-01 17:14:27"
        )
    except Exception as e:
        logger.error(f"Ошибка в команде /sub: {e}")
        await message.answer(f"❌ Произошла ошибка: {str(e)}")


@router.message(Command(commands=['delete']))
async def delete_user_command(message: Message):
    """Удаление пользователя из БД по Telegram ID"""

    # Проверка прав администратора
    if message.from_user.id not in ADMIN_IDS:
        return

    try:
        # Извлекаем аргументы команды
        args = message.text.split()

        if len(args) < 2:
            await message.answer("❌ Использование: /delete <telegram_id>\nНапример: /delete 123456789")
            return

        user_id_to_delete = int(args[1].strip())

        # Проверяем, существует ли пользователь в БД
        user_data = await sql.SELECT_ID(user_id_to_delete)

        if not user_data:
            await message.answer(f"❌ Пользователь с ID {user_id_to_delete} не найден в базе данных.")
            return

        # Получаем информацию о пользователе для уведомления
        user_info = {
            "user_id": user_data[1],  # User_id
            "ref": user_data[2],  # Ref
            "is_pay_null": user_data[4],  # Is_pay_null
            "is_admin": user_data[7] if len(user_data) > 7 else False  # Is_admin
        }

        # УДАЛЯЕМ ПОЛЬЗОВАТЕЛЯ ИЗ БД
        deletion_success = await sql.DELETE(user_id_to_delete)

        if deletion_success:
            # Логируем действие
            logger.info(f"Администратор {message.from_user.id} удалил пользователя {user_id_to_delete} из БД")

            # Формируем отчет об удалении
            report_message = (
                f"✅ Пользователь успешно удалён из базы данных\n\n"
                f"📋 Информация об удалённом пользователе:\n"
                f"├ ID: {user_info['user_id']}\n"
                f"├ Реферер: {user_info['ref'] if user_info['ref'] else 'нет'}\n"
                f"├ Оплачивал: {'✅ да' if user_info['is_pay_null'] else '❌ нет'}\n"
                f"└ Администратор: {'✅ да' if user_info['is_admin'] else '❌ нет'}\n\n"
                f"⚠️ Пользователь удалён только из базы данных бота.\n"
                f"   Подписка в панели управления (X3) остаётся активной.\n"
                f"   Чтобы удалить полностью, используйте команду /gift на 0 дней."
            )

            await message.answer(report_message)

        else:
            await message.answer(f"❌ Ошибка при удалении пользователя {user_id_to_delete}.\n"
                                 "Возможно, пользователь уже был удалён или произошла ошибка базы данных.")

    except ValueError:
        await message.answer("❌ Неверный формат Telegram ID.\n"
                             "Используйте только цифры, например: /delete 123456789")
    except Exception as e:
        logger.error(f"Ошибка в команде /delete: {e}")
        await message.answer(f"❌ Произошла ошибка при выполнении команды: {str(e)}")


@router.message(Command("online"))
async def check_online(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    users_x3 = await x3.get_all_users()

    active_telegram_ids = []
    for user in users_x3:
        if user['userTraffic']['firstConnectedAt']:
            connected_str = user['userTraffic']['onlineAt']
            try:
                connected_dt = datetime.fromisoformat(connected_str.replace('Z', '+00:00'))
                connected_date = connected_dt.date()
                if connected_date == datetime.now().date():
                    telegram_id = user.get('telegramId')
                    if telegram_id is not None:
                        active_telegram_ids.append(int(telegram_id))
            except (ValueError, TypeError):
                continue

    count_pay = 0
    count_trial = 0
    for tg_id in active_telegram_ids:
        end_date = await sql.get_subscription_end_date(tg_id)
        if end_date is not None:
            days_left = (end_date.date() - datetime.now().date()).days
            if days_left > 5:
                count_pay += 1
            else:
                count_trial += 1
    await message.answer(
        f"Всего юзеров в панели: {len(users_x3)}\n"
        f"Юзеров, которые были онлайн сегодня: {len(active_telegram_ids)}\n"
        f"Юзеры с платной подпиской: {count_pay}\n"
        f"Юзеры на триале: {count_trial}"
    )


@router.message(Command("balance_panel"))
async def check_online(message: Message):
    squad_1 = ['6ba41467-be68-438c-ad6e-5a02f7df826c']
    squad_2 = ['c6973051-58b7-484c-b669-6a123cda465b']
    squad_3 = ['a867561f-8736-4f67-8970-e20fddd00e5e']
    squad_4 = ['29b73cd8-8a68-41cd-99c7-5d30dbac4c71']
    squad_5 = ['d108d4a0-a121-4b52-baee-a97243208179']
    success_count = 0
    fail_count = 0
    if message.from_user.id not in ADMIN_IDS:
        return
    users_x3 = await x3.get_all_users()
    for user in users_x3:
        try:
            await asyncio.sleep(0.3)
            random_squad = random.choice([squad_1, squad_2, squad_3, squad_4, squad_5])
            username = user.get('username', '')
            if 'white' not in username and 'cascade-bridge-system' not in username:
                uuid = user.get('uuid')
                if user['userTraffic']['firstConnectedAt']:
                    connected_str = user['userTraffic']['onlineAt']
                    connected_dt = datetime.fromisoformat(connected_str.replace('Z', '+00:00'))
                    connected_date = connected_dt.date()
                    if connected_date == datetime.now().date() and uuid:
                        if await x3.update_user_squads(uuid, random_squad):
                            success_count += 1
                        else:
                            fail_count += 1
        except:
            pass
    await message.answer(f"{len(users_x3)} - всего юзеров в панели\n{success_count + fail_count} - онлайн сегодня\n{success_count} - обновлено\n{fail_count} - ошибка")


@router.message(Command(commands=['check_connect']))
async def force_check_connect_command(message: Message):
    """Принудительная проверка подключённых пользователей и обновление Is_tarif в БД"""
    if message.from_user.id not in ADMIN_IDS:
        return

    await message.answer("🔄 Запускаю проверку подключений всех пользователей...")
    try:
        await check_connect()  # функция уже содержит логику обновления Is_tarif
        await message.answer("✅ Проверка завершена. Подробности в логах.")
    except Exception as e:
        logger.error(f"Ошибка при выполнении force_check_connect: {e}")
        await message.answer(f"❌ Произошла ошибка: {e}")


@router.message(Command(commands=['sync_panel']))
async def sync_panel(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    await message.answer("🔄 Запускаю синхронизацию пользователей...")

    # 1. Получаем всех пользователей из панели и строим словарь {telegramId: user_data}
    users_panel = await x3.get_all_users()
    panel_dict = {}
    for user in users_panel:
        tg_id = user.get('telegramId')
        if tg_id is not None:
            panel_dict[tg_id] = user

    # 2. Получаем список пользователей, у которых is_pay_null=True и subscription_end_date=None
    users_for_sync = await sql.SELECT_SUBSCRIBED_NOT_IN_PANEL()

    # 3. Статистика
    updated = 0          # обновлено дат в БД
    added_to_panel = 0   # добавлено в панель
    not_found = 0        # не найдено в панели (остались в списке)

    # 4. Обрабатываем каждого пользователя из списка на синхронизацию
    for user_id in users_for_sync:
        # Проверяем, есть ли пользователь в панели
        if user_id in panel_dict:
            user_data = panel_dict[user_id]

            # Получаем expireAt и преобразуем в datetime
            expire_str = user_data.get('expireAt')
            if expire_str:
                try:
                    # Убираем 'Z' и добавляем временную зону UTC
                    expire_dt = datetime.fromisoformat(expire_str.replace('Z', '+00:00'))
                except Exception as e:
                    logger.error(f"Ошибка парсинга expireAt для {user_id}: {e}")
                    continue

                await sql.update_subscription_end_date(user_id, expire_dt)
                updated += 1
                logger.info(f"Обновлена дата для {user_id} до {expire_dt}")
        else:
            # Пользователя нет в панели – добавляем его с нулевым сроком
            # Используем стандартный addClient с day=0 (подписка сразу истекает)
            user_id_str = str(user_id)
            # Пытаемся добавить как обычного (без _white)
            result = await x3.addClient(5, user_id_str, user_id)
            if result:
                added_to_panel += 1
                logger.info(f"Добавлен в панель пользователь {user_id} (day=5)")
                await bot.send_message(user_id,
                                       'Добрый день. Мы создали Вам личный кабинет и начислили 5 дней пробного '
                                       'доступа.\nПерейдите по ссылке, нажав на кнопку 🔗 Подключить VPN',
                                       reply_markup=create_kb(1, connect_vpn='🔗 Подключить VPN'))
            else:
                not_found += 1
                logger.warning(f"Не удалось добавить в панель пользователя {user_id}")

    # 5. Итоговый отчёт
    report = (
        f"✅ Синхронизация завершена.\n"
        f"📊 Всего в панели: {len(users_panel)}\n"
        f"📋 Ожидало синхронизации: {len(users_for_sync)}\n"
        f"🔄 Обновлено дат в БД: {updated}\n"
        f"➕ Добавлено в панель (day=0): {added_to_panel}\n"
        f"❌ Не удалось добавить (ошибки): {not_found}"
    )
    await message.answer(report)
    logger.info(report)




@router.message(Command(commands=['sub_update']))
async def sub_update_command(message: Message):
    """Корректировка срока подписки (обычной или white) в панели и БД."""
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.text.split()
    white_flag = False

    # Разбор аргументов
    if len(args) >= 2 and args[1].lower() == 'white':
        white_flag = True
        if len(args) < 4:
            await message.answer("Использование: /sub_update white <telegram_id> <delta_days>")
            return
        user_id_str = args[2]
        delta_str = args[3]
    else:
        if len(args) < 3:
            await message.answer("Использование: /sub_update <telegram_id> <delta_days>")
            return
        user_id_str = args[1]
        delta_str = args[2]

    try:
        user_id = int(user_id_str)
        delta = int(delta_str)
    except ValueError:
        await message.answer("❌ Неверный формат ID или количества дней.")
        return

    username = str(user_id) + ('_white' if white_flag else '')

    # Проверка наличия пользователя в БД (необязательно, но для информации)
    user_data = await sql.SELECT_ID(user_id)
    if not user_data:
        await message.answer("⚠️ Пользователь не найден в БД, но операция может быть выполнена в панели.")

    try:
        success, new_expire = await x3.adjust_subscription_days(username, delta, user_id)

        if not success:
            await message.answer("❌ Ошибка при обновлении подписки в панели.")
            return

        if new_expire is not None:
            # Обновляем БД (пользователь существовал, изменения в панели успешны)
            if white_flag:
                await sql.update_white_subscription_end_date(user_id, new_expire)
            else:
                await sql.update_subscription_end_date(user_id, new_expire)
            await message.answer(
                f"✅ Подписка обновлена.\n"
                f"📅 Новая дата окончания: {new_expire.strftime('%Y-%m-%d %H:%M:%S')} UTC"
            )
        else:
            # Пользователь был создан через addClient, БД уже обновлена
            # Получаем дату из БД для отчёта
            user_full = await sql.SELECT_ID(user_id)
            if user_full:
                final_date = user_full[10] if white_flag else user_full[
                    9]  # white_subscription_end_date / subscription_end_date
                if final_date:
                    await message.answer(
                        f"✅ Пользователь создан в панели.\n"
                        f"📅 Дата окончания: {final_date.strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                else:
                    await message.answer("✅ Пользователь создан, но дата окончания не определена.")
            else:
                await message.answer("✅ Пользователь создан, но не удалось прочитать дату из БД.")
    except Exception as e:
        logger.error(f"Ошибка в /sub_update: {e}")
        await message.answer(f"❌ Произошла ошибка: {e}")


@router.message(Command(commands=['second_chance']))
async def second_chance_command(message: Message):
    """Добавляет 3 дня подписки пользователям с is_pay_null=True, is_tarif=True и subscription_end_date < 2026-03-06 01:00"""
    if message.from_user.id not in ADMIN_IDS:
        return

    # Фиксированная дата отсечения (UTC)
    cutoff = datetime(2026, 3, 6, 1, 0, 0)

    await message.answer("🔍 Ищу пользователей для акции second_chance...")

    async with sql.session_factory() as session:
        stmt = select(Users).where(
            Users.is_delete == False,
            Users.is_pay_null == True,
            Users.subscription_end_date.isnot(None),
            Users.subscription_end_date < cutoff
        )
        result = await session.execute(stmt)
        users = result.scalars().all()

    total = len(users)
    if total == 0:
        await message.answer("❌ Нет пользователей, подходящих под условия.")
        return

    await message.answer(f"✅ Найдено {total} пользователей. Начинаю обработку...")

    success_count = 0
    fail_count = 0
    msg_fail = 0
    ttclid_updated = 0

    for user in users:
        user_id = user.user_id
        username = str(user_id)  # обычная подписка (без _white)

        try:
            # 1. Добавляем 3 дня в панели и получаем новую дату
            success, new_expire = await x3.adjust_subscription_days(username, 3, user_id)
            if not success or new_expire is None:
                logger.error(f"Ошибка обновления подписки для {user_id}")
                fail_count += 1
                continue

            # 2. Обновляем дату в БД
            await sql.update_subscription_end_date(user_id, new_expire)

            # 3. Отправляем сообщение
            try:
                await bot.send_message(
                    chat_id=user_id,
                    text='''
⚡️ Новые сервера. Космическая скорость. Второй шанс.

Друзья, мы прокачали железо. Теперь наш VPN быстрый настолько, что вы забудете о буферизации. Идеально для игр и стриминговых сервисов.

Это важно: <b>Мы добавили вам еще 3 дня</b>, чтобы вы оценили обновленную скорость.

▶️ Если раньше были проблемы с подключением — вот видеоинструкция. Всё просто и понятно.

Попробуйте сами. 🌐 Будьте свободны!
                    ''',
                    reply_markup=create_kb(1, connect_vpn='🔗 Подключить VPN', video_faq='Смотреть видеоинструкцию')
                )
                success_count += 1
            except Exception as e:
                logger.error(f"Не удалось отправить сообщение {user_id}: {e}")
                msg_fail += 1

            # 4. Обновляем ttclid
            await sql.UPDATE_TTCLID(user_id, 'second_chance_100326')
            ttclid_updated += 1

            # Небольшая задержка, чтобы не нагружать API
            await asyncio.sleep(0.1)

        except Exception as e:
            logger.error(f"Критическая ошибка при обработке {user_id}: {e}")
            fail_count += 1

    # Итоговый отчёт
    report = (
        f"✅ Акция second_chance завершена.\n"
        f"📊 Всего подходящих: {total}\n"
        f"🔄 Успешно обработано: {success_count}\n"
        f"❌ Ошибок обновления подписки: {fail_count}\n"
        f"📨 Ошибок отправки сообщений: {msg_fail}\n"
        f"🏷 Обновлено ttclid: {ttclid_updated}"
    )
    await message.answer(report)
    logger.info(report)


@router.message(Command(commands=['get_second']))
async def get_second_command(message: Message):
    """Проверяет, сколько пользователей с ttclid='second_chance_100326' были онлайн после 10.03.2026"""
    if message.from_user.id not in ADMIN_IDS:
        return

    await message.answer("🔄 Получаю данные из панели и базы...")

    try:
        # 1. Получаем всех пользователей с нужным ttclid
        async with sql.session_factory() as session:
            stmt = select(Users.user_id).where(Users.ttclid == 'second_chance_100326')
            result = await session.execute(stmt)
            user_ids = [row[0] for row in result.all()]

        if not user_ids:
            await message.answer("❌ Нет пользователей с ttclid = second_chance_100326")
            return

        # 2. Загружаем всех пользователей из панели
        panel_users = await x3.get_all_users()  # список словарей с полными данными
        logger.info(f"Загружено {len(panel_users)} пользователей из панели")

        # 3. Строим множество telegram_id из панели для быстрого поиска
        #    и сохраняем дату последнего онлайна
        panel_dict = {}
        for user in panel_users:
            tg_id = user.get('telegramId')
            if tg_id is not None:
                panel_dict[int(tg_id)] = user

        # 4. Проверяем каждого пользователя из списка
        cutoff_date = datetime(2026, 3, 10, 0, 0, 0, tzinfo=timezone.utc)
        online_after_cutoff = 0
        not_found_in_panel = 0
        online_before_or_never = 0

        for uid in user_ids:
            user_panel = panel_dict.get(uid)
            if not user_panel:
                not_found_in_panel += 1
                continue

            # Проверяем onlineAt (последнее подключение)
            online_at_str = user_panel.get('userTraffic', {}).get('onlineAt')
            if not online_at_str:
                online_before_or_never += 1
                await bot.send_message(
                    chat_id=uid,
                    text='''
                ⚡️ Новые сервера. Космическая скорость. Второй шанс.

                Друзья, мы прокачали железо. Теперь наш VPN быстрый настолько, что вы забудете о буферизации. 

                Это важно: <b>Мы добавили вам еще 3 дня</b>, чтобы вы оценили обновленную скорость.

                ▶️ Если раньше были проблемы с подключением — вот видеоинструкция. Всё просто и понятно.

                Попробуйте сами. 🌐 Будьте свободны!
                                    ''',
                    reply_markup=create_kb(1, connect_vpn='🔗 Подключить VPN', video_faq='Смотреть видеоинструкцию')
                )
                await asyncio.sleep(0.05)
                continue

            try:
                online_dt = datetime.fromisoformat(online_at_str.replace('Z', '+00:00'))
                if online_dt >= cutoff_date:
                    online_after_cutoff += 1
                else:
                    online_before_or_never += 1
            except (ValueError, TypeError):
                online_before_or_never += 1

        # 5. Формируем ответ
        report = (
            f"📊 Статистика по ttclid = second_chance_100326\n"
            f"👥 Всего в БД: {len(user_ids)}\n"
            f"✅ Онлайн после 10.03.2026: {online_after_cutoff}\n"
            f"❌ Не были онлайн после 10.03.2026 (или никогда): {online_before_or_never}\n"
            f"🔍 Не найдены в панели: {not_found_in_panel}"
        )
        await message.answer(report)
        logger.info(f"Админ {message.from_user.id} выполнил /get_second: {report}")

    except Exception as e:
        logger.error(f"Ошибка в /get_second: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")