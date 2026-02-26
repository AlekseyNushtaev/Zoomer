import calendar
from datetime import datetime
from io import BytesIO
from typing import List, Optional

import openpyxl
from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message, BufferedInputFile
from openpyxl.styles import Alignment, Border, Side, PatternFill
from openpyxl.chart import LineChart, BarChart, Reference
from sqlalchemy import select, func, and_

from bot import sql
from config import ADMIN_IDS
from logging_config import logger
from config_bd.BaseModel import engine, users, payments, payments_stars, payments_cryptobot


router = Router()

REF_ZALIV = [
    '1012882762', '1751833324', '7715104509', '6045891248', '778794666',
    '6803123509', '7623377322', '8036879919', '8185054692', '7208737418',
    '7545883972', '7801801881', '7231201607', '7863386911', '7251811519',
    '7717099908', '6514719405', '8154969535', '8196772935', '7985311643',
    '7607443801', '7617180616', '7780587251', '7999153238', '8075803624',
    '7774377890', '7939767168'
]

EXCLUDE_IDS = list(range(45, 1046))


# ---------- Вспомогательные функции конвертации ----------
def convert_stars_to_rub(amount: int) -> Optional[int]:
    """
    Конвертирует сумму в звёздах в рубли.
    Возвращает None, если сумма не соответствует ни одному тарифу.
    """
    mapping = {
        66: 99,
        179: 269,
        199: 299,
        333: 499
    }
    return mapping.get(amount)


def convert_crypto_to_rub(currency: str, amount: str) -> Optional[int]:
    """
    Конвертирует сумму в криптовалюте (TON, USDT) в рубли.
    Возвращает None, если валюта или сумма не соответствуют тарифам.
    """
    mapping = {
        'TON': {'0.9': 99, '2.5': 269, '2.8': 299, '4.6': 499},
        'USDT': {'1.3': 99, '3.5': 269, '4.0': 299, '6.5': 499}
    }
    return mapping.get(currency, {}).get(amount)


# ---------- Вспомогательный класс для унификации записей о платежах ----------
class PaymentRecord:
    """Унифицированная запись о платеже."""
    def __init__(self, amount: int, is_gift: bool, time_created: datetime):
        self.amount = amount
        self.is_gift = is_gift
        self.time_created = time_created


@router.message(Command(commands=['stat']))
async def stat_command(message: Message):
    """Статистика по пользователям с указанным Ref или stamp (только для админов)."""
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.text.split()
    if len(args) < 2:
        await message.answer("❌ Использование: /stat <аргумент>")
        return

    arg = args[1].strip()
    total, with_sub, with_tarif, total_payments, source = sql.get_stat_by_ref_or_stamp(arg)

    if total is None:
        await message.answer(f"{arg} - нет совпадений")
    else:
        await message.answer(f"{arg} {total} {with_sub} {with_tarif} {total_payments}")


@router.message(Command(commands=['anal']))
async def analytics_command(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    # --- Определение текущего месяца ---
    now = datetime.now()
    start_date = datetime(now.year, now.month, 1, 0, 0, 0)
    last_day = calendar.monthrange(now.year, now.month)[1]
    end_date = datetime(now.year, now.month, last_day, 23, 59, 59)

    with engine.connect() as conn:
        # --- 1. Новые пользователи, взяли ключ, подключились ---
        stmt_users = select(
            users.c.Id,
            users.c.User_id,
            users.c.Ref,
            users.c.stamp,
            users.c.Is_pay_null,
            users.c.Is_tarif
        ).where(
            and_(
                users.c.Create_user.between(start_date, end_date),
                ~users.c.Id.in_(EXCLUDE_IDS)
            )
        )
        users_data = conn.execute(stmt_users).fetchall()

        # Инициализация списков
        new_total = []
        new_zaliv = []
        new_saraf = []

        key_total = []
        key_zaliv = []
        key_saraf = []

        connect_total = []
        connect_zaliv = []
        connect_saraf = []

        # Множества для быстрой проверки принадлежности к группам
        set_new_total = set()
        set_new_zaliv = set()
        set_new_saraf = set()

        for row in users_data:
            is_zaliv = (row.stamp != '') or (row.Ref in REF_ZALIV)
            user_id = row.User_id

            new_total.append(user_id)
            set_new_total.add(user_id)
            if is_zaliv:
                new_zaliv.append(user_id)
                set_new_zaliv.add(user_id)
            else:
                new_saraf.append(user_id)
                set_new_saraf.add(user_id)

            if row.Is_pay_null:
                key_total.append(user_id)
                if is_zaliv:
                    key_zaliv.append(user_id)
                else:
                    key_saraf.append(user_id)

            if row.Is_tarif:
                connect_total.append(user_id)
                if is_zaliv:
                    connect_zaliv.append(user_id)
                else:
                    connect_saraf.append(user_id)

        # --- 2. Формируем множество пользователей, которые когда-либо платили (все таблицы) ---
        # Платежи из основной таблицы payments (confirmed, не 1)
        stmt_paid_main = select(payments.c.user_id).distinct().where(
            and_(
                payments.c.status == 'confirmed',
                payments.c.amount != 1
            )
        )
        paid_main = {row[0] for row in conn.execute(stmt_paid_main)}

        # Платежи из payments_stars (confirmed)
        stmt_paid_stars = select(payments_stars.c.user_id).distinct().where(
            payments_stars.c.status == 'confirmed'
        )
        paid_stars = {row[0] for row in conn.execute(stmt_paid_stars)}

        # Платежи из payments_cryptobot (paid, не админские 0.02)
        stmt_paid_crypto = select(payments_cryptobot.c.user_id).distinct().where(
            and_(
                payments_cryptobot.c.status == 'paid',
                payments_cryptobot.c.amount > 0.02  # исключаем тестовые платежи
            )
        )
        paid_crypto = {row[0] for row in conn.execute(stmt_paid_crypto)}

        all_paid_users = paid_main.union(paid_stars).union(paid_crypto)

        # --- 3. Платежи новых пользователей за период (из всех таблиц, конвертированные) ---
        # Собираем все платежи за период в унифицированном виде
        all_period_payments: List[PaymentRecord] = []

        # Основные платежи
        stmt_main = select(
            payments.c.user_id,
            payments.c.amount,
            payments.c.is_gift,
            payments.c.time_created
        ).where(
            and_(
                payments.c.time_created.between(start_date, end_date),
                payments.c.amount != 1,
                payments.c.status == 'confirmed'
            )
        )
        for user_id, amount, is_gift, time_created in conn.execute(stmt_main):
            all_period_payments.append(PaymentRecord(amount, is_gift, time_created))

        # Звёздные платежи
        stmt_stars = select(
            payments_stars.c.user_id,
            payments_stars.c.amount,
            payments_stars.c.is_gift,
            payments_stars.c.time_created
        ).where(
            and_(
                payments_stars.c.time_created.between(start_date, end_date),
                payments_stars.c.status == 'confirmed'
            )
        )
        for user_id, amount, is_gift, time_created in conn.execute(stmt_stars):
            rub = convert_stars_to_rub(amount)
            if rub is not None:
                all_period_payments.append(PaymentRecord(rub, is_gift, time_created))

        # Крипто-платежи
        stmt_crypto = select(
            payments_cryptobot.c.user_id,
            payments_cryptobot.c.amount,
            payments_cryptobot.c.currency,
            payments_cryptobot.c.is_gift,
            payments_cryptobot.c.time_created
        ).where(
            and_(
                payments_cryptobot.c.time_created.between(start_date, end_date),
                payments_cryptobot.c.status == 'paid',
                payments_cryptobot.c.amount > 0.02
            )
        )
        for user_id, amount, currency, is_gift, time_created in conn.execute(stmt_crypto):
            rub = convert_crypto_to_rub(currency, str(amount))
            if rub is not None:
                all_period_payments.append(PaymentRecord(rub, is_gift, time_created))

        # Фильтруем только платежи новых пользователей (зарегистрированных в этом месяце)
        pay_sum_total = 0
        pay_sum_zaliv = 0
        pay_sum_saraf = 0
        pay_users_total = set()
        pay_users_zaliv = set()
        pay_users_saraf = set()

        # Переделаем: Сначала соберём все платежи за период с user_id, а потом отфильтруем.
        new_payments_data = []  # (user_id, amount)

        # Основные
        stmt_main2 = select(
            payments.c.user_id,
            payments.c.amount
        ).where(
            and_(
                payments.c.time_created.between(start_date, end_date),
                payments.c.amount != 1,
                payments.c.status == 'confirmed'
            )
        )
        for uid, amt in conn.execute(stmt_main2):
            if uid in set_new_total:
                new_payments_data.append((uid, amt))

        # Звёзды
        stmt_stars2 = select(
            payments_stars.c.user_id,
            payments_stars.c.amount
        ).where(
            and_(
                payments_stars.c.time_created.between(start_date, end_date),
                payments_stars.c.status == 'confirmed'
            )
        )
        for uid, amt in conn.execute(stmt_stars2):
            if uid in set_new_total:
                rub = convert_stars_to_rub(amt)
                if rub:
                    new_payments_data.append((uid, rub))

        # Крипто
        stmt_crypto2 = select(
            payments_cryptobot.c.user_id,
            payments_cryptobot.c.amount,
            payments_cryptobot.c.currency
        ).where(
            and_(
                payments_cryptobot.c.time_created.between(start_date, end_date),
                payments_cryptobot.c.status == 'paid',
                payments_cryptobot.c.amount > 0.02
            )
        )
        for uid, amt, cur in conn.execute(stmt_crypto2):
            if uid in set_new_total:
                rub = convert_crypto_to_rub(cur, str(amt))
                if rub:
                    new_payments_data.append((uid, rub))

        # Теперь суммируем по группам
        for uid, amount in new_payments_data:
            pay_sum_total += amount
            pay_users_total.add(uid)
            if uid in set_new_zaliv:
                pay_sum_zaliv += amount
                pay_users_zaliv.add(uid)
            elif uid in set_new_saraf:
                pay_sum_saraf += amount
                pay_users_saraf.add(uid)

        # --- 4. Общая статистика всех платежей за период (все пользователи) ---
        # Собираем все платежи за период в рублях
        all_payments = []  # (amount, is_gift, time_created)

        # Основные
        stmt_main_all = select(
            payments.c.amount,
            payments.c.is_gift,
            payments.c.time_created
        ).where(
            and_(
                payments.c.time_created.between(start_date, end_date),
                payments.c.amount != 1,
                payments.c.status == 'confirmed'
            )
        )
        for amount, is_gift, time_created in conn.execute(stmt_main_all):
            all_payments.append((amount, is_gift, time_created))

        # Звёзды
        stmt_stars_all = select(
            payments_stars.c.amount,
            payments_stars.c.is_gift,
            payments_stars.c.time_created
        ).where(
            and_(
                payments_stars.c.time_created.between(start_date, end_date),
                payments_stars.c.status == 'confirmed'
            )
        )
        for amount, is_gift, time_created in conn.execute(stmt_stars_all):
            rub = convert_stars_to_rub(amount)
            if rub:
                all_payments.append((rub, is_gift, time_created))

        # Крипто
        stmt_crypto_all = select(
            payments_cryptobot.c.amount,
            payments_cryptobot.c.currency,
            payments_cryptobot.c.is_gift,
            payments_cryptobot.c.time_created
        ).where(
            and_(
                payments_cryptobot.c.time_created.between(start_date, end_date),
                payments_cryptobot.c.status == 'paid',
                payments_cryptobot.c.amount > 0.02
            )
        )
        for amount, currency, is_gift, time_created in conn.execute(stmt_crypto_all):
            rub = convert_crypto_to_rub(currency, str(amount))
            if rub:
                all_payments.append((rub, is_gift, time_created))

        # Общая выручка и количество платежей
        total_revenue = sum(p[0] for p in all_payments)
        total_payments_count = len(all_payments)

        # AOV
        aov = total_revenue / total_payments_count if total_payments_count else 0

        # Общее количество пользователей (исключая тестовые ID)
        stmt_total_users = select(func.count(users.c.Id)).where(~users.c.Id.in_(EXCLUDE_IDS))
        total_users_count = conn.execute(stmt_total_users).scalar() or 0

        # ARPU
        arpu = total_revenue / total_users_count if total_users_count else 0

        # Разбивка по суммам: 99, 269, 299, 499 и подарки
        sum_99_count = 0
        sum_99_amount = 0
        sum_269_count = 0
        sum_269_amount = 0
        sum_299_count = 0
        sum_299_amount = 0
        sum_499_count = 0
        sum_499_amount = 0
        gift_count = 0
        gift_amount = 0

        for amount, is_gift, _ in all_payments:
            if is_gift:
                gift_count += 1
                gift_amount += amount
            else:
                if amount == 99:
                    sum_99_count += 1
                    sum_99_amount += amount
                elif amount == 269:
                    sum_269_count += 1
                    sum_269_amount += amount
                elif amount == 299:
                    sum_299_count += 1
                    sum_299_amount += amount
                elif amount == 499:
                    sum_499_count += 1
                    sum_499_amount += amount

        # Разбивка по 4 периодам внутри месяца (примерно равные)
        total_days = last_day
        chunk_size = total_days // 4
        period_starts = []
        period_ends = []
        for i in range(4):
            start_day = 1 + i * chunk_size
            if i == 3:
                end_day = last_day
            else:
                end_day = start_day + chunk_size - 1
            period_starts.append(datetime(now.year, now.month, start_day, 0, 0, 0))
            period_ends.append(datetime(now.year, now.month, end_day, 23, 59, 59))

        period_revenues = [0, 0, 0, 0]
        period_counts = [0, 0, 0, 0]

        for amount, is_gift, time_created in all_payments:
            for i, (p_start, p_end) in enumerate(zip(period_starts, period_ends)):
                if p_start <= time_created <= p_end:
                    period_revenues[i] += amount
                    period_counts[i] += 1
                    break

        # Формирование строк для периодов
        period_lines = []
        for i in range(4):
            rev = period_revenues[i]
            cnt = period_counts[i]
            avg = rev / cnt if cnt else 0
            period_lines.append(
                f"{i+1} Период ({period_starts[i].strftime('%d.%m')} – {period_ends[i].strftime('%d.%m')}): "
                f"{rev} ₽ / {cnt} плат. (ср. {avg:.2f} ₽)"
            )

    # --- 5. Формирование отчёта ---
    report = (
        f"📊 Аналитика за период {start_date.strftime('%d.%m.%Y')} – {end_date.strftime('%d.%m.%Y')}\n\n"
        f"👥 <b>Новые пользователи:</b>\n"
        f"├ Всего: {len(new_total)}\n"
        f"├ Залив: {len(new_zaliv)}\n"
        f"└ Сарафанка: {len(new_saraf)}\n\n"
        f"🔑 <b>Взяли ключ:</b>\n"
        f"├ Всего: {len(key_total)}\n"
        f"├ Залив: {len(key_zaliv)}\n"
        f"└ Сарафанка: {len(key_saraf)}\n\n"
        f"🔗 <b>Подключились:</b>\n"
        f"├ Всего: {len(connect_total)}\n"
        f"├ Залив: {len(connect_zaliv)}\n"
        f"└ Сарафанка: {len(connect_saraf)}\n\n"
        f"💰 <b>Платежи новых пользователей (сумма, исключая 1₽):</b>\n"
        f"├ Всего: {pay_sum_total} ₽ (уникальных плательщиков: {len(pay_users_total)})\n"
        f"├ Залив: {pay_sum_zaliv} ₽ (уникальных: {len(pay_users_zaliv)})\n"
        f"└ Сарафанка: {pay_sum_saraf} ₽ (уникальных: {len(pay_users_saraf)})\n\n"
        f"📈 <b>Общая статистика платежей за период:</b>\n"
        f"├ Общая выручка: {total_revenue} ₽\n"
        f"├ Всего платежей: {total_payments_count}\n"
        f"├ AOV (средний чек): {aov:.2f} ₽\n"
        f"├ ARPU (на всех пользователей*): {arpu:.2f} ₽\n"
        f"├ Платежей 99₽: {sum_99_count} шт., сумма {sum_99_amount} ₽\n"
        f"├ Платежей 269₽: {sum_269_count} шт., сумма {sum_269_amount} ₽\n"
        f"├ Платежей 299₽: {sum_299_count} шт., сумма {sum_299_amount} ₽\n"
        f"├ Платежей 499₽: {sum_499_count} шт., сумма {sum_499_amount} ₽\n"
        f"└ Подарки (is_gift): {gift_count} шт., сумма {gift_amount} ₽\n\n"
        f"📅 <b>Доход по периодам:</b>\n"
    )
    # Добавляем строки периодов
    for line in period_lines:
        report += f"├ {line}\n"
    # Примечание про ARPU
    report += f"\n* – общее количество пользователей (исключая ID 45–1045): {total_users_count}"

    await message.answer(report)


@router.message(Command(commands=['anal_export']))
async def analytics_export(message: Message):
    """Экспорт помесячной аналитики в Excel (с января текущего года по текущий месяц)"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Команда доступна только администраторам.")
        return

    await message.answer("🔄 Формирую помесячную аналитику...")

    try:
        now = datetime.now()
        current_year = now.year
        current_month = now.month

        months = []
        for month in range(1, current_month + 1):
            months.append((current_year, month))

        monthly_data = {}
        daily_data_by_month = {}

        for year, month in months:
            start_date = datetime(year, month, 1, 0, 0, 0)
            last_day = calendar.monthrange(year, month)[1]
            end_date = datetime(year, month, last_day, 23, 59, 59)
            month_key = start_date.strftime('%B %Y')

            with engine.connect() as conn:
                # --- 1. Новые пользователи за месяц ---
                stmt_new_users = select(
                    users.c.User_id,
                    users.c.Ref,
                    users.c.stamp,
                    users.c.Is_pay_null,
                    users.c.Is_tarif,
                    users.c.Create_user
                ).where(
                    and_(
                        users.c.Create_user.between(start_date, end_date),
                        ~users.c.Id.in_(EXCLUDE_IDS)
                    )
                )
                new_users = conn.execute(stmt_new_users).fetchall()

                # Инициализация списков для помесячной статистики
                new_total = []
                new_zaliv = []
                new_saraf = []
                key_total = []
                key_zaliv = []
                key_saraf = []
                connect_total = []
                connect_zaliv = []
                connect_saraf = []

                set_new_total = set()
                set_new_zaliv = set()
                set_new_saraf = set()

                # Для поденной статистики
                daily_stats = {day: {
                    'new': 0,
                    'key': 0,
                    'connect': 0,
                    'paid': 0
                } for day in range(1, last_day + 1)}

                for row in new_users:
                    is_zaliv = (row.stamp != '') or (row.Ref in REF_ZALIV)
                    uid = row.User_id
                    create_day = row.Create_user.day

                    new_total.append(uid)
                    set_new_total.add(uid)
                    if is_zaliv:
                        new_zaliv.append(uid)
                        set_new_zaliv.add(uid)
                    else:
                        new_saraf.append(uid)
                        set_new_saraf.add(uid)

                    if row.Is_pay_null:
                        key_total.append(uid)
                        if is_zaliv:
                            key_zaliv.append(uid)
                        else:
                            key_saraf.append(uid)

                    if row.Is_tarif:
                        connect_total.append(uid)
                        if is_zaliv:
                            connect_zaliv.append(uid)
                        else:
                            connect_saraf.append(uid)

                    daily_stats[create_day]['new'] += 1
                    if row.Is_pay_null:
                        daily_stats[create_day]['key'] += 1
                    if row.Is_tarif:
                        daily_stats[create_day]['connect'] += 1

                # --- 2. Множество плативших пользователей (все таблицы) ---
                stmt_paid_main = select(payments.c.user_id).distinct().where(
                    and_(
                        payments.c.status == 'confirmed',
                        payments.c.amount != 1
                    )
                )
                paid_main = {row[0] for row in conn.execute(stmt_paid_main)}

                stmt_paid_stars = select(payments_stars.c.user_id).distinct().where(
                    payments_stars.c.status == 'confirmed'
                )
                paid_stars = {row[0] for row in conn.execute(stmt_paid_stars)}

                stmt_paid_crypto = select(payments_cryptobot.c.user_id).distinct().where(
                    and_(
                        payments_cryptobot.c.status == 'paid',
                        payments_cryptobot.c.amount > 0.02
                    )
                )
                paid_crypto = {row[0] for row in conn.execute(stmt_paid_crypto)}

                all_paid_users = paid_main.union(paid_stars).union(paid_crypto)

                # Для поденной статистики отметим paid
                for uid in set_new_total:
                    if uid in all_paid_users:
                        # найдём день регистрации
                        for row in new_users:
                            if row.User_id == uid:
                                daily_stats[row.Create_user.day]['paid'] += 1
                                break

                # --- 3. Платежи новых пользователей за этот месяц (все таблицы) ---
                new_payments_amounts = []

                # Основные
                stmt_main_new = select(
                    payments.c.user_id,
                    payments.c.amount
                ).where(
                    and_(
                        payments.c.time_created.between(start_date, end_date),
                        payments.c.amount != 1,
                        payments.c.status == 'confirmed'
                    )
                )
                for uid, amt in conn.execute(stmt_main_new):
                    if uid in set_new_total:
                        new_payments_amounts.append((uid, amt))

                # Звёзды
                stmt_stars_new = select(
                    payments_stars.c.user_id,
                    payments_stars.c.amount
                ).where(
                    and_(
                        payments_stars.c.time_created.between(start_date, end_date),
                        payments_stars.c.status == 'confirmed'
                    )
                )
                for uid, amt in conn.execute(stmt_stars_new):
                    if uid in set_new_total:
                        rub = convert_stars_to_rub(amt)
                        if rub:
                            new_payments_amounts.append((uid, rub))

                # Крипто
                stmt_crypto_new = select(
                    payments_cryptobot.c.user_id,
                    payments_cryptobot.c.amount,
                    payments_cryptobot.c.currency
                ).where(
                    and_(
                        payments_cryptobot.c.time_created.between(start_date, end_date),
                        payments_cryptobot.c.status == 'paid',
                        payments_cryptobot.c.amount > 0.02
                    )
                )
                for uid, amt, cur in conn.execute(stmt_crypto_new):
                    if uid in set_new_total:
                        rub = convert_crypto_to_rub(cur, str(amt))
                        if rub:
                            new_payments_amounts.append((uid, rub))

                pay_sum_total = 0
                pay_sum_zaliv = 0
                pay_sum_saraf = 0
                pay_users_total = set()
                pay_users_zaliv = set()
                pay_users_saraf = set()

                for uid, amount in new_payments_amounts:
                    pay_sum_total += amount
                    pay_users_total.add(uid)
                    if uid in set_new_zaliv:
                        pay_sum_zaliv += amount
                        pay_users_zaliv.add(uid)
                    elif uid in set_new_saraf:
                        pay_sum_saraf += amount
                        pay_users_saraf.add(uid)

                # --- 4. Общие платежи за месяц (все пользователи) ---
                all_payments = []  # (amount, is_gift)

                # Основные
                stmt_main_all = select(
                    payments.c.amount,
                    payments.c.is_gift
                ).where(
                    and_(
                        payments.c.time_created.between(start_date, end_date),
                        payments.c.amount != 1,
                        payments.c.status == 'confirmed'
                    )
                )
                for amount, is_gift in conn.execute(stmt_main_all):
                    all_payments.append((amount, is_gift))

                # Звёзды
                stmt_stars_all = select(
                    payments_stars.c.amount,
                    payments_stars.c.is_gift
                ).where(
                    and_(
                        payments_stars.c.time_created.between(start_date, end_date),
                        payments_stars.c.status == 'confirmed'
                    )
                )
                for amount, is_gift in conn.execute(stmt_stars_all):
                    rub = convert_stars_to_rub(amount)
                    if rub:
                        all_payments.append((rub, is_gift))

                # Крипто
                stmt_crypto_all = select(
                    payments_cryptobot.c.amount,
                    payments_cryptobot.c.currency,
                    payments_cryptobot.c.is_gift
                ).where(
                    and_(
                        payments_cryptobot.c.time_created.between(start_date, end_date),
                        payments_cryptobot.c.status == 'paid',
                        payments_cryptobot.c.amount > 0.02
                    )
                )
                for amount, currency, is_gift in conn.execute(stmt_crypto_all):
                    rub = convert_crypto_to_rub(currency, str(amount))
                    if rub:
                        all_payments.append((rub, is_gift))

                total_revenue = sum(p[0] for p in all_payments)
                total_payments_count = len(all_payments)
                aov = total_revenue / total_payments_count if total_payments_count else 0

                stmt_users_cumulative = select(func.count(users.c.Id)).where(
                    and_(
                        users.c.Create_user <= end_date,
                        ~users.c.Id.in_(EXCLUDE_IDS)
                    )
                )
                cumulative_users = conn.execute(stmt_users_cumulative).scalar() or 1
                arpu = total_revenue / cumulative_users

                # Разбивка по суммам
                sum_99_count = 0
                sum_99_amount = 0
                sum_269_count = 0
                sum_269_amount = 0
                sum_299_count = 0
                sum_299_amount = 0
                sum_499_count = 0
                sum_499_amount = 0
                gift_count = 0
                gift_amount = 0

                for amount, is_gift in all_payments:
                    if is_gift:
                        gift_count += 1
                        gift_amount += amount
                    else:
                        if amount == 99:
                            sum_99_count += 1
                            sum_99_amount += amount
                        elif amount == 269:
                            sum_269_count += 1
                            sum_269_amount += amount
                        elif amount == 299:
                            sum_299_count += 1
                            sum_299_amount += amount
                        elif amount == 499:
                            sum_499_count += 1
                            sum_499_amount += amount

                # Сохраняем все метрики для месяца
                monthly_data[month_key] = {
                    'new_total': len(new_total),
                    'new_zaliv': len(new_zaliv),
                    'new_saraf': len(new_saraf),
                    'key_total': len(key_total),
                    'key_zaliv': len(key_zaliv),
                    'key_saraf': len(key_saraf),
                    'connect_total': len(connect_total),
                    'connect_zaliv': len(connect_zaliv),
                    'connect_saraf': len(connect_saraf),
                    'pay_new_sum_total': pay_sum_total,
                    'pay_new_users_total': len(pay_users_total),
                    'pay_new_sum_zaliv': pay_sum_zaliv,
                    'pay_new_users_zaliv': len(pay_users_zaliv),
                    'pay_new_sum_saraf': pay_sum_saraf,
                    'pay_new_users_saraf': len(pay_users_saraf),
                    'total_revenue': total_revenue,
                    'total_payments': total_payments_count,
                    'aov': aov,
                    'arpu': arpu,
                    'cumulative_users': cumulative_users,
                    'sum_99_count': sum_99_count,
                    'sum_99_amount': sum_99_amount,
                    'sum_269_count': sum_269_count,
                    'sum_269_amount': sum_269_amount,
                    'sum_299_count': sum_299_count,
                    'sum_299_amount': sum_299_amount,
                    'sum_499_count': sum_499_count,
                    'sum_499_amount': sum_499_amount,
                    'gift_count': gift_count,
                    'gift_amount': gift_amount,
                }

                # --- Поденные данные (кумулятивные) ---
                stmt_users_before = select(
                    users.c.User_id,
                    users.c.Is_pay_null,
                    users.c.Is_tarif
                ).where(
                    and_(
                        users.c.Create_user < start_date,
                        ~users.c.Id.in_(EXCLUDE_IDS)
                    )
                )
                users_before = conn.execute(stmt_users_before).fetchall()
                cum_users_before = len(users_before)
                cum_key_before = sum(1 for u in users_before if u.Is_pay_null)
                cum_connect_before = sum(1 for u in users_before if u.Is_tarif)

                daily_cumulative = []
                cum_users = cum_users_before
                cum_key = cum_key_before
                cum_connect = cum_connect_before

                for day in range(1, last_day + 1):
                    day_new = daily_stats[day]['new']
                    day_key = daily_stats[day]['key']
                    day_connect = daily_stats[day]['connect']
                    cum_users += day_new
                    cum_key += day_key
                    cum_connect += day_connect
                    daily_cumulative.append({
                        'day': day,
                        'cum_users': cum_users,
                        'cum_key': cum_key,
                        'cum_connect': cum_connect,
                        'new': day_new,
                        'key': day_key,
                        'connect': day_connect,
                        'paid': daily_stats[day]['paid']
                    })

                daily_data_by_month[month_key] = daily_cumulative

        # --- Создание Excel файла ---
        wb = openpyxl.Workbook()
        ws_main = wb.active
        ws_main.title = "Помесячная аналитика"

        headers = ['Показатель'] + list(monthly_data.keys())
        ws_main.append(headers)

        metric_rows = [
            ('Новые пользователи (всего)', 'new_total'),
            ('Новые пользователи (залив)', 'new_zaliv'),
            ('Новые пользователи (сарафан)', 'new_saraf'),
            ('Взяли ключ (всего)', 'key_total'),
            ('Взяли ключ (залив)', 'key_zaliv'),
            ('Взяли ключ (сарафан)', 'key_saraf'),
            ('Подключились (всего)', 'connect_total'),
            ('Подключились (залив)', 'connect_zaliv'),
            ('Подключились (сарафан)', 'connect_saraf'),
            ('Платежи новых (сумма, всего)', 'pay_new_sum_total'),
            ('Платежи новых (уникальных, всего)', 'pay_new_users_total'),
            ('Платежи новых (сумма, залив)', 'pay_new_sum_zaliv'),
            ('Платежи новых (уникальных, залив)', 'pay_new_users_zaliv'),
            ('Платежи новых (сумма, сарафан)', 'pay_new_sum_saraf'),
            ('Платежи новых (уникальных, сарафан)', 'pay_new_users_saraf'),
            ('Общая выручка (₽)', 'total_revenue'),
            ('Количество платежей', 'total_payments'),
            ('AOV (₽)', 'aov'),
            ('ARPU (₽)', 'arpu'),
            ('Пользователей на конец месяца', 'cumulative_users'),
            ('Платежей 99₽ (шт)', 'sum_99_count'),
            ('Сумма 99₽ (₽)', 'sum_99_amount'),
            ('Платежей 269₽ (шт)', 'sum_269_count'),
            ('Сумма 269₽ (₽)', 'sum_269_amount'),
            ('Платежей 299₽ (шт)', 'sum_299_count'),
            ('Сумма 299₽ (₽)', 'sum_299_amount'),
            ('Платежей 499₽ (шт)', 'sum_499_count'),
            ('Сумма 499₽ (₽)', 'sum_499_amount'),
            ('Подарков (шт)', 'gift_count'),
            ('Сумма подарков (₽)', 'gift_amount'),
        ]

        row_idx = 2
        for label, key in metric_rows:
            row = [label]
            ws_main.append(row)
            col_idx = 2
            for month in monthly_data.keys():
                value = monthly_data[month].get(key, 0)
                if key in ('aov', 'arpu'):
                    cell_value = round(value, 2)
                else:
                    cell_value = value if isinstance(value, int) else round(value, 2)
                ws_main.cell(row=row_idx, column=col_idx, value=cell_value)
                col_idx += 1
            row_idx += 1

        # Оформление (цвета и границы)
        yellow_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
        light_green_fill = PatternFill(start_color="CCFFCC", end_color="CCFFCC", fill_type="solid")
        light_red_fill = PatternFill(start_color="FFCCCC", end_color="FFCCCC", fill_type="solid")
        thin_border = Border(left=Side(style='thin'), right=Side(style='thin'),
                             top=Side(style='thin'), bottom=Side(style='thin'))

        # Заголовки
        for cell in ws_main[1]:
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.border = thin_border

        # Данные: раскраска и границы
        month_columns = list(monthly_data.keys())
        for r in range(2, row_idx):
            for c in range(1, ws_main.max_column + 1):
                cell = ws_main.cell(row=r, column=c)
                cell.border = thin_border
            # Январь (первый месяц) – жёлтый
            jan_cell = ws_main.cell(row=r, column=2)
            jan_cell.fill = yellow_fill
            # Сравнение с предыдущим месяцем
            for col_idx in range(3, 2 + len(month_columns)):
                current = ws_main.cell(row=r, column=col_idx)
                prev = ws_main.cell(row=r, column=col_idx-1)
                try:
                    cur_val = float(current.value)
                    prev_val = float(prev.value)
                except (TypeError, ValueError):
                    continue
                if cur_val > prev_val:
                    current.fill = light_green_fill
                elif cur_val < prev_val:
                    current.fill = light_red_fill

        # Автоподбор ширины
        for col in ws_main.columns:
            max_len = 0
            col_letter = col[0].column_letter
            for cell in col:
                if cell.value:
                    max_len = max(max_len, len(str(cell.value)))
            ws_main.column_dimensions[col_letter].width = min(max_len + 2, 50)

        ws_main.freeze_panes = 'B2'

        # --- Листы по месяцам с графиками ---
        for month_key, daily_data in daily_data_by_month.items():
            ws = wb.create_sheet(title=month_key[:31])
            ws.append(['День', 'Новые', 'Взяли ключ', 'Подключились', 'Платили',
                       'Всего пользователей (накопительно)', 'Всего ключей (накопительно)', 'Всего подключений (накопительно)'])
            for d in daily_data:
                ws.append([
                    d['day'],
                    d['new'],
                    d['key'],
                    d['connect'],
                    d['paid'],
                    d['cum_users'],
                    d['cum_key'],
                    d['cum_connect']
                ])

            for row in ws.iter_rows(min_row=1, max_row=len(daily_data)+1, min_col=1, max_col=8):
                for cell in row:
                    cell.border = thin_border

            for col in ws.columns:
                max_len = 0
                col_letter = col[0].column_letter
                for cell in col:
                    if cell.value:
                        max_len = max(max_len, len(str(cell.value)))
                ws.column_dimensions[col_letter].width = min(max_len + 2, 20)

            # Линейный график (накопительные)
            chart1 = LineChart()
            chart1.title = "Накопительные показатели"
            chart1.style = 13
            chart1.y_axis.title = "Количество"
            chart1.x_axis.title = "День месяца"
            data = Reference(ws, min_col=6, max_col=8, min_row=1, max_row=len(daily_data)+1)
            dates = Reference(ws, min_col=1, min_row=2, max_row=len(daily_data)+1)
            chart1.add_data(data, titles_from_data=True)
            chart1.set_categories(dates)
            if len(chart1.series) >= 3:
                chart1.series[0].graphicalProperties.line.solidFill = "0000FF"
                chart1.series[1].graphicalProperties.line.solidFill = "00B0F0"
                chart1.series[2].graphicalProperties.line.solidFill = "000000"
            ws.add_chart(chart1, "J2")

            # Столбцовая диаграмма (ежедневные)
            chart2 = BarChart()
            chart2.title = "Ежедневные показатели"
            chart2.style = 13
            chart2.y_axis.title = "Количество"
            chart2.x_axis.title = "День месяца"
            data2 = Reference(ws, min_col=2, max_col=5, min_row=1, max_row=len(daily_data)+1)
            chart2.add_data(data2, titles_from_data=True)
            chart2.set_categories(dates)
            ws.add_chart(chart2, "J20")

        excel_file = BytesIO()
        wb.save(excel_file)
        excel_file.seek(0)

        await message.answer_document(
            document=BufferedInputFile(
                excel_file.read(),
                filename=f"analytics_{current_year}_{current_month}.xlsx"
            ),
            caption=f"📊 Помесячная аналитика с января {current_year} по {now.strftime('%B %Y')}"
        )

        logger.info(f"Админ {message.from_user.id} выгрузил помесячную аналитику")

    except Exception as e:
        logger.exception("Ошибка при экспорте помесячной аналитики")
        await message.answer(f"❌ Ошибка: {str(e)}")
