from datetime import datetime
import openpyxl
from aiogram import Router
from openpyxl.styles import Alignment, Border, Side
from sqlalchemy import select

from config import ADMIN_IDS
from logging_config import logger
from config_bd.BaseModel import engine, gifts, users, payments, white_counter, online, payments_stars, \
    payments_cryptobot
from aiogram.types import Message, FSInputFile
from aiogram.filters import Command


router = Router()


@router.message(Command(commands=['export']))
async def export_database_to_excel(message: Message):
    """Экспорт базы данных в Excel файл с учетом нового столбца white_subscription_end_date"""

    # Проверка прав администратора
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Эта команда доступна только администраторам.")
        return

    try:
        await message.answer("🔄 Начинаю экспорт базы данных...")

        # Создаем новую книгу Excel
        wb = openpyxl.Workbook()

        # Удаляем дефолтный лист, если он есть
        if 'Sheet' in wb.sheetnames:
            default_sheet = wb['Sheet']
            wb.remove(default_sheet)

        # --- Лист USERS ---
        ws_users = wb.create_sheet(title="users")

        # Заголовки столбцов для users (ДОБАВЛЕН white_subscription_end_date)
        users_columns = [
            'ID', 'User ID', 'Ref', 'Is_delete', 'Is_pay_null', 'Is_tarif',
            'Create_user', 'Is_admin', 'has_discount', 'subscription_end_date',
            'white_subscription_end_date',
            'last_notification_date', 'last_broadcast_status', 'last_broadcast_date', 'stamp', 'ttclid'
        ]

        # Стили для заголовков
        header_alignment = Alignment(horizontal="center", vertical="center")
        thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )

        # Записываем заголовки
        for col_num, column_title in enumerate(users_columns, 1):
            cell = ws_users.cell(row=1, column=col_num, value=column_title)
            cell.alignment = header_alignment
            cell.border = thin_border

        # Получаем данные из таблицы users
        with engine.connect() as conn:
            result = conn.execute(select(users)).fetchall()

            for row_num, row_data in enumerate(result, 2):
                for col_num, cell_value in enumerate(row_data, 1):
                    # Форматируем даты для читаемости
                    if col_num == 10 and cell_value:  # subscription_end_date
                        if isinstance(cell_value, datetime):
                            cell_value = cell_value.strftime('%Y-%m-%d %H:%M:%S')
                    elif col_num == 11 and cell_value:  # white_subscription_end_date
                        if isinstance(cell_value, datetime):
                            cell_value = cell_value.strftime('%Y-%m-%d %H:%M:%S')
                    elif col_num == 13 and cell_value:  # last_notification_date
                        if isinstance(cell_value, datetime):
                            cell_value = cell_value.strftime('%Y-%m-%d')
                    elif col_num == 15 and cell_value:  # last_broadcast_date
                        if isinstance(cell_value, datetime):
                            cell_value = cell_value.strftime('%Y-%m-%d %H:%M:%S')

                    cell = ws_users.cell(row=row_num, column=col_num, value=cell_value)
                    cell.border = thin_border

        # Автоподбор ширины столбцов для users
        column_widths = {}
        for column in ws_users.columns:
            max_length = 0
            column_letter = column[0].column_letter

            # Устанавливаем минимальные ширины для некоторых столбцов
            if column_letter == 'A':  # ID
                min_width = 5
            elif column_letter == 'K':  # white_subscription_end_date (11-й столбец, K)
                min_width = 25
            elif column_letter == 'J':  # subscription_end_date
                min_width = 25
            else:
                min_width = 10

            for cell in column:
                try:
                    if cell.value:
                        cell_length = len(str(cell.value))
                        if cell_length > max_length:
                            max_length = cell_length
                except:
                    pass

            adjusted_width = max(min_width, min(max_length + 2, 50))
            column_widths[column_letter] = adjusted_width

        # Применяем вычисленные ширины
        for col_letter, width in column_widths.items():
            ws_users.column_dimensions[col_letter].width = width


        # --- Лист PAYMENTS ---
        ws_payments = wb.create_sheet(title="payments_sbp")

        # Заголовки столбцов для payments
        payments_columns = [
            'ID', 'User ID', 'Amount', 'Time Created', 'Is Gift', 'Status', 'Transaction_Id'
        ]

        # Записываем заголовки
        for col_num, column_title in enumerate(payments_columns, 1):
            cell = ws_payments.cell(row=1, column=col_num, value=column_title)
            cell.alignment = header_alignment
            cell.border = thin_border

        # Получаем данные из таблицы payments
        with engine.connect() as conn:
            result = conn.execute(select(payments)).fetchall()

            for row_num, row_data in enumerate(result, 2):
                for col_num, cell_value in enumerate(row_data, 1):
                    # Форматируем дату для столбца Time Created
                    if col_num == 4 and cell_value:  # Time Created
                        if isinstance(cell_value, datetime):
                            cell_value = cell_value.strftime('%Y-%m-%d %H:%M:%S')

                    cell = ws_payments.cell(row=row_num, column=col_num, value=cell_value)
                    cell.border = thin_border

        # Автоподбор ширины столбцов для payments
        for column in ws_payments.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws_payments.column_dimensions[column_letter].width = adjusted_width

        # --- Лист PAYMENTS_STARS ---
        ws_payments_stars = wb.create_sheet(title="payments_stars")

        # Заголовки столбцов для payments_stars
        payments_stars_columns = [
            'ID', 'User ID', 'Amount (Stars)', 'Time Created', 'Is Gift', 'Status'
        ]

        # Записываем заголовки
        for col_num, column_title in enumerate(payments_stars_columns, 1):
            cell = ws_payments_stars.cell(row=1, column=col_num, value=column_title)
            cell.alignment = header_alignment
            cell.border = thin_border

        # Получаем данные из таблицы payments_stars
        with engine.connect() as conn:
            result = conn.execute(select(payments_stars)).fetchall()

            for row_num, row_data in enumerate(result, 2):
                for col_num, cell_value in enumerate(row_data, 1):
                    # Форматируем дату для столбца Time Created
                    if col_num == 4 and cell_value:  # Time Created
                        if isinstance(cell_value, datetime):
                            cell_value = cell_value.strftime('%Y-%m-%d %H:%M:%S')
                    cell = ws_payments_stars.cell(row=row_num, column=col_num, value=cell_value)
                    cell.border = thin_border

        # Автоподбор ширины столбцов для payments_stars
        for column in ws_payments_stars.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws_payments_stars.column_dimensions[column_letter].width = adjusted_width

        # --- Лист PAYMENTS_CRYPTOBOT ---
        ws_payments_cryptobot = wb.create_sheet(title="payments_cryptobot")

        # Заголовки столбцов для payments_cryptobot
        payments_cryptobot_columns = [
            'ID', 'User ID', 'Amount', 'Currency', 'Time Created',
            'Is Gift', 'Status', 'Invoice ID', 'Payload'
        ]

        # Записываем заголовки
        for col_num, column_title in enumerate(payments_cryptobot_columns, 1):
            cell = ws_payments_cryptobot.cell(row=1, column=col_num, value=column_title)
            cell.alignment = header_alignment
            cell.border = thin_border

        # Получаем данные из таблицы payments_cryptobot
        with engine.connect() as conn:
            result = conn.execute(select(payments_cryptobot)).fetchall()

            for row_num, row_data in enumerate(result, 2):
                for col_num, cell_value in enumerate(row_data, 1):
                    # Форматируем дату для столбца Time Created (индекс 5)
                    if col_num == 5 and cell_value:
                        if isinstance(cell_value, datetime):
                            cell_value = cell_value.strftime('%Y-%m-%d %H:%M:%S')
                    cell = ws_payments_cryptobot.cell(row=row_num, column=col_num, value=cell_value)
                    cell.border = thin_border

        # Автоподбор ширины столбцов для payments_cryptobot
        for column in ws_payments_cryptobot.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws_payments_cryptobot.column_dimensions[column_letter].width = adjusted_width


        # --- Лист GIFTS ---
        ws_gifts = wb.create_sheet(title="gifts")

        # Заголовки столбцов для gifts
        gifts_columns = [
            'gift_id', 'giver_id', 'duration', 'recepient_id', 'white_flag', 'flag'
        ]

        # Записываем заголовки
        for col_num, column_title in enumerate(gifts_columns, 1):
            cell = ws_gifts.cell(row=1, column=col_num, value=column_title)
            cell.alignment = header_alignment
            cell.border = thin_border

        # Получаем данные из таблицы gifts
        with engine.connect() as conn:
            result = conn.execute(select(gifts)).fetchall()

            for row_num, row_data in enumerate(result, 2):
                for col_num, cell_value in enumerate(row_data, 1):
                    cell = ws_gifts.cell(row=row_num, column=col_num, value=cell_value)
                    cell.border = thin_border

        # Автоподбор ширины столбцов для gifts
        for column in ws_gifts.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws_gifts.column_dimensions[column_letter].width = adjusted_width

        # --- Лист ONLINE ---
        ws_online = wb.create_sheet(title="online")

        # Заголовки
        online_columns = ['ID', 'Дата сбора', 'Всего в панели', 'Активны сегодня', 'Платных', 'Триальных']
        for col_num, column_title in enumerate(online_columns, 1):
            cell = ws_online.cell(row=1, column=col_num, value=column_title)
            cell.alignment = header_alignment
            cell.border = thin_border

        # Данные
        with engine.connect() as conn:
            result = conn.execute(select(online)).fetchall()
            for row_num, row_data in enumerate(result, 2):
                for col_num, cell_value in enumerate(row_data, 1):
                    if col_num == 2 and cell_value:  # online_date
                        if isinstance(cell_value, datetime):
                            cell_value = cell_value.strftime('%Y-%m-%d %H:%M:%S')
                    cell = ws_online.cell(row=row_num, column=col_num, value=cell_value)
                    cell.border = thin_border

        # Автоширина
        for column in ws_online.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws_online.column_dimensions[column_letter].width = adjusted_width

        # --- Лист WHITE_COUNTER ---
        ws_white_counter = wb.create_sheet(title="white_counter")

        # Заголовки столбцов для white_counter
        white_counter_columns = ['ID', 'User ID', 'Time Created']

        # Стили для заголовков
        header_alignment = Alignment(horizontal="center", vertical="center")
        thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )

        # Записываем заголовки для white_counter
        for col_num, column_title in enumerate(white_counter_columns, 1):
            cell = ws_white_counter.cell(row=1, column=col_num, value=column_title)
            cell.alignment = header_alignment
            cell.border = thin_border

        # Получаем данные из таблицы white_counter
        with engine.connect() as conn:
            result = conn.execute(select(white_counter)).fetchall()

            for row_num, row_data in enumerate(result, 2):
                for col_num, cell_value in enumerate(row_data, 1):
                    # Форматируем даты для читаемости
                    if col_num == 3 and cell_value:  # Time Created
                        if isinstance(cell_value, datetime):
                            cell_value = cell_value.strftime('%Y-%m-%d %H:%M:%S')

                    cell = ws_white_counter.cell(row=row_num, column=col_num, value=cell_value)
                    cell.border = thin_border

        # Автоподбор ширины столбцов для white_counter
        for column in ws_white_counter.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws_white_counter.column_dimensions[column_letter].width = adjusted_width

        ws_payments_stars.freeze_panes = ws_payments_stars['A2']
        ws_online.freeze_panes = ws_online['A2']
        ws_users.freeze_panes = ws_users['A2']
        ws_gifts.freeze_panes = ws_gifts['A2']
        ws_payments.freeze_panes = ws_payments['A2']
        ws_white_counter.freeze_panes = ws_white_counter['A2']
        ws_payments_cryptobot.freeze_panes = ws_payments_cryptobot['A2']

        wb.save('export.xlsx')

        # Получаем статистику
        users_count = len(list(ws_users.iter_rows(min_row=2)))
        gifts_count = len(list(ws_gifts.iter_rows(min_row=2)))
        payments_count = len(list(ws_payments.iter_rows(min_row=2)))
        payments_stars_count = len(list(ws_payments_stars.iter_rows(min_row=2)))
        white_counter_count = len(list(ws_white_counter.iter_rows(min_row=2)))  # ДОБАВЛЕНО
        payments_cryptobot_count = len(list(ws_payments_cryptobot.iter_rows(min_row=2)))

        # Подсчитываем пользователей с white_subscription_end_date
        white_subscription_count = 0
        for row in ws_users.iter_rows(min_row=2, min_col=11, max_col=11):
            cell = row[0]
            if cell.value is not None and str(cell.value).strip() != '':
                white_subscription_count += 1

        # Отправляем файл
        await message.answer_document(
            document=FSInputFile('export.xlsx'),
            caption=f"📊 Экспорт базы данных\n"
                    f"📅 Создано: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
                    f"📊 Статистика:\n"
                    f"├ 👥 Пользователей: {users_count}\n"
                    f"├ 🎁 Подарков: {gifts_count}\n"
                    f"├ 💰 Платежей Platega: {payments_count}\n"
                    f"├ ⭐ Платежей Stars: {payments_stars_count}\n"
                    f"├ 💎 Крипто-платежей: {payments_cryptobot_count}\n"
                    f"├ ⚪ White-подписок: {white_subscription_count}\n"
                    f"└ 👁 White-кликов: {white_counter_count}"
        )

        logger.info(f"Администратор {message.from_user.id} экспортировал базу данных в Excel")
        logger.info(f"Статистика: {users_count} пользователей, {gifts_count} подарков, {payments_count} платежей")


    except Exception as e:
        error_message = f"❌ Ошибка при экспорте базы данных: {str(e)}"
        logger.error(error_message)
        logger.exception("Детали ошибки:")
        await message.answer(error_message)