from datetime import datetime
from sqlalchemy import insert, select, update, func
from sqlalchemy.orm import Session
from config_bd.BaseModel import engine, users, payments
from logging_config import logger


class SQL:

    def __init__(self):
        self.session = Session(engine)

    def INSERT(self, user_id: int, Is_pay_null: bool, Is_tarif: bool = False, ref: str = '',
               is_delete: bool = False, Is_admin: bool = False, stamp=None):
        try:
            ins = insert(users).values(
                User_id=user_id,
                Ref=ref,
                Is_delete=is_delete,
                Is_pay_null=Is_pay_null,
                Is_tarif=Is_tarif,
                Is_admin=Is_admin,
                stamp=stamp
            )
            self.session.execute(ins)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error inserting user {user_id}: {e}")

    def SELECT_ID(self, user_id=int):
        """
        Проверяет наличие пользователя в БД user и возвращает строку таблицы или NONE
        :param user_id: int
        :return: row | None
        """
        s = select(users).where(users.c.User_id == user_id)
        re = self.session.execute(s)
        result = re.fetchone()
        return result

    def SELECT_DB_ID(self, user_id=int):
        """
        Проверяет наличие пользователя в БД user и возвращает строку таблицы или NONE
        :param user_id: int
        :return: row | None
        """
        s = select(users).where(users.c.Id == user_id)
        re = self.session.execute(s)
        result = re.fetchone()
        return result

    def SELECT_Is_admin(self):
        try:
            s = select(users.c.User_id).where(users.c.Is_admin == True)
            result = self.session.execute(s).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error selecting admin users: {e}")
            return []

    def UPDATE_PAYNULL(self, user_id=int):
        """
        Назначаем и снимаем БЛОК в отношении пользователя
        :param user_id: int
        """
        s = update(users).where(users.c.User_id == user_id).values(Is_pay_null=True)
        self.session.execute(s)
        self.session.commit()

    def UPDATE_ADMIN(self, user_id=int):
        """
        Назначаем и снимаем БЛОК в отношении пользователя
        :param user_id: int
        """
        s = update(users).where(users.c.User_id == user_id).values(Is_admin=True)
        self.session.execute(s)
        self.session.commit()

    def UPDATE_TARIFF(self, user_id: int, booly: bool):
        """
        Назначаем и снимаем БЛОК в отношении пользователя
        :param user_id: int
        :param booly: bool
        """
        s = update(users).where(users.c.User_id == user_id).values(Is_tarif=booly)
        self.session.execute(s)
        self.session.commit()

    def UPDATE_TTCLID(self, user_id: int, ttclid: str):
        """
        Назначаем и снимаем БЛОК в отношении пользователя
        :param user_id: int
        :param booly: bool
        """
        s = update(users).where(users.c.User_id == user_id).values(ttclid=ttclid)
        self.session.execute(s)
        self.session.commit()

    def UPDATE_DELETE(self, user_id: int, booly: bool):
        """
        Назначаем и снимаем БЛОК в отношении пользователя
        :param user_id: int
        :param booly: bool
        """
        s = update(users).where(users.c.User_id == user_id).values(Is_delete=booly)
        self.session.execute(s)
        self.session.commit()

    def SELECT_REF(self, user_id: int):
        try:
            s = select(users).where(users.c.User_id == user_id, users.c.Is_pay_null == True)
            result = self.session.execute(s).fetchone()
            return result
        except Exception as e:
            logger.error(f"Error selecting ref for user {user_id}: {e}")
            return None

    def SELECT_COUNT_REF(self, user_id: int):
        try:
            s = select(users).where(users.c.Ref == user_id)
            result = self.session.execute(s).fetchall()
            return len(result)
        except Exception as e:
            logger.error(f"Error counting refs for user {user_id}: {e}")
            return 0

    def update_subscription_end_date(self, user_id: int, end_date: datetime):
        """
        Обновляет дату и время окончания подписки пользователя.
        """
        try:
            end_date = end_date.replace(microsecond=0)
            upd = update(users).where(users.c.User_id == user_id).values(subscription_end_date=end_date)
            self.session.execute(upd)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error updating subscription end date for user {user_id}: {e}")

    def update_white_subscription_end_date(self, user_id: int, end_date: datetime):
        """
        Обновляет дату и время окончания подписки пользователя.
        """
        try:
            end_date = end_date.replace(microsecond=0)
            upd = update(users).where(users.c.User_id == user_id).values(white_subscription_end_date=end_date)
            self.session.execute(upd)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error updating white subscription end date for user {user_id}: {e}")

    def get_subscription_end_date(self, user_id: int):
        """
        Возвращает дату и время окончания подписки пользователя.
        """
        try:
            s = select(users.c.subscription_end_date).where(users.c.User_id == user_id)
            result = self.session.execute(s).fetchone()
            if result:
                return result[0]  # Возвращает datetime объект или None
            return None
        except Exception as e:
            logger.error(f"Error getting subscription end date for user {user_id}: {e}")
            return None

    def notification_sent_today(self, user_id: int):
        """
        Проверяет, было ли отправлено уведомление сегодня.
        """
        try:
            today = datetime.now().date()  # Текущая дата без времени
            s = select(users.c.last_notification_date).where(users.c.User_id == user_id)
            result = self.session.execute(s).fetchone()
            if result:
                last_notification_date = result[0]
                if isinstance(last_notification_date, datetime):
                    last_notification_date = last_notification_date.date()  # Приводим к типу date
                return last_notification_date == today
            return False
        except Exception as e:
            logger.error(f"Error checking notification sent today for user {user_id}: {e}")
            return False

    def mark_notification_as_sent(self, user_id: int):
        """
        Помечает, что уведомление было отправлено сегодня, сохраняя только дату.
        """
        try:
            today = datetime.now().date()  # Получаем текущую дату без времени
            upd = update(users).where(users.c.User_id == user_id).values(last_notification_date=today)
            self.session.execute(upd)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error marking notification as sent for user {user_id}: {e}")

    def get_last_notification_date(self, user_id: int):
        """
        Возвращает дату последнего отправленного уведомления.
        """
        try:
            s = select(users.c.last_notification_date).where(users.c.User_id == user_id)
            result = self.session.execute(s).fetchone()
            if result:
                last_notification_date = result[0]
                if isinstance(last_notification_date, datetime):
                    last_notification_date = last_notification_date.date()  # Приводим к типу date
                return last_notification_date
            return None
        except Exception as e:
            logger.error(f"Error fetching last notification date for user {user_id}: {e}")
            return None

    def update_broadcast_status(self, user_id: int, status: str):
        try:
            upd = update(users).where(users.c.User_id == user_id).values(
                last_broadcast_status=status, last_broadcast_date=datetime.now().date()
            )
            self.session.execute(upd)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error updating broadcast status for user {user_id}: {e}")

    def SELECT_ID_BLOCK(self, user_id: int):
        try:
            s = select(users.c.User_id).where(users.c.User_id == user_id, users.c.Is_block == True)
            result = self.session.execute(s).fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error checking if user is blocked {user_id}: {e}")
            return None

    def UPDATE_BLOK(self, user_id: int, status: bool):
        try:
            upd = update(users).where(users.c.User_id == user_id).values(Is_block=status)
            self.session.execute(upd)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error updating block status for user {user_id}: {e}")

    def UPDATE_STATUS(self, user_id: int, is_delete: bool):
        try:
            upd = update(users).where(users.c.User_id == user_id).values(Is_delete=is_delete)
            self.session.execute(upd)
            self.session.commit()
            logger.info(f"Status for user {user_id} updated. Is_delete set to {is_delete}.")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error updating Is_delete status for user {user_id}: {e}")

    def SELECT_ALL_USERS(self):
        try:
            s = select(users.c.User_id).where(users.c.Is_delete == False)
            result = self.session.execute(s).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error selecting all users: {e}")
            return []

    # Добавьте эти методы в класс SQL после метода SELECT_ALL_USERS

    def SELECT_NOT_CONNECTED_SUBSCRIBE_YES(self):
        """
        Предоставляет все user_id где:
        - Is_Pay_null = TRUE
        - Is_tarif = False
        - subscription_end_date позже текущего времени
        - last_broadcast_date IS NULL OR last_broadcast_date != today
        """
        try:
            current_time = datetime.now()
            today = datetime.now().date()
            s = select(users.c.User_id).where(
                users.c.Is_pay_null == True,
                users.c.Is_tarif == False,
                users.c.Is_delete == False,
                users.c.subscription_end_date > current_time,
                (users.c.last_broadcast_date.is_(None)) |
                (func.date(users.c.last_broadcast_date) != today)
            )
            result = self.session.execute(s).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error selecting not connected subscribe yes users: {e}")
            return []

    def SELECT_NOT_CONNECTED_SUBSCRIBE_OFF(self):
        """
        Предоставляет все user_id где:
        - Is_Pay_null = TRUE
        - Is_tarif = False
        - subscription_end_date раньше текущего времени или NULL
        - last_broadcast_date IS NULL OR last_broadcast_date != today
        """
        try:
            current_time = datetime.now()
            today = datetime.now().date()
            s = select(users.c.User_id).where(
                users.c.Is_pay_null == True,
                users.c.Is_tarif == False,
                users.c.Is_delete == False,
                (users.c.subscription_end_date < current_time) |
                (users.c.subscription_end_date.is_(None)),
                (users.c.last_broadcast_date.is_(None)) |
                (func.date(users.c.last_broadcast_date) != today)
            )
            result = self.session.execute(s).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error selecting not connected subscribe off users: {e}")
            return []

    def SELECT_CONNECTED_SUBSCRIBE_OFF(self):
        """
        Предоставляет все user_id где:
        - Is_Pay_null = TRUE
        - Is_tarif = True
        - subscription_end_date раньше текущего времени или NULL
        - last_broadcast_date IS NULL OR last_broadcast_date != today
        """
        try:
            current_time = datetime.now()
            today = datetime.now().date()
            s = select(users.c.User_id).where(
                users.c.Is_pay_null == True,
                users.c.Is_tarif == True,
                users.c.Is_delete == False,
                (users.c.subscription_end_date < current_time) |
                (users.c.subscription_end_date.is_(None)),
                (users.c.last_broadcast_date.is_(None)) |
                (func.date(users.c.last_broadcast_date) != today)
            )
            result = self.session.execute(s).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error selecting connected subscribe off users: {e}")
            return []

    def SELECT_CONNECTED_SUBSCRIBE_YES(self):
        """
        Предоставляет все user_id где:
        - Is_Pay_null = TRUE
        - Is_tarif = True
        - subscription_end_date позже текущего времени
        - last_broadcast_date IS NULL OR last_broadcast_date != today
        """
        try:
            current_time = datetime.now()
            today = datetime.now().date()
            s = select(users.c.User_id).where(
                users.c.Is_pay_null == True,
                users.c.Is_tarif == True,
                users.c.Is_delete == False,
                users.c.subscription_end_date > current_time,
                (users.c.last_broadcast_date.is_(None)) |
                (func.date(users.c.last_broadcast_date) != today)
            )
            result = self.session.execute(s).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error selecting connected subscribe yes users: {e}")
            return []

    def SELECT_NOT_SUBSCRIBED(self):
        """
        Предоставляет все user_id где:
        - Is_Pay_null = False
        - Is_tarif = False
        - last_broadcast_date IS NULL OR last_broadcast_date != today
        """
        try:
            today = datetime.now().date()
            s = select(users.c.User_id).where(
                users.c.Is_pay_null == False,
                users.c.Is_tarif == False,
                users.c.Is_delete == False,
                (users.c.last_broadcast_date.is_(None)) |
                (func.date(users.c.last_broadcast_date) != today)
            )
            result = self.session.execute(s).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error selecting not subscribed users: {e}")
            return []

    def get_stat_by_ref_or_stamp(self, arg: str):
        """
        Возвращает статистику по пользователям, у которых Ref == arg,
        если таких нет – по пользователям с stamp == arg.
        Возвращает (total, with_sub, with_tarif, total_payments, source)
        или (None, None, None, None, None) если нет совпадений.
        """
        # 1. Ищем по Ref
        users = self.SELECT_USERS_BY_PARAMETER('Ref', arg)
        source = 'ref'
        if not users:
            # 2. Ищем по stamp
            users = self.SELECT_USERS_BY_PARAMETER('stamp', arg)
            source = 'stamp'

        if not users:
            return None, None, None, None, None

        total = len(users)
        with_sub = 0
        with_tarif = 0

        for user_id in users:
            user_data = self.SELECT_ID(user_id)
            if user_data:
                # subscription_end_date — индекс 9, Is_tarif — индекс 5
                if user_data[9] is not None:
                    with_sub += 1
                if user_data[5]:  # Is_tarif
                    with_tarif += 1

        # Сумма подтверждённых платежей этих пользователей
        total_payments = 0
        if users:
            with engine.connect() as conn:
                stmt = (
                    select(func.coalesce(func.sum(payments.c.amount), 0))
                    .where(payments.c.user_id.in_(users))
                    .where(payments.c.status == 'confirmed')
                )
                total_payments = conn.execute(stmt).scalar() or 0

        return total, with_sub, with_tarif, total_payments, source

    def SELECT_USERS_BY_PARAMETER(self, parameter: str, value: str):
        """Возвращает пользователей по выбранному параметру и его значению."""
        try:
            # Проверяем, что параметр допустим
            valid_parameters = ['Ref', 'Is_pay_null', 'stamp']  # <-- добавлено 'stamp'
            if parameter not in valid_parameters:
                logger.info(f"Invalid parameter: {parameter}")
                return []

            # Проверяем значение и приводим его к нужному типу, если требуется
            if parameter == 'Is_pay_null':
                try:
                    value = int(value)
                    value = bool(value)
                except ValueError:
                    logger.error(f"Invalid value type for parameter {parameter}: {value}")
                    return []

            # Формируем запрос к базе данных
            s = select(users.c.User_id).where(getattr(users.c, parameter) == value)
            result = self.session.execute(s).fetchall()

            logger.info(f"Query result for parameter '{parameter}' with value '{value}': len{result}")
            return [row[0] for row in result]
        except Exception as e:
            logger.error(
                f"Error in SELECT_USERS_BY_PARAMETER with parameter '{parameter}' and value '{value}': {e}")
            return []

    def GET_AVAILABLE_PARAMETERS(self):
        """Возвращает список доступных параметров для фильтрации пользователей."""
        # Отладочный вывод доступных параметров
        parameters = ['not_connected_subscribe_yes', 'not_connected_subscribe_off', 'connected_subscribe_off', 'connected_subscribe_yes', 'not_subscribed', 'all_users']
        logger.info(f"Available parameters: {parameters}")
        return parameters

    def DELETE(self, user_id: int):
        """
        Полностью удаляет пользователя из БД по User_id
        :param user_id: int - Telegram ID пользователя
        """
        try:
            # Удаляем пользователя по User_id
            d = users.delete().where(users.c.User_id == user_id)
            result = self.session.execute(d)
            self.session.commit()

            deleted_rows = result.rowcount
            logger.info(f"✅ Удалено пользователей: {deleted_rows} (User_id: {user_id})")
            return True
        except Exception as e:
            self.session.rollback()
            logger.error(f"❌ Ошибка удаления пользователя {user_id}: {e}")
            return False

    def reset_all_delete_flag(self):
        """
        Устанавливает Is_delete = False для всех записей в таблице users.
        Возвращает количество обновлённых строк.
        """
        try:
            upd = update(users).values(Is_delete=False)
            result = self.session.execute(upd)
            self.session.commit()
            updated_rows = result.rowcount
            logger.info(f"✅ Сброшен флаг Is_delete для {updated_rows} пользователей")
            return updated_rows
        except Exception as e:
            self.session.rollback()
            logger.error(f"❌ Ошибка при массовом сбросе Is_delete: {e}")
            raise


    def get_users_with_confirmed_payments(self, user_ids=None):
        """
        Возвращает список user_id, у которых есть хотя бы один платёж со статусом 'confirmed'.
        Если передан список user_ids, возвращаются только те, кто есть в этом списке.
        """
        try:
            stmt = select(payments.c.user_id).where(payments.c.status == 'confirmed').distinct()
            if user_ids:
                stmt = stmt.where(payments.c.user_id.in_(user_ids))
            result = self.session.execute(stmt).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error getting users with confirmed payments: {e}")
            return []

    def get_payment_stats_by_period(self, start_date: datetime, end_date: datetime):
        """
        Возвращает статистику платежей за период по группам ref и stamp.
        Для каждого платежа с суммой != 1, статус 'confirmed', дата между start_date и end_date включительно,
        находим пользователя и добавляем сумму в группы ref и stamp (если они заданы).
        Возвращает два словаря: ref_totals, stamp_totals.
        """
        try:
            # Приводим даты к началу и концу суток для включительности
            start = datetime.combine(start_date.date(), datetime.min.time())
            end = datetime.combine(end_date.date(), datetime.max.time())

            with engine.connect() as conn:
                # Получаем платежи за период, исключая сумму 1
                stmt = select(
                    payments.c.user_id,
                    payments.c.amount
                ).where(
                    payments.c.status == 'confirmed',
                    payments.c.amount != 1,
                    payments.c.time_created.between(start, end)
                )
                payments_data = conn.execute(stmt).fetchall()

            if not payments_data:
                return {}, {}

            # Собираем уникальные user_id из платежей
            user_ids = list(set(p[0] for p in payments_data))

            # Получаем данные всех этих пользователей одним запросом
            with engine.connect() as conn:
                stmt_users = select(users.c.User_id, users.c.Ref, users.c.stamp).where(users.c.User_id.in_(user_ids))
                users_data = conn.execute(stmt_users).fetchall()

            # Словарь для быстрого поиска ref и stamp по user_id
            user_map = {u[0]: (u[1], u[2]) for u in users_data}

            ref_totals = {}
            stamp_totals = {}

            for user_id, amount in payments_data:
                ref, stamp = user_map.get(user_id, (None, None))
                if ref:
                    ref_totals[ref] = ref_totals.get(ref, 0) + amount
                if stamp:
                    stamp_totals[stamp] = stamp_totals.get(stamp, 0) + amount

            return ref_totals, stamp_totals
        except Exception as e:
            logger.error(f"Error in get_payment_stats_by_period: {e}")
            return {}, {}