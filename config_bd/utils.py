import uuid

from sqlalchemy import select, update, func
from datetime import datetime, date
from typing import Optional, List, Tuple, Dict

from config_bd.models import AsyncSessionLocal, Users, Payments, Gifts, PaymentsCryptobot, PaymentsStars, Online, WhiteCounter
from logging_config import logger


class AsyncSQL:
    def __init__(self):
        self.session_factory = AsyncSessionLocal

    async def SELECT_ID(self, user_id: int) -> Optional[Tuple]:
        async with self.session_factory() as session:
            stmt = select(Users).where(Users.user_id == user_id)
            result = await session.execute(stmt)
            user = result.scalar_one_or_none()
            if user:
                return (
                    user.id, user.user_id, user.ref, user.is_delete,
                    user.is_pay_null, user.is_tarif, user.create_user,
                    user.is_admin, user.has_discount, user.subscription_end_date,
                    user.white_subscription_end_date, user.last_notification_date,
                    user.last_broadcast_status, user.last_broadcast_date,
                    user.stamp, user.ttclid
                )
            return None

    async def INSERT(self, user_id: int, Is_pay_null: bool, Is_tarif: bool = False,
                     ref: str = '', is_delete: bool = False, Is_admin: bool = False,
                     stamp=None):
        async with self.session_factory() as session:
            user = Users(
                user_id=user_id,
                ref=ref,
                is_delete=is_delete,
                is_pay_null=Is_pay_null,
                is_tarif=Is_tarif,
                is_admin=Is_admin,
                stamp=stamp
            )
            session.add(user)
            try:
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error(f"Error inserting user {user_id}: {e}")

    async def UPDATE_PAYNULL(self, user_id: int):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(is_pay_null=True)
            await session.execute(stmt)
            await session.commit()

    async def UPDATE_ADMIN(self, user_id: int):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(is_admin=True)
            await session.execute(stmt)
            await session.commit()

    async def UPDATE_TARIFF(self, user_id: int, booly: bool):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(is_tarif=booly)
            await session.execute(stmt)
            await session.commit()

    async def UPDATE_TTCLID(self, user_id: int, ttclid: str):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(ttclid=ttclid)
            await session.execute(stmt)
            await session.commit()

    async def UPDATE_DELETE(self, user_id: int, booly: bool):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(is_delete=booly)
            await session.execute(stmt)
            await session.commit()

    async def SELECT_REF(self, user_id: int) -> Optional[Tuple]:
        async with self.session_factory() as session:
            stmt = select(Users).where(Users.user_id == user_id, Users.is_pay_null == True)
            result = await session.execute(stmt)
            user = result.scalar_one_or_none()
            if user:
                return (user.id, user.user_id, user.ref, user.is_delete,
                        user.is_pay_null, user.is_tarif, user.create_user,
                        user.is_admin, user.has_discount, user.subscription_end_date,
                        user.white_subscription_end_date, user.last_notification_date,
                        user.last_broadcast_status, user.last_broadcast_date,
                        user.stamp, user.ttclid)
            return None

    async def SELECT_COUNT_REF(self, user_id: int) -> int:
        async with self.session_factory() as session:
            stmt = select(func.count(Users.user_id)).where(Users.ref == str(user_id))
            result = await session.execute(stmt)
            return result.scalar() or 0

    async def update_subscription_end_date(self, user_id: int, end_date: datetime):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(subscription_end_date=end_date)
            await session.execute(stmt)
            await session.commit()

    async def update_white_subscription_end_date(self, user_id: int, end_date: datetime):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(white_subscription_end_date=end_date)
            await session.execute(stmt)
            await session.commit()

    async def get_subscription_end_date(self, user_id: int) -> Optional[datetime]:
        async with self.session_factory() as session:
            stmt = select(Users.subscription_end_date).where(Users.user_id == user_id)
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def notification_sent_today(self, user_id: int) -> bool:
        async with self.session_factory() as session:
            stmt = select(Users.last_notification_date).where(Users.user_id == user_id)
            result = await session.execute(stmt)
            last = result.scalar_one_or_none()
            today = date.today()
            if last:
                if isinstance(last, datetime):
                    last = last.date()
                return last == today
            return False

    async def mark_notification_as_sent(self, user_id: int):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(last_notification_date=date.today())
            await session.execute(stmt)
            await session.commit()

    async def get_last_notification_date(self, user_id: int) -> Optional[date]:
        async with self.session_factory() as session:
            stmt = select(Users.last_notification_date).where(Users.user_id == user_id)
            result = await session.execute(stmt)
            val = result.scalar_one_or_none()
            if isinstance(val, datetime):
                return val.date()
            return val

    async def update_broadcast_status(self, user_id: int, status: str):
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(
                last_broadcast_status=status,
                last_broadcast_date=datetime.now()
            )
            await session.execute(stmt)
            await session.commit()

    async def SELECT_ALL_USERS(self) -> List[int]:
        async with self.session_factory() as session:
            stmt = select(Users.user_id).where(Users.is_delete == False)
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def SELECT_NOT_CONNECTED_SUBSCRIBE_YES(self) -> List[int]:
        async with self.session_factory() as session:
            current_time = datetime.now()
            today = date.today()
            stmt = select(Users.user_id).where(
                Users.is_pay_null == True,
                Users.is_tarif == False,
                Users.is_delete == False,
                Users.subscription_end_date > current_time,
                (Users.last_broadcast_date.is_(None)) | (func.date(Users.last_broadcast_date) != today)
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def SELECT_NOT_CONNECTED_SUBSCRIBE_OFF(self):
        async with self.session_factory() as session:
            current_time = datetime.now()
            today = datetime.now().date()
            stmt = select(Users.user_id).where(
                Users.is_pay_null == True,
                Users.is_tarif == False,
                Users.is_delete == False,
                (Users.subscription_end_date < current_time) |
                (Users.subscription_end_date.is_(None)),
                (Users.last_broadcast_date.is_(None)) |
                (func.date(Users.last_broadcast_date) != today)
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def SELECT_CONNECTED_SUBSCRIBE_OFF(self):
        async with self.session_factory() as session:
            current_time = datetime.now()
            today = datetime.now().date()
            stmt = select(Users.user_id).where(
                Users.is_pay_null == True,
                Users.is_tarif == True,
                Users.is_delete == False,
                (Users.subscription_end_date < current_time) |
                (Users.subscription_end_date.is_(None)),
                (Users.last_broadcast_date.is_(None)) |
                (func.date(Users.last_broadcast_date) != today)
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def SELECT_CONNECTED_SUBSCRIBE_YES(self):
        async with self.session_factory() as session:
            current_time = datetime.now()
            today = datetime.now().date()
            stmt = select(Users.user_id).where(
                Users.is_pay_null == True,
                Users.is_tarif == True,
                Users.is_delete == False,
                Users.subscription_end_date > current_time,
                (Users.last_broadcast_date.is_(None)) |
                (func.date(Users.last_broadcast_date) != today)
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def SELECT_NOT_SUBSCRIBED(self):
        async with self.session_factory() as session:
            today = datetime.now().date()
            stmt = select(Users.user_id).where(
                Users.is_pay_null == False,
                Users.is_tarif == False,
                Users.is_delete == False,
                (Users.last_broadcast_date.is_(None)) |
                (func.date(Users.last_broadcast_date) != today)
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def SELECT_CONNECTED_NEVER_PAID(self) -> List[int]:
        """
        Возвращает список user_id, у которых is_tarif=True, is_delete=False,
        и нет ни одной успешной оплаты (статус 'confirmed' в Payments или PaymentsStars,
        или статус 'paid' в PaymentsCryptobot).
        """
        async with self.session_factory() as session:
            # Подзапрос: все пользователи с успешными платежами
            paid_subq = (
                select(Payments.user_id)
                .where(Payments.status == 'confirmed')
                .union(
                    select(PaymentsStars.user_id).where(PaymentsStars.status == 'confirmed'),
                    select(PaymentsCryptobot.user_id).where(PaymentsCryptobot.status == 'paid')
                )
                .subquery()
            )
            stmt = select(Users.user_id).where(
                Users.is_tarif == True,
                Users.is_delete == False,
                Users.user_id.notin_(paid_subq)
            )
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def SELECT_USERS_BY_PARAMETER(self, parameter: str, value: str) -> List[int]:
        """
        Возвращает список user_id, у которых значение указанного параметра равно value.
        Допустимые параметры: 'Ref', 'Is_pay_null', 'stamp'.
        """
        # Маппинг имён параметров на атрибуты модели
        param_map = {
            'Ref': Users.ref,
            'Is_pay_null': Users.is_pay_null,
            'stamp': Users.stamp,
        }
        if parameter not in param_map:
            logger.info(f"Invalid parameter: {parameter}")
            return []

        attr = param_map[parameter]

        # Преобразование значения для булевых полей
        if parameter == 'Is_pay_null':
            try:
                val = bool(int(value))
            except ValueError:
                logger.error(f"Invalid value type for parameter {parameter}: {value}")
                return []
        else:
            val = value

        async with self.session_factory() as session:
            stmt = select(Users.user_id).where(attr == val)
            result = await session.execute(stmt)
            rows = result.all()
            logger.info(f"Query result for parameter '{parameter}' with value '{value}': {len(rows)}")
            return [row[0] for row in rows]

    async def get_stat_by_ref_or_stamp(self, arg: str) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[int], Optional[str]]:
        """
        Возвращает статистику по пользователям, у которых Ref == arg,
        если таких нет – по пользователям с stamp == arg.
        Возвращает (total, with_sub, with_tarif, total_payments, source)
        или (None, None, None, None, None) если нет совпадений.
        """
        # 1. Ищем по Ref
        users = await self.SELECT_USERS_BY_PARAMETER('Ref', arg)
        source = 'ref'
        if not users:
            # 2. Ищем по stamp
            users = await self.SELECT_USERS_BY_PARAMETER('stamp', arg)
            source = 'stamp'

        if not users:
            return None, None, None, None, None

        total = len(users)
        with_sub = 0
        with_tarif = 0

        for user_id in users:
            user_data = await self.SELECT_ID(user_id)
            if user_data:
                # subscription_end_date — индекс 9, Is_tarif — индекс 5
                if user_data[9] is not None:
                    with_sub += 1
                if user_data[5]:  # Is_tarif
                    with_tarif += 1

        # Сумма подтверждённых платежей этих пользователей
        total_payments = 0
        if users:
            async with self.session_factory() as session:
                stmt = select(func.coalesce(func.sum(Payments.amount), 0)).where(
                    Payments.user_id.in_(users),
                    Payments.status == 'confirmed'
                )
                result = await session.execute(stmt)
                total_payments = result.scalar() or 0

        return total, with_sub, with_tarif, total_payments, source

    def GET_AVAILABLE_PARAMETERS(self) -> List[str]:
        """Возвращает список доступных параметров для фильтрации пользователей."""
        return [
            'not_connected_subscribe_yes',
            'not_connected_subscribe_off',
            'connected_subscribe_off',
            'connected_subscribe_yes',
            'not_subscribed',
            'connected_never_paid',
            'all_users'
        ]

    async def DELETE(self, user_id: int) -> bool:
        """Полностью удаляет пользователя из БД по User_id."""
        async with self.session_factory() as session:
            stmt = select(Users).where(Users.user_id == user_id)
            result = await session.execute(stmt)
            user = result.scalar_one_or_none()
            if not user:
                logger.warning(f"User {user_id} not found for deletion")
                return False
            await session.delete(user)
            await session.commit()
            logger.info(f"✅ Удалено пользователей: 1 (User_id: {user_id})")
            return True

    async def reset_all_delete_flag(self) -> int:
        """Устанавливает Is_delete = False для всех записей в таблице users."""
        async with self.session_factory() as session:
            stmt = update(Users).values(is_delete=False)
            result = await session.execute(stmt)
            await session.commit()
            updated = result.rowcount
            logger.info(f"✅ Сброшен флаг Is_delete для {updated} пользователей")
            return updated

    async def get_users_with_confirmed_payments(self, user_ids: Optional[List[int]] = None) -> List[int]:
        """
        Возвращает список user_id, у которых есть хотя бы один платёж со статусом 'confirmed'.
        Если передан список user_ids, возвращаются только те, кто есть в этом списке.
        """
        async with self.session_factory() as session:
            stmt = select(Payments.user_id).where(Payments.status == 'confirmed').distinct()
            if user_ids:
                stmt = stmt.where(Payments.user_id.in_(user_ids))
            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def get_payment_stats_by_period(self, start_date: datetime, end_date: datetime) -> Tuple[Dict[str, int], Dict[str, int]]:
        """
        Возвращает статистику платежей за период по группам ref и stamp.
        Для каждого платежа с суммой != 1, статус 'confirmed', дата между start_date и end_date включительно,
        находим пользователя и добавляем сумму в группы ref и stamp (если они заданы).
        Возвращает два словаря: ref_totals, stamp_totals.
        """
        # Приводим даты к началу и концу суток для включительности
        start = datetime.combine(start_date.date(), datetime.min.time())
        end = datetime.combine(end_date.date(), datetime.max.time())

        async with self.session_factory() as session:
            # Получаем платежи за период, исключая сумму 1
            stmt_payments = select(
                Payments.user_id,
                Payments.amount
            ).where(
                Payments.status == 'confirmed',
                Payments.amount != 1,
                Payments.time_created.between(start, end)
            )
            payments_result = await session.execute(stmt_payments)
            payments_data = payments_result.all()

            if not payments_data:
                return {}, {}

            # Собираем уникальные user_id из платежей
            user_ids = list(set(p[0] for p in payments_data))

            # Получаем данные всех этих пользователей одним запросом
            stmt_users = select(
                Users.user_id,
                Users.ref,
                Users.stamp
            ).where(Users.user_id.in_(user_ids))
            users_result = await session.execute(stmt_users)
            users_data = users_result.all()

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

    async def update_broadcast_status(self, user_id: int, status: str) -> None:
        """
        Обновляет статус последней рассылки и дату для указанного пользователя.
        """
        async with self.session_factory() as session:
            stmt = update(Users).where(Users.user_id == user_id).values(
                last_broadcast_status=status,
                last_broadcast_date=datetime.now()  # сохраняем полную дату и время
            )
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error(f"Error updating broadcast status for user {user_id}: {e}")

    async def activate_gift(self, gift_id: str, recipient_id: int) -> Tuple[bool, Optional[int], Optional[bool]]:
        """
        Активирует подарок по gift_id для указанного получателя.
        Возвращает (успех, duration, white_flag) или (False, None, None) если подарок не найден или уже активирован.
        """
        async with self.session_factory() as session:
            # Проверяем существование и статус подарка
            stmt = select(Gifts).where(
                Gifts.gift_id == gift_id,
                Gifts.flag == False,
                Gifts.recepient_id == None
            )
            result = await session.execute(stmt)
            gift = result.scalar_one_or_none()

            if not gift:
                logger.warning(f"Gift {gift_id} not found or already activated")
                return False, None, None

            # Активируем подарок
            gift.flag = True
            gift.recepient_id = recipient_id
            try:
                await session.commit()
                logger.info(f"Gift {gift_id} activated for user {recipient_id}")
                return True, gift.duration, gift.white_flag
            except Exception as e:
                await session.rollback()
                logger.error(f"Error activating gift {gift_id} for user {recipient_id}: {e}")
                return False, None, None

    async def get_pending_platega_payments(self) -> List[Payments]:
        """Возвращает все платежи из таблицы payments со статусом 'pending'."""
        async with self.session_factory() as session:
            stmt = select(Payments).where(Payments.status == 'pending')
            result = await session.execute(stmt)
            return result.scalars().all()

    async def update_payment_status(self, transaction_id: str, new_status: str) -> None:
        """Обновляет статус платежа по transaction_id."""
        async with self.session_factory() as session:
            stmt = update(Payments).where(Payments.transaction_id == transaction_id).values(status=new_status)
            await session.execute(stmt)
            await session.commit()

    async def get_active_cryptobot_payments(self) -> List[PaymentsCryptobot]:
        """
        Возвращает все платежи Cryptobot со статусом 'active'.
        """
        async with self.session_factory() as session:
            stmt = select(PaymentsCryptobot).where(PaymentsCryptobot.status == 'active')
            result = await session.execute(stmt)
            return result.scalars().all()

    async def update_cryptobot_payment_status(self, payment_id: int, status: str) -> None:
        """
        Обновляет статус платежа Cryptobot.
        """
        async with self.session_factory() as session:
            stmt = update(PaymentsCryptobot).where(PaymentsCryptobot.id == payment_id).values(status=status)
            await session.execute(stmt)
            await session.commit()

    async def add_payment_stars(self, user_id: int, amount: int, is_gift: bool) -> None:
        """Добавляет запись в таблицу payments_stars."""
        async with self.session_factory() as session:
            payment = PaymentsStars(
                user_id=user_id,
                amount=amount,
                is_gift=is_gift,
                status='confirmed'  # по умолчанию confirmed, как в старом коде
            )
            session.add(payment)
            try:
                await session.commit()
                logger.success(
                    f"💰 Платёж Telegram Stars записан: user_id={user_id}, amount={amount}, is_gift={is_gift}")
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ Ошибка записи платежа Telegram Stars: {e}")

    async def create_gift(self, giver_id: int, duration: int, white_flag: bool) -> str:
        """Создаёт запись о подарке и возвращает gift_id."""
        gift_id = str(uuid.uuid4())
        async with self.session_factory() as session:
            gift = Gifts(
                gift_id=gift_id,
                giver_id=giver_id,
                duration=duration,
                recepient_id=None,
                white_flag=white_flag,
                flag=False
            )
            session.add(gift)
            try:
                await session.commit()
                logger.info(f"✅ Запись о подарке создана: gift_id={gift_id}")
                return gift_id
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ Ошибка создания подарка: {e}")
                raise

    async def add_online_stats(self, users_panel: int, users_active: int, users_pay: int, users_trial: int) -> None:
        """
        Сохраняет ежедневную статистику онлайн-активности.
        """
        async with self.session_factory() as session:
            online_record = Online(
                users_panel=users_panel,
                users_active=users_active,
                users_pay=users_pay,
                users_trial=users_trial
            )
            session.add(online_record)
            await session.commit()

    async def add_platega_payment(self, user_id: int, amount: int, status: str, transaction_id: str,
                                  is_gift: bool = False) -> None:
        """
        Записывает платёж Platega в таблицу payments.
        """
        async with self.session_factory() as session:
            payment = Payments(
                user_id=user_id,
                amount=amount,
                status=status,
                transaction_id=transaction_id,
                is_gift=is_gift
            )
            session.add(payment)
            try:
                await session.commit()
                logger.success(f"💰 Платёж Platega записан: user_id={user_id}, amount={amount}, is_gift={is_gift}")
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ Ошибка записи платежа Platega: {e}")
                raise

    async def add_payment(self, user_id: int, amount: int, status: str, transaction_id: str,
                          is_gift: bool = False) -> None:
        """
        Запись платежа Platega в таблицу payments.
        """
        async with self.session_factory() as session:
            payment = Payments(
                user_id=user_id,
                amount=amount,
                is_gift=is_gift,
                status=status,
                transaction_id=transaction_id
            )
            session.add(payment)
            await session.commit()
            logger.success(f"💰 Платёж Platega записан: user_id={user_id}, amount={amount}, is_gift={is_gift}")

    async def add_cryptobot_payment(self, user_id: int, amount: float, currency: str, is_gift: bool, invoice_id: str,
                                    payload: str) -> None:
        """
        Запись платежа Cryptobot в таблицу payments_cryptobot.
        """
        async with self.session_factory() as session:
            payment = PaymentsCryptobot(
                user_id=user_id,
                amount=amount,
                currency=currency,
                is_gift=is_gift,
                status='active',
                invoice_id=invoice_id,
                payload=payload
            )
            session.add(payment)
            await session.commit()
            logger.info(f"Cryptobot invoice created: {invoice_id} for user {user_id}")

    async def get_all_users(self) -> List[Users]:
        """Возвращает список всех пользователей."""
        async with self.session_factory() as session:
            result = await session.execute(select(Users))
            return result.scalars().all()

    async def get_all_payments(self) -> List[Payments]:
        """Возвращает список всех платежей Platega."""
        async with self.session_factory() as session:
            result = await session.execute(select(Payments))
            return result.scalars().all()

    async def get_all_payments_stars(self) -> List[PaymentsStars]:
        """Возвращает список всех платежей Telegram Stars."""
        async with self.session_factory() as session:
            result = await session.execute(select(PaymentsStars))
            return result.scalars().all()

    async def get_all_payments_cryptobot(self) -> List[PaymentsCryptobot]:
        """Возвращает список всех крипто-платежей."""
        async with self.session_factory() as session:
            result = await session.execute(select(PaymentsCryptobot))
            return result.scalars().all()

    async def get_all_gifts(self) -> List[Gifts]:
        """Возвращает список всех подарков."""
        async with self.session_factory() as session:
            result = await session.execute(select(Gifts))
            return result.scalars().all()

    async def get_all_online(self) -> List[Online]:
        """Возвращает список всех записей онлайн-статистики."""
        async with self.session_factory() as session:
            result = await session.execute(select(Online))
            return result.scalars().all()

    async def get_all_white_counter(self) -> List[WhiteCounter]:
        """Возвращает список всех записей white_counter."""
        async with self.session_factory() as session:
            result = await session.execute(select(WhiteCounter))
            return result.scalars().all()

    async def add_white_counter_if_not_exists(self, user_id: int) -> None:
        """
        Добавляет запись в white_counter, если её ещё нет для данного пользователя.
        """
        async with self.session_factory() as session:
            stmt = select(WhiteCounter).where(WhiteCounter.user_id == user_id)
            result = await session.execute(stmt)
            if not result.scalar_one_or_none():
                session.add(WhiteCounter(user_id=user_id))
                await session.commit()
                logger.info(f"✅ Добавлена запись в white_counter для пользователя {user_id}")
