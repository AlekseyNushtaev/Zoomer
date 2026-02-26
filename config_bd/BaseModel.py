from sqlalchemy import create_engine, MetaData, Table, String, Integer, Column, DateTime, Boolean, Date, BigInteger, \
    Float
from datetime import datetime

# Подключение к базе данных
engine = create_engine('sqlite:///sqlite3.db')
engine.connect()
metadata = MetaData()

# Шаблон таблицы
users = Table(
    'users',
    metadata,
    Column('Id', Integer(), unique=True, primary_key=True),
    Column('User_id', Integer(), unique=True, nullable=False),
    Column('Ref', String(100), nullable=True),
    Column('Is_delete', Boolean(), default=False),
    Column('Is_pay_null', Boolean(), default=False),
    Column('Is_tarif', Boolean(), default=False),
    Column('Create_user', DateTime(), default=datetime.now),
    Column('Is_admin', Boolean(), default=False),
    Column('has_discount', Boolean(), default=False),
    Column('subscription_end_date', DateTime(), nullable=True),
    Column('white_subscription_end_date', DateTime(), nullable=True),
    Column('last_notification_date', Date(), nullable=True),
    Column('last_broadcast_status', String(100), nullable=True),
    Column('last_broadcast_date', DateTime(), nullable=True),
    Column('stamp', String(100), nullable=False),
    Column('ttclid', String(100), nullable=True),
)


gifts = Table(
    'gifts',
    metadata,
    Column('gift_id', String(36), primary_key=True),
    Column('giver_id', BigInteger, nullable=False),  # телеграм id юзера который оплатил подарок
    Column('duration', Integer, nullable=False),  # продолжительность подписки
    Column('recepient_id', BigInteger, nullable=True),  # телеграм id получателя (если активирован)
    Column('white_flag', Boolean, default=False),  # что за подписка
    Column('flag', Boolean, default=False)  # флаг активации
)

payments = Table(
    'payments',
    metadata,
    Column('id', Integer(), primary_key=True, autoincrement=True),
    Column('user_id', BigInteger(), nullable=False),
    Column('amount', Integer(), nullable=False),
    Column('time_created', DateTime(), default=datetime.now),
    Column('is_gift', Boolean(), default=False),
    Column('status', String(), nullable=True),
    Column('transaction_id', String(), nullable=True),
)

payments_stars = Table(
    'payments_stars',
    metadata,
    Column('id', Integer(), primary_key=True, autoincrement=True),
    Column('user_id', BigInteger(), nullable=False),
    Column('amount', Integer(), nullable=False),
    Column('time_created', DateTime(), default=datetime.now),
    Column('is_gift', Boolean(), default=False),
    Column('status', String(), default='confirmed'),
)

payments_cryptobot = Table(
    'payments_cryptobot',
    metadata,
    Column('id', Integer(), primary_key=True, autoincrement=True),
    Column('user_id', BigInteger(), nullable=False),
    Column('amount', Float(), nullable=False),          # сумма в криптовалюте
    Column('currency', String(10), nullable=False),    # TON, USDT
    Column('time_created', DateTime(), default=datetime.now),
    Column('is_gift', Boolean(), default=False),
    Column('status', String(), default='pending'),      # pending, paid, expired
    Column('invoice_id', String(), nullable=True),      # ID счета в Cryptobot
    Column('payload', String(), nullable=True),         # данные для обработки после оплаты
)

white_counter = Table(
    'white_counter',
    metadata,
    Column('id', Integer(), primary_key=True, autoincrement=True),
    Column('user_id', BigInteger(), nullable=False),
    Column('time_created', DateTime(), default=datetime.now),
)

online = Table(
    'online',
    metadata,
    Column('online_id', Integer(), primary_key=True, autoincrement=True),
    Column('online_date', DateTime(), default=datetime.now, nullable=False),
    Column('users_panel', Integer(), nullable=False),
    Column('users_active', Integer(), nullable=False),
    Column('users_pay', Integer(), nullable=False),
    Column('users_trial', Integer(), nullable=False),
)

# Создание таблиц в базе данных
metadata.create_all(engine)
