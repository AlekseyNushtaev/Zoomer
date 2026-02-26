from sqlalchemy import select, update
from bot import bot
from config import CRYPTOBOT_API_TOKEN
from keyboard import keyboard_payment_cancel
from lexicon import lexicon
from logging_config import logger
from config_bd.BaseModel import engine, payments_cryptobot
from payments.pay_cryptobot import CryptoBotPayment
from payments.process_payload import process_confirmed_payment


async def check_cryptobot_payments():
    """Проверка статусов платежей Cryptobot и их обработка"""

    cryptobot = CryptoBotPayment(CRYPTOBOT_API_TOKEN)

    try:
        # Получаем все платежи со статусом 'pending'
        with engine.connect() as conn:
            stmt = select(payments_cryptobot).where(payments_cryptobot.c.status == 'active')
            result = conn.execute(stmt)
            pending_payments = result.fetchall()

        if not pending_payments:
            logger.info("No pending Cryptobot payments")
            return

        logger.info(f"Checking {len(pending_payments)} Cryptobot payments")

        processed = 0
        confirmed = 0
        expired = 0

        for payment in pending_payments:
            try:
                invoice_id = payment.invoice_id
                if not invoice_id:
                    continue

                status = await cryptobot.get_invoice_status(int(invoice_id))
                if status is None:
                    continue
                if status == 'active':
                    processed += 1
                    continue

                # Обновляем статус в БД
                with engine.connect() as conn:
                    update_stmt = update(payments_cryptobot).where(
                        payments_cryptobot.c.id == payment.id
                    ).values(status=status)
                    conn.execute(update_stmt)
                    conn.commit()

                logger.info(f"Payment {payment.id} status updated to {status}")

                if status == 'paid':
                    # Обрабатываем подтверждённый платёж
                    if payment.payload:
                        await process_confirmed_payment(payment.payload)
                    else:
                        logger.error(f"No payload for payment {payment.id}")
                    confirmed += 1
                elif status == 'expired':
                    expired += 1
                    # Уведомляем пользователя
                    try:
                        user_id = payment.user_id
                        cancel_text = lexicon['payment_cancel']
                        await bot.send_message(user_id, cancel_text, reply_markup=keyboard_payment_cancel())
                    except Exception as e:
                        logger.error(f"Failed to notify user {payment.user_id}: {e}")

                processed += 1

            except Exception as e:
                logger.error(f"Error processing payment {payment.id}: {e}")

        logger.info(f"Cryptobot check completed: processed={processed}, confirmed={confirmed}, expired={expired}")

    except Exception as e:
        logger.error(f"Error in check_cryptobot_payments: {e}")
