import aiohttp
from typing import Dict, Optional
from sqlalchemy import insert

from config import PLATEGA_API_KEY, PLATEGA_MERCHANT_ID
from config_bd.BaseModel import engine, payments
from logging_config import logger


async def record_payment(user_id: int, amount: int, status: str, transaction_id: str, is_gift: bool = False):
    """Запись платежа в таблицу payments"""
    try:
        with engine.connect() as conn:
            stmt = insert(payments).values(
                user_id=user_id,
                amount=amount,
                is_gift=is_gift,
                status=status,
                transaction_id=transaction_id
            )
            conn.execute(stmt)
            conn.commit()
        logger.success(f"💰 Платёж Platega записан: user_id={user_id}, amount={amount}, is_gift={is_gift}")
    except Exception as e:
        logger.error(f"❌ Ошибка записи платежа Platega: {e}")


class PlategaPayment:
    """Класс для работы с Platega.io API"""
    
    def __init__(self, api_key: str, merchant_id: str):
        self.api_key = api_key
        self.merchant_id = merchant_id
        self.base_url = "https://app.platega.io"
        self.headers = {
            "X-Secret": api_key,
            "X-MerchantId": merchant_id,
            "Content-Type": "application/json"
        }
    
    async def create_payment(
        self, 
        amount: float, 
        description: str,
        payment_method: int = 2,
        return_url: str = "https://t.me/zoomerskyvpn_bot",
        failed_url: str = "https://t.me/zoomerskyvpn_bot",
        payload: Optional[str] = None
    ) -> Dict:
        """Создание платежа через Platega.io"""
        url = f"{self.base_url}/transaction/process"
        
        data = {
            "paymentMethod": payment_method,
            "paymentDetails": {
                "amount": float(amount),
                "currency": "RUB"
            },
            "description": description,
            "return": return_url,
            "failedUrl": failed_url
        }
        
        if payload:
            data["payload"] = payload
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data, headers=self.headers) as response:
                    response_text = await response.text()
                    
                    if response.status == 200:
                        result = await response.json()
                        
                        return {
                            'status': result.get('status', 'PENDING').lower(),
                            'url': result.get('redirect', ''),
                            'id': result.get('transactionId', ''),
                            'payment_method': result.get('paymentMethod', 'UNKNOWN')
                        }
                    else:
                        logger.error(f"Platega API error {response.status}: {response_text}")
                        print(f"❌ Platega error: {response_text}")
                        raise Exception(f"Ошибка создания платежа: {response.status}")
                        
        except Exception as e:
            logger.error(f"Error creating Platega payment: {e}")
            raise

    async def check_payment(self, transaction_id):

        url = f"{self.base_url}/transaction/{transaction_id}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers) as response:
                    response_text = await response.text()

                    if response.status == 200:
                        result = await response.json()

                        return result
                    else:
                        logger.error(f"Platega API check error {response.status}: {response_text}")
                        print(f"❌ Platega check error: {response_text}")
                        raise Exception(f"Ошибка проверки платежа: {response.status}")

        except Exception as e:
            logger.error(f"Error checking Platega payment: {e}")
            raise


async def pay(val: str, des: str, user_id: str, duration: str, white: bool, payment_method: int = 2) -> Dict:
    """Создание платежа для совместимости с pay_yoo.py"""
    
    platega = PlategaPayment(PLATEGA_API_KEY, PLATEGA_MERCHANT_ID)
    payload = f"user_id:{user_id},duration:{duration},white:{white},gift:False,method:sbp,amount:{int(val)}"
    
    try:
        result = await platega.create_payment(
            amount=float(val),
            description=des,
            payment_method=payment_method,
            payload=payload
        )

        await record_payment(int(user_id), int(val), result['status'], result['id'])

        logger.info(f"✅ Platega payment created: {result['status']}")
        logger.info(f"🔗 Payment URL: {result['url']}")
        logger.info(f"🆔 Transaction ID: {result['id']}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error creating Platega payment: {e}")
        return {
            'status': 'error',
            'url': '',
            'id': ''
        }


async def pay_for_gift(val: str, des: str, user_id: str, duration: str, white: bool, payment_method: int = 2) -> Dict:
    """Создание платежа для совместимости с pay_yoo.py"""

    platega = PlategaPayment(PLATEGA_API_KEY, PLATEGA_MERCHANT_ID)
    payload = f"user_id:{user_id},duration:{duration},white:{white},gift:True,method:sbp,amount:{int(val)}"

    try:
        result = await platega.create_payment(
            amount=float(val),
            description=des,
            payment_method=payment_method,
            payload=payload
        )

        await record_payment(int(user_id), int(val), result['status'], result['id'], True)

        logger.info(f"✅ Platega payment for gift created: {result['status']}")
        logger.info(f"🔗 Payment URL for gift: {result['url']}")
        logger.info(f"🆔 Transaction ID for gift: {result['id']}")

        return result

    except Exception as e:
        logger.error(f"❌ Error creating Platega payment: {e}")
        return {
            'status': 'error',
            'url': '',
            'id': ''
        }
