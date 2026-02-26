import datetime
import hashlib
import uuid

import requests

from config import PANEL_API_TOKEN, PANEL_URL
from logging_config import logger
from config_bd.users import SQL
import random
import string


class X3:
    def __init__(self):
        """Инициализация класса с настройками подключения"""
        self.target_url = PANEL_URL
        self.api_token = PANEL_API_TOKEN
        
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_token}'
        }
        
        self.params = {
            "vyWdoTBH": "VmsLiQrN"
        }
        
        self.ses = requests.Session()
        self.ses.verify = False
        self.working_host = self.target_url
        self.is_authenticated = True

    def authenticate(self):
        """Заглушка для совместимости со старым кодом"""
        return True

    def ensure_authenticated(self):
        """Заглушка для совместимости со старым кодом"""
        return True

    def generate_client_id(self, tg_id):
        """Генерирует client_id на основе telegram id"""
        tg_id_str = str(tg_id).encode('utf-8')
        hash_object = hashlib.sha1(tg_id_str)
        client_id = hash_object.hexdigest()[:9]
        return client_id

    def list_from_host(self, host):
        """Заглушка для совместимости со старым кодом"""
        return {'obj': [{'settings': '{"clients": []}'}]}

    def test_connect(self):
        """Тестирует подключение к API"""
        try:
            response = self.ses.get(
                f"{self.target_url}/api/auth/status",
                params=self.params,
                timeout=5
            )
            logger.info(f"Тест подключения: {response.status_code}")
            return [response]
        except Exception as e:
            logger.error(f"Ошибка подключения: {e}")
            return []

    def list(self, start):
        """Получает список всех пользователей"""
        try:
            params = self.params
            params['size'] = 1000
            params['start'] = start
            response = self.ses.get(
                f'{self.target_url}/api/users',
                headers=self.headers,
                params=self.params,
                timeout=5
            )
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"HTTP {response.status_code}: {response.text}")
                return {'response': {'users': []}}
        except Exception as e:
            logger.error(f"Ошибка запроса: {e}")
            return {'response': {'users': []}}

    def _generate_password(self, length=12):
        """Генерирует случайный пароль"""
        chars = string.ascii_letters + string.digits
        return ''.join(random.choice(chars) for _ in range(length))

    def addClient(self, day, user_id_str, user_id):
        """Добавляет нового клиента"""
        try:
            client_id = self.generate_client_id(user_id)
            if 'white' in user_id_str:
                client_id = self.generate_client_id(user_id * 100)
            current_time = datetime.datetime.utcnow()
            expire_time = current_time + datetime.timedelta(days=day)
            vless_uuid = str(uuid.uuid1())

            if 'white' in user_id_str:
                squad_1 = ['41d180d4-4f4c-46d7-81f0-76f45356e777']
                squad_2 = ['db73ace8-663b-4ef4-91da-0bfa7abe6e90']
                squad = random.choice([squad_1, squad_2])
                trafficLimitStrategy = "MONTH"
                trafficLimitBytes = 80530636800
                hwidDeviceLimit = 1
            else:
                squad_1 = ['6ba41467-be68-438c-ad6e-5a02f7df826c']
                squad_2 = ['c6973051-58b7-484c-b669-6a123cda465b']
                squad = random.choice([squad_1, squad_2])
                trafficLimitStrategy = "NO_RESET"
                trafficLimitBytes = 0
                hwidDeviceLimit = 3

            data = {
                "username": user_id_str,
                "status": "ACTIVE",
                "shortUuid": client_id,
                "trojanPassword": self._generate_password(),
                "vlessUuid": vless_uuid,
                "ssPassword": self._generate_password(),
                "trafficLimitStrategy": trafficLimitStrategy,
                "trafficLimitBytes": trafficLimitBytes,
                "expireAt": expire_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                "createdAt": current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                "hwidDeviceLimit": hwidDeviceLimit,
                "telegramId": int(user_id),
                "description": "New user",
                "activeInternalSquads": squad
            }

            logger.info(f"Добавление клиента {user_id_str}, срок до: {expire_time}")

            response = self.ses.post(
                f"{self.target_url}/api/users",
                headers=self.headers,
                json=data,
                params=self.params,
                timeout=10
            )
            
            logger.info(f"Код ответа: {response.status_code}")
            
            if response.status_code in [200, 201]:
                response_data = response.json()
                if response_data.get("success", True):
                    subscription_end_date = expire_time.replace(tzinfo=datetime.timezone.utc)
                    sql = SQL()
                    if 'white' in user_id_str:
                        sql.update_white_subscription_end_date(user_id, subscription_end_date)
                    else:
                        sql.update_subscription_end_date(user_id, subscription_end_date)
                    logger.info(f"✅ Клиент {user_id_str} успешно добавлен")
                    return True
                else:
                    logger.warning(f"❌ API вернул ошибку: {response.text}")
                    return False
            else:
                logger.error(f"❌ Ошибка добавления клиента: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка при добавлении клиента {user_id_str}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def updateClient(self, day, user_id_str, user_id):
        """Обновляет клиента - добавляет дни к подписке"""
        try:
            # Получаем данные пользователя
            user_response = self.get_user_by_username(user_id_str)

            if not user_response or 'response' not in user_response:
                logger.error(f"❌ Пользователь {user_id_str} не найден")
                return False

            user = user_response['response']
            
            # Проверяем обязательные поля
            if 'uuid' not in user or 'expireAt' not in user:
                logger.error(f"❌ У пользователя {user_id_str} отсутствуют обязательные поля")
                return False

            uuid_user = user['uuid']
            
            # Парсим текущую дату истечения
            expire_at_str = user['expireAt']
            current_expire_at = datetime.datetime.fromisoformat(expire_at_str.replace('Z', '+00:00'))
            now = datetime.datetime.now(datetime.timezone.utc)

            # Определяем новую дату истечения
            if current_expire_at < now:
                # Подписка истекла - начинаем с текущего момента
                new_expire_at = now + datetime.timedelta(days=day)
                status = 'ACTIVE'  # Активируем подписку
                logger.info(f"Подписка пользователя {user_id_str} истекла. Активируем и добавляем {day} дней")
            else:
                # Подписка активна - добавляем к существующей дате
                new_expire_at = current_expire_at + datetime.timedelta(days=day)
                status = user.get('status', 'ACTIVE')
                logger.info(f"Подписка пользователя {user_id_str} активна. Добавляем {day} дней")

            # Обрабатываем activeInternalSquads
            raw_squads = user.get('activeInternalSquads', [])
            squads = []
            for s in raw_squads:
                if isinstance(s, dict) and 'uuid' in s:
                    squads.append(s['uuid'])
                elif isinstance(s, str):
                    squads.append(s)

            # Формируем данные для обновления
            data = {
                "uuid": uuid_user,
                "status": status,
                "expireAt": new_expire_at.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                "trafficLimitBytes": user.get('trafficLimitBytes', 0),
                "trafficLimitStrategy": user.get('trafficLimitStrategy', "NO_RESET"),
                "activeInternalSquads": squads
            }

            logger.info(f"Обновление пользователя {user_id_str}:")
            logger.info(f"  Старая дата: {current_expire_at.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"  Новая дата: {new_expire_at.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"  Добавлено дней: {day}")

            # Отправляем PATCH запрос
            response = self.ses.patch(
                f"{self.target_url}/api/users",
                headers=self.headers,
                json=data,
                params=self.params,
                timeout=10
            )
            
            logger.info(f"Код ответа: {response.status_code}")
            
            if response.status_code == 200:
                response_data = response.json()
                
                if response_data.get("success", True):
                    # Обновляем базу данных
                    sql = SQL()
                    if 'white' in user_id_str:
                        sql.update_white_subscription_end_date(user_id, new_expire_at)
                    else:
                        sql.update_subscription_end_date(user_id, new_expire_at)
                    logger.info(f"✅ Клиент {user_id_str} успешно обновлён, добавлено {day} дней")
                    return True
                else:
                    logger.error(f"❌ API вернул success=false: {response.text}")
                    return False
            else:
                logger.error(f"❌ Ошибка обновления: HTTP {response.status_code}")
                logger.error(f"Ответ: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении клиента {user_id_str}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def get_user_by_username(self, username):
        """Получает пользователя по username"""
        try:
            response = self.ses.get(
                f"{self.target_url}/api/users/by-username/{username}",
                headers=self.headers,
                params=self.params,
                timeout=5
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Ошибка получения пользователя {username}: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка получения пользователя {username}: {e}")
            return None

    def get_user_by_telegram_id(self, telegram_id):
        """Получает пользователя по Telegram ID"""
        try:
            response = self.ses.get(
                f"{self.target_url}/api/users/by-telegram-id/{telegram_id}",
                headers=self.headers,
                params=self.params,
                timeout=5
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return None
                
        except Exception as e:
            logger.error(f"Ошибка получения пользователя: {e}")
            return None

    def sublink(self, user_id_str: str):
        """Генерирует ссылку подписки для пользователя"""
        try:
            users = self.get_user_by_username(user_id_str)
            if users and 'response' in users and users['response']:
                user = users['response']
                return user.get('subscriptionUrl', '')
        except Exception as e:
            logger.error(f"Ошибка при генерации ссылки для {user_id_str}: {e}")
        
        return ""

    def time_active(self, user_id: str):
        """Получает информацию об активном времени пользователя"""
        dict_x = {}
        
        try:
            users = self.get_user_by_telegram_id(user_id)
            
            if users and 'response' in users and users['response']:
                user = users['response'][0]
                if user.get('status') == 'ACTIVE':
                    expiry_time = user.get('expireAt')
                    if expiry_time:
                        expiry_dt = datetime.datetime.fromisoformat(expiry_time.replace('Z', '+00:00'))
                        epoch = datetime.datetime.utcfromtimestamp(0)
                        expiry_ms = int((expiry_dt - epoch).total_seconds() * 1000.0)
                        dict_x[user.get('uuid', '0')] = expiry_ms
                        return dict_x

        except Exception as e:
            logger.error(f"Ошибка при получении времени активности для {user_id}: {e}")

        dict_x['0'] = '0'
        return dict_x

    def activ(self, user_id: str):
        """Проверяет активность подписки пользователя"""
        result = {'activ': '🔎 - Не подключён', 'time': '-'}

        try:
            users = self.get_user_by_username(user_id)

            if not users or 'response' not in users or not users['response']:
                logger.info(f"Пользователь {user_id} не найден в системе")
                return result

            user = users['response']
            current_time = int(datetime.datetime.utcnow().timestamp() * 1000)
            
            expiry_time_str = user.get('expireAt')
            if not expiry_time_str:
                return result
            
            expiry_dt = datetime.datetime.fromisoformat(expiry_time_str.replace('Z', '+00:00'))
            expiry_time = int(expiry_dt.timestamp() * 1000)
            
            # Форматируем время для отображения (добавляем 3 часа для МСК)
            expiry_dt_msk = expiry_dt + datetime.timedelta(hours=3)
            readable_time = expiry_dt_msk.strftime('%d-%m-%Y %H:%M') + ' МСК'
            result['time'] = readable_time

            if user.get('status') == 'ACTIVE' and expiry_time > current_time:
                result['activ'] = '✅ - Активен'
            else:
                result['activ'] = '❌ - Не Активен'
            
            return result

        except Exception as e:
            logger.error(f"Ошибка в методе activ для {user_id}: {e}")
            result['activ'] = '❌ - Внутренняя ошибка'
            return result

    def activ_list(self):
        """Получает список всех клиентов"""
        lst_users = []

        try:
            users_all = []
            for i in range(50):
                data = self.list(1000 * i + 1)
                if len(data['response']['users']) != 0:
                    users_all.extend(data['response']['users'])
                else:
                    break
            logger.info(f'Всего юзеров в панели - {len(users_all)}')
            for user in users_all:
                if user['firstConnectedAt'] and user['description'] != 'New user - without pay':
                    try:
                        lst_users.append([int(user['telegramId']), user['expireAt']])
                    except:
                        pass

        except Exception as e:
            logger.error(f"Ошибка при получении списка активности: {e}")

        return lst_users


    def get_all_users(self):
        """Получает список всех клиентов"""
        lst_users = []

        try:
            users_all = []
            for i in range(50):
                data = self.list(1000 * i + 1)
                if len(data['response']['users']) != 0:
                    users_all.extend(data['response']['users'])
                else:
                    break
            logger.info(f'Всего юзеров в панели - {len(users_all)}')
            for user in users_all:
                if user['description'] != 'New user - without pay':
                    try:
                        lst_users.append(user)
                    except:
                        pass

        except Exception as e:
            logger.error(f"Ошибка при получении списка активности: {e}")

        return lst_users

    def update_user_squads(self, user_uuid: str, squads: list):
        """
        Обновляет поле activeInternalSquads у пользователя по его UUID.
        :param user_uuid: UUID пользователя в панели
        :param squads: список squad UUID (например, ['2fcfd928-6f45-4a8c-a36b-742fca8efea0'])
        :return: True при успехе, False при ошибке
        """
        try:
            data = {
                "uuid": user_uuid,
                "activeInternalSquads": squads
            }
            response = self.ses.patch(
                f"{self.target_url}/api/users",
                headers=self.headers,
                json=data,
                params=self.params,
                timeout=10
            )
            if response.status_code == 200:
                response_data = response.json()
                if response_data.get("success", True):
                    logger.info(f"✅ Squad обновлён для UUID {user_uuid}")
                    return True
                else:
                    logger.error(f"❌ API вернул ошибку: {response.text}")
                    return False
            else:
                logger.error(f"❌ Ошибка HTTP {response.status_code}: {response.text}")
                return False
        except Exception as e:
            logger.error(f"❌ Исключение при обновлении squads: {e}")
            return False