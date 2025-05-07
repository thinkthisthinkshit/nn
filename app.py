import logging
import sys
import uuid
import psycopg2
import hashlib
import sqlite3
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web, ClientSession
from urllib.parse import urlencode
import traceback
import asyncio

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)
logger.info("Начало выполнения скрипта")

# Настройки
TOKEN = "8173622705:AAE88BPX5k1mHuwFFBlWJS8ixxa36EmuCC0"
YOOMONEY_WALLET = "4100118178122985"
NOTIFICATION_SECRET = "CoqQlgE3E5cTzyAKY1LSiLU1"
SAVE_PAYMENT_PATH = "/save_payment"
YOOMONEY_NOTIFY_PATH = "/yoomoney_notify"
DB_CONNECTION = "postgresql://postgres.waybpljhbabayeankhxx:Alex4382!@aws-0-eu-central-1.pooler.supabase.com:6543/postgres"
PRIVATE_CHANNEL_ID = -1002609563244

# Инициализация бота
logger.info("Попытка инициализации бота")
try:
    bot = Bot(token=TOKEN)
    storage = MemoryStorage()
    dp = Dispatcher(bot, storage=storage)
    logger.info("Бот и диспетчер успешно инициализированы")
except Exception as e:
    logger.error(f"Ошибка инициализации бота: {e}")
    sys.exit(1)

# Инициализация баз данных
def init_sqlite_db():
    conn = sqlite3.connect("payments.db")
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS payments
                 (label TEXT PRIMARY KEY, user_id TEXT, status TEXT)''')
    conn.commit()
    conn.close()

def init_postgres_db():
    conn = psycopg2.connect(DB_CONNECTION)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS payments
                 (label TEXT PRIMARY KEY, user_id TEXT, status TEXT)''')
    conn.commit()
    conn.close()

init_sqlite_db()
init_postgres_db()

# Обработчик команды /start
@dp.message_handler(commands=['start'])
async def start_command(message: types.Message):
    try:
        user_id = str(message.from_user.id)
        logger.info(f"Получена команда /start от user_id={user_id}")
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton(text="Пополнить", callback_data="pay"))
        welcome_text = (
            "Тариф: фулл\n"
            "Стоимость: 2.00 🇷🇺RUB\n"
            "Срок действия: 1 месяц\n\n"
            "Вы получите доступ к следующим ресурсам:\n"
            "• Мой кайф (канал)"
        )
        await message.answer(welcome_text, reply_markup=keyboard)
        logger.info(f"Отправлен ответ на /start для user_id={user_id}")
    except Exception as e:
        logger.error(f"Ошибка в обработчике /start: {e}")
        await message.answer("Произошла ошибка, попробуйте позже.")

# Обработчик команды /pay и кнопки "Пополнить"
@dp.message_handler(commands=['pay'])
@dp.callback_query_handler(text="pay")
async def pay_command(message_or_callback: types.Message | types.CallbackQuery):
    try:
        if isinstance(message_or_callback, types.Message):
            user_id = str(message_or_callback.from_user.id)
            chat_id = message_or_callback.chat.id
        else:
            user_id = str(message_or_callback.from_user.id)
            chat_id = message_or_callback.message.chat.id

        logger.info(f"Получена команда /pay от user_id={user_id}")

        # Создание платёжной ссылки
        payment_label = str(uuid.uuid4())
        payment_params = {
            "quickpay-form": "shop",
            "paymentType": "AC",
            "targets": f"Оплата подписки для user_id={user_id}",
            "sum": 2.00,
            "label": payment_label,
            "receiver": YOOMONEY_WALLET,
            "successURL": f"https://t.me/{(await bot.get_me()).username}"
        }
        payment_url = f"https://yoomoney.ru/quickpay/confirm.xml?{urlencode(payment_params)}"
       
        # Сохранение label:user_id в SQLite
        conn = sqlite3.connect("payments.db")
        c = conn.cursor()
        c.execute("INSERT INTO payments (label, user_id, status) VALUES (?, ?, ?)",
                  (payment_label, user_id, "pending"))
        conn.commit()
        conn.close()
       
        # Отправка label:user_id на /save_payment
        async with ClientSession() as session:
            try:
                async with session.post(f"http://localhost:8000{SAVE_PAYMENT_PATH}", json={"label": payment_label, "user_id": user_id}) as response:
                    if response.status == 200:
                        logger.info(f"label={payment_label} сохранён для user_id={user_id}")
                    else:
                        logger.error(f"Ошибка сохранения на /save_payment: {await response.text()}")
                        await bot.send_message(chat_id, "Ошибка сервера, попробуйте позже.")
                        return
            except Exception as e:
                logger.error(f"Ошибка связи с /save_payment: {e}")
                await bot.send_message(chat_id, "Ошибка сервера, попробуйте позже.")
                return
       
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton(text="Оплатить", url=payment_url))
        await bot.send_message(chat_id, "Перейдите по ссылке для оплаты 2 рублей:", reply_markup=keyboard)
        logger.info(f"Отправлена ссылка на оплату для user_id={user_id}, label={payment_label}")
    except Exception as e:
        logger.error(f"Ошибка в обработчике /pay: {e}")
        await bot.send_message(chat_id, "Произошла ошибка при создании платежа, попробуйте позже.")

# Проверка подлинности YooMoney уведомления
def verify_yoomoney_notification(data):
    params = [
        data.get("notification_type", ""),
        data.get("operation_id", ""),
        str(data.get("amount", "")),
        data.get("currency", ""),
        data.get("datetime", ""),
        data.get("sender", ""),
        data.get("codepro", ""),
        NOTIFICATION_SECRET,
        data.get("label", "")
    ]
    sha1_hash = hashlib.sha1("&".join(params).encode()).hexdigest()
    return sha1_hash == data.get("sha1_hash", "")

# Создание уникальной одноразовой инвайт-ссылки
async def create_unique_invite_link(user_id):
    try:
        invite_link = await bot.create_chat_invite_link(
            chat_id=PRIVATE_CHANNEL_ID,
            member_limit=1,
            name=f"Invite for user_{user_id}"
        )
        return invite_link.invite_link
    except Exception as e:
        logger.error(f"Ошибка создания инвайт-ссылки: {e}")
        return None

# Обработчик YooMoney уведомлений
async def handle_yoomoney_notify(request):
    try:
        data = await request.post()
        logger.info(f"Получено YooMoney уведомление: {data}")
       
        if not verify_yoomoney_notification(data):
            logger.error("Неверный sha1_hash в YooMoney уведомлении")
            return web.Response(status=400, text="Invalid hash")
       
        label = data.get("label")
        if not label:
            logger.error("Отсутствует label в YooMoney уведомлении")
            return web.Response(status=400, text="Missing label")
       
        if data.get("notification_type") in ["p2p-incoming", "card-incoming"]:
            conn = psycopg2.connect(DB_CONNECTION)
            c = conn.cursor()
            c.execute("SELECT user_id FROM payments WHERE label = %s", (label,))
            result = c.fetchone()
            if result:
                user_id = result[0]
                c.execute("UPDATE payments SET status = %s WHERE label = %s", ("success", label))
                conn.commit()
                await bot.send_message(user_id, "Оплата успешно получена! Доступ к каналу активирован.")
                invite_link = await create_unique_invite_link(user_id)
                if invite_link:
                    await bot.send_message(user_id, f"Присоединяйтесь к приватному каналу: {invite_link}")
                    logger.info(f"Успешная транзакция и отправка инвайт-ссылки для label={label}, user_id={user_id}")
                else:
                    await bot.send_message(user_id, "Ошибка создания ссылки на канал. Свяжитесь с поддержкой.")
                    logger.error(f"Не удалось создать инвайт-ссылку для user_id={user_id}")
            else:
                logger.error(f"Label {label} не найден в базе")
            conn.close()
       
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"Ошибка обработки YooMoney уведомления: {e}\n{traceback.format_exc()}")
        return web.Response(status=500)

# Обработчик сохранения label:user_id
async def handle_save_payment(request):
    try:
        data = await request.json()
        label = data.get("label")
        user_id = data.get("user_id")
        if not label or not user_id:
            logger.error("Отсутствует label или user_id в запросе")
            return web.Response(status=400, text="Missing label or user_id")
       
        conn = psycopg2.connect(DB_CONNECTION)
        c = conn.cursor()
        c.execute("INSERT INTO payments (label, user_id, status) VALUES (%s, %s, %s) ON CONFLICT (label) DO UPDATE SET user_id = %s, status = %s",
                  (label, user_id, "pending", user_id, "pending"))
        conn.commit()
        conn.close()
        logger.info(f"Сохранено: label={label}, user_id={user_id}")
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"Ошибка сохранения payment: {e}\n{traceback.format_exc()}")
        return web.Response(status=500)

# Запуск бота с polling
async def start_polling():
    logger.info("Запуск polling с повторными попытками")
    attempt = 1
    while True:
        try:
            logger.info(f"Попытка {attempt}: Пропуск старых обновлений")
            await dp.skip_updates()
            logger.info(f"Попытка {attempt}: Запуск polling")
            await dp.start_polling(timeout=20)
            logger.info("Polling успешно запущен")
            break
        except Exception as e:
            logger.error(f"Попытка {attempt}: Ошибка запуска polling: {e}\n{traceback.format_exc()}")
            logger.info("Повторная попытка через 5 секунд...")
            await asyncio.sleep(5)
            attempt += 1
            if attempt > 5:
                logger.error("Превышено количество попыток запуска polling")
                raise Exception("Не удалось запустить polling после 5 попыток")

# Настройка веб-сервера
app = web.Application()
app.router.add_post(YOOMONEY_NOTIFY_PATH, handle_yoomoney_notify)
app.router.add_post(SAVE_PAYMENT_PATH, handle_save_payment)

# Запуск polling и веб-сервера
async def main():
    try:
        # Запускаем polling в отдельной задаче
        asyncio.create_task(start_polling())
        # Запускаем веб-сервер
        logger.info("Инициализация веб-сервера")
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 8000)
        await site.start()
        logger.info("Веб-сервер запущен на порту 8000")
        # Держим приложение работающим
        while True:
            await asyncio.sleep(3600)
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске: {e}\n{traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
