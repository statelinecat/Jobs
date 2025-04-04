import sqlite3
import asyncio
from telegram import Bot
from telegram.error import TelegramError
import schedule
import time
import logging
from datetime import datetime, timedelta
from config import TOKEN
import os
import subprocess
import sys
import requests

# Настройки
SEARCH_HOURS = 240  # Ищем вакансии за последние N часов
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
CHECK_INTERVAL = 60  # Проверка каждые N минут

# Основные регионы
REGIONS = {
    1: "Москва",
    2: "Санкт-Петербург",
    4: "Новосибирск",
    3: "Екатеринбург",
    113: "Россия"  # Удаленная работа
}

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('vacancy_parser.log'),
        logging.StreamHandler()
    ]
)


def init_db():
    """Инициализация базы данных"""
    try:
        # База вакансий
        conn_vac = sqlite3.connect("vacancies.db")
        cursor_vac = conn_vac.cursor()
        cursor_vac.execute("""
            CREATE TABLE IF NOT EXISTS vacancies (
                id INTEGER PRIMARY KEY,
                title TEXT,
                link TEXT UNIQUE,
                company TEXT,
                salary TEXT,
                experience TEXT,
                work_format TEXT,
                region TEXT,
                published_at TIMESTAMP
            )
        """)
        conn_vac.commit()
        conn_vac.close()

        # База пользователей
        conn_users = sqlite3.connect("users.db")
        cursor_users = conn_users.cursor()
        cursor_users.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn_users.commit()
        conn_users.close()

        logging.info("Базы данных инициализированы")
    except Exception as e:
        logging.error(f"Ошибка при инициализации БД: {e}")


def get_db_connection(db_name):
    """Безопасное подключение к SQLite"""
    conn = None
    attempts = 0
    while attempts < 3:
        try:
            conn = sqlite3.connect(db_name, timeout=10)
            conn.execute("PRAGMA busy_timeout = 10000")
            return conn
        except sqlite3.OperationalError as e:
            logging.warning(f"Ошибка подключения к {db_name} (попытка {attempts + 1}): {e}")
            time.sleep(1)
            attempts += 1
    raise sqlite3.OperationalError(f"Не удалось подключиться к {db_name} после 3 попыток")


def get_hh_vacancies():
    """Получение вакансий с API HH.ru"""
    url = "https://api.hh.ru/vacancies"
    headers = {"User-Agent": USER_AGENT}
    all_vacancies = []

    for region_id, region_name in REGIONS.items():
        params = {
            "text": "Python",
            "area": region_id,
            "per_page": 20,
            "date_from": (datetime.now() - timedelta(hours=SEARCH_HOURS)).strftime('%Y-%m-%dT%H:%M:%S'),
            "order_by": "publication_time"
        }

        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            items = response.json().get("items", [])
            for item in items:
                item['region'] = region_name
            all_vacancies.extend(items)
            time.sleep(0.5)
        except Exception as e:
            logging.error(f"Ошибка для региона {region_name}: {e}")

    return all_vacancies


def parse_vacancies():
    """Парсинг данных о вакансиях"""
    raw_vacancies = get_hh_vacancies()
    vacancies = []

    for item in raw_vacancies:
        try:
            published_at = datetime.strptime(
                item.get("published_at"),
                "%Y-%m-%dT%H:%M:%S%z"
            ).strftime('%d.%m.%Y %H:%M')

            schedule = item.get("schedule", {})
            work_format = "Не указан"
            if schedule:
                if schedule.get("id") == "remote":
                    work_format = "Удаленная"
                elif schedule.get("id") == "flexible":
                    work_format = "Гибкий график"
                else:
                    work_format = "Офис"

            vacancies.append({
                "title": item.get("name", ""),
                "link": item.get("alternate_url", ""),
                "company": item.get("employer", {}).get("name", ""),
                "salary": format_salary(item.get("salary")),
                "experience": item.get("experience", {}).get("name", "Не указан"),
                "work_format": work_format,
                "region": item.get("region", "Неизвестно"),
                "published_at": published_at
            })
        except Exception as e:
            logging.warning(f"Ошибка обработки вакансии: {e}")

    return vacancies


def format_salary(salary_data):
    """Форматирование зарплаты"""
    if not salary_data:
        return "Не указана"

    from_val = salary_data.get("from")
    to_val = salary_data.get("to")
    currency = salary_data.get("currency", "RUR")

    if from_val and to_val:
        return f"{from_val:,}–{to_val:,} {currency}".replace(',', ' ')
    elif from_val:
        return f"от {from_val:,} {currency}".replace(',', ' ')
    elif to_val:
        return f"до {to_val:,} {currency}".replace(',', ' ')
    return "Не указана"


def filter_new_vacancies(vacancies):
    """Фильтрация новых вакансий"""
    if not vacancies:
        return []

    new_vacancies = []
    conn = None

    try:
        conn = get_db_connection("vacancies.db")
        cursor = conn.cursor()

        for vacancy in vacancies:
            try:
                cursor.execute("SELECT 1 FROM vacancies WHERE link = ?", (vacancy["link"],))
                if not cursor.fetchone():
                    new_vacancies.append(vacancy)
                    cursor.execute(
                        """INSERT INTO vacancies 
                        (title, link, company, salary, experience, work_format, region, published_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            vacancy["title"],
                            vacancy["link"],
                            vacancy["company"],
                            vacancy["salary"],
                            vacancy["experience"],
                            vacancy["work_format"],
                            vacancy["region"],
                            vacancy["published_at"]
                        )
                    )
                    conn.commit()
            except Exception as e:
                logging.error(f"Ошибка проверки вакансии: {e}")

    except Exception as e:
        logging.error(f"Ошибка работы с БД вакансий: {e}")
    finally:
        if conn:
            conn.close()

    return new_vacancies


async def send_telegram_alert(vacancies):
    """Отправка уведомлений пользователям"""
    if not vacancies:
        return

    bot = Bot(token=TOKEN)
    conn = None

    try:
        conn = get_db_connection("users.db")
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM users")
        users = cursor.fetchall()

        if not users:
            logging.info("Нет подписчиков для отправки уведомлений")
            return

        for user_id, in users:
            for vacancy in vacancies:
                try:
                    message = (
                        "🚀 *Новая вакансия!*\n"
                        f"📌 *{vacancy['title']}*\n"
                        f"🏢 *{vacancy['company']}*\n"
                        f"📍 *Регион:* {vacancy['region']}\n"
                        f"💰 *Зарплата:* {vacancy['salary']}\n"
                        f"🧑‍💻 *Опыт:* {vacancy['experience']}\n"
                        f"🏠 *Формат работы:* {vacancy['work_format']}\n"
                        f"⏳ *Опубликована:* {vacancy['published_at']}\n"
                        f"🔗 [Ссылка]({vacancy['link']})"
                    )
                    await bot.send_message(
                        chat_id=user_id,
                        text=message,
                        parse_mode="Markdown",
                        disable_web_page_preview=True
                    )
                    await asyncio.sleep(1)
                except TelegramError as e:
                    logging.error(f"Ошибка отправки пользователю {user_id}: {e}")
                    if "Forbidden: bot was blocked by the user" in str(e):
                        try:
                            with get_db_connection("users.db") as del_conn:
                                del_conn.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
                                del_conn.commit()
                                logging.info(f"Удален заблокировавший бота пользователь: {user_id}")
                        except Exception as db_error:
                            logging.error(f"Ошибка удаления пользователя: {db_error}")
    except Exception as e:
        logging.error(f"Ошибка при отправке уведомлений: {e}")
    finally:
        if conn:
            conn.close()


async def async_job():
    """Асинхронная задача парсера"""
    try:
        logging.info("Запуск проверки вакансий...")
        vacancies = parse_vacancies()
        new_vacancies = filter_new_vacancies(vacancies)

        if new_vacancies:
            logging.info(f"Найдено {len(new_vacancies)} новых вакансий")
            await send_telegram_alert(new_vacancies)
        else:
            logging.info("Новых вакансий не найдено")
    except Exception as e:
        logging.error(f"Ошибка в async_job: {e}")


def run_parser():
    """Запуск парсера"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    schedule.every(CHECK_INTERVAL).minutes.do(
        lambda: loop.run_until_complete(async_job())
    )

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    init_db()

    # Запуск бота в отдельном процессе
    bot_process = subprocess.Popen([sys.executable, "bot.py"])

    # Запуск парсера в основном процессе
    print("=" * 50)
    print(f"🚀 Парсер вакансий | Проверка каждые {CHECK_INTERVAL} мин")
    print(f"🤖 Бот запущен в фоновом режиме (PID: {bot_process.pid})")
    print("=" * 50)

    try:
        run_parser()
    except KeyboardInterrupt:
        bot_process.terminate()
        print("\n" + "=" * 50)
        print("🛑 Парсер остановлен, бот завершен")
        print("=" * 50)