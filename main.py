import requests
import sqlite3
import asyncio
from telegram import Bot
from telegram.error import TelegramError
import schedule
import time
import logging
from datetime import datetime, timedelta
from config import TOKEN, CHAT_ID
import os

# Настройки
SEARCH_HOURS = 240  # Ищем вакансии за последние N часов
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
CHECK_INTERVAL = 1  # Проверка каждые N минут

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
    """Инициализация базы данных с полем для опыта и формата работы"""
    try:
        conn = sqlite3.connect("vacancies.db")
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vacancies (
                id INTEGER PRIMARY KEY,
                title TEXT,
                link TEXT UNIQUE,
                company TEXT,
                salary TEXT,
                experience TEXT,
                work_format TEXT,
                region TEXT,
                published_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
    except Exception as e:
        logging.error(f"Ошибка при инициализации БД: {e}")
    finally:
        conn.close()


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
                item['region'] = region_name  # Добавляем название региона
            all_vacancies.extend(items)
            time.sleep(0.5)  # Задержка между запросами
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

            # Определяем формат работы
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
    conn = sqlite3.connect("vacancies.db")
    cursor = conn.cursor()

    # Проверяем существование таблицы
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vacancies'")
    if not cursor.fetchone():
        init_db()

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

    conn.close()
    return new_vacancies


async def async_send_telegram_alert(vacancies):
    """Отправка уведомлений в Telegram"""
    if not vacancies:
        return

    bot = Bot(token=TOKEN)

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
                chat_id=CHAT_ID,
                text=message,
                parse_mode="Markdown",
                disable_web_page_preview=True
            )
            await asyncio.sleep(1)
        except TelegramError as e:
            logging.error(f"Ошибка отправки в Telegram: {e}")


def send_telegram_alert(vacancies):
    """Синхронная обертка для отправки"""
    asyncio.run(async_send_telegram_alert(vacancies))


def job():
    """Основная задача для schedule"""
    try:
        logging.info("Запуск проверки вакансий")
        vacancies = parse_vacancies()
        new_vacancies = filter_new_vacancies(vacancies)

        if new_vacancies:
            send_telegram_alert(new_vacancies)
            logging.info(f"Найдено новых вакансий: {len(new_vacancies)}")
        else:
            logging.info("Новых вакансий не найдено")
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")


if __name__ == "__main__":
    if not os.path.exists("vacancies.db"):
        init_db()

    print("=" * 50)
    print(f"🚀 Парсер вакансий HH.ru | Проверка каждые {CHECK_INTERVAL} мин")
    print(f"⏳ Ищем вакансии за последние {SEARCH_HOURS} часов")
    print(f"🌍 Регионы: {', '.join(REGIONS.values())}")
    print("=" * 50 + "\n")

    schedule.every(CHECK_INTERVAL).minutes.do(job)

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n" + "=" * 50)
        print("🛑 Парсер остановлен")
        print("=" * 50)