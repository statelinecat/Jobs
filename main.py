import sqlite3
import schedule
import time
import logging
from datetime import datetime, timedelta
import requests
import signal

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
    """Инициализация базы данных с индексами"""
    try:
        conn = sqlite3.connect("vacancies.db")
        cursor = conn.cursor()

        # Удаляем старую таблицу, если существует
        cursor.execute("DROP TABLE IF EXISTS vacancies")

        # Создаем новую таблицу с правильной структурой
        cursor.execute("""
            CREATE TABLE vacancies (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                link TEXT UNIQUE NOT NULL,
                company TEXT,
                salary TEXT,
                experience TEXT,
                work_format TEXT,
                region TEXT,
                published_at TIMESTAMP,
                processed BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Создаем индексы
        cursor.execute("CREATE INDEX idx_link ON vacancies(link)")
        cursor.execute("CREATE INDEX idx_processed ON vacancies(processed)")
        cursor.execute("CREATE INDEX idx_created ON vacancies(created_at)")

        conn.commit()
        logging.info("База данных вакансий успешно инициализирована")
        return True
    except Exception as e:
        logging.error(f"Ошибка при инициализации БД: {e}")
        return False
    finally:
        if conn:
            conn.close()


def get_db_connection(db_name="vacancies.db"):
    """Безопасное подключение к SQLite с таймаутами"""
    conn = None
    attempts = 0
    while attempts < 3:
        try:
            conn = sqlite3.connect(db_name, timeout=20)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            return conn
        except sqlite3.OperationalError as e:
            logging.warning(f"Ошибка подключения (попытка {attempts + 1}): {e}")
            time.sleep(2 ** attempts)  # Экспоненциальная задержка
            attempts += 1
    raise sqlite3.OperationalError(f"Не удалось подключиться к {db_name} после 3 попыток")


def fetch_hh_vacancies():
    """Получение вакансий с API HH.ru с обработкой ошибок"""
    url = "https://api.hh.ru/vacancies"
    headers = {"User-Agent": USER_AGENT}
    all_vacancies = []

    for region_id, region_name in REGIONS.items():
        params = {
            "text": "Python",
            "area": region_id,
            "per_page": 50,
            "date_from": (datetime.now() - timedelta(hours=SEARCH_HOURS)).strftime('%Y-%m-%dT%H:%M:%S'),
            "order_by": "publication_time"
        }

        try:
            response = requests.get(url, headers=headers, params=params, timeout=15)
            response.raise_for_status()
            items = response.json().get("items", [])

            for item in items:
                item['region'] = region_name
                item['fetched_at'] = datetime.now().isoformat()

            all_vacancies.extend(items)
            time.sleep(0.7)  # Вежливая задержка между запросами

        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка для региона {region_name}: {str(e)}")
            continue

    return all_vacancies


def parse_vacancy(item):
    """Парсинг данных одной вакансии"""
    try:
        published_at = datetime.strptime(
            item.get("published_at"),
            "%Y-%m-%dT%H:%M:%S%z"
        ).strftime('%Y-%m-%d %H:%M:%S')

        schedule_data = item.get("schedule", {})
        work_format = "Не указан"
        if schedule_data:
            schedule_id = schedule_data.get("id")
            if schedule_id == "remote":
                work_format = "Удаленная"
            elif schedule_id == "flexible":
                work_format = "Гибкий график"
            else:
                work_format = "Офис"

        return {
            "id": item.get("id"),
            "title": item.get("name", "").strip(),
            "link": item.get("alternate_url", "").split('?')[0],  # Убираем параметры ссылки
            "company": (item.get("employer", {}).get("name") or "").strip(),
            "salary": format_salary(item.get("salary")),
            "experience": (item.get("experience", {}).get("name") or "Не указан").strip(),
            "work_format": work_format,
            "region": item.get("region", "Неизвестно"),
            "published_at": published_at,
            "fetched_at": item.get("fetched_at")
        }
    except Exception as e:
        logging.warning(f"Ошибка обработки вакансии {item.get('id')}: {e}")
        return None


def format_salary(salary_data):
    """Форматирование зарплаты с проверкой валюты"""
    if not salary_data:
        return "Не указана"

    currency_map = {
        "RUR": "₽",
        "USD": "$",
        "EUR": "€"
    }

    from_val = salary_data.get("from")
    to_val = salary_data.get("to")
    currency = currency_map.get(salary_data.get("currency", "RUR"), "₽")

    if from_val and to_val:
        return f"{from_val:,.0f}–{to_val:,.0f} {currency}".replace(',', ' ')
    elif from_val:
        return f"от {from_val:,.0f} {currency}".replace(',', ' ')
    elif to_val:
        return f"до {to_val:,.0f} {currency}".replace(',', ' ')
    return "Не указана"


def save_vacancies(vacancies):
    """Пакетное сохранение вакансий в БД"""
    if not vacancies:
        return 0

    new_count = 0
    conn = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        for vacancy in vacancies:
            if not vacancy:
                continue

            try:
                cursor.execute(
                    """INSERT OR IGNORE INTO vacancies 
                    (id, title, link, company, salary, experience, 
                     work_format, region, published_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        vacancy["id"],
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
                if cursor.rowcount > 0:
                    new_count += 1

            except Exception as e:
                logging.error(f"Ошибка сохранения вакансии {vacancy.get('id')}: {e}")

        conn.commit()

    except Exception as e:
        logging.error(f"Ошибка работы с БД: {e}")
        raise
    finally:
        if conn:
            conn.close()

    return new_count


def run_parser_job():
    """Основная задача парсера"""
    try:
        logging.info("Начало проверки вакансий...")

        # Получаем сырые данные
        raw_vacancies = fetch_hh_vacancies()
        if not raw_vacancies:
            logging.info("Нет данных от API HH.ru")
            return

        # Парсим и фильтруем
        parsed_vacancies = [parse_vacancy(item) for item in raw_vacancies]
        valid_vacancies = [v for v in parsed_vacancies if v is not None]

        # Сохраняем в БД
        new_count = save_vacancies(valid_vacancies)

        logging.info(f"Обработано вакансий: {len(valid_vacancies)}, новых: {new_count}")

    except Exception as e:
        logging.error(f"Критическая ошибка в парсере: {e}")
        raise


def graceful_shutdown(signum, frame):
    """Обработчик сигналов завершения"""
    logging.info("Получен сигнал завершения, остановка парсера...")
    exit(0)


def main():
    """Основная функция"""
    # Настройка обработчиков сигналов
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    # Инициализация БД
    init_db()

    # Первый запуск сразу
    run_parser_job()

    # Настройка периодического выполнения
    schedule.every(CHECK_INTERVAL).minutes.do(run_parser_job)

    logging.info(f"Парсер запущен, проверка каждые {CHECK_INTERVAL} мин.")

    # Основной цикл
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    print("=" * 50)
    print("🚀 Парсер вакансий HH.ru")
    print(f"🔍 Проверка каждые {CHECK_INTERVAL} минут")
    print(f"⏳ Поиск за последние {SEARCH_HOURS // 24} дней")
    print("=" * 50)

    try:
        main()
    except KeyboardInterrupt:
        print("\n" + "=" * 50)
        print("🛑 Парсер остановлен")
        print("=" * 50)
    except Exception as e:
        logging.critical(f"Фатальная ошибка: {e}")
        raise