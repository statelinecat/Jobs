import sqlite3
import logging
import time
from config import TOKEN
import requests

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('notifier.log'),
        logging.StreamHandler()
    ]
)


def get_db_connection(db_name):
    """Безопасное подключение к SQLite с повторными попытками"""
    attempts = 0
    while attempts < 3:
        try:
            conn = sqlite3.connect(db_name, timeout=20)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            return conn
        except sqlite3.Error as e:
            logging.warning(f"Ошибка подключения к {db_name} (попытка {attempts + 1}): {e}")
            time.sleep(2 ** attempts)  # Экспоненциальная задержка
            attempts += 1
    raise sqlite3.OperationalError(f"Не удалось подключиться к {db_name} после 3 попыток")


def init_databases():
    """Инициализация всех необходимых таблиц в базах данных"""
    try:
        # Инициализация базы пользователей
        with get_db_connection("users.db") as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

        # Инициализация базы уведомлений
        with get_db_connection("vacancies.db") as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sent_notifications (
                    id INTEGER PRIMARY KEY,
                    vacancy_id INTEGER,
                    user_id INTEGER,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(vacancy_id, user_id),
                    FOREIGN KEY(vacancy_id) REFERENCES vacancies(id)
                )
            """)
            conn.commit()

    except Exception as e:
        logging.error(f"Ошибка инициализации БД: {e}")
        raise


def send_telegram_message(chat_id, text):
    """Отправка сообщения через Telegram Bot API"""
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка отправки сообщения: {e}")
        return False


def format_vacancy_message(vacancy):
    """Форматирование сообщения о вакансии"""
    return (
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


def get_new_vacancies(limit=50):
    """Получение новых вакансий из базы"""
    try:
        with get_db_connection("vacancies.db") as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT v.id, v.title, v.link, v.company, v.salary, 
                       v.experience, v.work_format, v.region, v.published_at
                FROM vacancies v
                LEFT JOIN sent_notifications s ON v.id = s.vacancy_id
                WHERE s.vacancy_id IS NULL
                LIMIT ?
            """, (limit,))
            return cursor.fetchall()
    except Exception as e:
        logging.error(f"Ошибка получения вакансий: {e}")
        return []


def get_active_users():
    """Получение списка активных пользователей"""
    try:
        with get_db_connection("users.db") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id FROM users")
            return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logging.error(f"Ошибка получения пользователей: {e}")
        return []


def mark_as_sent(vacancy_id, user_id):
    """Помечаем вакансию как отправленную"""
    try:
        with get_db_connection("vacancies.db") as conn:
            conn.execute(
                "INSERT OR IGNORE INTO sent_notifications (vacancy_id, user_id) VALUES (?, ?)",
                (vacancy_id, user_id)
            )
            conn.commit()
    except Exception as e:
        logging.error(f"Ошибка отметки вакансии {vacancy_id} для пользователя {user_id}: {e}")


def check_and_notify():
    """Основная функция проверки и отправки уведомлений"""
    try:
        # Получаем новые вакансии
        new_vacancies = get_new_vacancies()
        if not new_vacancies:
            logging.info("Нет новых вакансий для отправки")
            return

        # Получаем активных пользователей
        users = get_active_users()
        if not users:
            logging.info("Нет активных пользователей для отправки")
            return

        # Отправляем уведомления
        for vacancy_row in new_vacancies:
            vacancy = {
                'id': vacancy_row[0],
                'title': vacancy_row[1],
                'link': vacancy_row[2],
                'company': vacancy_row[3],
                'salary': vacancy_row[4],
                'experience': vacancy_row[5],
                'work_format': vacancy_row[6],
                'region': vacancy_row[7],
                'published_at': vacancy_row[8]
            }

            message = format_vacancy_message(vacancy)

            for user_id in users:
                try:
                    if send_telegram_message(user_id, message):
                        mark_as_sent(vacancy['id'], user_id)
                    time.sleep(0.5)  # Задержка между сообщениями
                except Exception as e:
                    logging.error(f"Ошибка обработки пользователя {user_id}: {e}")

    except Exception as e:
        logging.error(f"Критическая ошибка в check_and_notify: {e}")


def main():
    """Точка входа в скрипт"""
    logging.info("Запуск нотификатора вакансий...")
    init_databases()

    while True:
        check_and_notify()
        time.sleep(300)  # Проверка каждые 5 минут


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Нотификатор остановлен")
    except Exception as e:
        logging.critical(f"Фатальная ошибка: {e}")