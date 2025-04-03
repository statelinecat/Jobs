import requests
from bs4 import BeautifulSoup


def parse_hh(query="Python developer", max_pages=3):
    base_url = f"https://hh.ru/search/vacancy?text={query}&page="
    vacancies = []

    for page in range(max_pages):
        url = base_url + str(page)
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")

        for item in soup.find_all("div", class_="vacancy-serp-item"):
            title = item.find("a", class_="bloko-link").text
            link = item.find("a", class_="bloko-link")["href"]
            company = item.find("a", class_="bloko-link_secondary").text
            vacancies.append({"title": title, "link": link, "company": company})

    return vacancies


import sqlite3


def init_db():
    conn = sqlite3.connect("vacancies.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vacancies (
            id INTEGER PRIMARY KEY,
            title TEXT,
            link TEXT UNIQUE,
            company TEXT
        )
    """)
    conn.commit()
    conn.close()


def save_to_db(vacancies):
    conn = sqlite3.connect("vacancies.db")
    cursor = conn.cursor()

    for vacancy in vacancies:
        try:
            cursor.execute(
                "INSERT INTO vacancies (title, link, company) VALUES (?, ?, ?)",
                (vacancy["title"], vacancy["link"], vacancy["company"])
            )
            conn.commit()
        except sqlite3.IntegrityError:  # Если вакансия уже есть в БД
            continue

    conn.close()


from telegram import Bot
from telegram.error import TelegramError


def send_telegram_alert(new_vacancies):
    bot = Bot(token="YOUR_TELEGRAM_BOT_TOKEN")
    chat_id = "YOUR_CHAT_ID"

    for vacancy in new_vacancies:
        message = (
            f"🚀 Новая вакансия!\n"
            f"📌 {vacancy['title']}\n"
            f"🏢 {vacancy['company']}\n"
            f"🔗 {vacancy['link']}"
        )
        try:
            bot.send_message(chat_id=chat_id, text=message)
        except TelegramError as e:
            print(f"Ошибка отправки: {e}")

import schedule
import time

def job():
    vacancies = parse_hh()
    new_vacancies = filter_new_vacancies(vacancies)  # Функция для сравнения с БД
    save_to_db(new_vacancies)
    send_telegram_alert(new_vacancies)

schedule.every().day.at("10:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(60)