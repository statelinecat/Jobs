import sqlite3
import pandas as pd
import asyncio
from telegram import Bot
from telegram.error import TelegramError
from datetime import datetime
from config import TOKEN, CHAT_ID
import os


def generate_excel_report():
    """Генерация Excel-отчета с данными о вакансиях"""
    try:
        if not os.path.exists("vacancies.db"):
            print("❌ Файл vacancies.db не найден")
            return None

        conn = sqlite3.connect("vacancies.db")

        # Получаем данные с опытом работы и форматом работы
        df = pd.read_sql_query("""
            SELECT 
                title AS 'Должность',
                company AS 'Компания',
                region AS 'Регион',
                salary AS 'Зарплата',
                experience AS 'Опыт работы',
                work_format AS 'Формат работы',
                published_at AS 'Дата публикации',
                link AS 'Ссылка'
            FROM vacancies
            ORDER BY published_at DESC
        """, conn)

        conn.close()

        if df.empty:
            print("⚠ В базе нет данных о вакансиях")
            return None

        # Создаем отчет с текущей датой в названии
        report_date = datetime.now().strftime("%Y-%m-%d_%H-%M")
        excel_filename = f"vacancies_report_{report_date}.xlsx"

        # Сохраняем в Excel с форматированием
        with pd.ExcelWriter(excel_filename, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Вакансии')

            workbook = writer.book
            worksheet = writer.sheets['Вакансии']

            # Настраиваем стили
            header_format = workbook.add_format({
                'bold': True,
                'align': 'center',
                'valign': 'vcenter',
                'border': 1
            })

            # Авто-ширина колонок
            for col_num, column_title in enumerate(df.columns):
                max_len = max(
                    df[column_title].astype(str).map(len).max(),
                    len(column_title)
                ) + 2
                worksheet.set_column(col_num, col_num, max_len)

            # Форматируем заголовки
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)

        return excel_filename

    except Exception as e:
        print(f"❌ Ошибка при генерации отчета: {e}")
        return None


async def send_report_to_telegram(filename):
    """Отправка отчета в Telegram"""
    try:
        bot = Bot(token=TOKEN)

        # Получаем статистику для сообщения
        conn = sqlite3.connect("vacancies.db")
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM vacancies")
        total_vacancies = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT company) FROM vacancies")
        unique_companies = cursor.fetchone()[0]

        cursor.execute("""
            SELECT experience, COUNT(*) 
            FROM vacancies 
            GROUP BY experience
        """)
        experience_stats = cursor.fetchall()

        cursor.execute("""
            SELECT work_format, COUNT(*) 
            FROM vacancies 
            GROUP BY work_format
        """)
        work_format_stats = cursor.fetchall()

        conn.close()

        # Формируем текст сообщения
        experience_text = "\n".join([f"• {exp}: {count}" for exp, count in experience_stats])
        work_format_text = "\n".join([f"• {fmt}: {count}" for fmt, count in work_format_stats])

        caption = (
            f"📊 *Отчет по вакансиям*\n"
            f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
            f"📌 Всего вакансий: {total_vacancies}\n"
            f"🏢 Уникальных компаний: {unique_companies}\n"
            f"\n🧑‍💻 *Распределение по опыту:*\n{experience_text}\n"
            f"\n🏠 *Формат работы:*\n{work_format_text}"
        )

        # Отправляем файл
        with open(filename, 'rb') as file:
            await bot.send_document(
                chat_id=CHAT_ID,
                document=file,
                caption=caption,
                filename=filename,
                parse_mode="Markdown"
            )

        print(f"✅ Отчет {filename} отправлен")
        return True

    except Exception as e:
        print(f"❌ Ошибка отправки: {e}")
        return False


async def main():
    print("\n" + "=" * 50)
    print("🔄 Начало генерации отчета...")
    print("=" * 50)

    excel_file = generate_excel_report()

    if excel_file:
        if await send_report_to_telegram(excel_file):
            try:
                os.remove(excel_file)
                print(f"🗑 Временный файл {excel_file} удален")
            except Exception as e:
                print(f"⚠ Не удалось удалить файл: {e}")
    else:
        print("❌ Отчет не сгенерирован")


if __name__ == "__main__":
    asyncio.run(main())