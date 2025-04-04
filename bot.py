import sqlite3
import pandas as pd
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from config import TOKEN
import logging
import asyncio
from datetime import datetime
import os
import time

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


def get_db_connection(db_name="vacancies.db"):
    """Безопасное подключение к SQLite с таймаутом"""
    conn = None
    attempts = 0
    while attempts < 3:
        try:
            conn = sqlite3.connect(db_name, timeout=10)
            conn.execute("PRAGMA busy_timeout = 10000")
            return conn
        except sqlite3.OperationalError as e:
            logger.warning(f"Ошибка подключения к {db_name} (попытка {attempts + 1}): {e}")
            time.sleep(1)
            attempts += 1
    raise sqlite3.OperationalError(f"Не удалось подключиться к {db_name} после 3 попыток")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /start - регистрация пользователя"""
    user = update.effective_user
    try:
        conn = get_db_connection("users.db")
        cursor = conn.cursor()

        cursor.execute("SELECT 1 FROM users WHERE user_id = ?", (user.id,))
        if not cursor.fetchone():
            cursor.execute(
                "INSERT INTO users (user_id, username, first_name, last_name) VALUES (?, ?, ?, ?)",
                (user.id, user.username, user.first_name, user.last_name)
            )
            conn.commit()
            await update.message.reply_text(
                "✅ Вы успешно зарегистрированы!\n"
                "Теперь вы будете получать уведомления о новых вакансиях.\n"
                "Используйте /report для получения отчета по вакансиям."
            )
            logger.info(f"Новый пользователь: {user.id} {user.username}")
        else:
            await update.message.reply_text("Вы уже зарегистрированы!")

    except Exception as e:
        logger.error(f"Ошибка регистрации пользователя: {e}")
        await update.message.reply_text("⚠ Произошла ошибка при регистрации")
    finally:
        if conn:
            conn.close()


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /stop - отписка от уведомлений"""
    user = update.effective_user
    try:
        conn = get_db_connection("users.db")
        cursor = conn.cursor()
        cursor.execute("DELETE FROM users WHERE user_id = ?", (user.id,))
        conn.commit()

        if cursor.rowcount > 0:
            await update.message.reply_text(
                "Вы больше не будете получать уведомления.\n"
                "Чтобы снова подписаться, отправьте /start"
            )
            logger.info(f"Пользователь отписался: {user.id}")
        else:
            await update.message.reply_text("Вы не были подписаны на уведомления.")

    except Exception as e:
        logger.error(f"Ошибка отписки пользователя: {e}")
        await update.message.reply_text("⚠ Произошла ошибка при отписке")
    finally:
        if conn:
            conn.close()


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /help"""
    await update.message.reply_text(
        "🤖 Бот вакансий HH.ru\n\n"
        "Доступные команды:\n"
        "/start - подписаться на уведомления\n"
        "/stop - отписаться от уведомлений\n"
        "/report - получить отчет по вакансиям\n"
        "/help - показать это сообщение"
    )


async def generate_excel_report():
    """Генерация Excel-отчета с данными о вакансиях"""
    try:
        conn = get_db_connection()

        # Получаем данные из базы (без комментариев в SQL)
        query = """
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
            LIMIT 1000
        """

        df = pd.read_sql_query(query, conn)

        if df.empty:
            return False, "В базе нет данных о вакансиях"

        # Создаем отчет с текущей датой в названии
        report_date = datetime.now().strftime("%Y-%m-%d_%H-%M")
        excel_filename = f"vacancies_report_{report_date}.xlsx"

        # Сохраняем в Excel с форматированием
        writer = pd.ExcelWriter(excel_filename, engine='xlsxwriter')
        df.to_excel(writer, index=False, sheet_name='Вакансии')

        # Форматирование
        workbook = writer.book
        worksheet = writer.sheets['Вакансии']

        # Настраиваем стили
        header_format = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'border': 1,
            'fg_color': '#D7E4BC'
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

        writer.close()

        return True, excel_filename

    except sqlite3.Error as e:
        logger.error(f"Ошибка SQL при генерации отчета: {e}")
        return False, f"Ошибка базы данных: {e}"
    except Exception as e:
        logger.error(f"Ошибка при генерации отчета: {e}")
        return False, f"Ошибка при генерации отчета: {e}"
    finally:
        if conn:
            conn.close()


async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /report"""
    user = update.effective_user
    await update.message.reply_text("🔄 Формирую отчет, пожалуйста подождите...")

    success, result = await generate_excel_report()

    if success:
        try:
            with open(result, 'rb') as file:
                await context.bot.send_document(
                    chat_id=user.id,
                    document=file,
                    caption="📊 Отчет по вакансиям",
                    filename=os.path.basename(result)
                )
            await update.message.reply_text("✅ Отчет успешно сформирован!")
        except Exception as e:
            await update.message.reply_text(f"⚠ Ошибка при отправке отчета: {e}")
        finally:
            try:
                os.remove(result)
            except Exception as e:
                logger.error(f"Ошибка удаления временного файла: {e}")
    else:
        await update.message.reply_text(result)


def main():
    """Запуск бота"""
    application = Application.builder().token(TOKEN).build()

    # Регистрируем обработчики команд
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stop", stop))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("report", report_command))

    application.run_polling()
    logger.info("Бот запущен и готов к работе")


if __name__ == '__main__':
    main()