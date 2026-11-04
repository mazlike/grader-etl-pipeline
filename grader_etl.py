"""
Итоговый проект: API грейдера -> обработка -> PostgreSQL, с логированием.
Доп. задания: агрегаты за день -> Google Sheets, уведомления на почту.

Все настройки берутся из переменных окружения или файла .env (см. .env.example).
Запуск:
    python grader_etl.py                                   # вчерашние сутки (UTC)
    python grader_etl.py "2026-05-30 00:00:00" "2026-05-31 00:00:00"
"""
import ast
import logging
import mimetypes
import os
import smtplib
import ssl
import sys
import time
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage

import psycopg2
import requests
from psycopg2.extras import execute_values

try:
    from dotenv import load_dotenv
    load_dotenv(
        dotenv_path=os.path.join(os.path.dirname(__file__), ".env"),
        override=True,
    )
except ImportError:  # python-dotenv не обязателен, если переменные заданы в системе
    pass

# ---------- Настройки ----------
API_URL = "https://api.itresume.ru/statistics/"
CLIENT = os.getenv("API_CLIENT", "Skillfactory")
CLIENT_KEY = os.getenv("API_CLIENT_KEY", "M2MGWS")

DB_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "dbname": os.getenv("PG_DB", "grader"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", ""),
}

# Google Sheets (звёздочка №1)
GOOGLE_CREDS_FILE = os.getenv("GOOGLE_CREDS_FILE", "google_credentials.json")
# Поддерживаем оба имени: SPREADSHEET_ID используется в .env из задания,
# GSHEET_ID оставлен для обратной совместимости.
GSHEET_ID = os.getenv("GSHEET_ID") or os.getenv("SPREADSHEET_ID", "")
GSHEET_WORKSHEET = os.getenv("GSHEET_WORKSHEET", "daily_stats")

# Почта (звёздочки №2)
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.mail.ru")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
EMAIL_TO = os.getenv("EMAIL_TO", "")

LOG_DIR = "logs"
LOG_KEEP_DAYS = 3
REQUEST_TIMEOUT = 120
DT_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS grader_attempts (
    id                      SERIAL PRIMARY KEY,
    user_id                 TEXT NOT NULL,
    oauth_consumer_key      TEXT,
    lis_result_sourcedid    TEXT,
    lis_outcome_service_url TEXT,
    is_correct              BOOLEAN,
    attempt_type            TEXT NOT NULL,
    created_at              TIMESTAMP NOT NULL,
    UNIQUE (user_id, created_at, attempt_type)
);
"""

INSERT_SQL = """
INSERT INTO grader_attempts
    (user_id, oauth_consumer_key, lis_result_sourcedid,
     lis_outcome_service_url, is_correct, attempt_type, created_at)
VALUES %s
ON CONFLICT (user_id, created_at, attempt_type) DO NOTHING
"""

STATS_SQL = """
SELECT
    count(*),
    count(*) FILTER (WHERE attempt_type = 'run'),
    count(*) FILTER (WHERE attempt_type = 'submit'),
    count(*) FILTER (WHERE attempt_type = 'submit' AND is_correct),
    count(DISTINCT user_id)
FROM grader_attempts
WHERE created_at >= %s AND created_at < %s
"""

SHEET_HEADER = ["date", "total_attempts", "runs", "submits",
                "successful_submits", "success_rate_%", "unique_users",
                "skipped_invalid"]


# ---------- Логирование ----------
def setup_logging():
    """Лог пишется в logs/YYYY-MM-DD.log; логи старше 3 дней удаляются."""
    os.makedirs(LOG_DIR, exist_ok=True)
    today = datetime.now().date()

    for name in os.listdir(LOG_DIR):
        if not name.endswith(".log"):
            continue
        try:
            log_date = datetime.strptime(name[:-4], "%Y-%m-%d").date()
        except ValueError:
            continue
        if (today - log_date).days >= LOG_KEEP_DAYS:
            os.remove(os.path.join(LOG_DIR, name))

    log_path = os.path.join(LOG_DIR, f"{today}.log")
    logging.basicConfig(
        format="%(asctime)s | %(name)s | %(levelname)s: %(message)s",
        level=logging.INFO,
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return log_path


# ---------- Получение данных ----------
def fetch_data(start: str, end: str):
    params = {"client": CLIENT, "client_key": CLIENT_KEY, "start": start, "end": end}
    logging.info("Скачивание данных началось: %s - %s", start, end)
    t0 = time.time()
    try:
        response = requests.get(API_URL, params=params, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as e:
        logging.error("Ошибка соединения с API: %s", e)
        raise

    if response.status_code != 200:
        logging.error("Ошибка доступа к API, status_code=%s, ответ: %s",
                      response.status_code, response.text[:300])
        response.raise_for_status()

    data = response.json()
    logging.info("Скачивание завершено: получено %d записей за %.2f сек",
                 len(data), time.time() - t0)
    return data


# ---------- Обработка и валидация ----------
def parse_dt(value):
    for fmt in (DT_FORMAT, "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(str(value), fmt)
        except ValueError:
            continue
    raise ValueError(f"некорректная дата: {value!r}")


def parse_record(rec: dict):
    """Возвращает кортеж для вставки в БД. При проблемах кидает ValueError."""
    user_id = rec.get("lti_user_id")
    if not isinstance(user_id, str) or not user_id:
        raise ValueError(f"некорректный lti_user_id: {user_id!r}")

    attempt_type = rec.get("attempt_type")
    if attempt_type not in ("run", "submit"):
        raise ValueError(f"некорректный attempt_type: {attempt_type!r}")

    is_correct = rec.get("is_correct")
    if is_correct is not None:
        if isinstance(is_correct, bool):
            pass
        elif is_correct in (0, 1):
            is_correct = bool(is_correct)
        else:
            raise ValueError(f"некорректный is_correct: {is_correct!r}")

    created_at = parse_dt(rec.get("created_at"))

    raw_params = rec.get("passback_params")
    if isinstance(raw_params, dict):
        params = raw_params
    else:
        try:
            params = ast.literal_eval(raw_params)
        except (ValueError, SyntaxError):
            raise ValueError("не удалось разобрать passback_params")
    if not isinstance(params, dict):
        raise ValueError("passback_params не является словарём")

    oauth_key = params.get("oauth_consumer_key")
    sourcedid = params.get("lis_result_sourcedid")
    outcome_url = params.get("lis_outcome_service_url")
    for field, value in (("oauth_consumer_key", oauth_key),
                         ("lis_result_sourcedid", sourcedid),
                         ("lis_outcome_service_url", outcome_url)):
        if value is not None and not isinstance(value, str):
            raise ValueError(f"поле {field} должно быть строкой, получено {value!r}")

    # лишние поля в passback_params игнорируем, отсутствующие -> None
    return (user_id, oauth_key or None, sourcedid or None, outcome_url or None,
            is_correct, attempt_type, created_at)


def process_data(raw_data):
    logging.info("Обработка данных началась")
    rows, skipped = [], 0
    for i, rec in enumerate(raw_data):
        try:
            rows.append(parse_record(rec))
        except (ValueError, TypeError, AttributeError) as e:
            skipped += 1
            logging.warning("Запись #%d пропущена: %s", i, e)
    logging.info("Обработка завершена: валидных %d, пропущено %d", len(rows), skipped)
    return rows, skipped


# ---------- База данных ----------
def load_to_db(rows):
    if not rows:
        logging.info("Нет данных для загрузки в БД")
        return
    logging.info("Заполнение базы данных началось")
    t0 = time.time()
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn, conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            execute_values(cur, INSERT_SQL, rows, page_size=1000)
        logging.info("Заполнение базы завершено: %d записей за %.2f сек",
                     len(rows), time.time() - t0)
    except psycopg2.Error as e:
        logging.error("Ошибка работы с PostgreSQL: %s", e)
        raise
    finally:
        if conn is not None:
            conn.close()


def get_period_stats(start: str, end: str, skipped: int):
    """Агрегаты за период считаются по данным в БД (с учётом дедупликации)."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            cur.execute(STATS_SQL, (start, end))
            total, runs, submits, ok_submits, users = cur.fetchone()
    finally:
        conn.close()

    stats = {
        "date": parse_dt(start).strftime("%Y-%m-%d"),
        "total_attempts": total,
        "runs": runs,
        "submits": submits,
        "successful_submits": ok_submits,
        "success_rate": round(ok_submits / submits * 100, 2) if submits else 0.0,
        "unique_users": users,
        "skipped_invalid": skipped,
    }
    logging.info("Агрегаты за %s: %s", stats["date"], stats)
    return stats


# ---------- Звёздочка №1: Google Sheets ----------
def upload_to_google_sheets(stats: dict):
    if not GSHEET_ID:
        logging.warning("GSHEET_ID не задан, загрузка в Google Sheets пропущена")
        return
    import gspread  # импорт здесь, чтобы основной скрипт работал и без gspread

    logging.info("Загрузка агрегатов в Google Sheets началась")
    credentials_path = GOOGLE_CREDS_FILE
    if not os.path.isabs(credentials_path):
        credentials_path = os.path.join(os.path.dirname(__file__), credentials_path)
    if not os.path.isfile(credentials_path):
        raise FileNotFoundError(
            "Не найден JSON-ключ сервисного аккаунта Google: "
            f"{credentials_path}. API-ключа недостаточно для записи в Sheets."
        )

    gc = gspread.service_account(filename=credentials_path)
    sheet = gc.open_by_key(GSHEET_ID)
    try:
        ws = sheet.worksheet(GSHEET_WORKSHEET)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=GSHEET_WORKSHEET, rows=1000, cols=len(SHEET_HEADER))

    values = ws.get_all_values()
    if not values:
        ws.append_row(SHEET_HEADER)
    elif values[0] != SHEET_HEADER:
        # Восстанавливаем заголовок, если лист был создан или заполнен вручную.
        ws.insert_row(SHEET_HEADER, index=1)
    values = ws.get_all_values()

    # при повторном запуске за ту же дату строка заменяется, а не дублируется
    for idx, row in enumerate(values, start=1):
        if row and row[0] == stats["date"]:
            ws.delete_rows(idx)
            break

    ws.append_row([stats["date"], stats["total_attempts"], stats["runs"],
                   stats["submits"], stats["successful_submits"],
                   stats["success_rate"], stats["unique_users"],
                   stats["skipped_invalid"]])
    logging.info("Загрузка в Google Sheets завершена")


# ---------- Звёздочки №2: почта ----------
def send_email(subject: str, body: str, attachment_path: str = None):
    if not (SMTP_USER and SMTP_PASSWORD and EMAIL_TO):
        logging.warning("Почтовые настройки не заданы, письмо не отправлено")
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = EMAIL_TO
    msg.set_content(body)

    if attachment_path and os.path.exists(attachment_path):
        ctype, _ = mimetypes.guess_type(attachment_path)
        maintype, subtype = (ctype or "text/plain").split("/", 1)
        with open(attachment_path, "rb") as f:
            msg.add_attachment(f.read(), maintype=maintype, subtype=subtype,
                               filename=os.path.basename(attachment_path))

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context) as server:
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg=msg)
    logging.info("Письмо отправлено на %s", EMAIL_TO)


def safe_send_email(subject, body, attachment_path=None):
    """Ошибка отправки письма не должна ронять основной скрипт."""
    try:
        send_email(subject, body, attachment_path)
    except Exception:
        logging.exception("Не удалось отправить письмо")


# ---------- Точка входа ----------
def main():
    log_path = setup_logging()
    script_start = time.time()
    logging.info("Скрипт запущен")

    if len(sys.argv) == 3:
        start, end = sys.argv[1], sys.argv[2]
    else:
        today = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        start = (today - timedelta(days=1)).strftime(DT_FORMAT)
        end = today.strftime(DT_FORMAT)

    try:
        raw = fetch_data(start, end)
        rows, skipped = process_data(raw)
        load_to_db(rows)
        stats = get_period_stats(start, end, skipped)
    except Exception as e:
        logging.exception("Скрипт завершился с ошибкой")
        safe_send_email(
            "[grader ETL] ОШИБКА",
            f"Скрипт завершился с ошибкой за период {start} - {end}:\n{e!r}\n\n"
            f"Подробности во вложенном логе.",
            log_path,
        )
        sys.exit(1)

    sheets_status = "ок"
    try:
        upload_to_google_sheets(stats)
    except Exception:
        sheets_status = "ошибка (см. лог)"
        logging.exception("Не удалось загрузить данные в Google Sheets")

    elapsed = time.time() - script_start
    logging.info("Скрипт завершён успешно за %.2f сек", elapsed)

    body = (
        f"Загрузка за {stats['date']} выполнена успешно за {elapsed:.1f} сек.\n\n"
        f"Всего попыток: {stats['total_attempts']}\n"
        f"  run: {stats['runs']}\n"
        f"  submit: {stats['submits']}\n"
        f"Успешных submit: {stats['successful_submits']} ({stats['success_rate']}%)\n"
        f"Уникальных пользователей: {stats['unique_users']}\n"
        f"Пропущено невалидных записей: {stats['skipped_invalid']}\n"
        f"Google Sheets: {sheets_status}\n"
    )
    safe_send_email(f"[grader ETL] Готово: {stats['date']}", body, log_path)


if __name__ == "__main__":
    main()