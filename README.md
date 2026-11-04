## Запуск ETL

Установить зависимости и запустить обработку вчерашних суток:

```powershell
uv sync
uv run python grader_etl.py
```

Для произвольного периода:

```powershell
uv run python grader_etl.py "2026-05-30 00:00:00" "2026-05-31 00:00:00"
```

### Google Sheets

Для записи агрегатов нужен JSON-ключ сервисного аккаунта Google Cloud. Разместите
его по пути из `GOOGLE_CREDS_FILE` (по умолчанию
`google_credentials.json`) и предоставьте сервисному аккаунту доступ редактора
к таблице. В `.env` укажите `SPREADSHEET_ID` и `GSHEET_WORKSHEET`.

API-ключ Google и адрес сервисного аккаунта сами по себе не дают права на
изменение таблицы: для записи используется JSON-ключ сервисного аккаунта.

После загрузки в таблице создаётся или обновляется строка за дату с колонками:
`date`, `total_attempts`, `runs`, `submits`, `successful_submits`,
`success_rate_%`, `unique_users`, `skipped_invalid`.