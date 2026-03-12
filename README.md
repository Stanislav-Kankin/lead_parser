# lead_parser Google MVP v1

MVP для поиска потенциальных компаний по текстовому запросу.

## Что делает
- принимает поисковый запрос
- пытается найти домены через DuckDuckGo HTML выдачу
- чистит мусорные домены
- сохраняет результаты в SQLite
- показывает найденные компании в Telegram-боте

## Почему DuckDuckGo, а не Google
Для MVP это проще и стабильнее. Логику проекта потом можно расширить или заменить источник.

## Запуск локально
```bash
python -m venv venv
# Windows PowerShell
.\venv\Scripts\activate

py -m pip install -r requirements.txt
copy .env.example .env
# заполни BOT_TOKEN в .env

py -m bot.bot
```
