# lead_parser Google MVP v1.2

В этой версии источник поиска переведен с ручного парсинга DuckDuckGo HTML
на библиотеку ddgs.

## Зачем
DuckDuckGo HTML/lite может отдавать пустую или нестабильную выдачу.
Библиотека ddgs уже умеет работать с разными backend-ами.

## Запуск
```bash
python -m venv venv
# Windows PowerShell
.\venv\Scripts\activate
py -m pip install -r requirements.txt
copy .env.example .env
# заполни BOT_TOKEN в .env
py -m bot.bot
```
