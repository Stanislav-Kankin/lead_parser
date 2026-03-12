# lead_parser Google MVP v1.1

Улучшенная MVP-версия для поиска потенциальных компаний по текстовому запросу.

## Что изменилось
- добавлен query builder
- поиск идёт по нескольким поисковым маскам
- улучшена фильтрация мусорных доменов
- улучшен ICP classifier
- бот показывает больше найденных доменов

## Пример запроса
- ноутбуки
- косметика
- мебель
- crm
- логистика

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
