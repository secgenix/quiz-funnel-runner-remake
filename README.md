# Quiz Funnel Runner - Rebuild Edition

Полностью обновленная и переработанная версия проекта для автоматического прохождения quiz-воронок до экрана оплаты (paywall) с сохранением артефактов и журналов.
Этот вариант проекта переделан через `codex-cli`.

## Что это

`Quiz Funnel Runner` запускает мобильный браузер (эмуляция iPhone 13), проходит шаги воронки, выбирает ответы, заполняет формы и останавливается при достижении paywall/checkout.

Проект ориентирован на быстрый массовый прогон URL-воронок и последующий анализ по скриншотам и логам.

## Ключевые возможности

- Полуавтономное прохождение воронок: выбор ответов, обработка форм, переходы по шагам.
- Классификация экранов: `question`, `info`, `input`, `email`, `paywall`, `other`, `checkout`.
- Обход cookie/pop-up оверлеев и типовых блокирующих элементов.
- По умолчанию запуск без визуальных окон (headless).
- Визуальный режим только через `--debug`.
- Последовательный и параллельный прогон списка URL.
- Сохранение скриншотов по шагам и агрегированная структура результатов.
- Расширенное логирование действий и ошибок (русские сообщения).
- **Telegram-бот** для удаленного управления и получения результатов.

## Технологии

- Python 3.9+
- Playwright (Chromium)
- aiogram 3.x (Telegram-бот)
- SQLite (хранение состояния задач)

## Установка

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

## Быстрый старт

### Запуск раннера (CLI)

1. Отредактируйте `config.json`.
2. Запустите:

```bash
python main.py
```

Параллельный запуск:

```bash
python main.py --parallel
```

Запуск с пользовательским конфигом:

```bash
python main.py --config config.json
```

Режим отладки (визуальное отображение окон браузера):

```bash
python main.py --debug
```

### Запуск Telegram-бота

1. Создайте бота через [@BotFather](https://t.me/BotFather) и получите токен.
2. Узнайте свой Telegram User ID через [@userinfobot](https://t.me/userinfobot).
3. Скопируйте `.env.example` в `.env`:

```bash
cp .env.example .env
```

4. Заполните `.env`:

```ini
TELEGRAM_BOT_TOKEN=1234567890:AABBccDDeeFFggHHiiJJkkLLmmNNooP
TELEGRAM_ADMIN_ID=123456789
```

5. Запустите бота:

```bash
python bot.py
```

**Примечание:** Токен и admin_id из `.env` имеют приоритет над значениями в `config.json`.

## Конфигурация (`config.json`)

### Для раннера

- `funnels`: массив URL для прогона.
- `max_funnels`: ограничение количества URL из списка.
- `max_steps`: верхний лимит шагов на воронку.
- `slow_mo_ms`: пауза между действиями Playwright.
- `fill_values`: значения для заполнения форм.

Пример:

```json
{
  "funnels": [
    "https://coursiv.io/dynamic?prc_id=1069",
    "https://coursiv.io/dynamic",
    "https://dance-bit.com/welcomeBellyRef"
  ],
  "max_funnels": 3,
  "max_steps": 80,
  "slow_mo_ms": 50,
  "fill_values": {
    "name": "John",
    "email": "testuser{ts}@gmail.com",
    "age": "30",
    "height": "170",
    "weight": "70",
    "goal_weight": "60",
    "default_number": "25",
    "date_of_birth": "01/01/1990"
  }
}
```

### Для Telegram-бота

**Важно:** Токен и admin_id рекомендуется хранить в `.env` файле, а не в `config.json`.

```json
{
  "bot": {
    "use_only_admin": true
  },
  "runner": {
    "max_steps": 80,
    "max_funnels": 5,
    "slow_mo_ms": 50,
    "headless": true,
    "fill_values": { ... }
  },
  "google_drive": {
    "enabled": false,
    "credentials_file": "credentials.json",
    "token_file": "token.json",
    "folder_id": "",
    "root_folder_name": "Quiz Funnel Runner Results"
  },
  "captcha": {
    "enabled": false,
    "service": "2captcha",
    "api_key": ""
  }
}
```

#### Переменные окружения (.env)

| Переменная | Описание |
|------------|----------|
| `TELEGRAM_BOT_TOKEN` | Токен бота от @BotFather |
| `TELEGRAM_ADMIN_ID` | Ваш Telegram User ID |
| `GOOGLE_DRIVE_ENABLED` | Включение Google Drive интеграции через `.env` |
| `GOOGLE_DRIVE_CREDENTIALS_FILE` | Путь к OAuth client JSON или service account JSON |
| `GOOGLE_DRIVE_TOKEN_FILE` | Файл для сохранения OAuth токена |
| `GOOGLE_DRIVE_FOLDER_ID` | ID готовой корневой папки в Google Drive |
| `GOOGLE_DRIVE_ROOT_FOLDER_NAME` | Имя корневой папки, которую нужно создать/переиспользовать, если `folder_id` пуст |

#### Параметры бота (config.json)

| Параметр | Описание |
|----------|----------|
| `use_only_admin` | Если `true`, только администраторы могут использовать бота |

## Telegram-бот

### Команды

| Команда | Описание |
|---------|----------|
| `/start` | Запуск бота, приветственное сообщение |
| `/status` | Статус последней задачи |
| `/history` | История последних 10 задач |
| `/cancel` | Отмена текущей задачи |
| `/drive` | Ссылка на Google Drive с результатами последней задачи |
| `/help` | Справка по командам |

### Использование

1. **Отправьте URL воронки** - бот создаст задачу и начнет обработку
2. **Отправьте несколько URL** (каждый с новой строки) - бот создаст несколько задач
3. **Отправьте ссылку на Google Sheets/Docs** - бот прочитает все URL из документа
4. **Получайте уведомления** о прогрессе и результатах

Бот автоматически:
- Создает задачу в очереди
- Отправляет уведомление о начале обработки
- Сообщает о прогрессе (каждые 5 шагов)
- Отправляет итоговый отчет со скриншотом и логом
- Загружает результаты в Google Drive (если включено)

### Ограничения

- Максимум **3 одновременные задачи** (настраивается в `bot.py`)
- Максимум **5 задач в очереди** на пользователя

## Google Drive интеграция

Интеграция теперь рассчитана на реальное использование:
- повторно использует клиент Google Drive API между загрузками;
- автоматически создаёт или переиспользует нужные папки;
- не перезаливает файл, если в целевой папке уже есть файл с тем же именем и размером;
- использует retry/backoff для временных ошибок `403`, `429` и `5xx`;
- пишет диагностические логи по авторизации, квотам и сетевым ошибкам;
- поддерживает OAuth desktop client JSON и service account JSON.

### Быстрая настройка

1. Следуйте инструкции в [`GOOGLE_DRIVE_SETUP.md`](GOOGLE_DRIVE_SETUP.md)
2. Скопируйте `credentials.json` в директорию проекта
3. Заполните переменные Google Drive в `.env`:

```ini
GOOGLE_DRIVE_ENABLED=true
GOOGLE_DRIVE_CREDENTIALS_FILE=client_secret.json
GOOGLE_DRIVE_TOKEN_FILE=token.json
GOOGLE_DRIVE_FOLDER_ID=1ABC123xyz...
GOOGLE_DRIVE_ROOT_FOLDER_NAME=Quiz Funnel Runner Results
```

4. Включите интеграцию в `config.json` или через `.env`:

```json
{
  "google_drive": {
    "enabled": true,
    "credentials_file": "client_secret.json",
    "token_file": "token.json",
    "folder_id": "",
    "root_folder_name": "Quiz Funnel Runner Results"
  }
}
```

Рекомендуемая схема:
- если у вас уже есть папка в Drive, задайте `GOOGLE_DRIVE_FOLDER_ID`;
- если папки ещё нет, оставьте `GOOGLE_DRIVE_FOLDER_ID` пустым и задайте `GOOGLE_DRIVE_ROOT_FOLDER_NAME` — папка будет создана автоматически и затем переиспользоваться.

### Структура файлов в Google Drive

```
Google Drive/
└── <folder_id>/
    ├── <slug>/                    # Папка воронки
    │   ├── 01_question.png
    │   ├── 02_info.png
    │   ├── ...
    │   ├── log.txt
    │   └── manifest.json
    └── _classified/
        ├── question/
        ├── info/
        ├── input/
        ├── email/
        ├── paywall/
        ├── other/
        └── checkout/
```

### Команда /drive

Используйте `/drive` для получения ссылки на Google Drive с результатами последней завершенной задачи.

## Результаты прогона

После запуска создается директория `results/`:

- `results/<slug>/`:
  - пошаговые скриншоты (`01_question.png`, `02_info.png`, ...)
  - `log.txt` по конкретной воронке
  - `manifest.json` с метаданными
- `results/_classified/<type>/`:
  - агрегированные скриншоты по классам экранов
- `results/summary.json`:
  - итоговая сводка по всем URL
- `tasks.db`:
  - база данных задач (для бота)

## Структура проекта

```
quiz-funnel-runner-remake/
├── main.py           # Основной раннер воронок
├── bot.py            # Telegram-бот на aiogram
├── config.py         # Модуль конфигурации
├── models.py         # Модели данных и TaskManager
├── config.json       # Конфиг запуска
├── requirements.txt  # Зависимости Python
├── plan.md           # План доработки
├── README.md         # Этот файл
└── results/          # Выходные данные прогона
```

## Примечания

- Рекомендуется запускать в чистом окружении и периодически очищать `results/`.
- Для нестабильных воронок полезно снижать `slow_mo_ms`/повышать `max_steps` точечно под конкретный URL.
- Бот использует SQLite для хранения состояния задач (`tasks.db`).
- При перезапуске бота незавершенные задачи сохраняются в базе.
