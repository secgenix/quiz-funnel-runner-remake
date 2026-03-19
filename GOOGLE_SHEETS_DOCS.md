# Инструкция по чтению URL из Google Sheets/Docs

## Возможности

Бот умеет автоматически читать URL из Google документов через **Service Account**:

- **Google Sheets** — читает URL из первой колонки
- **Google Docs** — извлекает URL из текста документа

## Настройка

### Шаг 1: Получите email Service Account

1. Откройте `credentials.json` (файл Service Account)
2. Найдите поле `client_email`
3. Скопируйте email (вида `quiz-funnel-runner@project-id.iam.gserviceaccount.com`)

### Шаг 2: Предоставьте доступ к документам

#### Для Google Sheets:

1. Откройте Google Sheet
2. Нажмите **"Share"** (Поделиться)
3. Вставьте email Service Account
4. Предоставьте права **"Viewer"** (Просмотр)
5. Нажмите **"Share"**

#### Для Google Docs:

1. Откройте Google Doc
2. Нажмите **"Share"** (Поделиться)
3. Вставьте email Service Account
4. Предоставьте права **"Viewer"** (Просмотр)
5. Нажмите **"Share"**

### Шаг 3: Проверка конфигурации

Убедитесь, что в `config.json` указан правильный файл credentials:

```json
{
  "google_drive": {
    "enabled": true,
    "credentials_file": "credentials.json"
  }
}
```

## Как использовать

### Google Sheets

1. Создайте Google Sheet со списком URL
2. Разместите URL в **первой колонке** (колонка A)
3. Отправьте ссылку на документ боту
4. Бот прочитает все URL и создаст задачи

**Пример структуры:**

```
| A (колонка 1)                              |
|--------------------------------------------|
| https://example.com/quiz1                  |
| https://example.com/quiz2                  |
| https://example.com/quiz3?utm_source=fb    |
| ...                                        |
```

### Google Docs

1. Создайте Google Doc
2. Вставьте URL в текст документа (каждый с новой строки)
3. Отправьте ссылку на документ боту
4. Бот извлечет все URL из текста

**Пример содержимого:**

```
Список воронок для обработки:

https://example.com/quiz1
https://example.com/quiz2
https://example.com/quiz3

Дополнительные ссылки:
- https://example.com/bonus1
- https://example.com/bonus2
```

## Формат URL

Бот распознает URL в следующих форматах:
- `https://domain.com/path`
- `http://domain.com/path`
- URL с параметрами: `https://domain.com/path?param=value`

## Ограничения

- Максимум **5 задач в очереди** на пользователя
- Из Google Sheets читается **первая колонка** (A)
- URL должны начинаться с `http://` или `https://`

## Примеры

### ✅ Правильно

**Google Sheets:**
```
https://coursiv.io/dynamic
https://madmuscles.com/funnel/default
https://quiz.fitme.expert/intro
```

**Google Docs:**
```
Воронки для теста:
1. https://coursiv.io/dynamic
2. https://madmuscles.com/funnel/default

Дополнительно:
- https://quiz.fitme.expert/intro
```

### ❌ Неправильно

```
coursiv.io/dynamic              # Нет протокола
www.example.com/quiz            # Нет протокола
ftp://example.com/file          # Неподдерживаемый протокол
```

## Команды бота

После отправки ссылки на Google Sheet/Doc:

1. Бот сообщит о начале чтения
2. Покажет количество найденных URL
3. Создаст задачи для каждого URL
4. Отправит подтверждение со списком задач

## Мониторинг прогресса

Используйте команды для отслеживания:

- `/status` — статус последней задачи
- `/history` — история всех задач
- `/drive` — ссылка на результаты в Google Drive

## Troubleshooting

### Ошибка 403: Forbidden

**Проблема:** Бот сообщает об ошибке доступа

**Решение:**
1. Проверьте, что email Service Account добавлен в доступ к документу
2. Email находится в `credentials.json` в поле `client_email`
3. Предоставьте права **"Viewer"** или выше

### Бот не читает URL из документа

**Проблема:** Бот сообщает "Не найдено URL для обработки"

**Решение:**
1. Убедитесь, что URL начинаются с `http://` или `https://`
2. Проверьте доступ Service Account к документу
3. Для Sheets: убедитесь, что URL в первой колонке

### Ошибка инициализации сервиса

**Проблема:** "Файл credentials не найден"

**Решение:**
1. Убедитесь, что `credentials.json` находится в директории проекта
2. Проверьте путь в `config.json`

## Отличие от OAuth 2.0

| Характеристика | Service Account | OAuth 2.0 |
|---------------|-----------------|-----------|
| Авторизация | Автоматическая | Требует браузера |
| Доступ | Только к общим документам | К любым вашим документам |
| Настройка | Проще для автоматизации | Требует токена |
| Квота | Не имеет квоты | Использует вашу квоту |

**Рекомендация:** Используйте Service Account для автоматизации, OAuth 2.0 для личных нужд.

## Дополнительные ресурсы

- [GOOGLE_DRIVE_SETUP.md](GOOGLE_DRIVE_SETUP.md) — настройка Google Drive API
- [README.md](README.md) — общая документация проекта
