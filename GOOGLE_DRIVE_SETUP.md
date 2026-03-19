# Инструкция по настройке Google Drive API (OAuth 2.0)

## Шаг 1: Создание проекта в Google Cloud Console

1. Перейдите в [Google Cloud Console](https://console.cloud.google.com/)
2. Войдите в свой Google аккаунт
3. Нажмите **"Select a project"** → **"NEW PROJECT"**
4. Введите имя проекта (например, `Quiz Funnel Runner`)
5. Нажмите **"CREATE"** и дождитесь создания проекта

## Шаг 2: Включение Google Drive API

1. В левом меню перейдите в **"APIs & Services"** → **"Library"**
2. В поиске введите **"Google Drive API"**
3. Нажмите на **"Google Drive API"** в результатах поиска
4. Нажмите кнопку **"ENABLE"**

## Шаг 3: Настройка OAuth consent screen

1. В левом меню перейдите в **"APIs & Services"** → **"OAuth consent screen"**
2. Выберите **"External"** (для личного использования)
3. Нажмите **"CREATE"**
4. Заполните форму:
   - **App name**: `Quiz Funnel Runner`
   - **User support email**: ваш email
   - **App logo**: (опционально)
   - **App domain**: (оставьте пустым)
   - **Developer contact**: ваш email
5. Нажмите **"SAVE AND CONTINUE"**
6. На странице **"Scopes"** нажмите **"SAVE AND CONTINUE"** (scopes добавятся автоматически)
7. На странице **"Test users"** нажмите **"ADD USERS"** и добавьте ваш Google email
8. Нажмите **"SAVE AND CONTINUE"**
9. Проверьте резюме и нажмите **"BACK TO DASHBOARD"**

## Шаг 4: Создание OAuth 2.0 Client ID

1. В левом меню перейдите в **"APIs & Services"** → **"Credentials"**
2. Нажмите **"+ CREATE CREDENTIALS"** → **"OAuth client ID"**
3. Выберите тип приложения: **"Desktop app"**
4. Введите имя: `Quiz Funnel Runner Desktop`
5. Нажмите **"CREATE"**
6. Скачайте JSON файл с учетными данными
7. Переименуйте файл в `client_secret.json`

## Шаг 5: Копирование файла с учетными данными

1. Скопируйте `client_secret.json` в директорию проекта:
   ```
   C:\Users\eugene\Desktop\quiz-funnel-runner-remake\client_secret.json
   ```

## Шаг 6: Настройка конфигурации

Откройте `config.json` и заполните секцию `google_drive`:

```json
{
  "google_drive": {
    "enabled": true,
    "credentials_file": "client_secret.json",
    "folder_id": ""
  }
}
```

Параметры:
- `enabled`: `true` для включения интеграции
- `credentials_file`: путь к файлу `client_secret.json`
- `folder_id`: ID папки для загрузки (опционально, если пусто - файлы загружаются в корень вашего Drive)

### Как получить folder_id (опционально)

1. Создайте папку в вашем Google Drive
2. Откройте папку в браузере
3. Скопируйте ID из URL:
   - Пример: `https://drive.google.com/drive/folders/1ABC123xyz...` → ID = `1ABC123xyz...`

## Шаг 7: Установка зависимостей

```bash
.venv\Scripts\activate
pip install -r requirements.txt
```

## Шаг 8: Первая авторизация

При первом запуске бота или теста откроется браузер с запросом доступа:

1. Выберите ваш Google аккаунт
2. Нажмите **"Allow"** для предоставления доступа
3. Браузер покажет **"The authentication flow has completed"**
4. Токен сохранится в файле `token.pickle`

**Важно:** Токен обновляется автоматически. При проблемах удалите `token.pickle` для повторной авторизации.

## Шаг 9: Проверка работы

Запустите тест:

```bash
python test_drive.py
```

Тест проверит:
- ✅ Конфигурацию
- ✅ Подключение к Google Drive
- ✅ Создание тестовой папки
- ✅ Загрузку тестового файла
- ✅ Полную загрузку результатов воронки

## Структура загружаемых файлов

После загрузки результаты воронки будут организованы так:

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
        │   ├── <slug>__01_question.png
        │   └── ...
        ├── info/
        ├── input/
        ├── email/
        ├── paywall/
        ├── other/
        └── checkout/
```

## Возможные ошибки и решения

### Ошибка 403: Access forbidden
- Убедитесь, что ваш email добавлен в "Test users" на OAuth consent screen
- Проверьте, что API включен

### Ошибка "client_secret.json not found"
- Скопируйте файл в директорию проекта
- Проверьте путь в `config.json`

### Ошибка "Token expired"
- Удалите файл `token.pickle`
- Запустите тест заново для повторной авторизации

### Ошибка "The OAuth consent screen is not configured"
- Настройте OAuth consent screen (Шаг 3)
- Добавьте ваш email в "Test users"

## Безопасность

⚠️ **Никогда не коммитьте следующие файлы в Git:**
- `client_secret.json`
- `token.pickle`
- `.env`

Файл `.gitignore` уже содержит эти файлы для безопасности.

## Сброс авторизации

Для сброса авторизации и выбора другого аккаунта:

```bash
# Windows
del token.pickle

# Linux/Mac
rm token.pickle
```

Затем запустите бота или тест заново.

## Дополнительные ресурсы

- [Документация Google Drive API](https://developers.google.com/drive/api/v3/about-sdk)
- [OAuth 2.0 для Desktop приложений](https://developers.google.com/identity/protocols/oauth2/native-app)
- [Python Quickstart](https://developers.google.com/drive/api/v3/quickstart/python)
