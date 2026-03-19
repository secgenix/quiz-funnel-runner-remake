# Инструкция по настройке Google Drive API

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

## Шаг 3: Создание Service Account

1. В левом меню перейдите в **"APIs & Services"** → **"Credentials"**
2. Нажмите **"+ CREATE CREDENTIALS"** → **"Service account"**
3. Заполните форму:
   - **Service account name**: `quiz-funnel-runner`
   - **Service account ID**: (заполнится автоматически)
   - **Description**: `Service account для загрузки файлов в Google Drive`
4. Нажмите **"CREATE AND CONTINUE"**
5. На следующем шаге нажмите **"SKIP"** (мы не будем назначать роли)
6. Нажмите **"DONE"**

## Шаг 4: Создание ключа для Service Account

1. На странице **"Credentials"** найдите созданную service account
2. Кликните на email service account (вида `quiz-funnel-runner@...iam.gserviceaccount.com`)
3. Перейдите на вкладку **"KEYS"**
4. Нажмите **"ADD KEY"** → **"Create new key"**
5. Выберите тип ключа **JSON**
6. Нажмите **"CREATE"**
7. Файл с ключом автоматически скачается на ваш компьютер
8. **Важно:** Сохраните этот файл в безопасном месте!

## Шаг 5: Копирование файла с ключом

1. Переименуйте скачанный файл в `credentials.json`
2. Скопируйте файл в директорию проекта:
   ```
   C:\Users\eugene\Desktop\quiz-funnel-runner-remake\credentials.json
   ```

## Шаг 6: Предоставление доступа к Google Drive

### Вариант А: Загрузка в общую папку (рекомендуется)

1. Создайте новую папку в вашем Google Drive
2. Кликните правой кнопкой → **"Share"**
3. Введите email service account (вида `quiz-funnel-runner@project-id.iam.gserviceaccount.com`)
4. Предоставьте доступ **"Editor"**
5. Нажмите **"Share"**
6. Скопируйте ID папки из URL (часть после `/folders/`)
   - Пример: `https://drive.google.com/drive/folders/1ABC123xyz...` → ID = `1ABC123xyz...`

### Вариант Б: Загрузка в корневую папку service account

Service account имеет собственное пространство в Drive. Файлы будут доступны только через API.

## Шаг 7: Настройка конфигурации

Откройте `config.json` и заполните секцию `google_drive`:

```json
{
  "google_drive": {
    "enabled": true,
    "credentials_file": "credentials.json",
    "folder_id": "1ABC123xyz..."
  }
}
```

Параметры:
- `enabled`: `true` для включения интеграции
- `credentials_file`: путь к файлу с ключом
- `folder_id`: ID папки для загрузки (из шага 6)

## Шаг 8: Установка зависимостей

```bash
.venv\Scripts\activate
pip install -r requirements.txt
```

## Шаг 9: Проверка работы

Создайте тестовый скрипт `test_drive.py`:

```python
from drive_uploader import GoogleDriveUploader

uploader = GoogleDriveUploader(
    credentials_file="credentials.json",
    folder_id="ВАШ_FOLDER_ID"
)

if uploader.service:
    print("✅ Google Drive подключен успешно!")
    
    # Тест создания папки
    folder_id = uploader.create_folder("Test Folder")
    print(f"Создана папка: {folder_id}")
else:
    print("❌ Ошибка подключения Google Drive")
```

Запустите:
```bash
python test_drive.py
```

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

### Ошибка 403: Forbidden
- Убедитесь, что service account имеет доступ к папке
- Проверьте, что API включен в Google Cloud Console

### Ошибка 404: File not found
- Проверьте правильность `folder_id`
- Убедитесь, что файл `credentials.json` существует

### Ошибка "credentials.json not found"
- Скопируйте файл в директорию проекта
- Проверьте путь в `config.json`

## Безопасность

⚠️ **Никогда не коммитьте `credentials.json` в Git!**

Файл `.gitignore` уже содержит `.env` и `credentials.json` для безопасности.

## Дополнительные ресурсы

- [Документация Google Drive API](https://developers.google.com/drive/api/v3/about-sdk)
- [Service Account аутентификация](https://cloud.google.com/docs/authentication/provide-credentials-adc#service-account)
- [Python Quickstart](https://developers.google.com/drive/api/v3/quickstart/python)
