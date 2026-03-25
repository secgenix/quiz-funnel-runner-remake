"""
Тест Google Drive интеграции
Проверяет подключение и загрузку тестовых файлов
"""
import os
import sys
import json
import logging
from pathlib import Path

from config import get_config
from drive_uploader import GoogleDriveUploader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def print_header(text: str):
    """Вывод заголовка"""
    print("\n" + "=" * 60)
    print(f"  {text}")
    print("=" * 60 + "\n")


def print_result(success: bool, message: str):
    """Вывод результата теста"""
    status = "✅" if success else "❌"
    print(f"{status} {message}")
    return success


def test_config():
    """Проверка конфигурации"""
    print_header("1. Проверка конфигурации")
    
    cfg = get_config()
    
    success = True
    
    # Проверка Google Drive настроек
    print(f"Google Drive включен: {cfg.google_drive.enabled}")
    print(f"Файл учетных данных: {cfg.google_drive.credentials_file}")
    print(f"Folder ID: {cfg.google_drive.folder_id or '(не указан)'}")
    print(f"Token file: {cfg.google_drive.token_file}")
    print(f"Root folder name: {cfg.google_drive.root_folder_name or '(не указано)'}")
    
    if not cfg.google_drive.enabled:
        print_result(False, "Google Drive отключен в config.json")
        success = False
    else:
        print_result(True, "Google Drive включен в config.json")
    
    if not os.path.exists(cfg.google_drive.credentials_file):
        print_result(False, f"Файл учетных данных не найден: {cfg.google_drive.credentials_file}")
        success = False
    else:
        print_result(True, "Файл учетных данных найден")
    
    # Проверка содержимого credentials файла
    try:
        with open(cfg.google_drive.credentials_file, 'r') as f:
            creds = json.load(f)
        
        is_service_account = creds.get('type') == 'service_account'
        is_oauth_client = 'installed' in creds or 'web' in creds

        if is_service_account:
            required_fields = ['type', 'project_id', 'private_key_id', 'private_key',
                              'client_email', 'client_id', 'auth_uri', 'token_uri']
            missing_fields = [f for f in required_fields if f not in creds]
            if missing_fields:
                print_result(False, f"Отсутствуют поля в service account credentials: {missing_fields}")
                success = False
            else:
                print_result(True, "Service account credentials файл валиден")
                print(f"  Project ID: {creds.get('project_id', 'N/A')}")
                print(f"  Client Email: {creds.get('client_email', 'N/A')}")
        elif is_oauth_client:
            oauth_section = creds.get('installed') or creds.get('web') or {}
            required_fields = ['client_id', 'project_id', 'auth_uri', 'token_uri']
            missing_fields = [f for f in required_fields if f not in oauth_section]
            if missing_fields:
                print_result(False, f"Отсутствуют поля в OAuth credentials: {missing_fields}")
                success = False
            else:
                print_result(True, "OAuth credentials файл валиден")
                print(f"  Project ID: {oauth_section.get('project_id', 'N/A')}")
                print(f"  Client ID: {oauth_section.get('client_id', 'N/A')}")
        else:
            print_result(False, "Неизвестный формат Google credentials файла")
            success = False
    
    except json.JSONDecodeError as e:
        print_result(False, f"Невалидный JSON в credentials файле: {e}")
        success = False
    except Exception as e:
        print_result(False, f"Ошибка чтения credentials файла: {e}")
        success = False
    
    return success


def test_drive_connection():
    """Проверка подключения к Google Drive"""
    print_header("2. Проверка подключения к Google Drive API")
    
    cfg = get_config()
    
    if not os.path.exists(cfg.google_drive.credentials_file):
        print_result(False, "Пропущено (нет credentials файла)")
        return False
    
    try:
        uploader = GoogleDriveUploader(
            credentials_file=cfg.google_drive.credentials_file,
            folder_id=cfg.google_drive.folder_id
        )
        
        if not uploader.service:
            print_result(False, "Не удалось инициализировать сервис")
            return False
        
        print_result(True, "Google Drive сервис инициализирован")
        
        # Проверяем доступ к папке
        if cfg.google_drive.folder_id:
            try:
                folder_info = uploader.get_file_info(cfg.google_drive.folder_id)
                if folder_info:
                    print_result(True, f"Папка найдена: {folder_info.get('name', 'N/A')}")
                    print(f"  ID: {cfg.google_drive.folder_id}")
                    print(f"  Ссылка: {folder_info.get('webViewLink', 'N/A')}")
                else:
                    print_result(False, "Папка не найдена или нет доступа")
                    return False
            except Exception as e:
                print_result(False, f"Ошибка проверки папки: {e}")
                return False
        else:
            print("⚠️ Folder ID не указан, будет использоваться корневая папка Service Account")
        
        return True
        
    except Exception as e:
        print_result(False, f"Ошибка подключения: {e}")
        return False


def test_create_folder():
    """Проверка создания папки"""
    print_header("3. Проверка создания папки")
    
    cfg = get_config()
    
    try:
        uploader = GoogleDriveUploader(
            credentials_file=cfg.google_drive.credentials_file,
            folder_id=cfg.google_drive.folder_id
        )
        
        if not uploader.service:
            print_result(False, "Сервис не инициализирован")
            return False
        
        # Создаем тестовую папку
        test_folder_name = "test_drive_check"
        print(f"Создание папки: {test_folder_name}")
        
        folder_id = uploader.create_folder(test_folder_name)
        
        if folder_id:
            print_result(True, f"Папка создана: {folder_id}")
            print(f"  Ссылка: https://drive.google.com/drive/folders/{folder_id}")
            
            # Проверяем возможность удаления (опционально)
            print("\n⚠️ Тестовая папка создана. Удалите её вручную после проверки.")
            print(f"  URL: https://drive.google.com/drive/folders/{folder_id}")
            
            return True
        else:
            print_result(False, "Не удалось создать папку")
            return False
            
    except Exception as e:
        print_result(False, f"Ошибка: {e}")
        return False


def test_upload_file():
    """Проверка загрузки файла"""
    print_header("4. Проверка загрузки файла")
    
    cfg = get_config()
    
    try:
        uploader = GoogleDriveUploader(
            credentials_file=cfg.google_drive.credentials_file,
            folder_id=cfg.google_drive.folder_id
        )
        
        if not uploader.service:
            print_result(False, "Сервис не инициализирован")
            return False
        
        # Создаем тестовый файл
        test_content = "Quiz Funnel Runner - Google Drive Test\n" + "=" * 40
        test_filename = "test_upload_check.txt"
        test_filepath = Path("results") / test_filename
        
        # Создаем директорию results если нет
        os.makedirs("results", exist_ok=True)
        
        with open(test_filepath, 'w', encoding='utf-8') as f:
            f.write(test_content)
        
        print(f"Создан тестовый файл: {test_filepath}")
        
        # Загружаем файл
        print(f"Загрузка файла в Google Drive...")
        
        # Если folder_id не указан, создаем тестовую папку
        folder_id = cfg.google_drive.folder_id
        if not folder_id:
            folder_id = uploader.create_folder("test_drive_check")
            print(f"Создана тестовая папка: {folder_id}")
        
        file_url = uploader.upload_file(str(test_filepath), folder_id)
        
        if file_url:
            print_result(True, f"Файл загружен")
            print(f"  Ссылка: {file_url}")
            
            # Удаляем локальный тестовый файл
            test_filepath.unlink()
            print(f"  Локальный файл удален")
            
            return True
        else:
            print_result(False, "Не удалось загрузить файл")
            return False
            
    except Exception as e:
        print_result(False, f"Ошибка: {e}")
        return False


def test_full_upload():
    """Проверка полной загрузки результатов воронки"""
    print_header("5. Проверка полной загрузки результатов")
    
    cfg = get_config()
    
    # Ищем существующие результаты
    results_dir = Path("results")
    
    if not results_dir.exists():
        print_result(False, "Директория results не найдена")
        print("  Сначала запустите обработку воронки через бота")
        return False
    
    # Ищем папку с результатами (не _classified)
    funnel_dirs = [
        d for d in results_dir.iterdir() 
        if d.is_dir() and not d.name.startswith('_')
    ]
    
    if not funnel_dirs:
        print_result(False, "Нет папок с результатами воронок")
        print("  Сначала запустите обработку воронки через бота")
        return False
    
    # Берем первую папку
    test_dir = funnel_dirs[0]
    print(f"Тестовая папка: {test_dir.name}")
    
    # Проверяем наличие файлов
    png_files = list(test_dir.glob("*.png"))
    log_file = test_dir / "log.txt"
    manifest_file = test_dir / "manifest.json"
    
    print(f"  Найдено скриншотов: {len(png_files)}")
    print(f"  log.txt: {'✅' if log_file.exists() else '❌'}")
    print(f"  manifest.json: {'✅' if manifest_file.exists() else '❌'}")
    
    if len(png_files) == 0:
        print_result(False, "Нет скриншотов для загрузки")
        return False
    
    try:
        uploader = GoogleDriveUploader(
            credentials_file=cfg.google_drive.credentials_file,
            folder_id=cfg.google_drive.folder_id
        )
        
        if not uploader.service:
            print_result(False, "Сервис не инициализирован")
            return False
        
        print(f"\nЗагрузка результатов в Google Drive...")
        
        drive_url = uploader.upload_funnel_results(
            slug=test_dir.name,
            result_dir=str(test_dir)
        )
        
        if drive_url:
            print_result(True, f"Результаты загружены")
            print(f"  Ссылка: {drive_url}")
            return True
        else:
            print_result(False, "Не удалось загрузить результаты")
            return False
            
    except Exception as e:
        print_result(False, f"Ошибка: {e}")
        return False


def main():
    """Основная функция"""
    print_header("🧪 ТЕСТ GOOGLE DRIVE ИНТЕГРАЦИИ")
    
    cfg = get_config()
    
    # Проверка .env
    print("📁 Конфигурация:")
    print(f"  Config file: config.json")
    print(f"  Credentials: {cfg.google_drive.credentials_file}")
    print(f"  Folder ID: {cfg.google_drive.folder_id or '(не указан)'}")
    print(f"  Token file: {cfg.google_drive.token_file}")
    print(f"  Root folder name: {cfg.google_drive.root_folder_name or '(не указано)'}")
    print(f"  Enabled: {cfg.google_drive.enabled}")
    
    # Счетчик тестов
    results = []
    
    # Запускаем тесты
    results.append(("Конфигурация", test_config()))
    
    if cfg.google_drive.enabled and os.path.exists(cfg.google_drive.credentials_file):
        results.append(("Подключение", test_drive_connection()))
        results.append(("Создание папки", test_create_folder()))
        results.append(("Загрузка файла", test_upload_file()))
        results.append(("Полная загрузка", test_full_upload()))
    else:
        print("\n⚠️ Google Drive отключен или нет credentials файла")
        print("  Пропускаем тесты подключения")
    
    # Итоги
    print_header("📊 ИТОГИ")
    
    passed = sum(1 for _, r in results if r)
    total = len(results)
    
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} | {name}")
    
    print(f"\nРезультат: {passed}/{total} тестов пройдено")
    
    if passed == total:
        print("\n🎉 Все тесты пройдены! Google Drive интеграция работает корректно.")
        sys.exit(0)
    elif passed > 0:
        print("\n⚠️ Часть тестов не пройдена. Проверьте настройки.")
        sys.exit(1)
    else:
        print("\n❌ Все тесты провалены. Проверьте конфигурацию.")
        sys.exit(1)


if __name__ == "__main__":
    main()
