"""
Google Drive интеграция для Quiz Funnel Runner
Загрузка скриншотов и результатов в Google Drive через OAuth 2.0
"""
import os
import io
import logging
import pickle
from typing import Optional, List, Dict, Any
from pathlib import Path

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

# Разрешения для OAuth 2.0
SCOPES = ['https://www.googleapis.com/auth/drive.file']

# Файлы для хранения токена
TOKEN_FILE = "token.pickle"


class GoogleDriveUploader:
    """Класс для загрузки файлов в Google Drive через OAuth 2.0"""

    def __init__(self, credentials_file: str, folder_id: str = ""):
        """
        Инициализация загрузчика
        
        Args:
            credentials_file: Путь к JSON файлу с учетными данными OAuth (client_secret.json)
            folder_id: ID корневой папки в Google Drive (опционально)
        """
        self.credentials_file = credentials_file
        self.folder_id = folder_id
        self.service = None
        self._initialize_service()

    def _initialize_service(self) -> None:
        """Инициализация сервиса Google Drive API через OAuth 2.0"""
        try:
            creds = None
            
            # Проверяем сохраненный токен
            if os.path.exists(TOKEN_FILE):
                with open(TOKEN_FILE, 'rb') as token:
                    creds = pickle.load(token)

            # Если токена нет или он невалиден, запускаем авторизацию
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    logger.info("Обновление токена...")
                    creds.refresh(Request())
                else:
                    logger.info("Запуск авторизации OAuth 2.0...")
                    
                    if not os.path.exists(self.credentials_file):
                        logger.error(f"Файл credentials не найден: {self.credentials_file}")
                        logger.error("Скачайте client_secret.json из Google Cloud Console")
                        return
                    
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.credentials_file,
                        SCOPES
                    )
                    
                    # Запускаем локальный сервер для авторизации
                    creds = flow.run_local_server(
                        port=0,
                        host='localhost',
                        open_browser=True
                    )
                    
                    logger.info("Авторизация успешна!")
                
                # Сохраняем токен
                with open(TOKEN_FILE, 'wb') as token:
                    pickle.dump(creds, token)
                    logger.info(f"Токен сохранен в {TOKEN_FILE}")

            self.service = build('drive', 'v3', credentials=creds)
            logger.info("Google Drive сервис инициализирован")

        except Exception as e:
            logger.error(f"Ошибка инициализации Google Drive: {e}")
            self.service = None

    def create_folder(self, name: str, parent_folder_id: str = "") -> Optional[str]:
        """
        Создание папки в Google Drive
        
        Args:
            name: Имя папки
            parent_folder_id: ID родительской папки (если пустой, используется folder_id из конфига)
            
        Returns:
            ID созданной папки или None
        """
        if not self.service:
            return None

        try:
            parent_id = parent_folder_id or self.folder_id
            
            file_metadata = {
                'name': name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            if parent_id:
                file_metadata['parents'] = [parent_id]

            folder = self.service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()

            folder_id = folder.get('id')
            logger.info(f"Создана папка: {name} (ID: {folder_id})")
            
            return folder_id

        except HttpError as error:
            logger.error(f"Ошибка создания папки: {error}")
            return None

    def upload_file(self, file_path: str, folder_id: str = "", 
                    make_shareable: bool = False) -> Optional[str]:
        """
        Загрузка файла в Google Drive
        
        Args:
            file_path: Путь к файлу для загрузки
            folder_id: ID папки для загрузки (если пустой, используется folder_id из конфига)
            make_shareable: Сделать файл доступным по ссылке (для OAuth не требуется)
            
        Returns:
            Ссылка на файл или None
        """
        if not self.service:
            return None

        if not os.path.exists(file_path):
            logger.error(f"Файл не найден: {file_path}")
            return None

        try:
            parent_id = folder_id or self.folder_id
            
            file_metadata = {
                'name': os.path.basename(file_path)
            }
            
            if parent_id:
                file_metadata['parents'] = [parent_id]

            media = MediaFileUpload(file_path, resumable=True)

            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()

            file_id = file.get('id')
            logger.info(f"Загружен файл: {os.path.basename(file_path)} (ID: {file_id})")

            # Для OAuth 2.0 файл уже доступен пользователю
            # Возвращаем ссылку
            return f"https://drive.google.com/file/d/{file_id}/view"

        except HttpError as error:
            logger.error(f"Ошибка загрузки файла: {error}")
            return None

    def upload_funnel_results(self, slug: str, result_dir: str, 
                             drive_folder_id: str = "") -> Optional[str]:
        """
        Загрузка результатов воронки в Google Drive
        
        Args:
            slug: Уникальный идентификатор воронки (имя папки)
            result_dir: Локальная директория с результатами
            drive_folder_id: ID папки в Drive (если пустой, создается новая)
            
        Returns:
            Ссылка на папку с результатами или None
        """
        if not self.service:
            return None

        try:
            # Создаем или используем существующую папку воронки
            if not drive_folder_id:
                # Создаем папку с именем slug
                folder_id = self.create_folder(slug, self.folder_id)
            else:
                folder_id = drive_folder_id

            if not folder_id:
                return None

            # Собираем файлы для загрузки
            files_to_upload = []
            
            # Скриншоты
            for filename in os.listdir(result_dir):
                if filename.endswith('.png'):
                    files_to_upload.append(os.path.join(result_dir, filename))
            
            # Лог
            log_path = os.path.join(result_dir, 'log.txt')
            if os.path.exists(log_path):
                files_to_upload.append(log_path)
            
            # Manifest
            manifest_path = os.path.join(result_dir, 'manifest.json')
            if os.path.exists(manifest_path):
                files_to_upload.append(manifest_path)

            # Загружаем файлы
            uploaded_files = []
            for file_path in files_to_upload:
                file_url = self.upload_file(file_path, folder_id)
                if file_url:
                    uploaded_files.append(file_url)
                    logger.info(f"Загружен: {file_path}")

            # Создаем папку _classified внутри папки воронки
            classified_local_dir = os.path.join(result_dir, '..', '_classified')
            if os.path.exists(classified_local_dir):
                classified_folder_id = self.create_folder('_classified', folder_id)
                
                if classified_folder_id:
                    # Загружаем классифицированные скриншоты
                    for screen_type in ['question', 'info', 'input', 'email', 'paywall', 'other', 'checkout']:
                        type_dir = os.path.join(classified_local_dir, screen_type)
                        if os.path.exists(type_dir):
                            type_folder_id = self.create_folder(screen_type, classified_folder_id)
                            
                            if type_folder_id:
                                # Загружаем скриншоты этого типа
                                for filename in os.listdir(type_dir):
                                    if filename.startswith(slug) and filename.endswith('.png'):
                                        file_path = os.path.join(type_dir, filename)
                                        self.upload_file(file_path, type_folder_id)

            logger.info(f"Загрузка результатов воронки {slug} завершена")
            
            # Возвращаем ссылку на основную папку
            return f"https://drive.google.com/drive/folders/{folder_id}"

        except Exception as e:
            logger.error(f"Ошибка загрузки результатов воронки: {e}")
            return None

    def get_file_info(self, file_id: str) -> Optional[Dict[str, Any]]:
        """
        Получение информации о файле
        
        Args:
            file_id: ID файла в Google Drive
            
        Returns:
            Информация о файле или None
        """
        if not self.service:
            return None

        try:
            file_info = self.service.files().get(
                fileId=file_id,
                fields='id, name, size, mimeType, webViewLink, createdTime'
            ).execute()

            return file_info

        except HttpError as error:
            logger.error(f"Ошибка получения информации о файле: {error}")
            return None

    def list_folder_contents(self, folder_id: str = "") -> List[Dict[str, Any]]:
        """
        Получение списка файлов в папке
        
        Args:
            folder_id: ID папки (если пустой, используется folder_id из конфига)
            
        Returns:
            Список файлов
        """
        if not self.service:
            return []

        try:
            parent_id = folder_id or self.folder_id
            
            if not parent_id:
                return []

            query = f"'{parent_id}' in parents and trashed = false"
            
            results = self.service.files().list(
                q=query,
                pageSize=100,
                fields="files(id, name, mimeType, size, createdTime)"
            ).execute()

            files = results.get('files', [])
            return files

        except HttpError as error:
            logger.error(f"Ошибка получения списка файлов: {error}")
            return []

    def revoke_token(self) -> bool:
        """
        Отозвать токен доступа (для сброса авторизации)
        
        Returns:
            True если успешно
        """
        try:
            if os.path.exists(TOKEN_FILE):
                os.remove(TOKEN_FILE)
                logger.info(f"Токен удален: {TOKEN_FILE}")
                return True
            return False
        except Exception as e:
            logger.error(f"Ошибка удаления токена: {e}")
            return False


def upload_to_drive(credentials_file: str, file_path: str, 
                   folder_name: str = "", parent_folder_id: str = "") -> Optional[str]:
    """
    Удобная функция для быстрой загрузки файла в Google Drive
    
    Args:
        credentials_file: Путь к файлу учетных данных OAuth
        file_path: Путь к файлу для загрузки
        folder_name: Имя папки для создания (опционально)
        parent_folder_id: ID родительской папки
        
    Returns:
        Ссылка на файл или None
    """
    uploader = GoogleDriveUploader(credentials_file, parent_folder_id)
    
    if not uploader.service:
        return None
    
    folder_id = ""
    if folder_name:
        folder_id = uploader.create_folder(folder_name, parent_folder_id)
    
    return uploader.upload_file(file_path, folder_id)
