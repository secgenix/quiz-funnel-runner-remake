"""
Google Sheets/Docs интеграция для чтения URL
Использует Service Account для авторизации
Поддерживает:
- Google Sheets (чтение колонки с URL)
- Google Docs (парсинг текста на наличие URL)
"""
import re
import logging
from typing import List, Optional
from urllib.parse import urlparse

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

# Разрешения для Service Account
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/documents.readonly',
    'https://www.googleapis.com/auth/drive.readonly'
]


class GoogleLinksReader:
    """Класс для чтения URL из Google Sheets и Docs через Service Account"""

    def __init__(self, credentials_file: str):
        """
        Инициализация читалки ссылок
        
        Args:
            credentials_file: Путь к JSON файлу с учетными данными Service Account
        """
        self.credentials_file = credentials_file
        self.sheets_service = None
        self.docs_service = None
        self._initialize_services()

    def _initialize_services(self) -> None:
        """Инициализация сервисов Google Sheets и Docs API через Service Account"""
        try:
            import os
            
            if not os.path.exists(self.credentials_file):
                logger.error(f"Файл credentials не найден: {self.credentials_file}")
                logger.error("Используйте Service Account credentials (не OAuth client_secret)")
                return
            
            # Загружаем credentials Service Account
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_file,
                scopes=SCOPES
            )

            self.sheets_service = build('sheets', 'v4', credentials=credentials)
            self.docs_service = build('docs', 'v1', credentials=credentials)
            logger.info("Google Sheets и Docs сервисы инициализированы (Service Account)")

        except Exception as e:
            logger.error(f"Ошибка инициализации сервисов: {e}")
            self.sheets_service = None
            self.docs_service = None

    def is_google_sheet_url(self, url: str) -> bool:
        """Проверка, является ли URL Google Sheet"""
        parsed = urlparse(url)
        return (
            'docs.google.com' in parsed.netloc and 
            '/spreadsheets/' in parsed.path
        )

    def is_google_doc_url(self, url: str) -> bool:
        """Проверка, является ли URL Google Doc"""
        parsed = urlparse(url)
        return (
            'docs.google.com' in parsed.netloc and 
            '/document/' in parsed.path
        )

    def extract_sheet_id(self, url: str) -> Optional[str]:
        """Извлечение ID Google Sheet из URL"""
        # Пример: https://docs.google.com/spreadsheets/d/1ABC123xyz/edit#gid=0
        match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', url)
        return match.group(1) if match else None

    def extract_doc_id(self, url: str) -> Optional[str]:
        """Извлечение ID Google Doc из URL"""
        # Пример: https://docs.google.com/document/d/1ABC123xyz/edit
        match = re.search(r'/document/d/([a-zA-Z0-9-_]+)', url)
        return match.group(1) if match else None

    def read_urls_from_sheet(self, sheet_url: str, column_index: int = 0) -> List[str]:
        """
        Чтение URL из Google Sheets
        
        Args:
            sheet_url: URL Google Sheet
            column_index: Индекс колонки (0-based, по умолчанию первая колонка)
            
        Returns:
            Список URL
        """
        if not self.sheets_service:
            logger.error("Sheets сервис не инициализирован")
            return []

        sheet_id = self.extract_sheet_id(sheet_url)
        if not sheet_id:
            logger.error(f"Не удалось извлечь ID из URL: {sheet_url}")
            return []

        try:
            # Читаем все данные из указанной колонки
            range_name = f"{chr(65 + column_index)}1:{chr(65 + column_index)}"
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=range_name
            ).execute()

            values = result.get('values', [])
            
            # Извлекаем URL из колонки
            urls = []
            for row in values:
                if len(row) > 0:
                    cell_value = row[0].strip()
                    # Проверяем, что это URL
                    if cell_value and (cell_value.startswith('http://') or cell_value.startswith('https://')):
                        urls.append(cell_value)
                    # Если это не URL, пробуем найти URL в тексте
                    elif cell_value:
                        found_urls = extract_urls_from_text(cell_value)
                        urls.extend(found_urls)

            logger.info(f"Прочитано {len(urls)} URL из Google Sheet")
            return urls

        except HttpError as error:
            if error.resp.status == 403:
                logger.error("Ошибка доступа 403: Предоставьте Service Account доступ к документу")
                logger.error("Email Service Account: добавьте в 'Share' документа")
            logger.error(f"Ошибка чтения Google Sheet: {error}")
            return []

    def read_urls_from_doc(self, doc_url: str) -> List[str]:
        """
        Чтение URL из Google Docs
        
        Args:
            doc_url: URL Google Doc
            
        Returns:
            Список URL
        """
        if not self.docs_service:
            logger.error("Docs сервис не инициализирован")
            return []

        doc_id = self.extract_doc_id(doc_url)
        if not doc_id:
            logger.error(f"Не удалось извлечь ID из URL: {doc_url}")
            return []

        try:
            # Получаем содержимое документа
            doc = self.docs_service.documents().get(documentId=doc_id).execute()
            
            # Извлекаем текст из документа
            text_content = []
            for element in doc.get('body', {}).get('content', []):
                if 'paragraph' in element:
                    paragraph = element['paragraph']
                    for text_element in paragraph.get('elements', []):
                        if 'textRun' in text_element:
                            text_content.append(text_element['textRun'].get('content', ''))
            
            full_text = ''.join(text_content)
            
            # Извлекаем URL из текста
            urls = extract_urls_from_text(full_text)
            
            # Также проверяем ссылки в документе
            links = self._extract_links_from_doc(doc)
            urls.extend(links)
            
            # Удаляем дубликаты
            urls = list(set(urls))
            
            logger.info(f"Прочитано {len(urls)} URL из Google Doc")
            return urls

        except HttpError as error:
            if error.resp.status == 403:
                logger.error("Ошибка доступа 403: Предоставьте Service Account доступ к документу")
                logger.error("Email Service Account: добавьте в 'Share' документа")
            logger.error(f"Ошибка чтения Google Doc: {error}")
            return []

    def _extract_links_from_doc(self, doc: dict) -> List[str]:
        """Извлечение ссылок из Google Doc"""
        urls = []
        
        try:
            for element in doc.get('body', {}).get('content', []):
                if 'paragraph' in element:
                    paragraph = element['paragraph']
                    for text_element in paragraph.get('elements', []):
                        if 'textRun' in text_element:
                            text_run = text_element['textRun']
                            # Проверяем наличие ссылки
                            if 'textStyle' in text_run and 'link' in text_run['textStyle']:
                                link_url = text_run['textStyle']['link'].get('url', '')
                                if link_url:
                                    urls.append(link_url)
        except Exception as e:
            logger.debug(f"Ошибка извлечения ссылок: {e}")
        
        return urls

    def read_urls(self, url: str, column_index: int = 0) -> List[str]:
        """
        Универсальный метод для чтения URL
        
        Args:
            url: URL Google Sheet или Doc
            column_index: Индекс колонки (только для Sheets)
            
        Returns:
            Список URL
        """
        if self.is_google_sheet_url(url):
            return self.read_urls_from_sheet(url, column_index)
        elif self.is_google_doc_url(url):
            return self.read_urls_from_doc(url)
        else:
            logger.warning(f"URL не является Google Sheet или Doc: {url}")
            return []


def extract_urls_from_text(text: str) -> List[str]:
    """
    Извлечение URL из текста
    
    Args:
        text: Текст для поиска URL
        
    Returns:
        Список найденных URL
    """
    # Regex для поиска URL
    url_pattern = re.compile(
        r'https?://'  # http:// или https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain
        r'localhost|'  # localhost
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # or IP
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)?', re.IGNORECASE
    )
    
    matches = url_pattern.findall(text)
    
    # Очищаем URL от лишних символов
    cleaned_urls = []
    for url in matches:
        # Удаляем trailing punctuation
        url = url.rstrip('.,;:!?)]}\'"')
        if url not in cleaned_urls:
            cleaned_urls.append(url)
    
    return cleaned_urls


def is_google_url(url: str) -> bool:
    """Проверка, является ли URL Google Sheet или Doc"""
    parsed = urlparse(url)
    if 'docs.google.com' not in parsed.netloc:
        return False
    
    return '/spreadsheets/' in parsed.path or '/document/' in parsed.path
