"""
Google Drive интеграция для Quiz Funnel Runner.

Поддерживает:
- OAuth desktop credentials с автоматическим сохранением токена;
- service account credentials;
- повторное использование клиента между загрузками;
- осторожную работу с квотами и повторные попытки с backoff;
- кеширование найденных папок и содержимого директорий;
- пропуск повторной загрузки одинаковых файлов.
"""

import json
import logging
import mimetypes
import os
import random
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from google.auth.exceptions import RefreshError, TransportError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive"]
DEFAULT_TOKEN_FILE = "token.json"
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
DEFAULT_UPLOAD_FIELDS = "id,name,size,webViewLink,webContentLink,md5Checksum,parents"
RETRIABLE_STATUS_CODES = {403, 408, 409, 429, 500, 502, 503, 504}
RETRIABLE_REASONS = {
    "rateLimitExceeded",
    "userRateLimitExceeded",
    "backendError",
    "internalError",
    "quotaExceeded",
    "sharingRateLimitExceeded",
}


def _normalize_drive_name(name: str) -> str:
    value = (name or "").strip()
    return " ".join(value.split())


def _escape_drive_query(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _safe_file_size(path: str) -> Optional[int]:
    try:
        return os.path.getsize(path)
    except OSError:
        return None


class GoogleDriveUploader:
    """Надёжный загрузчик файлов и папок в Google Drive."""

    _shared_lock = threading.RLock()
    _service_cache: Dict[Tuple[str, str, Tuple[str, ...]], Resource] = {}
    _folder_cache: Dict[Tuple[str, str, str], str] = {}
    _folder_children_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}

    def __init__(
        self,
        credentials_file: str,
        folder_id: str = "",
        token_file: Optional[str] = None,
        root_folder_name: str = "",
        open_browser: bool = True,
        max_retries: int = 5,
    ):
        self.credentials_file = (credentials_file or "").strip()
        self.folder_id = (folder_id or "").strip()
        self.token_file = (token_file or os.getenv("GOOGLE_DRIVE_TOKEN_FILE") or DEFAULT_TOKEN_FILE).strip()
        self.root_folder_name = _normalize_drive_name(root_folder_name or os.getenv("GOOGLE_DRIVE_ROOT_FOLDER_NAME", ""))
        self.open_browser = open_browser
        self.max_retries = max(1, int(max_retries))
        self.service: Optional[Resource] = None
        self.initialization_error: Optional[str] = None
        self._initialize_service()

    def _initialize_service(self) -> None:
        if not self.credentials_file:
            self.initialization_error = "Google Drive credentials file is not configured"
            logger.warning(self.initialization_error)
            return

        credentials_path = Path(self.credentials_file)
        if not credentials_path.exists():
            self.initialization_error = f"Google Drive credentials file not found: {credentials_path}"
            logger.warning(self.initialization_error)
            return

        cache_key = (str(credentials_path.resolve()), str(Path(self.token_file).resolve()), tuple(SCOPES))

        with self._shared_lock:
            cached_service = self._service_cache.get(cache_key)
            if cached_service is not None:
                self.service = cached_service
                logger.debug("Google Drive client reused from cache")
                return

        try:
            creds = self._load_credentials(credentials_path)
            service = build("drive", "v3", credentials=creds, cache_discovery=False)
            with self._shared_lock:
                self._service_cache[cache_key] = service
            self.service = service
            logger.info("Google Drive client initialized successfully")
        except Exception as exc:
            self.initialization_error = str(exc)
            self.service = None
            logger.exception("Failed to initialize Google Drive client")

    def _load_credentials(self, credentials_path: Path):
        with credentials_path.open("r", encoding="utf-8") as fh:
            raw_credentials = json.load(fh)

        if raw_credentials.get("type") == "service_account":
            logger.info("Using Google Drive service account credentials")
            return ServiceAccountCredentials.from_service_account_file(str(credentials_path), scopes=SCOPES)

        if "installed" not in raw_credentials and "web" not in raw_credentials:
            raise ValueError(
                "Unsupported Google credentials format. Expected desktop OAuth client or service account JSON"
            )

        creds = None
        token_path = Path(self.token_file)
        if token_path.exists():
            try:
                creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
            except Exception as exc:
                logger.warning("Stored Google token is invalid and will be replaced: %s", exc)

        if creds and creds.valid:
            return creds

        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                self._save_token(creds, token_path)
                logger.info("Google OAuth token refreshed")
                return creds
            except (RefreshError, TransportError) as exc:
                logger.warning("Google token refresh failed, interactive auth required: %s", exc)

        logger.info("Starting Google OAuth authorization flow")
        flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), SCOPES)
        creds = flow.run_local_server(port=0, open_browser=self.open_browser)
        self._save_token(creds, token_path)
        logger.info("Google OAuth authorization completed")
        return creds

    def _save_token(self, creds: Credentials, token_path: Path) -> None:
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(creds.to_json(), encoding="utf-8")
        logger.info("Google OAuth token saved to %s", token_path)

    def _extract_error_reason(self, error: HttpError) -> Tuple[Optional[int], str]:
        status = getattr(getattr(error, "resp", None), "status", None)
        reason = ""
        try:
            payload = json.loads(error.content.decode("utf-8"))
            details = payload.get("error", {})
            errors = details.get("errors") or []
            if errors:
                reason = str(errors[0].get("reason") or "")
            if not reason:
                reason = str(details.get("status") or details.get("message") or "")
        except Exception:
            reason = str(error)
        return status, reason

    def _is_retriable_error(self, error: Exception) -> bool:
        if isinstance(error, HttpError):
            status, reason = self._extract_error_reason(error)
            return status in RETRIABLE_STATUS_CODES or reason in RETRIABLE_REASONS
        return isinstance(error, (TimeoutError, ConnectionError, OSError, TransportError, RefreshError))

    def _execute_with_retry(self, request_factory: Callable[[], Any], operation: str):
        last_error = None
        for attempt in range(1, self.max_retries + 1):
            try:
                request = request_factory()
                return request.execute()
            except Exception as exc:
                last_error = exc
                retriable = self._is_retriable_error(exc)
                if not retriable or attempt >= self.max_retries:
                    break
                delay = min(30.0, (2 ** (attempt - 1)) + random.uniform(0.3, 1.2))
                logger.warning(
                    "Google Drive operation '%s' failed (attempt %s/%s). Retrying in %.1fs. Error: %s",
                    operation,
                    attempt,
                    self.max_retries,
                    delay,
                    exc,
                )
                time.sleep(delay)
        if isinstance(last_error, HttpError):
            status, reason = self._extract_error_reason(last_error)
            logger.error("Google Drive operation '%s' failed. status=%s reason=%s", operation, status, reason)
        elif last_error is not None:
            logger.error("Google Drive operation '%s' failed: %s", operation, last_error)
        raise last_error

    def _base_folder_id(self, parent_folder_id: str = "") -> str:
        return (parent_folder_id or self.folder_id or "").strip()

    def _resolve_root_folder_id(self) -> str:
        parent_id = self.folder_id
        if self.root_folder_name:
            return self.get_or_create_folder(self.root_folder_name, parent_id)
        return parent_id

    def _list_children_map(self, folder_id: str) -> Dict[str, Dict[str, Any]]:
        folder_id = (folder_id or "").strip()
        if not folder_id:
            return {}

        with self._shared_lock:
            cached = self._folder_children_cache.get(folder_id)
            if cached is not None:
                return cached

        query = f"'{folder_id}' in parents and trashed = false"
        response = self._execute_with_retry(
            lambda: self.service.files().list(
                q=query,
                pageSize=200,
                fields="files(id,name,mimeType,size,md5Checksum,webViewLink,parents)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            ),
            operation=f"list folder children {folder_id}",
        )
        files = response.get("files", [])
        mapped = {str(item.get("id")): item for item in files if item.get("id")}
        with self._shared_lock:
            self._folder_children_cache[folder_id] = mapped
        return mapped

    def _remember_child(self, folder_id: str, file_info: Dict[str, Any]) -> None:
        folder_id = (folder_id or "").strip()
        file_id = str(file_info.get("id") or "").strip()
        if not folder_id or not file_id:
            return
        with self._shared_lock:
            children = self._folder_children_cache.setdefault(folder_id, {})
            children[file_id] = file_info

    def _find_existing_folder(self, name: str, parent_folder_id: str = "") -> Optional[str]:
        normalized_name = _normalize_drive_name(name)
        parent_id = self._base_folder_id(parent_folder_id)
        cache_key = (parent_id, normalized_name, "folder")
        with self._shared_lock:
            cached = self._folder_cache.get(cache_key)
            if cached:
                return cached

        if parent_id:
            for item in self._list_children_map(parent_id).values():
                if item.get("mimeType") == FOLDER_MIME_TYPE and _normalize_drive_name(item.get("name", "")) == normalized_name:
                    folder_id = str(item.get("id"))
                    with self._shared_lock:
                        self._folder_cache[cache_key] = folder_id
                    return folder_id

        escaped_name = _escape_drive_query(normalized_name)
        query_parts = [
            f"name = '{escaped_name}'",
            f"mimeType = '{FOLDER_MIME_TYPE}'",
            "trashed = false",
        ]
        if parent_id:
            query_parts.append(f"'{parent_id}' in parents")
        response = self._execute_with_retry(
            lambda: self.service.files().list(
                q=" and ".join(query_parts),
                pageSize=10,
                fields="files(id,name,parents,webViewLink)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            ),
            operation=f"find folder {normalized_name}",
        )
        matches = response.get("files", [])
        if matches:
            folder_id = str(matches[0].get("id"))
            with self._shared_lock:
                self._folder_cache[cache_key] = folder_id
                if parent_id:
                    self._remember_child(parent_id, {**matches[0], "mimeType": FOLDER_MIME_TYPE})
            return folder_id
        return None

    def get_or_create_folder(self, name: str, parent_folder_id: str = "") -> str:
        if not self.service:
            raise RuntimeError(self.initialization_error or "Google Drive service is not initialized")

        normalized_name = _normalize_drive_name(name)
        if not normalized_name:
            raise ValueError("Folder name must not be empty")

        existing_id = self._find_existing_folder(normalized_name, parent_folder_id)
        if existing_id:
            logger.debug("Using existing Google Drive folder '%s' (%s)", normalized_name, existing_id)
            return existing_id

        parent_id = self._base_folder_id(parent_folder_id)
        body: Dict[str, Any] = {"name": normalized_name, "mimeType": FOLDER_MIME_TYPE}
        if parent_id:
            body["parents"] = [parent_id]

        response = self._execute_with_retry(
            lambda: self.service.files().create(
                body=body,
                fields="id,name,parents,webViewLink",
                supportsAllDrives=True,
            ),
            operation=f"create folder {normalized_name}",
        )
        folder_id = str(response.get("id"))
        cache_key = (parent_id, normalized_name, "folder")
        with self._shared_lock:
            self._folder_cache[cache_key] = folder_id
            if parent_id:
                self._remember_child(parent_id, {**response, "mimeType": FOLDER_MIME_TYPE})
        logger.info("Google Drive folder ready: %s (%s)", normalized_name, folder_id)
        return folder_id

    def create_folder(self, name: str, parent_folder_id: str = "") -> Optional[str]:
        try:
            return self.get_or_create_folder(name, parent_folder_id)
        except Exception as exc:
            logger.error("Failed to create or reuse Google Drive folder '%s': %s", name, exc)
            return None

    def _find_existing_file(self, file_name: str, folder_id: str, local_size: Optional[int]) -> Optional[Dict[str, Any]]:
        if not folder_id:
            return None
        for item in self._list_children_map(folder_id).values():
            if item.get("mimeType") == FOLDER_MIME_TYPE:
                continue
            if item.get("name") != file_name:
                continue
            remote_size_raw = item.get("size")
            try:
                remote_size = int(remote_size_raw) if remote_size_raw is not None else None
            except (TypeError, ValueError):
                remote_size = None
            if local_size is not None and remote_size is not None and local_size == remote_size:
                return item
        return None

    def upload_file(self, file_path: str, folder_id: str = "", make_shareable: bool = False) -> Optional[str]:
        del make_shareable

        if not self.service:
            logger.warning("Google Drive upload skipped: service is not initialized")
            return None

        path = Path(file_path)
        if not path.exists() or not path.is_file():
            logger.error("Google Drive upload skipped, file not found: %s", file_path)
            return None

        parent_id = self._base_folder_id(folder_id)
        file_name = path.name
        local_size = _safe_file_size(str(path))
        existing = self._find_existing_file(file_name, parent_id, local_size) if parent_id else None
        if existing:
            logger.info("Skipping re-upload for unchanged file '%s' in Google Drive", file_name)
            return existing.get("webViewLink") or f"https://drive.google.com/file/d/{existing.get('id')}/view"

        mime_type = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
        metadata: Dict[str, Any] = {"name": file_name}
        if parent_id:
            metadata["parents"] = [parent_id]

        media = MediaFileUpload(str(path), mimetype=mime_type, resumable=True)
        try:
            response = self._execute_with_retry(
                lambda: self.service.files().create(
                    body=metadata,
                    media_body=media,
                    fields=DEFAULT_UPLOAD_FIELDS,
                    supportsAllDrives=True,
                ),
                operation=f"upload file {file_name}",
            )
        finally:
            try:
                media.stream().close()
            except Exception:
                pass

        uploaded_size_raw = response.get("size")
        try:
            uploaded_size = int(uploaded_size_raw) if uploaded_size_raw is not None else None
        except (TypeError, ValueError):
            uploaded_size = None

        if local_size is not None and uploaded_size is not None and local_size != uploaded_size:
            logger.error(
                "Uploaded Google Drive file size mismatch for '%s': local=%s remote=%s",
                file_name,
                local_size,
                uploaded_size,
            )
            return None

        if parent_id:
            self._remember_child(parent_id, response)

        logger.info("Google Drive file uploaded successfully: %s", file_name)
        return response.get("webViewLink") or f"https://drive.google.com/file/d/{response.get('id')}/view"

    def upload_funnel_results(self, slug: str, result_dir: str, drive_folder_id: str = "") -> Optional[str]:
        if not self.service:
            logger.warning("Google Drive results upload skipped: service is not initialized")
            return None

        result_path = Path(result_dir)
        if not result_path.exists() or not result_path.is_dir():
            logger.error("Results directory for Google Drive upload not found: %s", result_dir)
            return None

        try:
            root_parent_id = drive_folder_id.strip() if drive_folder_id else self._resolve_root_folder_id()
            funnel_folder_id = self.get_or_create_folder(slug, root_parent_id)

            primary_files = sorted(result_path.glob("*.png"))
            for extra_name in ["log.txt", "manifest.json"]:
                candidate = result_path / extra_name
                if candidate.exists():
                    primary_files.append(candidate)

            uploaded_count = 0
            for file_path in primary_files:
                if self.upload_file(str(file_path), funnel_folder_id):
                    uploaded_count += 1

            classified_root = result_path.parent / "_classified"
            if classified_root.exists() and classified_root.is_dir():
                screen_types = ["question", "info", "input", "email", "paywall", "other", "checkout"]
                classified_folder_id = None
                for screen_type in screen_types:
                    type_dir = classified_root / screen_type
                    matching_files = sorted(type_dir.glob(f"{slug}*.png")) if type_dir.exists() else []
                    if not matching_files:
                        continue
                    if classified_folder_id is None:
                        classified_folder_id = self.get_or_create_folder("_classified", funnel_folder_id)
                    type_folder_id = self.get_or_create_folder(screen_type, classified_folder_id)
                    for file_path in matching_files:
                        if self.upload_file(str(file_path), type_folder_id):
                            uploaded_count += 1

            logger.info("Google Drive upload finished for slug='%s', uploaded_files=%s", slug, uploaded_count)
            return f"https://drive.google.com/drive/folders/{funnel_folder_id}"
        except Exception as exc:
            logger.error("Failed to upload funnel results to Google Drive for '%s': %s", slug, exc)
            return None

    def get_file_info(self, file_id: str) -> Optional[Dict[str, Any]]:
        if not self.service or not file_id:
            return None
        try:
            return self._execute_with_retry(
                lambda: self.service.files().get(
                    fileId=file_id,
                    fields="id,name,size,mimeType,webViewLink,createdTime,parents",
                    supportsAllDrives=True,
                ),
                operation=f"get file info {file_id}",
            )
        except Exception as exc:
            logger.error("Failed to get Google Drive file info for '%s': %s", file_id, exc)
            return None

    def list_folder_contents(self, folder_id: str = "") -> List[Dict[str, Any]]:
        if not self.service:
            return []
        parent_id = self._base_folder_id(folder_id)
        if not parent_id:
            return []
        try:
            return list(self._list_children_map(parent_id).values())
        except Exception as exc:
            logger.error("Failed to list Google Drive folder contents for '%s': %s", parent_id, exc)
            return []

    def revoke_token(self) -> bool:
        token_path = Path(self.token_file)
        try:
            if token_path.exists():
                token_path.unlink()
                logger.info("Google OAuth token removed: %s", token_path)
                return True
            return False
        except Exception as exc:
            logger.error("Failed to remove Google OAuth token '%s': %s", token_path, exc)
            return False


def upload_to_drive(
    credentials_file: str,
    file_path: str,
    folder_name: str = "",
    parent_folder_id: str = "",
    token_file: Optional[str] = None,
    root_folder_name: str = "",
) -> Optional[str]:
    """Удобная функция для единичной загрузки файла в Google Drive."""
    uploader = GoogleDriveUploader(
        credentials_file=credentials_file,
        folder_id=parent_folder_id,
        token_file=token_file,
        root_folder_name=root_folder_name,
    )
    if not uploader.service:
        return None
    target_folder_id = parent_folder_id
    if folder_name:
        target_folder_id = uploader.get_or_create_folder(folder_name, parent_folder_id)
    return uploader.upload_file(file_path, target_folder_id)
