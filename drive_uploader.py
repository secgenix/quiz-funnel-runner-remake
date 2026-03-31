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
import queue
import random
import threading
import time
from dataclasses import dataclass
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
UPLOAD_STATE_FILE = ".drive_upload_state.json"


class DriveFolderMissingError(RuntimeError):
    """Папка Google Drive была удалена или недоступна и требует пересоздания."""

    def __init__(self, folder_id: str, message: Optional[str] = None):
        self.folder_id = (folder_id or "").strip()
        super().__init__(message or f"Google Drive folder is missing: {self.folder_id}")


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
    _folder_cache: Dict[Tuple[str, str, str], str] = {}
    _folder_children_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}
    _folder_ops_lock = threading.RLock()

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
        self._credentials_path: Optional[Path] = None
        self._credentials_cache = None
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

        try:
            self._credentials_path = credentials_path
            creds = self._load_credentials(credentials_path)
            service = build("drive", "v3", credentials=creds, cache_discovery=False)
            self.service = service
            logger.info("Google Drive client initialized successfully")
        except Exception as exc:
            self.initialization_error = str(exc)
            self.service = None
            logger.exception("Failed to initialize Google Drive client")

    def _rebuild_service(self) -> None:
        credentials_path = self._credentials_path or Path(self.credentials_file)
        if not credentials_path.exists():
            raise FileNotFoundError(f"Google Drive credentials file not found: {credentials_path}")
        creds = self._load_credentials(credentials_path, force_reload=True)
        self.service = build("drive", "v3", credentials=creds, cache_discovery=False)
        self.initialization_error = None
        logger.info("Google Drive client rebuilt after transport failure")

    def _load_credentials(self, credentials_path: Path, force_reload: bool = False):
        if self._credentials_cache is None or force_reload:
            with credentials_path.open("r", encoding="utf-8") as fh:
                self._credentials_cache = json.load(fh)

        raw_credentials = self._credentials_cache

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
            if status == 200:
                return True
            return status in RETRIABLE_STATUS_CODES or reason in RETRIABLE_REASONS
        return isinstance(error, (TimeoutError, ConnectionError, OSError, TransportError, RefreshError))

    def _is_not_found_error(self, error: Exception) -> bool:
        if isinstance(error, DriveFolderMissingError):
            return True
        if isinstance(error, HttpError):
            status, reason = self._extract_error_reason(error)
            return status == 404 or reason == "notFound"
        return False

    def _invalidate_folder_cache(self, folder_id: str) -> None:
        folder_id = (folder_id or "").strip()
        if not folder_id:
            return
        with self._shared_lock:
            self._folder_children_cache.pop(folder_id, None)
            for cache_key in [key for key, value in self._folder_cache.items() if value == folder_id or key[0] == folder_id]:
                self._folder_cache.pop(cache_key, None)
            for children in self._folder_children_cache.values():
                children.pop(folder_id, None)

    def _folder_exists(self, folder_id: str) -> bool:
        folder_id = (folder_id or "").strip()
        if not folder_id:
            return False
        try:
            response = self._execute_with_retry(
                lambda: self.service.files().get(
                    fileId=folder_id,
                    fields="id,mimeType,trashed,parents,name",
                    supportsAllDrives=True,
                ),
                operation=f"validate folder {folder_id}",
            )
            return response.get("mimeType") == FOLDER_MIME_TYPE and not bool(response.get("trashed"))
        except Exception as exc:
            if self._is_not_found_error(exc):
                self._invalidate_folder_cache(folder_id)
                return False
            raise

    def _normalize_parent_folder_id(self, parent_folder_id: str = "") -> str:
        parent_id = self._base_folder_id(parent_folder_id)
        if not parent_id:
            return ""
        if self._folder_exists(parent_id):
            return parent_id
        logger.warning("Google Drive parent folder is missing, fallback to Drive root: %s", parent_id)
        self._invalidate_folder_cache(parent_id)
        return ""

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
                try:
                    self._rebuild_service()
                except Exception as rebuild_error:
                    logger.warning(
                        "Google Drive client rebuild failed after '%s' attempt %s: %s",
                        operation,
                        attempt,
                        rebuild_error,
                    )
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
        parent_id = self._normalize_parent_folder_id(self.folder_id)
        if self.root_folder_name:
            return self.get_or_create_folder(self.root_folder_name, parent_id)
        return parent_id

    def _resolve_classified_folder_id(self, root_parent_id: str) -> str:
        return self.get_or_create_folder("_classified", root_parent_id)

    def _list_children_map(self, folder_id: str) -> Dict[str, Dict[str, Any]]:
        folder_id = (folder_id or "").strip()
        if not folder_id:
            return {}

        with self._shared_lock:
            cached = self._folder_children_cache.get(folder_id)
            if cached is not None:
                return cached

        query = f"'{folder_id}' in parents and trashed = false"
        try:
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
        except Exception as exc:
            if self._is_not_found_error(exc):
                self._invalidate_folder_cache(folder_id)
                raise DriveFolderMissingError(folder_id) from exc
            raise
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
            if cached and self._folder_exists(cached):
                return cached
            if cached:
                self._folder_cache.pop(cache_key, None)

        if parent_id:
            for item in self._list_children_map(parent_id).values():
                if item.get("mimeType") == FOLDER_MIME_TYPE and _normalize_drive_name(item.get("name", "")) == normalized_name:
                    folder_id = str(item.get("id"))
                    if not self._folder_exists(folder_id):
                        self._invalidate_folder_cache(folder_id)
                        continue
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

        with self._folder_ops_lock:
            parent_folder_id = self._normalize_parent_folder_id(parent_folder_id)
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
        if parent_id and not self._folder_exists(parent_id):
            raise DriveFolderMissingError(parent_id)
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
                        classified_folder_id = self._resolve_classified_folder_id(root_parent_id)
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


@dataclass
class DriveUploadTask:
    run_id: str
    relative_path: str
    absolute_path: str
    drive_subdir: str = ""


class ParallelDriveUploadManager:
    """Параллельный и отказоустойчивый менеджер загрузок артефактов в Google Drive."""

    def __init__(
        self,
        credentials_file: str,
        folder_id: str = "",
        token_file: str = DEFAULT_TOKEN_FILE,
        root_folder_name: str = "",
        max_workers: int = 4,
        per_file_retries: int = 3,
    ):
        self.credentials_file = credentials_file
        self.folder_id = folder_id
        self.token_file = token_file
        self.root_folder_name = root_folder_name
        self.max_workers = max(1, int(max_workers))
        self.per_file_retries = max(1, int(per_file_retries))
        self._lock = threading.RLock()
        self._queue: "queue.Queue[Optional[DriveUploadTask]]" = queue.Queue()
        self._workers: List[threading.Thread] = []
        self._stop_requested = False
        self._started = False
        self._control_uploader: Optional[GoogleDriveUploader] = None
        self._worker_local = threading.local()
        self._recovery_lock = threading.RLock()
        self._runs: Dict[str, Dict[str, Any]] = {}

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._control_uploader = GoogleDriveUploader(
                credentials_file=self.credentials_file,
                folder_id=self.folder_id,
                token_file=self.token_file,
                root_folder_name=self.root_folder_name,
            )
            self._stop_requested = False
            self._started = True
            self._workers = []
            for worker_index in range(self.max_workers):
                worker = threading.Thread(
                    target=self._worker_loop,
                    args=(worker_index + 1,),
                    daemon=True,
                    name=f"drive-upload-worker-{worker_index + 1}",
                )
                worker.start()
                self._workers.append(worker)
        logger.info("Parallel Google Drive uploader started, workers=%s", self.max_workers)

    def stop(self, wait: bool = True) -> None:
        with self._lock:
            if not self._started:
                return
            self._stop_requested = True
            workers = list(self._workers)
        if wait:
            self._queue.join()
        for _ in workers:
            self._queue.put(None)
        for worker in workers:
            worker.join(timeout=30)
        with self._lock:
            self._workers.clear()
            self._started = False
        logger.info("Parallel Google Drive uploader stopped")

    def cancel_pending(self, reason: str = "Stopped by user") -> Dict[str, int]:
        drained_tasks: List[DriveUploadTask] = []
        with self._lock:
            self._stop_requested = True

        while True:
            try:
                queued_item = self._queue.get_nowait()
            except queue.Empty:
                break

            if queued_item is None:
                self._queue.task_done()
                continue

            drained_tasks.append(queued_item)
            self._queue.task_done()

        cancelled_runs = set()
        queued_entries = 0
        uploading_entries = 0
        with self._lock:
            for queued_item in drained_tasks:
                run = self._runs.get(queued_item.run_id)
                if not run:
                    continue
                entry = run["entries"].setdefault(queued_item.relative_path, {})
                if entry.get("status") == "uploaded":
                    continue
                entry["status"] = "cancelled"
                entry["last_error"] = reason[:500]
                entry["cancelled_at"] = time.time()
                entry.pop("retry_after", None)
                entry.pop("failed_at", None)
                cancelled_runs.add(queued_item.run_id)

            for run_id, run in self._runs.items():
                entries = run.get("entries") or {}
                for entry in entries.values():
                    status = str(entry.get("status") or "")
                    if status == "queued":
                        entry["status"] = "cancelled"
                        entry["last_error"] = reason[:500]
                        entry["cancelled_at"] = time.time()
                        entry.pop("retry_after", None)
                        entry.pop("failed_at", None)
                        queued_entries += 1
                        cancelled_runs.add(run_id)
                    elif status == "uploading":
                        uploading_entries += 1

            for run_id in cancelled_runs:
                self._write_state_locked(run_id)
                self._update_completion_state_locked(run_id)

        return {
            "cleared_tasks": len(drained_tasks),
            "queued_entries_cancelled": queued_entries,
            "active_uploads": uploading_entries,
            "runs_affected": len(cancelled_runs),
        }

    def register_run(self, run_id: str, slug: str, result_dir: str) -> Optional[str]:
        run_path = Path(result_dir)
        run_path.mkdir(parents=True, exist_ok=True)
        state = self._load_state_file(run_path)
        if not state:
            state = {
                "run_id": run_id,
                "slug": slug,
                "result_dir": str(run_path),
                "folder_id": "",
                "folder_url": "",
                "finalized": False,
                "entries": {},
                "created_at": time.time(),
                "updated_at": time.time(),
            }

        existing_folder_id = str(state.get("folder_id") or "").strip()
        if existing_folder_id and self._control_uploader is not None and not self._control_uploader._folder_exists(existing_folder_id):
            logger.warning("Recovered Google Drive folder is missing for run=%s, recreating tree", run_id)
            state["folder_id"] = ""
            state["folder_url"] = ""

        if not state.get("folder_id"):
            folder_id, folder_url = self._ensure_run_folder(slug)
            state["folder_id"] = folder_id
            state["folder_url"] = folder_url

        with self._lock:
            existing = self._runs.get(run_id)
            event = existing.get("event") if existing else threading.Event()
            target_folders = existing.get("target_folders", {}) if existing else {}
            self._runs[run_id] = {
                "run_id": run_id,
                "slug": state.get("slug") or slug,
                "result_dir": str(run_path),
                "folder_id": state.get("folder_id", ""),
                "folder_url": state.get("folder_url", ""),
                "entries": state.get("entries", {}),
                "event": event,
                "target_folders": target_folders,
                "finalized": bool(state.get("finalized")),
            }
            self._write_state_locked(run_id)
            self._update_completion_state_locked(run_id)
        return str(state.get("folder_url") or "") or None

    def enqueue_file(self, run_id: str, file_path: str, drive_subdir: str = "") -> bool:
        absolute = Path(file_path)
        if not absolute.exists() or not absolute.is_file():
            logger.warning("Drive upload skipped, file does not exist yet: %s", file_path)
            return False

        with self._lock:
            if self._stop_requested:
                logger.info("Drive upload skipped because uploader is stopping: %s", file_path)
                return False
            run = self._runs.get(run_id)
            if not run:
                logger.warning("Drive upload skipped, run is not registered: %s", run_id)
                return False

            result_dir = Path(run["result_dir"])
            try:
                relative = absolute.resolve().relative_to(result_dir.resolve())
            except Exception:
                classified_root = result_dir.parent / "_classified"
                try:
                    relative = absolute.resolve().relative_to(classified_root.resolve())
                    relative = Path("_classified") / relative
                except Exception:
                    relative = Path(absolute.name)

            relative_path = relative.as_posix()
            entries = run["entries"]
            entry = entries.get(relative_path, {})
            status = str(entry.get("status") or "")
            if status in {"queued", "uploading", "uploaded"}:
                return False

            entries[relative_path] = {
                "relative_path": relative_path,
                "drive_subdir": drive_subdir.strip("/"),
                "status": "queued",
                "attempts": int(entry.get("attempts") or 0),
                "last_error": str(entry.get("last_error") or ""),
                "file_url": str(entry.get("file_url") or ""),
                "queued_at": time.time(),
            }
            run["event"].clear()
            self._write_state_locked(run_id)

        self._queue.put(
            DriveUploadTask(
                run_id=run_id,
                relative_path=relative_path,
                absolute_path=str(absolute),
                drive_subdir=drive_subdir.strip("/"),
            )
        )
        return True

    def finalize_run(self, run_id: str) -> Optional[str]:
        with self._lock:
            run = self._runs.get(run_id)
            if not run:
                return None
            result_dir = Path(run["result_dir"])

        self._enqueue_missing_files(run_id, result_dir)

        with self._lock:
            run = self._runs.get(run_id)
            if not run:
                return None
            run["finalized"] = True
            self._write_state_locked(run_id)
            self._update_completion_state_locked(run_id)
            return str(run.get("folder_url") or "") or None

    def wait_for_run(self, run_id: str, timeout: Optional[float] = None) -> bool:
        with self._lock:
            run = self._runs.get(run_id)
            if not run:
                return True
            event = run["event"]
        return event.wait(timeout=timeout)

    def get_run_folder_url(self, run_id: str) -> Optional[str]:
        with self._lock:
            run = self._runs.get(run_id)
            if not run:
                return None
            return str(run.get("folder_url") or "") or None

    def recover_pending_runs(self, results_root: str = "results") -> int:
        root = Path(results_root)
        if not root.exists() or not root.is_dir():
            return 0

        recovered = 0
        for state_file in root.glob(f"*/{UPLOAD_STATE_FILE}"):
            if state_file.parent.name.startswith("_"):
                continue
            state = self._load_state_file(state_file.parent)
            if not state:
                continue
            run_id = str(state.get("run_id") or state_file.parent.name)
            slug = str(state.get("slug") or state_file.parent.name)
            self.register_run(run_id, slug, str(state_file.parent))
            self._enqueue_missing_files(run_id, state_file.parent)
            self._requeue_from_state(run_id)
            recovered += 1
        if recovered:
            logger.info("Recovered %s pending Google Drive upload run(s)", recovered)
        return recovered

    def _ensure_ready(self) -> GoogleDriveUploader:
        uploader = getattr(self._worker_local, "uploader", None)
        if uploader is None:
            uploader = GoogleDriveUploader(
                credentials_file=self.credentials_file,
                folder_id=self.folder_id,
                token_file=self.token_file,
                root_folder_name=self.root_folder_name,
            )
            self._worker_local.uploader = uploader
        if uploader is None:
            raise RuntimeError("Parallel Google Drive uploader is not started")
        if not uploader.service:
            raise RuntimeError(uploader.initialization_error or "Google Drive service is not initialized")
        return uploader

    def _ensure_run_folder(self, slug: str) -> Tuple[str, str]:
        uploader = self._control_uploader
        if uploader is None:
            raise RuntimeError("Parallel Google Drive uploader is not started")
        if not uploader.service:
            raise RuntimeError(uploader.initialization_error or "Google Drive service is not initialized")
        root_parent_id = self.folder_id.strip() if self.folder_id else uploader._resolve_root_folder_id()
        folder_id = uploader.get_or_create_folder(slug, root_parent_id)
        return folder_id, f"https://drive.google.com/drive/folders/{folder_id}"

    def _enqueue_missing_files(self, run_id: str, result_dir: Path) -> None:
        if not result_dir.exists():
            return

        for candidate in sorted(result_dir.glob("*.png")):
            self.enqueue_file(run_id, str(candidate))

        for extra_name in ["log.txt", "manifest.json"]:
            candidate = result_dir / extra_name
            if candidate.exists():
                self.enqueue_file(run_id, str(candidate))

        classified_root = result_dir.parent / "_classified"
        slug = result_dir.name
        if classified_root.exists() and classified_root.is_dir():
            for screen_type_dir in sorted(classified_root.iterdir()):
                if not screen_type_dir.is_dir():
                    continue
                for candidate in sorted(screen_type_dir.glob(f"{slug}*.png")):
                    self.enqueue_file(run_id, str(candidate), drive_subdir=f"_classified/{screen_type_dir.name}")

    def _requeue_from_state(self, run_id: str) -> None:
        with self._lock:
            run = self._runs.get(run_id)
            if not run:
                return
            result_dir = Path(run["result_dir"])
            entries = dict(run["entries"])

        for relative_path, entry in entries.items():
            status = str(entry.get("status") or "")
            if status == "uploaded":
                continue
            relative = Path(relative_path)
            if relative.parts and relative.parts[0] == "_classified":
                absolute_path = result_dir.parent / relative
            else:
                absolute_path = result_dir / relative
            if not absolute_path.exists() or not absolute_path.is_file():
                continue
            with self._lock:
                current_run = self._runs.get(run_id)
                if not current_run:
                    return
                current_entry = current_run["entries"].setdefault(relative_path, {})
                current_entry["status"] = "queued"
                current_entry["attempts"] = 0
                current_entry["last_error"] = ""
                current_entry.pop("retry_after", None)
                current_entry.pop("failed_at", None)
                current_entry.setdefault("drive_subdir", str(entry.get("drive_subdir") or ""))
                self._write_state_locked(run_id)
            self._queue.put(
                DriveUploadTask(
                    run_id=run_id,
                    relative_path=relative_path,
                    absolute_path=str(absolute_path),
                    drive_subdir=str(entry.get("drive_subdir") or ""),
                )
            )

    def _worker_loop(self, worker_id: int) -> None:
        while True:
            task = self._queue.get()
            if task is None:
                self._queue.task_done()
                break
            try:
                self._process_task(task, worker_id)
            except Exception as exc:
                logger.error(
                    "Drive upload worker #%s failed for run=%s file=%s: %s",
                    worker_id,
                    task.run_id,
                    task.relative_path,
                    exc,
                )
            finally:
                self._queue.task_done()

    def _process_task(self, task: DriveUploadTask, worker_id: int) -> None:
        with self._lock:
            run = self._runs.get(task.run_id)
            if not run:
                return
            entry = run["entries"].get(task.relative_path)
            if not entry or entry.get("status") == "uploaded":
                self._update_completion_state_locked(task.run_id)
                return
            entry["status"] = "uploading"
            entry["last_attempt_at"] = time.time()
            self._write_state_locked(task.run_id)

        absolute_path = Path(task.absolute_path)
        if not absolute_path.exists() or not absolute_path.is_file():
            self._mark_failure(task, f"File not found: {task.absolute_path}")
            return

        try:
            target_folder_id = self._resolve_target_folder(task.run_id, task.drive_subdir)
            file_url = self._ensure_ready().upload_file(str(absolute_path), target_folder_id)
            if not file_url:
                raise RuntimeError("Google Drive upload returned empty link")
            with self._lock:
                run = self._runs.get(task.run_id)
                if not run:
                    return
                entry = run["entries"].setdefault(task.relative_path, {})
                entry["status"] = "uploaded"
                entry["file_url"] = file_url
                entry["uploaded_at"] = time.time()
                entry["last_error"] = ""
                self._write_state_locked(task.run_id)
                self._update_completion_state_locked(task.run_id)
            logger.info(
                "Drive upload worker #%s uploaded %s for run=%s",
                worker_id,
                task.relative_path,
                task.run_id,
            )
        except Exception as exc:
            if self._is_missing_folder_error(exc):
                if self._recover_run_folders(task.run_id):
                    self._requeue_task(task, reset_attempts=True)
                    logger.warning(
                        "Drive upload worker #%s recovered missing folder tree for run=%s and requeued %s",
                        worker_id,
                        task.run_id,
                        task.relative_path,
                    )
                    return
            self._mark_failure(task, str(exc))

    def _is_missing_folder_error(self, error: Exception) -> bool:
        if isinstance(error, DriveFolderMissingError):
            return True
        uploader = self._control_uploader
        if uploader is None:
            return False
        try:
            return uploader._is_not_found_error(error)
        except Exception:
            return False

    def _recover_run_folders(self, run_id: str) -> bool:
        with self._recovery_lock:
            with self._lock:
                run = self._runs.get(run_id)
                if not run:
                    return False
                slug = str(run.get("slug") or "").strip()
                stale_folder_id = str(run.get("folder_id") or "").strip()

            uploader = self._control_uploader
            if uploader is None:
                return False

            if stale_folder_id:
                uploader._invalidate_folder_cache(stale_folder_id)

            folder_id, folder_url = self._ensure_run_folder(slug)

            with self._lock:
                run = self._runs.get(run_id)
                if not run:
                    return False
                run["folder_id"] = folder_id
                run["folder_url"] = folder_url
                run["target_folders"] = {}
                self._write_state_locked(run_id)
            return True

    def _requeue_task(self, task: DriveUploadTask, reset_attempts: bool = False) -> None:
        with self._lock:
            run = self._runs.get(task.run_id)
            if not run:
                return
            entry = run["entries"].setdefault(task.relative_path, {})
            entry["status"] = "queued"
            if reset_attempts:
                entry["attempts"] = 0
                entry["last_error"] = ""
            entry.pop("retry_after", None)
            entry.pop("failed_at", None)
            run["event"].clear()
            self._write_state_locked(task.run_id)
            self._update_completion_state_locked(task.run_id)
        self._queue.put(task)

    def _mark_failure(self, task: DriveUploadTask, error_message: str) -> None:
        should_retry = False
        retry_delay = 0.0
        attempt_number = 0
        with self._lock:
            run = self._runs.get(task.run_id)
            if not run:
                return
            entry = run["entries"].setdefault(task.relative_path, {})
            previous_attempts = max(0, int(entry.get("attempts") or 0))
            attempt_number = previous_attempts + 1
            entry["attempts"] = attempt_number
            entry["last_error"] = error_message[:500]
            entry["status"] = "failed"
            entry["failed_at"] = time.time()
            should_retry = attempt_number < self.per_file_retries and not self._stop_requested
            if should_retry:
                retry_delay = min(20.0, (2 ** (attempt_number - 1)) + random.uniform(0.2, 1.0))
                entry["status"] = "queued"
                entry["retry_after"] = time.time() + retry_delay
            self._write_state_locked(task.run_id)
            self._update_completion_state_locked(task.run_id)

        logger.warning(
            "Drive upload failed for run=%s file=%s attempt=%s/%s: %s",
            task.run_id,
            task.relative_path,
            attempt_number,
            self.per_file_retries,
            error_message,
        )

        if should_retry:
            time.sleep(retry_delay)
            self._queue.put(task)

    def _resolve_target_folder(self, run_id: str, drive_subdir: str) -> str:
        drive_subdir = (drive_subdir or "").strip("/")
        uploader = self._ensure_ready()
        with self._lock:
            run = self._runs.get(run_id)
            if not run:
                raise RuntimeError(f"Google Drive run is not registered: {run_id}")
            run_folder_id = str(run.get("folder_id") or "")
            if run_folder_id and not uploader._folder_exists(run_folder_id):
                raise DriveFolderMissingError(run_folder_id)
            if not drive_subdir:
                return run_folder_id
            cached = run["target_folders"].get(drive_subdir)
            if cached:
                if uploader._folder_exists(cached):
                    return cached
                run["target_folders"].pop(drive_subdir, None)
                self._write_state_locked(run_id)

        root_parent_id = self.folder_id.strip() if self.folder_id else uploader._resolve_root_folder_id()
        current_folder_id = run_folder_id
        path_parts = [part.strip() for part in drive_subdir.split("/") if part.strip()]
        if path_parts and path_parts[0] == "_classified":
            current_folder_id = uploader._resolve_classified_folder_id(root_parent_id)
            path_parts = path_parts[1:]
        current_path_parts: List[str] = []
        for part in path_parts:
            normalized = part.strip()
            current_path_parts.append(normalized)
            cache_key = "/".join(current_path_parts)
            if drive_subdir.startswith("_classified/"):
                cache_key = f"_classified/{cache_key}"
            with self._lock:
                run = self._runs.get(run_id)
                if not run:
                    raise RuntimeError(f"Google Drive run is not registered: {run_id}")
                cached = run["target_folders"].get(cache_key)
                if cached:
                    if not uploader._folder_exists(cached):
                        run["target_folders"].pop(cache_key, None)
                        self._write_state_locked(run_id)
                    else:
                        current_folder_id = cached
                        continue
            current_folder_id = uploader.get_or_create_folder(normalized, current_folder_id)
            with self._lock:
                run = self._runs.get(run_id)
                if not run:
                    raise RuntimeError(f"Google Drive run is not registered: {run_id}")
                run["target_folders"][cache_key] = current_folder_id
        return current_folder_id

    def _load_state_file(self, result_dir: Path) -> Dict[str, Any]:
        state_path = result_dir / UPLOAD_STATE_FILE
        if not state_path.exists():
            return {}
        try:
            return json.loads(state_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Failed to read Google Drive upload state '%s': %s", state_path, exc)
            return {}

    def _write_state_locked(self, run_id: str) -> None:
        run = self._runs.get(run_id)
        if not run:
            return
        state_path = Path(run["result_dir"]) / UPLOAD_STATE_FILE
        state_payload = {
            "run_id": run_id,
            "slug": run["slug"],
            "result_dir": run["result_dir"],
            "folder_id": run.get("folder_id") or "",
            "folder_url": run.get("folder_url") or "",
            "finalized": bool(run.get("finalized")),
            "entries": run.get("entries") or {},
            "updated_at": time.time(),
        }
        tmp_path = state_path.with_suffix(state_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(state_payload, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp_path.replace(state_path)

    def _update_completion_state_locked(self, run_id: str) -> None:
        run = self._runs.get(run_id)
        if not run:
            return
        entries = run.get("entries") or {}
        unfinished = any(entry.get("status") in {"queued", "uploading"} for entry in entries.values())
        retry_pending = any(
            entry.get("status") == "queued" and float(entry.get("retry_after") or 0) > time.time()
            for entry in entries.values()
        )
        if run.get("finalized") and not unfinished and not retry_pending:
            run["event"].set()
        else:
            run["event"].clear()
