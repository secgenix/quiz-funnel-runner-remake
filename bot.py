"""
Telegram бот для Quiz Funnel Runner
Интеграция с aiogram 3.x
"""

import asyncio
import contextlib
import logging
import os
import re
import signal
import shutil
import threading
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Set
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    CallbackQuery,
    FSInputFile,
    InputFile,
    ReplyKeyboardMarkup,
    KeyboardButton,
    BotCommand,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import get_config, init_config
from models import TaskManager, FunnelTask, TaskStatus
from drive_uploader import GoogleDriveUploader, ParallelDriveUploadManager
from google_links_reader import GoogleLinksReader, is_google_url

from runner import get_slug, load_runner_config, run_funnel
from firecrawl_quiz_runner import run_firecrawl_fallback

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import json

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Глобальные объекты
bot: Optional[Bot] = None
dp: Optional[Dispatcher] = None
task_manager: Optional[TaskManager] = None
thread_pool: Optional[ThreadPoolExecutor] = None
drive_upload_executor: Optional[ThreadPoolExecutor] = None
drive_upload_manager: Optional[ParallelDriveUploadManager] = None


class ErrorData:
    """Данные об ошибке для архива"""

    def __init__(
        self,
        url: str,
        error_message: str,
        screenshot_path: Optional[str],
        log_path: Optional[str],
    ):
        self.url = url
        self.error_message = error_message
        self.screenshot_path = screenshot_path
        self.log_path = log_path
        self.domain = self._extract_domain(url)

    def _extract_domain(self, url: str) -> str:
        """Извлекает домен из URL для имени папки"""
        try:
            parsed = urlparse(url)
            return parsed.netloc.replace("www.", "")
        except:
            return "unknown_domain"


class ErrorCollector:
    """Коллектор для сбора ошибок и создания архива"""

    def __init__(self, output_dir: str = "error_reports"):
        self.output_dir = output_dir
        self.errors: List[ErrorData] = []
        self._lock = asyncio.Lock()

    async def add_error(
        self,
        url: str,
        error_message: str,
        screenshot_path: Optional[str],
        log_path: Optional[str],
    ) -> None:
        """Добавляет ошибку в коллектор"""
        async with self._lock:
            error_data = ErrorData(url, error_message, screenshot_path, log_path)
            self.errors.append(error_data)
            logger.info(
                f"Добавлена ошибка в коллектор: {url[:50]}... | {error_message[:50]}..."
            )

    def create_archive(self) -> Optional[str]:
        """
        Создает архив с ошибками.
        Структура: error_reports_<timestamp>.zip
        Внутри: папки с именем домена, в каждой:
          - screenshot.png
          - log.txt
          - url_and_error.txt
        """
        if not self.errors:
            logger.info("Нет ошибок для создания архива")
            return None

        os.makedirs(self.output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_name = f"error_reports_{timestamp}.zip"
        archive_path = os.path.join(self.output_dir, archive_name)

        # Группируем ошибки по доменам
        errors_by_domain: Dict[str, List[ErrorData]] = {}
        for error in self.errors:
            if error.domain not in errors_by_domain:
                errors_by_domain[error.domain] = []
            errors_by_domain[error.domain].append(error)

        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for domain, domain_errors in errors_by_domain.items():
                # Создаем папку для домена
                domain_folder = f"errors_{domain}"

                for idx, error in enumerate(domain_errors):
                    file_prefix = f"{domain_folder}/error_{idx + 1:03d}"

                    # 1. Добавляем скриншот
                    if error.screenshot_path and os.path.exists(error.screenshot_path):
                        try:
                            zipf.write(
                                error.screenshot_path, f"{file_prefix}_screenshot.png"
                            )
                        except Exception as e:
                            logger.error(f"Ошибка добавления скриншота в архив: {e}")

                    # 2. Добавляем лог
                    if error.log_path and os.path.exists(error.log_path):
                        try:
                            zipf.write(error.log_path, f"{file_prefix}_log.txt")
                        except Exception as e:
                            logger.error(f"Ошибка добавления лога в архив: {e}")

                    # 3. Создаем и добавляем файл с URL и описанием ошибки
                    error_info_content = f"URL: {error.url}\n\n"
                    error_info_content += f"Ошибка: {error.error_message}\n"

                    try:
                        zipf.writestr(
                            f"{file_prefix}_url_and_error.txt", error_info_content
                        )
                    except Exception as e:
                        logger.error(
                            f"Ошибка добавления информации об ошибке в архив: {e}"
                        )

        logger.info(
            f"Создан архив с ошибками: {archive_path} ({len(self.errors)} ошибок)"
        )
        return archive_path

    def get_errors_count(self) -> int:
        """Возвращает количество собранных ошибок"""
        return len(self.errors)


# Глобальный коллектор ошибок
error_collector: Optional[ErrorCollector] = None


class DriveUploadJob:
    """Задача фоновой загрузки результатов в Google Drive."""

    def __init__(
        self,
        task_id: int,
        user_id: int,
        slug: str,
        result_dir: str,
        credentials_file: str,
        folder_id: str,
        token_file: str,
        root_folder_name: str,
    ):
        self.task_id = task_id
        self.user_id = user_id
        self.slug = slug
        self.result_dir = result_dir
        self.credentials_file = credentials_file
        self.folder_id = folder_id
        self.token_file = token_file
        self.root_folder_name = root_folder_name


class DriveUploadQueue:
    """Фоновая очередь загрузки артефактов в Google Drive."""

    def __init__(self):
        self.queue: asyncio.Queue[Optional[DriveUploadJob]] = asyncio.Queue()
        self._workers: List[asyncio.Task] = []
        self._inflight: Set[int] = set()
        self._lock = asyncio.Lock()
        self._running = False

    async def start(self, workers: int) -> None:
        if self._running:
            return
        self._running = True
        worker_count = max(1, int(workers))
        self._workers = [
            asyncio.create_task(self._worker_loop(idx + 1))
            for idx in range(worker_count)
        ]
        logger.info(f"☁️ Фоновая очередь Google Drive запущена, workers={worker_count}")

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        for _ in self._workers:
            await self.queue.put(None)
        await self.queue.join()
        for worker in self._workers:
            try:
                await worker
            except asyncio.CancelledError:
                pass
        self._workers.clear()
        logger.info("☁️ Фоновая очередь Google Drive остановлена")

    async def force_stop(self) -> Dict[str, int]:
        """Немедленно прекращает приём новых задач и очищает все ещё не начавшиеся jobs."""
        self._running = False
        cleared_jobs = 0
        while True:
            try:
                queued_job = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            if queued_job is not None:
                cleared_jobs += 1
            self.queue.task_done()

        workers = list(self._workers)
        for _ in workers:
            await self.queue.put(None)
        for worker in workers:
            try:
                await asyncio.wait_for(worker, timeout=5)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                worker.cancel()
        self._workers.clear()
        logger.info(
            "☁️ Фоновая очередь Google Drive принудительно остановлена, cleared_jobs=%s",
            cleared_jobs,
        )
        return {
            "cleared_jobs": cleared_jobs,
            "active_jobs": len(self._inflight),
        }

    async def enqueue(self, job: DriveUploadJob) -> bool:
        async with self._lock:
            if job.task_id in self._inflight:
                logger.info(
                    f"Google Drive upload уже запланирован для task_id={job.task_id}"
                )
                return False
            self._inflight.add(job.task_id)
        await self.queue.put(job)
        logger.info(
            f"Google Drive upload поставлен в фон для task_id={job.task_id}, slug={job.slug}"
        )
        return True

    async def _worker_loop(self, worker_id: int) -> None:
        while True:
            job = await self.queue.get()
            if job is None:
                self.queue.task_done()
                break
            try:
                await self._process_job(job, worker_id)
            except Exception as e:
                logger.error(
                    f"Ошибка фоновой Google Drive загрузки task_id={job.task_id}: {e}"
                )
            finally:
                async with self._lock:
                    self._inflight.discard(job.task_id)
                self.queue.task_done()

    async def _process_job(self, job: DriveUploadJob, worker_id: int) -> None:
        logger.info(
            f"Google Drive worker #{worker_id}: начало загрузки task_id={job.task_id}, slug={job.slug}"
        )
        drive_url = await upload_to_google_drive(
            slug=job.slug,
            result_dir=job.result_dir,
            credentials_file=job.credentials_file,
            folder_id=job.folder_id,
            token_file=job.token_file,
            root_folder_name=job.root_folder_name,
        )
        if not drive_url:
            logger.warning(
                f"Google Drive worker #{worker_id}: загрузка завершилась без ссылки task_id={job.task_id}"
            )
            return

        task = await task_manager.get_task(job.task_id)
        if not task:
            logger.warning(
                f"Google Drive worker #{worker_id}: задача task_id={job.task_id} не найдена в БД"
            )
            return

        await task_manager.complete_task(
            task_id=task.id,
            steps_total=task.steps_total,
            paywall_reached=task.paywall_reached,
            error=task.error,
            screenshot_path=task.screenshot_path,
            log_path=task.log_path,
            manifest_path=task.manifest_path,
            drive_folder_url=drive_url,
            last_url=task.last_url,
        )
        logger.info(
            f"Google Drive worker #{worker_id}: ссылка сохранена для task_id={job.task_id}: {drive_url}"
        )


drive_upload_queue: Optional[DriveUploadQueue] = None


class TaskStopRegistry:
    """Потокобезопасный реестр запросов на остановку запущенных задач."""

    def __init__(self):
        self._events: Dict[int, threading.Event] = {}
        self._user_tasks: Dict[int, Set[int]] = defaultdict(set)
        self._lock = threading.Lock()

    def register(self, task_id: int, user_id: int) -> threading.Event:
        with self._lock:
            event = threading.Event()
            self._events[task_id] = event
            self._user_tasks[user_id].add(task_id)
            return event

    def unregister(self, task_id: int, user_id: int) -> None:
        with self._lock:
            self._events.pop(task_id, None)
            if user_id in self._user_tasks:
                self._user_tasks[user_id].discard(task_id)
                if not self._user_tasks[user_id]:
                    self._user_tasks.pop(user_id, None)

    def request_stop(self, task_id: int) -> bool:
        with self._lock:
            event = self._events.get(task_id)
            if not event:
                return False
            event.set()
            return True

    def request_stop_for_user(self, user_id: int) -> int:
        with self._lock:
            task_ids = list(self._user_tasks.get(user_id, set()))
            for task_id in task_ids:
                event = self._events.get(task_id)
                if event:
                    event.set()
            return len(task_ids)


task_stop_registry = TaskStopRegistry()

# Пакетные уведомления о старте задач (для сообщений с несколькими URL)
start_batch_notifications: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
start_batch_lock = asyncio.Lock()

# Ограничения
MAX_CONCURRENT_TASKS = 3  # Максимум одновременных задач
MAX_QUEUE_PER_USER = 5  # Максимум задач в очереди на пользователя


class FormStates(StatesGroup):
    """Состояния FSM"""

    waiting_for_url = State()


MENU_BUTTONS_MAP = {
    "📊 Статус": "/status",
    "📜 История": "/history",
    "⛔ Стоп все": "/cancel",
    "🧹 Очистить": "/clear",
    "📁 Drive": "/drive",
    "🛑 Drive стоп": "/drive_stop",
    "ℹ️ Помощь": "/help",
}


def build_main_menu() -> ReplyKeyboardMarkup:
    """Компактное главное меню"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Статус"), KeyboardButton(text="📜 История")],
            [KeyboardButton(text="⛔ Стоп все"), KeyboardButton(text="🧹 Очистить")],
            [KeyboardButton(text="📁 Drive"), KeyboardButton(text="🛑 Drive стоп")],
            [KeyboardButton(text="ℹ️ Помощь")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Вставьте URL или выберите действие из меню",
    )


def build_drive_controls():
    """Inline-кнопка принудительной остановки Drive upload."""
    builder = InlineKeyboardBuilder()
    builder.button(
        text="🛑 Остановить Drive upload", callback_data="stop_drive_uploads"
    )
    builder.adjust(1)
    return builder.as_markup()


async def stop_drive_uploads(force_reason: str) -> Dict[str, int]:
    """Принудительно останавливает дальнейшие загрузки в Google Drive и очищает очередь."""
    result = {
        "drive_queue_cleared": 0,
        "drive_queue_active": 0,
        "manager_cleared": 0,
        "manager_entries_cancelled": 0,
        "manager_active": 0,
        "runs_affected": 0,
    }

    if drive_upload_queue:
        stats = await drive_upload_queue.force_stop()
        result["drive_queue_cleared"] = int(stats.get("cleared_jobs", 0))
        result["drive_queue_active"] = int(stats.get("active_jobs", 0))

    if drive_upload_manager:
        stats = drive_upload_manager.cancel_pending(reason=force_reason)
        result["manager_cleared"] = int(stats.get("cleared_tasks", 0))
        result["manager_entries_cancelled"] = int(
            stats.get("queued_entries_cancelled", 0)
        )
        result["manager_active"] = int(stats.get("active_uploads", 0))
        result["runs_affected"] = int(stats.get("runs_affected", 0))

    return result


def build_active_task_controls(tasks: List[FunnelTask]):
    """Inline-кнопки для остановки активных задач."""
    active_tasks = [
        t for t in tasks if t.status in (TaskStatus.PENDING, TaskStatus.PROCESSING)
    ]
    if not active_tasks:
        return None

    builder = InlineKeyboardBuilder()
    if len(active_tasks) > 1:
        builder.button(text="⛔ Остановить все активные", callback_data="stop_all")

    for task in active_tasks[:8]:
        label = f"⛔ Стоп #{task.id}"
        if getattr(task, "stop_requested", False):
            label = f"🛑 Ожидает #{task.id}"
        builder.button(text=label, callback_data=f"stop_task_{task.id}")

    builder.adjust(1)
    return builder.as_markup()


def short_url(url: str, max_len: int = 60) -> str:
    """Безопасно сокращает URL для компактного вывода"""
    if not url:
        return "-"
    return url if len(url) <= max_len else f"{url[: max_len - 1]}…"


def normalize_url_for_compare(url: Optional[str]) -> str:
    """Нормализует URL для сравнения без лишнего шума."""
    if not url:
        return ""
    normalized = str(url).strip()
    if not normalized:
        return ""
    return normalized.rstrip("/")


def should_show_resume_url(
    start_url: Optional[str], current_url: Optional[str]
) -> bool:
    """Показывает resume URL только если он действительно отличается от стартового."""
    start_norm = normalize_url_for_compare(start_url)
    current_norm = normalize_url_for_compare(current_url)
    return bool(current_norm and current_norm != start_norm)


def format_error_summary(error: Optional[str]) -> str:
    """Краткая, человеко-понятная сводка ошибки без технического шума."""
    raw = (error or "").strip()
    if not raw:
        return "Ошибка выполнения"

    mappings = {
        "navigation_timeout": "Страница не открылась вовремя",
        "stuck_loop": "Сценарий зациклился",
        "Task stuck, cleared by user": "Задача была сброшена вручную",
    }
    if raw in mappings:
        return mappings[raw]

    if raw.startswith("runner_exception:"):
        details = raw.split(":", 1)[1].strip()
        return f"Сбой раннера: {details}" if details else "Сбой раннера"

    if raw.startswith("firecrawl_exception:"):
        details = raw.split(":", 1)[1].strip()
        return f"Сбой Firecrawl: {details}" if details else "Сбой Firecrawl"

    firecrawl_mappings = {
        "firecrawl_api_key_missing": "Firecrawl не настроен",
        "firecrawl_unknown_state": "Firecrawl не смог безопасно продолжить сценарий",
        "firecrawl_max_steps_reached": "Firecrawl дошел до лимита шагов",
    }
    if raw in firecrawl_mappings:
        return firecrawl_mappings[raw]

    compact = raw.replace("_", " ")
    return compact[:160]


def build_result_lines(
    task: FunnelTask, *, is_success: bool, resume_url: Optional[str] = None
) -> List[str]:
    """Единый компактный формат итоговых сообщений."""
    lines: List[str] = []

    if is_success:
        lines.append("✅ <b>Успех</b>")
        if task.paywall_reached:
            lines.append("💳 Paywall найден")
        else:
            lines.append("📄 Сценарий завершен")
    else:
        lines.append("❌ <b>Ошибка</b>")
        lines.append(f"⚠️ {format_error_summary(task.error)}")

    lines.append(f"#️⃣ <b>#{task.id}</b>")
    lines.append(f"🔗 <code>{short_url(task.url, 85)}</code>")

    meta_parts: List[str] = []
    if task.steps_total:
        meta_parts.append(f"шагов: {task.steps_total}")
    if task.completed_at:
        duration = task.completed_at - (task.started_at or task.created_at)
        meta_parts.append(f"{duration.total_seconds():.1f} сек")
    if meta_parts:
        lines.append(f"⏱ {' • '.join(meta_parts)}")

    if resume_url:
        lines.append(f"↩️ <a href='{resume_url}'>Продолжить с текущего места</a>")

    if task.drive_folder_url:
        lines.append(f"📁 <a href='{task.drive_folder_url}'>Drive</a>")

    if is_success:
        lines.append("📎 Скриншот: финальный экран")

    return lines


def build_result_text(
    task: FunnelTask, *, is_success: bool, resume_url: Optional[str] = None
) -> str:
    return "\n".join(
        build_result_lines(task, is_success=is_success, resume_url=resume_url)
    )


def find_paywall_screenshot(task: FunnelTask) -> Optional[str]:
    """Ищет скриншот paywall/checkout для успешной задачи"""
    candidates: List[Path] = []

    # Пробуем директорию из manifest_path
    if getattr(task, "manifest_path", None):
        manifest_dir = Path(task.manifest_path).parent
        if manifest_dir.exists():
            candidates.extend(manifest_dir.glob("*_paywall.png"))
            candidates.extend(manifest_dir.glob("*_checkout.png"))

    # Пробуем директорию из screenshot_path
    if task.screenshot_path:
        screenshot_dir = Path(task.screenshot_path).parent
        if screenshot_dir.exists():
            candidates.extend(screenshot_dir.glob("*_paywall.png"))
            candidates.extend(screenshot_dir.glob("*_checkout.png"))

    # Удаляем дубликаты и выбираем последний по имени (max step)
    unique_candidates = sorted({str(p) for p in candidates})
    if unique_candidates:
        return unique_candidates[-1]

    return None


async def send_firecrawl_screenshot_links(
    bot: Bot, user_id: int, screenshot_urls: List[str]
) -> None:
    unique_urls: List[str] = []
    seen: Set[str] = set()
    for url in screenshot_urls:
        normalized = str(url or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique_urls.append(normalized)

    if not unique_urls:
        return

    lines = ["🖼 <b>Firecrawl screenshots</b>"]
    for index, screenshot_url in enumerate(unique_urls[:10], start=1):
        lines.append(f'{index}. <a href="{screenshot_url}">screenshot {index}</a>')

    try:
        await bot.send_message(
            user_id,
            "\n".join(lines),
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=build_main_menu(),
        )
    except Exception as e:
        logger.error(f"Ошибка отправки Firecrawl screenshot links: {e}")


async def register_start_batch(user_id: int, total_tasks: int) -> None:
    """Регистрирует пакет задач для единого стартового уведомления"""
    if total_tasks <= 1:
        return
    async with start_batch_lock:
        start_batch_notifications[user_id].append(
            {
                "remaining": total_tasks,
                "total": total_tasks,
                "notified": False,
            }
        )


async def consume_start_notification_policy(user_id: int) -> Dict[str, Any]:
    """Возвращает политику отправки стартового уведомления для очередной задачи пользователя"""
    async with start_batch_lock:
        batches = start_batch_notifications.get(user_id)
        if not batches:
            return {"suppress_individual": False, "summary_count": 0}

        current = batches[0]
        summary_count = 0
        if not current["notified"]:
            current["notified"] = True
            summary_count = int(current["total"])

        current["remaining"] = int(current["remaining"]) - 1
        if current["remaining"] <= 0:
            batches.pop(0)
            if not batches:
                start_batch_notifications.pop(user_id, None)

        return {"suppress_individual": True, "summary_count": summary_count}


def is_valid_url(url: str) -> bool:
    """Проверка валидности URL"""
    if not url or not isinstance(url, str):
        return False

    # Быстрая проверка начала URL
    url = url.strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        return False

    # Более гибкий regex для URL с параметрами и спецсимволами
    pattern = re.compile(
        r"^https?://"  # http:// или https://
        r"(?:[^\s<>\"{}|\\^`\[\]]+)",  # домен и путь
        re.IGNORECASE,
    )

    # Проверяем наличие домена
    try:
        from urllib.parse import urlparse

        parsed = urlparse(url)
        if not parsed.netloc:
            return False
        return pattern.match(url) is not None
    except Exception:
        return False


def check_user_access(user_id: int) -> bool:
    """Проверка доступа пользователя"""
    cfg = get_config()
    if user_id in cfg.bot.admin_ids:
        return True

    if cfg.bot.use_only_admin:
        return False

    return True


async def get_task_status_text(task: FunnelTask) -> str:
    """Форматирование статуса задачи для вывода"""
    status_emoji = {
        TaskStatus.PENDING: "⏳",
        TaskStatus.PROCESSING: "🔄",
        TaskStatus.COMPLETED: "✅",
        TaskStatus.FAILED: "❌",
        TaskStatus.CANCELLED: "⛔",
    }

    status_names = {
        TaskStatus.PENDING: "В очереди",
        TaskStatus.PROCESSING: "Выполняется",
        TaskStatus.COMPLETED: "Завершено",
        TaskStatus.FAILED: "Ошибка",
        TaskStatus.CANCELLED: "Отменено",
    }

    text = f"{status_emoji.get(task.status, '❓')} <b>#{task.id}</b> • {status_names.get(task.status, 'Неизвестно')}\n"
    text += f"🔗 <code>{short_url(task.url)}</code>\n"

    if task.status == TaskStatus.PROCESSING:
        text += f"🔄 Шаг: {task.current_step}/{task.steps_total}\n"
        if task.progress_message:
            text += f"<i>{short_url(task.progress_message, 80)}</i>\n"
        if getattr(task, "stop_requested", False):
            text += "🛑 Остановка уже запрошена\n"

    if task.status == TaskStatus.CANCELLED:
        if task.current_step:
            text += f"🧭 Выполнено шагов: {task.current_step}\n"
        if task.progress_message:
            text += f"<i>{short_url(task.progress_message, 80)}</i>\n"

    if task.status == TaskStatus.COMPLETED:
        text += f"🧭 Шагов: {task.steps_total}\n"
        text += f"💳 Paywall: {'✅' if task.paywall_reached else '❌'}\n"

        if task.drive_folder_url:
            text += f"📁 <a href='{task.drive_folder_url}'>Google Drive</a>\n"

    if task.error:
        text += f"⚠️ <code>{task.error[:120]}</code>\n"

    if task.completed_at:
        duration = task.completed_at - (task.started_at or task.created_at)
        text += f"⏱ {duration.total_seconds():.1f} сек\n"

    return text


async def notify_task_start(bot: Bot, user_id: int, task: FunnelTask) -> None:
    """Уведомление о начале обработки задачи"""
    try:
        await bot.send_message(
            user_id,
            f"🚀 Запущена задача <b>#{task.id}</b>\n"
            f"🔗 <code>{short_url(task.url)}</code>",
            parse_mode="HTML",
            reply_markup=build_active_task_controls([task]) or build_main_menu(),
        )
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления о старте: {e}")


async def notify_batch_start(bot: Bot, user_id: int, total_tasks: int) -> None:
    """Единое уведомление о старте пакета задач"""
    try:
        await bot.send_message(
            user_id,
            f"🚀 <b>Запуск</b> • задач: {total_tasks}",
            parse_mode="HTML",
            reply_markup=build_main_menu(),
        )
    except Exception as e:
        logger.error(f"Ошибка отправки пакетного уведомления о старте: {e}")


async def notify_task_progress(bot: Bot, user_id: int, task: FunnelTask) -> None:
    """Уведомление о прогрессе задачи"""
    try:
        # Отправляем прогресс реже, чтобы чат оставался компактным
        if (
            task.current_step in (1, 10, 20, 30, 40, 50, 60, 70, 80)
            or task.current_step == task.steps_total
        ):
            await bot.send_message(
                user_id,
                f"🔄 <b>#{task.id}</b> • {task.current_step}/{task.steps_total}\n"
                f"{short_url(task.progress_message or 'Выполняется…', 90)}",
                parse_mode="HTML",
                reply_markup=build_main_menu(),
            )
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления о прогрессе: {e}")


async def notify_task_complete(bot: Bot, user_id: int, task: FunnelTask) -> None:
    """Уведомление о завершении задачи"""
    try:
        text = build_result_text(task, is_success=True)

        # Для успешного прохождения отправляем именно paywall/checkout скриншот (если найден)
        screenshot_to_send = task.screenshot_path
        if task.paywall_reached:
            paywall_screenshot = find_paywall_screenshot(task)
            if paywall_screenshot:
                screenshot_to_send = paywall_screenshot

        if screenshot_to_send and os.path.exists(screenshot_to_send):
            try:
                photo = FSInputFile(screenshot_to_send)
                await bot.send_photo(
                    user_id,
                    photo,
                    caption=text,
                    parse_mode="HTML",
                    reply_markup=build_main_menu(),
                )
            except Exception:
                await bot.send_message(
                    user_id, text, parse_mode="HTML", reply_markup=build_main_menu()
                )
        else:
            await bot.send_message(
                user_id, text, parse_mode="HTML", reply_markup=build_main_menu()
            )

    except Exception as e:
        logger.error(f"Ошибка отправки уведомления о завершении: {e}")


async def notify_task_error(
    bot: Bot, user_id: int, task: FunnelTask, error: str
) -> None:
    """Компактное уведомление об ошибке + сохранение в коллектор."""
    try:
        logger.warning(f"Ошибка задачи #{task.id}: {error[:200]}")

        resume_url: Optional[str] = None
        task_last_url = getattr(task, "last_url", None)
        if should_show_resume_url(task.url, task_last_url):
            resume_url = task_last_url

        text = build_result_text(task, is_success=False, resume_url=resume_url)

        if task.screenshot_path and os.path.exists(task.screenshot_path):
            try:
                photo = FSInputFile(task.screenshot_path)
                await bot.send_photo(
                    user_id,
                    photo,
                    caption=text,
                    parse_mode="HTML",
                    reply_markup=build_main_menu(),
                )
            except Exception:
                await bot.send_message(
                    user_id, text, parse_mode="HTML", reply_markup=build_main_menu()
                )
        else:
            await bot.send_message(
                user_id, text, parse_mode="HTML", reply_markup=build_main_menu()
            )

        # Сохраняем в коллектор ошибок
        if error_collector:
            await error_collector.add_error(
                url=task.url,
                error_message=error,
                screenshot_path=task.screenshot_path,
                log_path=task.log_path,
            )
    except Exception as e:
        logger.error(f"Ошибка при сохранении ошибки в коллектор: {e}")


async def notify_task_cancelled(bot: Bot, user_id: int, task: FunnelTask) -> None:
    """Уведомление о корректной остановке задачи с частичным результатом."""
    try:
        resume_url: Optional[str] = None
        task_last_url = getattr(task, "last_url", None)
        if should_show_resume_url(task.url, task_last_url):
            resume_url = task_last_url

        lines = [
            "⛔ <b>Остановлено</b>",
            f"#️⃣ <b>#{task.id}</b>",
            f"🔗 <code>{short_url(task.url, 85)}</code>",
        ]
        meta_parts: List[str] = []
        if task.current_step:
            meta_parts.append(f"выполнено шагов: {task.current_step}")
        if task.completed_at:
            duration = task.completed_at - (task.started_at or task.created_at)
            meta_parts.append(f"{duration.total_seconds():.1f} сек")
        if meta_parts:
            lines.append(f"⏱ {' • '.join(meta_parts)}")
        if task.progress_message:
            lines.append(f"ℹ️ {task.progress_message}")
        if resume_url:
            lines.append(f"↩️ <a href='{resume_url}'>Продолжить с текущего места</a>")
        if task.drive_folder_url:
            lines.append(f"📁 <a href='{task.drive_folder_url}'>Drive</a>")

        text = "\n".join(lines)

        if task.screenshot_path and os.path.exists(task.screenshot_path):
            try:
                photo = FSInputFile(task.screenshot_path)
                await bot.send_photo(
                    user_id,
                    photo,
                    caption=text,
                    parse_mode="HTML",
                    reply_markup=build_main_menu(),
                )
                return
            except Exception:
                pass

        await bot.send_message(
            user_id, text, parse_mode="HTML", reply_markup=build_main_menu()
        )
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления об остановке: {e}")


# ====================
# Обработчики команд
# ====================


async def cmd_start(message: Message, state: FSMContext) -> None:
    """Обработка команды /start"""
    user_id = message.from_user.id

    if not check_user_access(user_id):
        await message.answer(
            "❌ <b>Доступ запрещен</b>\n\n"
            "У вас нет прав для использования этого бота. "
            "Обратитесь к администратору.",
            parse_mode="HTML",
        )
        return

    await state.clear()
    await message.answer(
        "👋 <b>Quiz Funnel Runner</b>\n"
        "Отправьте URL (или несколько URL с новой строки).\n"
        "Команды доступны в меню ниже.",
        parse_mode="HTML",
        reply_markup=build_main_menu(),
    )
    await state.set_state(FormStates.waiting_for_url)


async def cmd_status(message: Message) -> None:
    """Обработка команды /status"""
    user_id = message.from_user.id

    if not check_user_access(user_id):
        await message.answer("❌ Доступ запрещен")
        return

    # Получаем последнюю активную задачу
    tasks = await task_manager.get_user_tasks(user_id, limit=1)
    if not tasks:
        await message.answer(
            "📭 Задач пока нет. Отправьте URL.", reply_markup=build_main_menu()
        )
        return

    task = tasks[0]
    text = await get_task_status_text(task)
    active_tasks = await task_manager.get_tasks_by_statuses(
        user_id,
        [TaskStatus.PENDING, TaskStatus.PROCESSING],
        limit=20,
    )
    await message.answer(
        text,
        parse_mode="HTML",
        reply_markup=build_active_task_controls(active_tasks) or build_main_menu(),
    )


async def cmd_history(message: Message) -> None:
    """Обработка команды /history"""
    user_id = message.from_user.id

    if not check_user_access(user_id):
        await message.answer("❌ Доступ запрещен")
        return

    tasks = await task_manager.get_user_tasks(user_id, limit=10)
    if not tasks:
        await message.answer("📭 История пуста.", reply_markup=build_main_menu())
        return

    text = "📜 <b>Последние задачи:</b>\n"
    for i, task in enumerate(tasks, 1):
        status_emoji = {
            TaskStatus.PENDING: "⏳",
            TaskStatus.PROCESSING: "🔄",
            TaskStatus.COMPLETED: "✅",
            TaskStatus.FAILED: "❌",
            TaskStatus.CANCELLED: "⛔",
        }
        progress_suffix = ""
        if task.status == TaskStatus.PROCESSING and task.current_step:
            progress_suffix = f" • {task.current_step}/{task.steps_total}"
        elif task.status == TaskStatus.CANCELLED and task.current_step:
            progress_suffix = f" • остановлено на {task.current_step}"
        if getattr(task, "stop_requested", False):
            progress_suffix += " • 🛑 запрос остановки"
        text += f"{i}) {status_emoji.get(task.status, '❓')} #{task.id} • <code>{short_url(task.url, 42)}</code>{progress_suffix}\n"

    await message.answer(
        text,
        parse_mode="HTML",
        reply_markup=build_active_task_controls(tasks) or build_main_menu(),
    )


async def cmd_cancel(message: Message) -> None:
    """Обработка команды /cancel"""
    user_id = message.from_user.id

    if not check_user_access(user_id):
        await message.answer("❌ Доступ запрещен")
        return

    active_tasks = await task_manager.get_tasks_by_statuses(
        user_id,
        [TaskStatus.PENDING, TaskStatus.PROCESSING],
        limit=100,
    )
    if not active_tasks:
        await message.answer(
            "📭 Нет активных задач для остановки.", reply_markup=build_main_menu()
        )
        return

    stopped_ids = await task_manager.request_stop_for_user_active_tasks(user_id)
    task_stop_registry.request_stop_for_user(user_id)

    if stopped_ids:
        await message.answer(
            f"⛔ Запрошена остановка задач: <b>{len(stopped_ids)}</b>\n"
            "Выполняющиеся задачи завершат текущий безопасный шаг, сохранят результат и перейдут в историю.",
            parse_mode="HTML",
            reply_markup=build_main_menu(),
        )
    else:
        await message.answer(
            "❌ Не удалось остановить активные задачи.", reply_markup=build_main_menu()
        )


async def cmd_clear(message: Message) -> None:
    """Обработка команды /clear - сброс зависших задач"""
    user_id = message.from_user.id

    if not check_user_access(user_id):
        await message.answer("❌ Доступ запрещен", parse_mode="HTML")
        return

    # Получаем все задачи пользователя в статусе processing
    all_tasks = await task_manager.get_user_tasks(user_id, limit=100)
    stuck_tasks = [t for t in all_tasks if t.status == TaskStatus.PROCESSING]

    if not stuck_tasks:
        await message.answer("✅ Нет зависших задач.")
        return

    # Сбрасываем статус на failed
    cleared_count = 0
    for task in stuck_tasks:
        await task_manager.update_status(task.id, TaskStatus.FAILED)
        await task_manager.complete_task(
            task_id=task.id,
            steps_total=0,
            paywall_reached=False,
            error="Task stuck, cleared by user",
        )
        cleared_count += 1

    await message.answer(
        f"✅ Очищено зависших задач: <b>{cleared_count}</b>",
        parse_mode="HTML",
        reply_markup=build_main_menu(),
    )


async def cmd_drive(message: Message) -> None:
    """Обработка команды /drive - получение ссылки на Google Drive"""
    user_id = message.from_user.id

    if not check_user_access(user_id):
        await message.answer("❌ Доступ запрещен", parse_mode="HTML")
        return

    # Получаем последнюю завершенную задачу
    tasks = await task_manager.get_user_tasks(user_id, limit=10)
    if not tasks:
        await message.answer("📭 У вас пока нет задач.")
        return

    # Ищем задачу с Google Drive ссылкой
    for task in tasks:
        if task.status == TaskStatus.COMPLETED and task.drive_folder_url:
            await message.answer(
                f"📁 <b>Google Drive</b>\n"
                f"🔗 <code>{short_url(task.url)}</code>\n"
                f"🧭 Шагов: {task.steps_total} • Paywall: {'✅' if task.paywall_reached else '❌'}\n\n"
                f"🔗 <a href='{task.drive_folder_url}'>Открыть папку в Google Drive</a>",
                parse_mode="HTML",
                reply_markup=build_drive_controls(),
            )
            return

    await message.answer(
        "📭 Нет задач с загруженными результатами в Google Drive.\n\n"
        "Проверьте, что интеграция Google Drive включена в config.json.",
        reply_markup=build_main_menu(),
    )


async def cmd_help(message: Message) -> None:
    """Обработка команды /help"""
    await message.answer(
        "ℹ️ <b>Команды:</b>\n"
        "/status • /history • /cancel • /clear • /drive • /drive_stop • /help\n\n"
        "⛔ /cancel — мягко остановить все активные задачи с сохранением промежуточного результата.\n"
        "🛑 /drive_stop — принудительно остановить загрузку файлов в Google Drive и очистить незагруженную очередь.\n"
        " В истории доступны кнопки точечной остановки отдельных задач.\n\n"
        "📝 Отправьте URL (или список URL) для запуска задач.",
        parse_mode="HTML",
        reply_markup=build_main_menu(),
    )


async def cmd_drive_stop(message: Message) -> None:
    """Принудительная остановка загрузок в Google Drive и очистка очереди."""
    user_id = message.from_user.id

    if not check_user_access(user_id):
        await message.answer("❌ Доступ запрещен", parse_mode="HTML")
        return

    if not drive_upload_manager and not drive_upload_queue:
        await message.answer(
            "ℹ️ Google Drive загрузчик не запущен.",
            reply_markup=build_main_menu(),
        )
        return

    stats = await stop_drive_uploads("Drive uploads stopped by user")
    active_hint = max(
        int(stats.get("drive_queue_active", 0)), int(stats.get("manager_active", 0))
    )
    await message.answer(
        "🛑 <b>Google Drive upload остановлен</b>\n"
        f"🧹 Удалено из внутренних очередей: <b>{int(stats.get('drive_queue_cleared', 0)) + int(stats.get('manager_cleared', 0))}</b>\n"
        f"📄 Отменено незагруженных записей: <b>{int(stats.get('manager_entries_cancelled', 0))}</b>\n"
        f"📁 Затронуто сессий: <b>{int(stats.get('runs_affected', 0))}</b>\n"
        + (
            f"⏳ Уже начатых загрузок сейчас выполняется: <b>{active_hint}</b>\n"
            if active_hint
            else ""
        )
        + "Новые файлы в Drive в этой сессии больше не будут ставиться в очередь.",
        parse_mode="HTML",
        reply_markup=build_main_menu(),
    )


async def handle_menu_buttons(message: Message, state: FSMContext) -> None:
    """Обработка кнопок меню как алиасов команд"""
    if not message.text:
        return
    command = MENU_BUTTONS_MAP.get(message.text.strip())
    if not command:
        return

    if command == "/status":
        await cmd_status(message)
    elif command == "/history":
        await cmd_history(message)
    elif command == "/cancel":
        await cmd_cancel(message)
    elif command == "/clear":
        await cmd_clear(message)
    elif command == "/drive":
        await cmd_drive(message)
    elif command == "/drive_stop":
        await cmd_drive_stop(message)
    elif command == "/help":
        await cmd_help(message)


# ====================
# Обработчики сообщений
# ====================


async def handle_url_message(message: Message, state: FSMContext) -> None:
    """Обработка URL от пользователя"""
    user_id = message.from_user.id

    if not check_user_access(user_id):
        await message.answer("❌ Доступ запрещен", parse_mode="HTML")
        return

    # Проверяем текущее состояние
    current_state = await state.get_state()
    if current_state != FormStates.waiting_for_url:
        await state.set_state(FormStates.waiting_for_url)

    text = message.text.strip()

    # Проверяем, это Google Sheet/Doc или список URL
    urls = []
    is_google_link = False

    # Проверяем каждый URL в сообщении
    input_urls = [u.strip() for u in text.split("\n") if u.strip()]

    for url in input_urls:
        if is_google_url(url):
            is_google_link = True
            # Читаем URL из Google Sheet/Doc
            cfg = get_config()
            reader = GoogleLinksReader(cfg.google_drive.credentials_file)

            await message.answer(
                f"🔄 <b>Чтение URL из Google документа...</b>\n\n"
                f"<code>{url[:60]}</code>",
                parse_mode="HTML",
                reply_markup=build_main_menu(),
            )

            # Определяем тип документа и читаем URL
            if reader.is_google_sheet_url(url):
                sheet_urls = reader.read_urls_from_sheet(url)
                urls.extend(sheet_urls)
                await message.answer(
                    f"✅ <b>Прочитано {len(sheet_urls)} URL из Google Sheets</b>",
                    parse_mode="HTML",
                    reply_markup=build_main_menu(),
                )
            elif reader.is_google_doc_url(url):
                doc_urls = reader.read_urls_from_doc(url)
                urls.extend(doc_urls)
                await message.answer(
                    f"✅ <b>Прочитано {len(doc_urls)} URL из Google Docs</b>",
                    parse_mode="HTML",
                    reply_markup=build_main_menu(),
                )
        else:
            urls.append(url)

    # Если это не Google документ, используем исходные URL
    if not is_google_link:
        urls = input_urls

    # Валидация URL
    invalid_urls = []
    for url in urls:
        if not is_valid_url(url):
            invalid_urls.append(url)

    if invalid_urls:
        await message.answer(
            f"❌ <b>Некорректные URL:</b>\n"
            + "\n".join(f"<code>{u}</code>" for u in invalid_urls[:5])
            + "\n\nПожалуйста, проверьте формат URL.",
            parse_mode="HTML",
            reply_markup=build_main_menu(),
        )
        return

    if not urls:
        await message.answer(
            "❌ <b>Не найдено URL для обработки</b>\n\n"
            "Убедитесь, что документ содержит URL (начинаются с http:// или https://)",
            parse_mode="HTML",
            reply_markup=build_main_menu(),
        )
        return

    # Сохраняем все URL в очередь
    total_urls = len(urls)
    added_count = await task_manager.add_urls_to_queue(user_id, urls)

    # Если пользователь отправил больше одной ссылки — при старте задач будет одно общее сообщение
    if total_urls > 1 and added_count > 0:
        await register_start_batch(user_id, added_count)

    # Создаем задачи только для доступных слотов
    user_tasks = await task_manager.get_user_tasks(
        user_id, limit=MAX_QUEUE_PER_USER + 1
    )
    pending_count = sum(1 for t in user_tasks if t.status == TaskStatus.PENDING)
    active_count = await task_manager.get_active_task_count()
    available_slots = MAX_CONCURRENT_TASKS - active_count
    max_pending = MAX_QUEUE_PER_USER - pending_count

    # Создаем задачи в доступных слотах
    urls_to_create = min(available_slots, max_pending, added_count)

    created_tasks = []
    if urls_to_create > 0:
        # Получаем URL из очереди
        urls_to_process = await task_manager.pop_queued_urls(user_id, urls_to_create)
        for url in urls_to_process:
            task = await task_manager.create_task(user_id, url)
            created_tasks.append(task)

        # Уведомляем процессор очереди о создании задач
        await queue_processor.increment_tasks_created(len(created_tasks))

    # Отправляем только компактную сводку по количеству сформированных задач
    await message.answer(
        f"✅ Сформировано задач: <b>{len(created_tasks)}</b>\n"
        f"⏳ В очереди: {max(0, total_urls - len(created_tasks))}",
        parse_mode="HTML",
        reply_markup=build_main_menu(),
    )

    # Запускаем процессор очереди (если еще не запущен)
    await queue_processor.start()


async def handle_callback_query(callback: CallbackQuery) -> None:
    """Обработка callback query"""
    data = callback.data

    if data == "stop_all":
        user_id = callback.from_user.id

        if not check_user_access(user_id):
            await callback.answer("❌ Доступ запрещен", show_alert=True)
            return

        stopped_ids = await task_manager.request_stop_for_user_active_tasks(user_id)
        task_stop_registry.request_stop_for_user(user_id)
        if stopped_ids:
            await callback.answer(
                f"⛔ Остановка запрошена для {len(stopped_ids)} задач"
            )
        else:
            await callback.answer("ℹ️ Активных задач не найдено", show_alert=True)
        return

    if data.startswith("stop_task_"):
        task_id = int(data.split("_")[2])
        user_id = callback.from_user.id

        if not check_user_access(user_id):
            await callback.answer("❌ Доступ запрещен", show_alert=True)
            return

        task = await task_manager.get_task(task_id)
        if not task or task.user_id != user_id:
            await callback.answer("❌ Задача не найдена", show_alert=True)
            return

        stopped = await task_manager.request_stop(task_id)
        task_stop_registry.request_stop(task_id)
        if stopped:
            await callback.answer(f"⛔ Остановка задачи #{task_id} запрошена")
        else:
            await callback.answer("ℹ️ Задача уже завершена", show_alert=True)

    if data == "stop_drive_uploads":
        user_id = callback.from_user.id

        if not check_user_access(user_id):
            await callback.answer("❌ Доступ запрещен", show_alert=True)
            return

        stats = await stop_drive_uploads("Drive uploads stopped from Telegram callback")
        await callback.answer(
            f"🛑 Drive queue cleared: {int(stats.get('drive_queue_cleared', 0)) + int(stats.get('manager_cleared', 0))}",
            show_alert=True,
        )
        return


# ====================
# Google Drive загрузка
# ====================


def upload_to_google_drive_sync(
    slug: str,
    result_dir: str,
    credentials_file: str,
    folder_id: str,
    token_file: str = "token.json",
    root_folder_name: str = "",
) -> Optional[str]:
    """
    Загрузка результатов воронки в Google Drive

    Args:
        slug: Уникальный идентификатор воронки
        result_dir: Локальная директория с результатами
        credentials_file: Путь к файлу учетных данных
        folder_id: ID папки в Google Drive

    Returns:
        Ссылка на папку в Google Drive или None
    """
    try:
        if not os.path.exists(credentials_file):
            logger.error(f"Файл учетных данных не найден: {credentials_file}")
            return None

        uploader = GoogleDriveUploader(
            credentials_file=credentials_file,
            folder_id=folder_id,
            token_file=token_file,
            root_folder_name=root_folder_name,
        )

        if not uploader.service:
            logger.error("Не удалось инициализировать Google Drive сервис")
            return None

        logger.info(f"Загрузка результатов воронки {slug} в Google Drive...")
        drive_url = uploader.upload_funnel_results(slug, result_dir)

        if drive_url:
            logger.info(f"Результаты загружены в Google Drive: {drive_url}")
        else:
            logger.warning("Не удалось загрузить результаты в Google Drive")

        return drive_url

    except Exception as e:
        logger.error(f"Ошибка загрузки в Google Drive: {e}")
        return None


async def upload_to_google_drive(
    slug: str,
    result_dir: str,
    credentials_file: str,
    folder_id: str,
    token_file: str = "token.json",
    root_folder_name: str = "",
) -> Optional[str]:
    loop = asyncio.get_running_loop()
    executor = drive_upload_executor or thread_pool
    return await loop.run_in_executor(
        executor,
        upload_to_google_drive_sync,
        slug,
        result_dir,
        credentials_file,
        folder_id,
        token_file,
        root_folder_name,
    )


# ====================
# Очередь задач
# ====================


class TaskQueueProcessor:
    """Фоновый процессор очереди задач"""

    def __init__(self):
        self.is_running = False
        self.task = None
        self._total_tasks_created = 0
        self._total_tasks_completed = 0
        self._lock = asyncio.Lock()
        self._active_tasks: Set[asyncio.Task] = set()

    async def increment_tasks_created(self, count: int = 1) -> None:
        """Увеличивает счетчик созданных задач"""
        async with self._lock:
            self._total_tasks_created += count

    async def increment_tasks_completed(self, count: int = 1) -> None:
        """Увеличивает счетчик завершенных задач"""
        async with self._lock:
            self._total_tasks_completed += count
            # Проверяем, завершены ли все задачи
            if (
                self._total_tasks_completed >= self._total_tasks_created
                and self._total_tasks_created > 0
            ):
                await self._create_final_error_archive()

    async def _create_final_error_archive(self) -> None:
        """Создает финальный архив с ошибками после завершения всех задач"""
        if error_collector and error_collector.get_errors_count() > 0:
            try:
                archive_path = error_collector.create_archive()
                if archive_path:
                    logger.info(f"📦 Архив с ошибками создан: {archive_path}")
                    # Отправляем архив первому пользователю из списка задач
                    await self._send_archive_to_user(archive_path)
            except Exception as e:
                logger.error(f"Ошибка создания финального архива: {e}")

    async def _send_archive_to_user(self, archive_path: str) -> None:
        """Отправляет архив пользователю"""
        try:
            # Получаем всех пользователей с завершенными задачами
            all_tasks = await task_manager.get_all_tasks(limit=100)
            if all_tasks:
                # Отправляем первому пользователю
                user_id = all_tasks[0].user_id
                try:
                    photo = FSInputFile(archive_path)
                    await bot.send_document(user_id, photo)
                    logger.info(f"Архив отправлен пользователю {user_id}")
                except Exception as e:
                    logger.error(f"Ошибка отправки архива пользователю: {e}")
                    logger.info(f"Архив доступен по пути: {archive_path}")
        except Exception as e:
            logger.error(f"Ошибка при отправке архива: {e}")
            logger.info(f"Архив доступен по пути: {archive_path}")

    async def start(self):
        """Запуск процессора очереди"""
        if self.is_running:
            return

        self.is_running = True
        self.task = asyncio.create_task(self._process_queue_loop())
        logger.info("🔄 Процессор очереди запущен")

    async def stop(self):
        """Остановка процессора очереди"""
        self.is_running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 Процессор очереди остановлен")

    def _track_active_task(self, task: asyncio.Task) -> None:
        self._active_tasks.add(task)

        def _on_done(done_task: asyncio.Task) -> None:
            self._active_tasks.discard(done_task)
            with contextlib.suppress(asyncio.CancelledError, Exception):
                done_task.result()

        task.add_done_callback(_on_done)

    async def request_stop_all_active_tasks(self) -> int:
        if not task_manager:
            return 0
        all_tasks = await task_manager.get_all_tasks(limit=500)
        active_tasks = [
            task
            for task in all_tasks
            if task.status in (TaskStatus.PENDING, TaskStatus.PROCESSING)
        ]
        stop_count = 0
        for task in active_tasks:
            if await task_manager.request_stop(task.id):
                stop_count += 1
            task_stop_registry.request_stop(task.id)
        return stop_count

    async def wait_for_active_tasks(self, timeout: float = 15.0) -> int:
        pending = list(self._active_tasks)
        if not pending:
            return 0
        done, still_pending = await asyncio.wait(pending, timeout=timeout)
        del done
        return len(still_pending)

    async def _process_queue_loop(self):
        """Основной цикл обработки очереди"""
        while self.is_running:
            try:
                await self._process_pending_tasks()
                await self._process_queued_urls()
            except Exception as e:
                logger.error(f"Ошибка в процессоре очереди: {e}")

            # Пауза между проверками
            await asyncio.sleep(2)

    async def _process_queued_urls(self):
        """Обработка URL из очереди"""
        # Получаем активных задач
        active_count = await task_manager.get_active_task_count()
        available_slots = MAX_CONCURRENT_TASKS - active_count

        if available_slots <= 0:
            return

        # Получаем всех пользователей с URL в очереди
        user_ids = await task_manager.get_all_users_with_queued_urls()

        for user_id in user_ids:
            # Проверяем сколько у пользователя pending задач
            user_tasks = await task_manager.get_user_tasks(
                user_id, limit=MAX_QUEUE_PER_USER + 1
            )
            pending_count = sum(1 for t in user_tasks if t.status == TaskStatus.PENDING)

            # Если есть свободные слоты
            user_available = min(
                MAX_CONCURRENT_TASKS - active_count, MAX_QUEUE_PER_USER - pending_count
            )
            if user_available > 0:
                # Получаем URL из очереди
                queued_urls = await task_manager.pop_queued_urls(
                    user_id, user_available
                )

                if queued_urls:
                    logger.info(
                        f"Пользователь {user_id}: создано {len(queued_urls)} задач из очереди"
                    )
                    # Создаем задачи
                    for url in queued_urls:
                        await task_manager.create_task(user_id, url)
                    # Уведомляем процессор очереди о создании задач
                    await queue_processor.increment_tasks_created(len(queued_urls))

    async def _process_pending_tasks(self):
        """Обработка ожидающих задач"""
        # Получаем все pending задачи
        pending_tasks = await task_manager.get_pending_tasks()

        if not pending_tasks:
            return

        # Проверяем количество активных задач
        active_count = await task_manager.get_active_task_count()
        available_slots = MAX_CONCURRENT_TASKS - active_count

        if available_slots <= 0:
            return

        # Запускаем задачи в доступных слотах
        tasks_to_run = pending_tasks[:available_slots]

        for task in tasks_to_run:
            # Проверяем, не начала ли задача уже выполняться
            current_task = await task_manager.get_task(task.id)
            if current_task and current_task.status == TaskStatus.PENDING:
                self._track_active_task(asyncio.create_task(self._execute_task(task)))

    async def _execute_task(self, task: FunnelTask) -> None:
        """Выполнение одной задачи"""
        global thread_pool

        # Обновляем статус на processing
        await task_manager.update_status(task.id, TaskStatus.PROCESSING)
        loop = asyncio.get_running_loop()
        stop_event = task_stop_registry.register(task.id, task.user_id)

        # Уведомляем пользователя о старте (пакетно для multi-URL)
        cfg = get_config()
        start_policy = await consume_start_notification_policy(task.user_id)
        if start_policy.get("summary_count", 0) > 0:
            await notify_batch_start(
                bot, task.user_id, int(start_policy["summary_count"])
            )
        if not start_policy.get("suppress_individual", False):
            await notify_task_start(bot, task.user_id, task)

        try:

            def progress_callback(
                current_task_id: int,
                current_step: int,
                total_steps: int,
                message: str = "",
                last_url: Optional[str] = None,
            ) -> None:
                future = asyncio.run_coroutine_threadsafe(
                    task_manager.update_progress(
                        current_task_id,
                        current_step,
                        total_steps,
                        message,
                        last_url,
                    ),
                    loop,
                )

                def _consume_result(done_future):
                    try:
                        done_future.result()
                    except Exception as callback_error:
                        logger.error(
                            f"Ошибка обновления прогресса task_id={current_task_id}: {callback_error}"
                        )

                future.add_done_callback(_consume_result)

            # Запускаем воронку в thread pool
            result = await asyncio.get_event_loop().run_in_executor(
                thread_pool,
                run_funnel_sync_wrapper,
                task.url,
                cfg.runner.__dict__,
                task.id,
                task.user_id,
                progress_callback,
                stop_event,
            )

            # Папка создается заранее, а файлы загружаются сразу после сохранения.
            drive_url = result.get("drive_folder_url")

            # Обновляем результаты
            await task_manager.complete_task(
                task_id=task.id,
                steps_total=result.get("steps_total", 0),
                paywall_reached=result.get("paywall_reached", False),
                error=None if result.get("stopped") else result.get("error"),
                screenshot_path=result.get("last_screenshot"),
                log_path=result.get("log_path"),
                manifest_path=result.get("manifest_path"),
                drive_folder_url=drive_url,
                last_url=result.get("last_url"),
                final_status=TaskStatus.CANCELLED if result.get("stopped") else None,
                progress_message=(
                    result.get("progress_message")
                    or (
                        f"Остановлено пользователем на шаге {result.get('steps_total', 0)}"
                        if result.get("stopped")
                        else ""
                    )
                ),
            )

            # Получаем обновленную задачу
            completed_task = await task_manager.get_task(task.id)

            # Уведомляем о завершении
            if result.get("stopped"):
                await notify_task_cancelled(bot, task.user_id, completed_task)
            elif result.get("error"):
                await notify_task_error(
                    bot, task.user_id, completed_task, result["error"]
                )
            else:
                await notify_task_complete(bot, task.user_id, completed_task)

            screenshot_urls = result.get("fallback_screenshot_urls") or []
            if screenshot_urls:
                await send_firecrawl_screenshot_links(
                    bot, task.user_id, screenshot_urls
                )

            # Уведомляем процессор очереди о завершении задачи
            await queue_processor.increment_tasks_completed()

        except Exception as e:
            logger.error(f"Ошибка обработки задачи #{task.id}: {e}")
            await task_manager.update_status(task.id, TaskStatus.FAILED)
            await task_manager.complete_task(
                task.id, 0, False, error=str(e), last_url=task.url
            )
            failed_task = await task_manager.get_task(task.id)
            await notify_task_error(bot, task.user_id, failed_task or task, str(e))
            # Уведомляем процессор очереди о завершении задачи
            await queue_processor.increment_tasks_completed()
        finally:
            task_stop_registry.unregister(task.id, task.user_id)


# Глобальный процессор очереди
queue_processor = TaskQueueProcessor()


def run_funnel_sync_wrapper(
    url: str,
    config_dict: dict,
    task_id: int,
    user_id: int,
    progress_callback=None,
    stop_event=None,
) -> dict:
    """
    Обертка для общего runner.py с поддержкой Telegram-прогресса и Drive-интеграции.
    """
    config = load_runner_config("config.json")
    if isinstance(config_dict, dict):
        runner_section = config.get("runner")
        if isinstance(runner_section, dict):
            runner_section.update(config_dict)
        else:
            config["runner"] = dict(config_dict)
    slug = get_slug(url)
    upload_run_id = f"task-{task_id or slug}"

    drive_folder_url: Optional[str] = None
    if drive_upload_manager is not None:
        try:
            drive_folder_url = drive_upload_manager.register_run(
                upload_run_id, slug, os.path.join("results", slug)
            )
        except Exception as e:
            logger.error(
                f"Не удалось зарегистрировать Drive upload run {upload_run_id}: {e}"
            )

    def enqueue_drive_artifact(
        file_path: Optional[str], drive_subdir: str = ""
    ) -> None:
        if not file_path or drive_upload_manager is None:
            return
        try:
            drive_upload_manager.enqueue_file(
                upload_run_id, file_path, drive_subdir=drive_subdir
            )
        except Exception as e:
            logger.error(
                f"Не удалось поставить файл в Drive очередь: {file_path} | {e}"
            )

    def request_progress(
        current_step: int,
        total_steps: int,
        message: str = "",
        last_url: Optional[str] = None,
    ) -> None:
        if progress_callback:
            try:
                progress_callback(task_id, current_step, total_steps, message, last_url)
            except Exception:
                pass

    result = run_funnel(
        url=url,
        config=config,
        is_headless=True,
        progress_callback=request_progress,
        stop_event=stop_event,
        artifact_callback=enqueue_drive_artifact,
        write_manifest=True,
    )

    if result.get("error") and not result.get("stopped"):
        resume_url = (result.get("last_url") or url).strip() or url
        logger.info(
            "Запуск Firecrawl fallback для task_id=%s | error=%s | resume_url=%s",
            task_id,
            result.get("error"),
            resume_url,
        )

        def firecrawl_progress(
            current_step: int,
            total_steps: int,
            message: str = "",
            last_url: Optional[str] = None,
        ) -> None:
            if progress_callback:
                try:
                    progress_callback(
                        task_id,
                        result.get("steps_total", 0) + current_step,
                        result.get("steps_total", 0) + total_steps,
                        message,
                        last_url,
                    )
                except Exception:
                    pass

        firecrawl_result = run_firecrawl_fallback(
            start_url=resume_url,
            max_steps=max(
                1,
                int(config.get("runner", {}).get("max_steps", 80))
                - int(result.get("steps_total", 0)),
            ),
            progress_callback=firecrawl_progress,
            log_callback=lambda message: logger.info(
                "task_id=%s | %s", task_id, message
            ),
        )

        if firecrawl_result.used:
            logger.info(
                "Firecrawl fallback завершен для task_id=%s | status=%s | paywall=%s | last_url=%s | error=%s",
                task_id,
                firecrawl_result.status,
                firecrawl_result.paywall_reached,
                firecrawl_result.last_url,
                firecrawl_result.error,
            )
            result["fallback_provider"] = firecrawl_result.provider
            result["fallback_status"] = firecrawl_result.status
            result["fallback_screenshot_urls"] = firecrawl_result.screenshot_urls
            result["steps_total"] = int(result.get("steps_total", 0)) + int(
                firecrawl_result.steps_total or 0
            )
            result["last_url"] = (
                firecrawl_result.last_url or result.get("last_url") or url
            )
            result["progress_message"] = (
                firecrawl_result.progress_message
                or result.get("progress_message")
                or ""
            )

            if firecrawl_result.last_screenshot:
                result["last_screenshot"] = firecrawl_result.last_screenshot

            if firecrawl_result.paywall_reached:
                result["paywall_reached"] = True
                result["error"] = None
            elif firecrawl_result.error:
                result["error"] = firecrawl_result.error

            if firecrawl_result.screenshot_urls:
                logger.info(
                    "Firecrawl screenshot URLs для task_id=%s: %s",
                    task_id,
                    ", ".join(firecrawl_result.screenshot_urls),
                )
        else:
            logger.warning(
                "Firecrawl fallback не был использован для task_id=%s | status=%s | error=%s",
                task_id,
                firecrawl_result.status,
                firecrawl_result.error,
            )

    result["drive_folder_url"] = drive_folder_url

    if drive_upload_manager is not None:
        try:
            result["drive_folder_url"] = drive_upload_manager.finalize_run(
                upload_run_id
            ) or result.get("drive_folder_url")
        except Exception as finalize_error:
            logger.error(
                f"Не удалось финализировать Drive run {upload_run_id}: {finalize_error}"
            )

    return result


# ====================
# Запуск бота
# ====================


async def start_bot() -> None:
    """Запуск бота"""
    global \
        bot, \
        dp, \
        task_manager, \
        thread_pool, \
        drive_upload_executor, \
        error_collector, \
        drive_upload_queue, \
        drive_upload_manager

    # Инициализация конфигурации
    init_config()
    cfg = get_config()

    if not cfg.bot.token:
        logger.error("❌ Telegram bot token не найден в config.json")
        logger.error("Пожалуйста, добавьте токен в config.json")
        return

    # Инициализация
    bot = Bot(token=cfg.bot.token)
    dp = Dispatcher(storage=MemoryStorage())
    task_manager = TaskManager()
    thread_pool = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_TASKS)
    drive_upload_executor = ThreadPoolExecutor(
        max_workers=max(1, cfg.google_drive.max_parallel_uploads)
    )
    error_collector = ErrorCollector()
    drive_upload_queue = DriveUploadQueue()
    drive_upload_manager = None
    if cfg.google_drive.enabled:
        drive_upload_manager = ParallelDriveUploadManager(
            credentials_file=cfg.google_drive.credentials_file,
            folder_id=cfg.google_drive.folder_id,
            token_file=cfg.google_drive.token_file,
            root_folder_name=cfg.google_drive.root_folder_name,
            max_workers=max(1, cfg.google_drive.max_parallel_uploads),
        )
        drive_upload_manager.start()
        drive_upload_manager.recover_pending_runs("results")

    # Регистрируем роутеры
    router = Router()

    # Команды
    router.message.register(cmd_start, CommandStart())
    router.message.register(cmd_status, Command("status"))
    router.message.register(cmd_history, Command("history"))
    router.message.register(cmd_cancel, Command("cancel"))
    router.message.register(cmd_clear, Command("clear"))
    router.message.register(cmd_drive, Command("drive"))
    router.message.register(cmd_drive_stop, Command("drive_stop"))
    router.message.register(cmd_help, Command("help"))

    # Сообщения (URL)
    router.message.register(
        handle_menu_buttons, F.text.in_(list(MENU_BUTTONS_MAP.keys()))
    )
    router.message.register(handle_url_message, FormStates.waiting_for_url)
    router.message.register(handle_url_message)  # Без состояния, тоже принимаем URL

    # Callback query
    router.callback_query.register(handle_callback_query)

    dp.include_router(router)

    # Регистрируем команды в Telegram menu
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Запуск бота"),
            BotCommand(command="status", description="Статус последней задачи"),
            BotCommand(command="history", description="Последние задачи"),
            BotCommand(command="cancel", description="Остановить все активные задачи"),
            BotCommand(command="clear", description="Сбросить зависшие задачи"),
            BotCommand(command="drive", description="Открыть результаты в Drive"),
            BotCommand(command="drive_stop", description="Остановить загрузки в Drive"),
            BotCommand(command="help", description="Краткая помощь"),
        ]
    )

    # Запуск
    logger.info(f"🤖 Бот запущен...")
    logger.info(f"📊 Максимум одновременных задач: {MAX_CONCURRENT_TASKS}")
    logger.info(f"📋 Максимум задач в очереди на пользователя: {MAX_QUEUE_PER_USER}")

    # Запускаем процессор очереди
    await queue_processor.start()
    await drive_upload_queue.start(1)

    try:
        await dp.start_polling(bot)
    except asyncio.CancelledError:
        logger.info("🤖 Polling остановлен")
        raise


async def stop_bot() -> None:
    """Остановка бота"""
    global \
        bot, \
        dp, \
        thread_pool, \
        drive_upload_executor, \
        drive_upload_queue, \
        drive_upload_manager

    if dp:
        with contextlib.suppress(Exception):
            await dp.stop_polling()

    stop_count = await queue_processor.request_stop_all_active_tasks()
    if stop_count:
        logger.info("⛔ Запрошена остановка активных задач: %s", stop_count)

    with contextlib.suppress(Exception):
        await queue_processor.stop()

    remaining_active = 0
    with contextlib.suppress(Exception):
        remaining_active = await queue_processor.wait_for_active_tasks(timeout=15.0)
    if remaining_active:
        logger.warning(
            "Некоторые runner-задачи не завершились до таймаута shutdown: %s",
            remaining_active,
        )

    if drive_upload_queue:
        with contextlib.suppress(Exception):
            await drive_upload_queue.force_stop()

    if drive_upload_manager:
        with contextlib.suppress(Exception):
            drive_upload_manager.cancel_pending(reason="Bot shutdown")
        with contextlib.suppress(Exception):
            drive_upload_manager.stop(wait=False)

    if bot:
        with contextlib.suppress(Exception):
            await bot.close()
        logger.info("🤖 Бот остановлен")

    if thread_pool:
        thread_pool.shutdown(wait=False, cancel_futures=True)
        logger.info("🔧 Thread pool остановлен")

    if drive_upload_executor:
        drive_upload_executor.shutdown(wait=False, cancel_futures=True)
        logger.info("☁️ Google Drive thread pool остановлен")


async def run_bot() -> None:
    """Единый lifecycle бота с graceful shutdown."""
    current_loop = asyncio.get_running_loop()

    def _request_shutdown() -> None:
        current_task = asyncio.current_task(loop=current_loop)
        if current_task:
            current_task.cancel()

    for sig_name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
            current_loop.add_signal_handler(sig, _request_shutdown)

    try:
        await start_bot()
    except asyncio.CancelledError:
        logger.info("👋 Запущен graceful shutdown бота")
    except KeyboardInterrupt:
        logger.info("👋 Остановка по сигналу пользователя...")
    finally:
        await stop_bot()


def main() -> None:
    """Точка входа"""
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        logger.info("👋 Остановка по сигналу пользователя завершена")


if __name__ == "__main__":
    main()
