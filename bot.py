"""
Telegram бот для Quiz Funnel Runner
Интеграция с aiogram 3.x
"""
import asyncio
import logging
import os
import re
import shutil
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
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
from drive_uploader import GoogleDriveUploader
from google_links_reader import GoogleLinksReader, is_google_url

# Импортируем функции из main.py
from main import (
    run_funnel,
    get_slug,
    classify_screen,
    perform_action,
    close_popups,
    get_screen_hash,
    get_ui_step,
    wait_for_transition,
    find_continue_button,
    warmup_page_for_full_screenshot,
    ensure_privacy_checkbox_checked,
    ensure_consent_checkbox_checked,
    is_probable_paywall_url,
    resolve_fill_values,
    FILLABLE_INPUT_SELECTOR,
    DEBUG_CLASSIFY,
    WORKOUT_ISSUES_STEP_KEY,
    WORKOUT_FREQUENCY_STEP_KEY,
    DATE_OF_BIRTH_STEP_KEY,
    save_error_artifacts,
    check_and_handle_form_blockers,
    detect_page_stuck_state,
    detect_url_loop,
)

from playwright.sync_api import sync_playwright, Page, TimeoutError
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import json
import hashlib
import argparse

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Глобальные объекты
bot: Optional[Bot] = None
dp: Optional[Dispatcher] = None
task_manager: Optional[TaskManager] = None
thread_pool: Optional[ThreadPoolExecutor] = None


class ErrorData:
    """Данные об ошибке для архива"""
    def __init__(self, url: str, error_message: str, screenshot_path: Optional[str], log_path: Optional[str]):
        self.url = url
        self.error_message = error_message
        self.screenshot_path = screenshot_path
        self.log_path = log_path
        self.domain = self._extract_domain(url)
    
    def _extract_domain(self, url: str) -> str:
        """Извлекает домен из URL для имени папки"""
        try:
            parsed = urlparse(url)
            return parsed.netloc.replace('www.', '')
        except:
            return "unknown_domain"


class ErrorCollector:
    """Коллектор для сбора ошибок и создания архива"""
    
    def __init__(self, output_dir: str = "error_reports"):
        self.output_dir = output_dir
        self.errors: List[ErrorData] = []
        self._lock = asyncio.Lock()
    
    async def add_error(self, url: str, error_message: str, screenshot_path: Optional[str], log_path: Optional[str]) -> None:
        """Добавляет ошибку в коллектор"""
        async with self._lock:
            error_data = ErrorData(url, error_message, screenshot_path, log_path)
            self.errors.append(error_data)
            logger.info(f"Добавлена ошибка в коллектор: {url[:50]}... | {error_message[:50]}...")
    
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
        
        with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for domain, domain_errors in errors_by_domain.items():
                # Создаем папку для домена
                domain_folder = f"errors_{domain}"
                
                for idx, error in enumerate(domain_errors):
                    file_prefix = f"{domain_folder}/error_{idx+1:03d}"
                    
                    # 1. Добавляем скриншот
                    if error.screenshot_path and os.path.exists(error.screenshot_path):
                        try:
                            zipf.write(error.screenshot_path, f"{file_prefix}_screenshot.png")
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
                        zipf.writestr(f"{file_prefix}_url_and_error.txt", error_info_content)
                    except Exception as e:
                        logger.error(f"Ошибка добавления информации об ошибке в архив: {e}")
        
        logger.info(f"Создан архив с ошибками: {archive_path} ({len(self.errors)} ошибок)")
        return archive_path
    
    def get_errors_count(self) -> int:
        """Возвращает количество собранных ошибок"""
        return len(self.errors)


# Глобальный коллектор ошибок
error_collector: Optional[ErrorCollector] = None

# Пакетные уведомления о старте задач (для сообщений с несколькими URL)
start_batch_notifications: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
start_batch_lock = asyncio.Lock()

# Ограничения
MAX_CONCURRENT_TASKS = 3  # Максимум одновременных задач
MAX_QUEUE_PER_USER = 5    # Максимум задач в очереди на пользователя


class FormStates(StatesGroup):
    """Состояния FSM"""
    waiting_for_url = State()


MENU_BUTTONS_MAP = {
    "📊 Статус": "/status",
    "📜 История": "/history",
    "⛔ Отмена": "/cancel",
    "🧹 Очистить": "/clear",
    "📁 Drive": "/drive",
    "ℹ️ Помощь": "/help",
}


def build_main_menu() -> ReplyKeyboardMarkup:
    """Компактное главное меню"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Статус"), KeyboardButton(text="📜 История")],
            [KeyboardButton(text="⛔ Отмена"), KeyboardButton(text="🧹 Очистить")],
            [KeyboardButton(text="📁 Drive"), KeyboardButton(text="ℹ️ Помощь")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Вставьте URL или выберите действие из меню",
    )


def short_url(url: str, max_len: int = 60) -> str:
    """Безопасно сокращает URL для компактного вывода"""
    if not url:
        return "-"
    return url if len(url) <= max_len else f"{url[:max_len - 1]}…"


def normalize_url_for_compare(url: Optional[str]) -> str:
    """Нормализует URL для сравнения без лишнего шума."""
    if not url:
        return ""
    normalized = str(url).strip()
    if not normalized:
        return ""
    return normalized.rstrip("/")


def should_show_resume_url(start_url: Optional[str], current_url: Optional[str]) -> bool:
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

    compact = raw.replace("_", " ")
    return compact[:160]


def build_result_lines(task: FunnelTask, *, is_success: bool, resume_url: Optional[str] = None) -> List[str]:
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


def build_result_text(task: FunnelTask, *, is_success: bool, resume_url: Optional[str] = None) -> str:
    return "\n".join(build_result_lines(task, is_success=is_success, resume_url=resume_url))


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


async def register_start_batch(user_id: int, total_tasks: int) -> None:
    """Регистрирует пакет задач для единого стартового уведомления"""
    if total_tasks <= 1:
        return
    async with start_batch_lock:
        start_batch_notifications[user_id].append({
            "remaining": total_tasks,
            "total": total_tasks,
            "notified": False,
        })


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
    if not (url.startswith('http://') or url.startswith('https://')):
        return False
    
    # Более гибкий regex для URL с параметрами и спецсимволами
    pattern = re.compile(
        r'^https?://'  # http:// или https://
        r'(?:[^\s<>\"{}|\\^`\[\]]+)',  # домен и путь
        re.IGNORECASE
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
    if cfg.bot.use_only_admin:
        return user_id in cfg.bot.admin_ids
    return user_id in cfg.bot.allowed_users or user_id in cfg.bot.admin_ids


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
            reply_markup=build_main_menu(),
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
        if task.current_step in (1, 10, 20, 30, 40, 50, 60, 70, 80) or task.current_step == task.steps_total:
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
                await bot.send_photo(user_id, photo, caption=text, parse_mode="HTML", reply_markup=build_main_menu())
            except Exception:
                await bot.send_message(user_id, text, parse_mode="HTML", reply_markup=build_main_menu())
        else:
            await bot.send_message(user_id, text, parse_mode="HTML", reply_markup=build_main_menu())

    except Exception as e:
        logger.error(f"Ошибка отправки уведомления о завершении: {e}")


async def notify_task_error(bot: Bot, user_id: int, task: FunnelTask, error: str) -> None:
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
                await bot.send_photo(user_id, photo, caption=text, parse_mode="HTML", reply_markup=build_main_menu())
            except Exception:
                await bot.send_message(user_id, text, parse_mode="HTML", reply_markup=build_main_menu())
        else:
            await bot.send_message(user_id, text, parse_mode="HTML", reply_markup=build_main_menu())
        
        # Сохраняем в коллектор ошибок
        if error_collector:
            await error_collector.add_error(
                url=task.url,
                error_message=error,
                screenshot_path=task.screenshot_path,
                log_path=task.log_path
            )
    except Exception as e:
        logger.error(f"Ошибка при сохранении ошибки в коллектор: {e}")


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
            parse_mode="HTML"
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
        await message.answer("📭 Задач пока нет. Отправьте URL.", reply_markup=build_main_menu())
        return

    task = tasks[0]
    text = await get_task_status_text(task)
    await message.answer(text, parse_mode="HTML", reply_markup=build_main_menu())


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
        text += f"{i}) {status_emoji.get(task.status, '❓')} #{task.id} • <code>{short_url(task.url, 42)}</code>\n"

    await message.answer(text, parse_mode="HTML", reply_markup=build_main_menu())


async def cmd_cancel(message: Message) -> None:
    """Обработка команды /cancel"""
    user_id = message.from_user.id

    if not check_user_access(user_id):
        await message.answer("❌ Доступ запрещен")
        return

    # Получаем активные задачи
    tasks = await task_manager.get_user_tasks(user_id, limit=1)
    if not tasks:
        await message.answer("📭 Нет активных задач для отмены.")
        return

    task = tasks[0]
    if task.status not in (TaskStatus.PENDING, TaskStatus.PROCESSING):
        await message.answer("ℹ️ Задача уже завершена.")
        return

    cancelled = await task_manager.cancel_task(task.id)
    if cancelled:
        await message.answer(f"✅ Задача #{task.id} отменена.", reply_markup=build_main_menu())
    else:
        await message.answer("❌ Не удалось отменить задачу.", reply_markup=build_main_menu())


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
            error="Task stuck, cleared by user"
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
                reply_markup=build_main_menu(),
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
        "/status • /history • /cancel • /clear • /drive • /help\n\n"
        "📝 Отправьте URL (или список URL) для запуска задач.",
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
    input_urls = [u.strip() for u in text.split('\n') if u.strip()]
    
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
    user_tasks = await task_manager.get_user_tasks(user_id, limit=MAX_QUEUE_PER_USER + 1)
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

    if data.startswith("cancel_"):
        task_id = int(data.split("_")[1])
        user_id = callback.from_user.id

        if not check_user_access(user_id):
            await callback.answer("❌ Доступ запрещен", show_alert=True)
            return

        cancelled = await task_manager.cancel_task(task_id)
        if cancelled:
            await callback.answer(f"✅ Задача #{task_id} отменена")
        else:
            await callback.answer("❌ Не удалось отменить задачу", show_alert=True)


# ====================
# Google Drive загрузка
# ====================

async def upload_to_google_drive(
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
        # Проверяем наличие файла учетных данных
        if not os.path.exists(credentials_file):
            logger.error(f"Файл учетных данных не найден: {credentials_file}")
            return None
        
        # Создаем загрузчик
        uploader = GoogleDriveUploader(
            credentials_file=credentials_file,
            folder_id=folder_id,
            token_file=token_file,
            root_folder_name=root_folder_name,
        )
        
        if not uploader.service:
            logger.error("Не удалось инициализировать Google Drive сервис")
            return None
        
        # Загружаем результаты
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

    async def increment_tasks_created(self, count: int = 1) -> None:
        """Увеличивает счетчик созданных задач"""
        async with self._lock:
            self._total_tasks_created += count

    async def increment_tasks_completed(self, count: int = 1) -> None:
        """Увеличивает счетчик завершенных задач"""
        async with self._lock:
            self._total_tasks_completed += count
            # Проверяем, завершены ли все задачи
            if self._total_tasks_completed >= self._total_tasks_created and self._total_tasks_created > 0:
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
            user_tasks = await task_manager.get_user_tasks(user_id, limit=MAX_QUEUE_PER_USER + 1)
            pending_count = sum(1 for t in user_tasks if t.status == TaskStatus.PENDING)
            
            # Если есть свободные слоты
            user_available = min(MAX_CONCURRENT_TASKS - active_count, MAX_QUEUE_PER_USER - pending_count)
            if user_available > 0:
                # Получаем URL из очереди
                queued_urls = await task_manager.pop_queued_urls(user_id, user_available)

                if queued_urls:
                    logger.info(f"Пользователь {user_id}: создано {len(queued_urls)} задач из очереди")
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
                asyncio.create_task(self._execute_task(task))
    
    async def _execute_task(self, task: FunnelTask) -> None:
        """Выполнение одной задачи"""
        global thread_pool
        
        # Обновляем статус на processing
        await task_manager.update_status(task.id, TaskStatus.PROCESSING)
        
        # Уведомляем пользователя о старте (пакетно для multi-URL)
        cfg = get_config()
        start_policy = await consume_start_notification_policy(task.user_id)
        if start_policy.get("summary_count", 0) > 0:
            await notify_batch_start(bot, task.user_id, int(start_policy["summary_count"]))
        if not start_policy.get("suppress_individual", False):
            await notify_task_start(bot, task.user_id, task)
        
        try:
            # Запускаем воронку в thread pool
            result = await asyncio.get_event_loop().run_in_executor(
                thread_pool,
                run_funnel_sync_wrapper,
                task.url,
                cfg.runner.__dict__,
                task.id,
                task.user_id,
                None,
            )
            
            # Загружаем в Google Drive если включено
            drive_url = None
            if cfg.google_drive.enabled and not result.get("error"):
                try:
                    drive_url = await upload_to_google_drive(
                        slug=result.get("slug", ""),
                        result_dir=result.get("path", ""),
                        credentials_file=cfg.google_drive.credentials_file,
                        folder_id=cfg.google_drive.folder_id,
                        token_file=cfg.google_drive.token_file,
                        root_folder_name=cfg.google_drive.root_folder_name,
                    )
                except Exception as e:
                    logger.error(f"Ошибка загрузки в Google Drive: {e}")
            
            # Обновляем результаты
            await task_manager.complete_task(
                task_id=task.id,
                steps_total=result.get("steps_total", 0),
                paywall_reached=result.get("paywall_reached", False),
                error=result.get("error"),
                screenshot_path=result.get("last_screenshot"),
                log_path=result.get("log_path"),
                manifest_path=result.get("manifest_path"),
                drive_folder_url=drive_url,
                last_url=result.get("last_url"),
            )
            
            # Получаем обновленную задачу
            completed_task = await task_manager.get_task(task.id)
             
            # Уведомляем о завершении
            if result.get("error"):
                await notify_task_error(bot, task.user_id, completed_task, result["error"])
            else:
                await notify_task_complete(bot, task.user_id, completed_task)
            
            # Уведомляем процессор очереди о завершении задачи
            await queue_processor.increment_tasks_completed()

        except Exception as e:
            logger.error(f"Ошибка обработки задачи #{task.id}: {e}")
            await task_manager.update_status(task.id, TaskStatus.FAILED)
            await task_manager.complete_task(task.id, 0, False, error=str(e), last_url=task.url)
            failed_task = await task_manager.get_task(task.id)
            await notify_task_error(bot, task.user_id, failed_task or task, str(e))
            # Уведомляем процессор очереди о завершении задачи
            await queue_processor.increment_tasks_completed()


# Глобальный процессор очереди
queue_processor = TaskQueueProcessor()


def run_funnel_sync_wrapper(url: str, config_dict: dict, task_id: int, user_id: int,
                            progress_callback=None) -> dict:
    """
    Обертка для run_funnel с поддержкой прогресса
    progress_callback - синхронная функция для обновления прогресса
    """
    from main import save_error_artifacts as save_error_artifacts_main
    
    # Создаем временный конфиг для run_funnel
    temp_config = {
        "max_steps": config_dict.get("max_steps", 80),
        "slow_mo_ms": config_dict.get("slow_mo_ms", 100),
        "fill_values": config_dict.get("fill_values", {}),
    }

    slug = get_slug(url)
    res_dir = os.path.join('results', slug)
    os.makedirs(res_dir, exist_ok=True)

    classified_dir = os.path.join('results', '_classified')
    for cat in ['question', 'info', 'input', 'email', 'paywall', 'other', 'checkout']:
        os.makedirs(os.path.join(classified_dir, cat), exist_ok=True)

    result = {
        "url": url,
        "slug": slug,
        "steps_total": 0,
        "paywall_reached": False,
        "start_url": url,
        "last_url": "",
        "path": res_dir,
        "error": None,
        "last_screenshot": None,
        "log_path": None,
        "manifest_path": None,
    }

    max_steps = temp_config.get("max_steps", 80)
    slow_mo = temp_config.get("slow_mo_ms", 100)
    fill_values = resolve_fill_values(temp_config)

    log_path = os.path.join(res_dir, 'log.txt')
    result["log_path"] = log_path
    
    # Список для хранения логов (нужен для сохранения при ошибке)
    log_lines = []

    with open(log_path, 'w', encoding='utf-8') as f:
        def log(m):
            l = f"[{time.strftime('%H:%M:%S')}] {m}\n"
            f.write(l)
            print(l.strip())
            log_lines.append(l.strip())

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, slow_mo=slow_mo)
                page = browser.new_context(**p.devices['iPhone 13']).new_page()
                
                # Перехватываем console.error для сбора JS ошибок
                def handle_console(msg):
                    if msg.type == "error":
                        try:
                            page.evaluate("""(text) => {
                                if (!window.__qwen_console_errors) {
                                    window.__qwen_console_errors = [];
                                }
                                window.__qwen_console_errors.push(text);
                                if (window.__qwen_console_errors.length > 100) {
                                    window.__qwen_console_errors.shift();
                                }
                            }""", msg.text)
                        except:
                            pass
                page.on("console", handle_console)
                
                log(f"Переход на {url} (slug: {slug})")

                try:
                    page.goto(url, wait_until='load', timeout=60000)
                    result["last_url"] = page.url or url
                except TimeoutError:
                    result["error"] = "navigation_timeout"
                    result["last_url"] = page.url or url
                    log("Ошибка: таймаут открытия страницы")
                    # Сохраняем артефакты при ошибке
                    try:
                        save_error_artifacts_main(page, url, result["error"], log_lines, log)
                    except:
                        pass
                    browser.close()
                    return result

                step = 1
                history_counts = defaultdict(int)
                step_attempts = defaultdict(int)
                last_error_artifacts_saved = False
                url_history = [page.url]  # История URL для детекции циклов

                while step <= max_steps:
                    # Обновляем прогресс через callback
                    if progress_callback:
                        progress_callback(task_id, step, max_steps, f"Обработка шага {step}...")

                    curr_u = page.url
                    if any(k in curr_u for k in ["magic", "analyzing", "loading", "preparePlan"]):
                        time.sleep(10)
                        curr_u = page.url

                    close_popups(page, log)
                    time.sleep(1)
                    curr_h = get_screen_hash(page)
                    st = classify_screen(page, log)
                    ui_before = get_ui_step(page)

                    if st in ['paywall', 'checkout']:
                        warmup_page_for_full_screenshot(page, log)
                        curr_h = get_screen_hash(page)

                    step_key = f"{urlparse(curr_u).path}|{ui_before}|{st}"
                    repeat_attempt = step_attempts[step_key]
                    step_attempts[step_key] += 1

                    curr_id = f"{curr_u}|{curr_h}"
                    history_counts[curr_id] += 1
                    loop_limit = 8 if WORKOUT_ISSUES_STEP_KEY in curr_u.lower() else 3

                    if history_counts[curr_id] >= loop_limit:
                        log(f"Обнаружено зацикливание на {curr_u}. Остановка.")
                        result["error"] = "stuck_loop"
                        result["last_url"] = page.url or curr_u or url
                        # Сохраняем артефакты при зацикливании
                        try:
                            save_error_artifacts_main(page, url, result["error"], log_lines, log)
                            last_error_artifacts_saved = True
                        except:
                            pass
                        break

                    screen_name = f"{step:02d}_{st}.png"
                    local_path = os.path.join(res_dir, screen_name)
                    result["last_screenshot"] = local_path

                    try:
                        page.screenshot(path=local_path, full_page=True)
                        shutil.copy2(local_path, os.path.join(classified_dir, st, f"{slug}__{screen_name}"))
                    except Exception as e:
                        log(f"Ошибка сохранения скриншота: {str(e)[:120]}")

                    log(f"Шаг:{step} | тип:{st} | ui_step:{ui_before} | url:{page.url[:60]}")
                    act = perform_action(page, st, log, res_dir, curr_h, curr_u, fill_values, repeat_attempt=repeat_attempt)

                    time.sleep(1)
                    ui_after = get_ui_step(page)

                    log(f"Результат действия:{act}")

                    # Добавляем URL в историю
                    url_history.append(page.url)
                    # Проверяем на циклы
                    if detect_url_loop(page, url_history, log):
                        log(f"Обнаружен циклический переход. Пробуем альтернативное действие.")
                        # Увеличиваем счетчик попыток для текущего шага
                        step_attempts[step_key] = min(step_attempts[step_key] + 2, 5)

                    result["steps_total"] = step
                    result["last_url"] = page.url

                    if st in ['paywall', 'checkout'] or "stopped" in act or "reached" in act:
                        if st in ['paywall', 'checkout'] or "paywall" in act:
                            result["paywall_reached"] = True
                        break

                    step += 1
                
                # Если была ошибка и артефакты еще не сохранены - сохраняем
                if result["error"] and not last_error_artifacts_saved:
                    try:
                        result["last_url"] = page.url or result.get("last_url") or url
                        save_error_artifacts_main(page, url, result["error"], log_lines, log)
                    except:
                        pass

                result["last_url"] = page.url or result.get("last_url") or url
                browser.close()

                # Создаем manifest.json
                manifest = {
                    "url": url,
                    "started_at": datetime.now().isoformat(),
                    "completed_at": datetime.now().isoformat(),
                    "status": "success" if result["paywall_reached"] else "completed",
                    "steps_total": result["steps_total"],
                    "paywall_reached": result["paywall_reached"],
                    "error": result["error"],
                }
                manifest_path = os.path.join(res_dir, 'manifest.json')
                with open(manifest_path, 'w', encoding='utf-8') as mf:
                    json.dump(manifest, mf, indent=2, ensure_ascii=False)
                result["manifest_path"] = manifest_path

        except Exception as e:
            result["error"] = f"runner_exception:{str(e)[:180]}"
            log(f"Критическая ошибка раннера: {str(e)[:180]}")
            # Сохраняем артефакты при критической ошибке
            try:
                result["last_url"] = locals().get("page").url if "page" in locals() and page else result.get("last_url") or url
                save_error_artifacts_main(page, url, result["error"], log_lines, log)
            except:
                pass

    return result


# ====================
# Запуск бота
# ====================

async def start_bot() -> None:
    """Запуск бота"""
    global bot, dp, task_manager, thread_pool, error_collector

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
    error_collector = ErrorCollector()

    # Регистрируем роутеры
    router = Router()

    # Команды
    router.message.register(cmd_start, CommandStart())
    router.message.register(cmd_status, Command("status"))
    router.message.register(cmd_history, Command("history"))
    router.message.register(cmd_cancel, Command("cancel"))
    router.message.register(cmd_clear, Command("clear"))
    router.message.register(cmd_drive, Command("drive"))
    router.message.register(cmd_help, Command("help"))

    # Сообщения (URL)
    router.message.register(handle_menu_buttons, F.text.in_(list(MENU_BUTTONS_MAP.keys())))
    router.message.register(handle_url_message, FormStates.waiting_for_url)
    router.message.register(handle_url_message)  # Без состояния, тоже принимаем URL

    # Callback query
    router.callback_query.register(handle_callback_query)

    dp.include_router(router)

    # Регистрируем команды в Telegram menu
    await bot.set_my_commands([
        BotCommand(command="start", description="Запуск бота"),
        BotCommand(command="status", description="Статус последней задачи"),
        BotCommand(command="history", description="Последние задачи"),
        BotCommand(command="cancel", description="Отменить активную задачу"),
        BotCommand(command="clear", description="Сбросить зависшие задачи"),
        BotCommand(command="drive", description="Открыть результаты в Drive"),
        BotCommand(command="help", description="Краткая помощь"),
    ])

    # Запуск
    logger.info(f"🤖 Бот запущен...")
    logger.info(f"📊 Максимум одновременных задач: {MAX_CONCURRENT_TASKS}")
    logger.info(f"📋 Максимум задач в очереди на пользователя: {MAX_QUEUE_PER_USER}")

    # Запускаем процессор очереди
    await queue_processor.start()

    await dp.start_polling(bot)


async def stop_bot() -> None:
    """Остановка бота"""
    global bot, thread_pool

    # Останавливаем процессор очереди
    await queue_processor.stop()

    if bot:
        await bot.close()
        logger.info("🤖 Бот остановлен")

    if thread_pool:
        thread_pool.shutdown(wait=True)
        logger.info("🔧 Thread pool остановлен")


def main() -> None:
    """Точка входа"""
    try:
        asyncio.run(start_bot())
    except KeyboardInterrupt:
        logger.info("👋 Остановка по сигналу пользователя...")
    finally:
        asyncio.run(stop_bot())


if __name__ == "__main__":
    main()
