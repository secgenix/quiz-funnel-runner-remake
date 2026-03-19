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
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery, FSInputFile, InputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import get_config, init_config
from models import TaskManager, FunnelTask, TaskStatus
from drive_uploader import GoogleDriveUploader

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

# Ограничения
MAX_CONCURRENT_TASKS = 3  # Максимум одновременных задач
MAX_QUEUE_PER_USER = 5    # Максимум задач в очереди на пользователя


class FormStates(StatesGroup):
    """Состояния FSM"""
    waiting_for_url = State()


def is_valid_url(url: str) -> bool:
    """Проверка валидности URL"""
    pattern = re.compile(
        r'^https?://'
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'
        r'localhost|'
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
        r'(?::\d+)?'
        r'(?:/?|[/?]\S+)$', re.IGNORECASE
    )
    return url is not None and pattern.match(url)


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

    text = f"{status_emoji.get(task.status, '❓')} <b>Статус:</b> {status_names.get(task.status, 'Неизвестно')}\n"
    text += f"<b>URL:</b> <code>{task.url}</code>\n"

    if task.status == TaskStatus.PROCESSING:
        text += f"<b>Прогресс:</b> Шаг {task.current_step}/{task.steps_total}\n"
        if task.progress_message:
            text += f"<i>{task.progress_message}</i>\n"

    if task.status == TaskStatus.COMPLETED:
        text += f"<b>Шагов пройдено:</b> {task.steps_total}\n"
        text += f"<b>Paywall:</b> {'✅ Достигнут' if task.paywall_reached else '❌ Не достигнут'}\n"
        
        if task.drive_folder_url:
            text += f"<b>Google Drive:</b> <a href='{task.drive_folder_url}'>Открыть папку</a>\n"

    if task.error:
        text += f"<b>Ошибка:</b> <code>{task.error[:100]}</code>\n"

    if task.completed_at:
        duration = task.completed_at - (task.started_at or task.created_at)
        text += f"<b>Время выполнения:</b> {duration.total_seconds():.1f} сек\n"

    return text


async def notify_task_start(bot: Bot, user_id: int, task: FunnelTask) -> None:
    """Уведомление о начале обработки задачи"""
    try:
        await bot.send_message(
            user_id,
            f"🚀 <b>Начало обработки воронки</b>\n\n"
            f"<b>URL:</b> <code>{task.url}</code>\n"
            f"<b>ID задачи:</b> <code>#{task.id}</code>\n\n"
            f"Ожидайте, я сообщу о прогрессе и результатах.",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления о старте: {e}")


async def notify_task_progress(bot: Bot, user_id: int, task: FunnelTask) -> None:
    """Уведомление о прогрессе задачи"""
    try:
        # Отправляем прогресс не чаще чем каждые 5 шагов
        if task.current_step % 5 == 0 or task.current_step == task.steps_total:
            await bot.send_message(
                user_id,
                f"🔄 <b>Прогресс обработки</b>\n\n"
                f"<b>ID задачи:</b> #{task.id}\n"
                f"<b>Шаг:</b> {task.current_step}/{task.steps_total}\n"
                f"{task.progress_message}",
                parse_mode="HTML"
            )
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления о прогрессе: {e}")


async def notify_task_complete(bot: Bot, user_id: int, task: FunnelTask) -> None:
    """Уведомление о завершении задачи"""
    try:
        text = f"✅ <b>Обработка воронки завершена</b>\n\n"
        text += await get_task_status_text(task)

        # Если есть скриншоты, отправляем последний
        if task.screenshot_path and os.path.exists(task.screenshot_path):
            try:
                photo = FSInputFile(task.screenshot_path)
                await bot.send_photo(user_id, photo, caption=text, parse_mode="HTML")
            except Exception:
                await bot.send_message(user_id, text, parse_mode="HTML")
        else:
            await bot.send_message(user_id, text, parse_mode="HTML")

        # Если есть лог, отправляем файлом
        if task.log_path and os.path.exists(task.log_path):
            try:
                log_file = FSInputFile(task.log_path)
                await bot.send_document(user_id, log_file)
            except Exception as e:
                logger.error(f"Ошибка отправки лога: {e}")

    except Exception as e:
        logger.error(f"Ошибка отправки уведомления о завершении: {e}")


async def notify_task_error(bot: Bot, user_id: int, task: FunnelTask, error: str) -> None:
    """Уведомление об ошибке задачи"""
    try:
        await bot.send_message(
            user_id,
            f"❌ <b>Ошибка обработки воронки</b>\n\n"
            f"<b>ID задачи:</b> #{task.id}\n"
            f"<b>URL:</b> <code>{task.url}</code>\n"
            f"<b>Ошибка:</b> <code>{error[:200]}</code>",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления об ошибке: {e}")


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
        "👋 <b>Добро пожаловать в Quiz Funnel Runner!</b>\n\n"
        "Я автоматизирую прохождение quiz-воронок и сохраняю скриншоты каждого шага.\n\n"
        "📋 <b>Что я умею:</b>\n"
        "• Прохожу воронки от начала до paywall/checkout\n"
        "• Делаю скриншоты всех экранов\n"
        "• Классифицирую типы экранов\n"
        "• Отправляю отчеты в Telegram\n\n"
        "🔗 <b>Как использовать:</b>\n"
        "1. Отправьте мне URL воронки\n"
        "2. Или используйте /status для проверки статуса\n"
        "3. Или /history для просмотра истории\n\n"
        "📝 <b>Отправьте URL для начала:</b>",
        parse_mode="HTML"
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
        await message.answer("📭 У вас пока нет задач.\n\nОтправьте URL воронки для начала.")
        return

    task = tasks[0]
    text = await get_task_status_text(task)
    await message.answer(text, parse_mode="HTML")


async def cmd_history(message: Message) -> None:
    """Обработка команды /history"""
    user_id = message.from_user.id

    if not check_user_access(user_id):
        await message.answer("❌ Доступ запрещен")
        return

    tasks = await task_manager.get_user_tasks(user_id, limit=10)
    if not tasks:
        await message.answer("📭 История пуста.")
        return

    text = "📜 <b>История задач (последние 10):</b>\n\n"
    for i, task in enumerate(tasks, 1):
        status_emoji = {
            TaskStatus.PENDING: "⏳",
            TaskStatus.PROCESSING: "🔄",
            TaskStatus.COMPLETED: "✅",
            TaskStatus.FAILED: "❌",
            TaskStatus.CANCELLED: "⛔",
        }
        text += f"{i}. {status_emoji.get(task.status, '❓')} #{task.id} | {task.url[:50]}...\n"

    await message.answer(text, parse_mode="HTML")


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
        await message.answer(f"✅ Задача #{task.id} отменена.")
    else:
        await message.answer("❌ Не удалось отменить задачу.")


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
                f"📁 <b>Результаты в Google Drive</b>\n\n"
                f"<b>URL воронки:</b> <code>{task.url}</code>\n"
                f"<b>Шагов:</b> {task.steps_total}\n"
                f"<b>Paywall:</b> {'✅ Достигнут' if task.paywall_reached else '❌'}\n\n"
                f"🔗 <a href='{task.drive_folder_url}'>Открыть папку в Google Drive</a>",
                parse_mode="HTML"
            )
            return

    await message.answer(
        "📭 Нет задач с загруженными результатами в Google Drive.\n\n"
        "Убедитесь, что Google Drive интеграция включена в config.json."
    )


async def cmd_help(message: Message) -> None:
    """Обработка команды /help"""
    await message.answer(
        "ℹ️ <b>Справка по командам:</b>\n\n"
        "/start - Запуск бота\n"
        "/status - Статус последней задачи\n"
        "/history - История задач\n"
        "/cancel - Отмена текущей задачи\n"
        "/drive - Ссылка на Google Drive с результатами\n"
        "/help - Эта справка\n\n"
        "📝 <b>Также вы можете:</b>\n"
        "• Отправить URL воронки для обработки\n"
        "• Отправить несколько URL (каждый с новой строки)",
        parse_mode="HTML"
    )


# ====================
# Обработчики сообщений
# ====================

async def handle_url_message(message: Message, state: FSMContext) -> None:
    """Обработка URL от пользователя"""
    user_id = message.from_user.id

    if not check_user_access(user_id):
        await message.answer("❌ Доступ запрещен")
        return

    # Проверяем текущее состояние
    current_state = await state.get_state()
    if current_state != FormStates.waiting_for_url:
        await state.set_state(FormStates.waiting_for_url)

    text = message.text.strip()

    # Проверяем, это список URL или один URL
    urls = [u.strip() for u in text.split('\n') if u.strip()]

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
            parse_mode="HTML"
        )
        return

    # Проверяем лимит очереди
    user_tasks = await task_manager.get_user_tasks(user_id, limit=MAX_QUEUE_PER_USER + 1)
    pending_count = sum(1 for t in user_tasks if t.status == TaskStatus.PENDING)
    if pending_count >= MAX_QUEUE_PER_USER:
        await message.answer(
            f"⚠️ <b>Лимит очереди</b>\n\n"
            f"У вас уже {pending_count} задач в очереди. Максимум: {MAX_QUEUE_PER_USER}\n"
            f"Дождитесь завершения или отмените задачи через /cancel",
            parse_mode="HTML"
        )
        return

    # Создаем задачи
    created_tasks = []
    for url in urls[:10]:  # Максимум 10 URL за раз
        task = await task_manager.create_task(user_id, url)
        created_tasks.append(task)

    # Отправляем подтверждение
    if len(created_tasks) == 1:
        await message.answer(
            f"✅ <b>Задача создана</b>\n\n"
            f"<b>ID:</b> #{created_tasks[0].id}\n"
            f"<b>URL:</b> <code>{created_tasks[0].url}</code>\n\n"
            f"Используйте /status для проверки прогресса.",
            parse_mode="HTML"
        )
    else:
        await message.answer(
            f"✅ <b>Создано задач: {len(created_tasks)}</b>\n\n"
            + "\n".join(f"#{t.id} - <code>{t.url[:50]}</code>" for t in created_tasks),
            parse_mode="HTML"
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

async def upload_to_google_drive(slug: str, result_dir: str, 
                                 credentials_file: str, folder_id: str) -> Optional[str]:
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
        uploader = GoogleDriveUploader(credentials_file, folder_id)
        
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
            except Exception as e:
                logger.error(f"Ошибка в процессоре очереди: {e}")
            
            # Пауза между проверками
            await asyncio.sleep(2)
    
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
        
        # Уведомляем пользователя
        cfg = get_config()
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
            )
            
            # Получаем обновленную задачу
            completed_task = await task_manager.get_task(task.id)
            
            # Уведомляем о завершении
            if result.get("error"):
                await notify_task_error(bot, task.user_id, completed_task, result["error"])
            else:
                await notify_task_complete(bot, task.user_id, completed_task)
                
        except Exception as e:
            logger.error(f"Ошибка обработки задачи #{task.id}: {e}")
            await task_manager.update_status(task.id, TaskStatus.FAILED)
            await task_manager.complete_task(task.id, 0, False, error=str(e))
            await notify_task_error(bot, task.user_id, task, str(e))


# Глобальный процессор очереди
queue_processor = TaskQueueProcessor()


def run_funnel_sync_wrapper(url: str, config_dict: dict, task_id: int, user_id: int, 
                            progress_callback=None) -> dict:
    """
    Обертка для run_funnel с поддержкой прогресса
    progress_callback - синхронная функция для обновления прогресса
    """
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

    with open(log_path, 'w', encoding='utf-8') as f:
        def log(m):
            l = f"[{time.strftime('%H:%M:%S')}] {m}\n"
            f.write(l)
            print(l.strip())

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, slow_mo=slow_mo)
                page = browser.new_context(**p.devices['iPhone 13']).new_page()
                log(f"Переход на {url} (slug: {slug})")

                try:
                    page.goto(url, wait_until='load', timeout=60000)
                except TimeoutError:
                    result["error"] = "navigation_timeout"
                    log("Ошибка: таймаут открытия страницы")
                    browser.close()
                    return result

                step = 1
                history_counts = defaultdict(int)
                step_attempts = defaultdict(int)

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

                    result["steps_total"] = step
                    result["last_url"] = page.url

                    if st in ['paywall', 'checkout'] or "stopped" in act or "reached" in act:
                        if st in ['paywall', 'checkout'] or "paywall" in act:
                            result["paywall_reached"] = True
                        break

                    step += 1

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

    return result


# ====================
# Запуск бота
# ====================

async def start_bot() -> None:
    """Запуск бота"""
    global bot, dp, task_manager, thread_pool

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

    # Регистрируем роутеры
    router = Router()

    # Команды
    router.message.register(cmd_start, CommandStart())
    router.message.register(cmd_status, Command("status"))
    router.message.register(cmd_history, Command("history"))
    router.message.register(cmd_cancel, Command("cancel"))
    router.message.register(cmd_drive, Command("drive"))
    router.message.register(cmd_help, Command("help"))

    # Сообщения (URL)
    router.message.register(handle_url_message, FormStates.waiting_for_url)
    router.message.register(handle_url_message)  # Без состояния, тоже принимаем URL

    # Callback query
    router.callback_query.register(handle_callback_query)

    dp.include_router(router)

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
