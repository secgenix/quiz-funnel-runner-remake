"""
Модели данных для Quiz Funnel Runner
"""
import sqlite3
import json
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any, Sequence
from pathlib import Path

logger = logging.getLogger(__name__)


class TaskStatus(str, Enum):
    """Статусы задач"""
    PENDING = "pending"       # В очереди
    PROCESSING = "processing" # В процессе
    COMPLETED = "completed"   # Завершено успешно
    FAILED = "failed"         # Ошибка
    CANCELLED = "cancelled"   # Отменено пользователем


@dataclass
class FunnelTask:
    """Задача на прохождение воронки"""
    id: int
    user_id: int
    url: str
    status: TaskStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    steps_total: int = 0
    paywall_reached: bool = False
    error: Optional[str] = None
    screenshot_path: Optional[str] = None
    log_path: Optional[str] = None
    manifest_path: Optional[str] = None
    drive_folder_url: Optional[str] = None
    last_url: Optional[str] = None
    progress_message: str = ""
    current_step: int = 0
    stop_requested: bool = False
    stop_requested_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Преобразование в словарь"""
        return {
            "id": self.id,
            "user_id": self.user_id,
            "url": self.url,
            "status": self.status.value,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "steps_total": self.steps_total,
            "paywall_reached": self.paywall_reached,
            "error": self.error,
            "screenshot_path": self.screenshot_path,
            "log_path": self.log_path,
            "manifest_path": self.manifest_path,
            "drive_folder_url": self.drive_folder_url,
            "last_url": self.last_url,
            "progress_message": self.progress_message,
            "current_step": self.current_step,
            "stop_requested": self.stop_requested,
            "stop_requested_at": self.stop_requested_at.isoformat() if self.stop_requested_at else None,
        }

    @classmethod
    def from_row(cls, row: tuple) -> "FunnelTask":
        """Создание из строки БД"""
        return cls(
            id=row[0],
            user_id=row[1],
            url=row[2],
            status=TaskStatus(row[3]),
            created_at=datetime.fromisoformat(row[4]) if row[4] else None,
            started_at=datetime.fromisoformat(row[5]) if row[5] else None,
            completed_at=datetime.fromisoformat(row[6]) if row[6] else None,
            steps_total=row[7] or 0,
            paywall_reached=bool(row[8]),
            error=row[9],
            screenshot_path=row[10],
            log_path=row[11],
            manifest_path=row[12],
            drive_folder_url=row[13],
            last_url=row[14],
            progress_message=row[15] or "",
            current_step=row[16] or 0,
            stop_requested=bool(row[17]) if len(row) > 17 else False,
            stop_requested_at=datetime.fromisoformat(row[18]) if len(row) > 18 and row[18] else None,
        )


class TaskManager:
    """Менеджер задач на основе SQLite"""

    def __init__(self, db_path: str = "tasks.db"):
        self.db_path = db_path
        self._init_db()
        self._lock = asyncio.Lock()

    def _init_db(self) -> None:
        """Инициализация базы данных"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                steps_total INTEGER DEFAULT 0,
                paywall_reached INTEGER DEFAULT 0,
                error TEXT,
                screenshot_path TEXT,
                log_path TEXT,
                manifest_path TEXT,
                drive_folder_url TEXT,
                last_url TEXT,
                progress_message TEXT DEFAULT '',
                current_step INTEGER DEFAULT 0,
                stop_requested INTEGER DEFAULT 0,
                stop_requested_at TEXT
            )
        """)

        cursor.execute("PRAGMA table_info(tasks)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        if "last_url" not in existing_columns:
            cursor.execute("ALTER TABLE tasks ADD COLUMN last_url TEXT")
        if "progress_message" not in existing_columns:
            cursor.execute("ALTER TABLE tasks ADD COLUMN progress_message TEXT DEFAULT ''")
        if "current_step" not in existing_columns:
            cursor.execute("ALTER TABLE tasks ADD COLUMN current_step INTEGER DEFAULT 0")
        if "stop_requested" not in existing_columns:
            cursor.execute("ALTER TABLE tasks ADD COLUMN stop_requested INTEGER DEFAULT 0")
        if "stop_requested_at" not in existing_columns:
            cursor.execute("ALTER TABLE tasks ADD COLUMN stop_requested_at TEXT")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS url_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                created_at TEXT NOT NULL,
                task_id INTEGER,
                FOREIGN KEY (task_id) REFERENCES tasks(id)
            )
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_tasks_user_id ON tasks(user_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_url_queue_user_id ON url_queue(user_id)
        """)

        conn.commit()
        conn.close()

    async def create_task(self, user_id: int, url: str) -> FunnelTask:
        """Создание новой задачи"""
        async with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            now = datetime.now().isoformat()
            cursor.execute("""
                INSERT INTO tasks (user_id, url, status, created_at)
                VALUES (?, ?, ?, ?)
            """, (user_id, url, TaskStatus.PENDING.value, now))

            task_id = cursor.lastrowid
            conn.commit()
            conn.close()

            return FunnelTask(
                id=task_id,
                user_id=user_id,
                url=url,
                status=TaskStatus.PENDING,
                created_at=datetime.now(),
            )

    async def get_task(self, task_id: int) -> Optional[FunnelTask]:
        """Получение задачи по ID"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
        row = cursor.fetchone()
        conn.close()

        if row:
            return FunnelTask.from_row(row)
        return None

    async def get_user_tasks(self, user_id: int, limit: int = 10) -> List[FunnelTask]:
        """Получение задач пользователя"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM tasks 
            WHERE user_id = ? 
            ORDER BY created_at DESC 
            LIMIT ?
        """, (user_id, limit))

        rows = cursor.fetchall()
        conn.close()

        return [FunnelTask.from_row(row) for row in rows]

    async def get_pending_tasks(self) -> List[FunnelTask]:
        """Получение всех ожидающих задач"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM tasks 
            WHERE status = ? 
            ORDER BY created_at ASC
        """, (TaskStatus.PENDING.value,))

        rows = cursor.fetchall()
        conn.close()

        return [FunnelTask.from_row(row) for row in rows]

    async def get_tasks_by_statuses(
        self,
        user_id: int,
        statuses: Sequence[TaskStatus],
        limit: int = 100,
    ) -> List[FunnelTask]:
        """Получение задач пользователя по списку статусов."""
        if not statuses:
            return []

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        placeholders = ", ".join("?" for _ in statuses)
        cursor.execute(f"""
            SELECT * FROM tasks
            WHERE user_id = ? AND status IN ({placeholders})
            ORDER BY created_at DESC
            LIMIT ?
        """, (user_id, *[status.value for status in statuses], limit))

        rows = cursor.fetchall()
        conn.close()
        return [FunnelTask.from_row(row) for row in rows]

    async def update_status(self, task_id: int, status: TaskStatus) -> None:
        """Обновление статуса задачи"""
        async with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            updates = ["status = ?"]
            params: List[Any] = [status.value]

            if status == TaskStatus.PROCESSING:
                updates.append("started_at = ?")
                params.append(datetime.now().isoformat())
                updates.append("stop_requested = 0")
                updates.append("stop_requested_at = NULL")
            elif status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
                updates.append("completed_at = ?")
                params.append(datetime.now().isoformat())

            params.append(task_id)
            cursor.execute(f"""
                UPDATE tasks 
                SET {', '.join(updates)}
                WHERE id = ?
            """, params)

            conn.commit()
            conn.close()

    async def update_progress(self, task_id: int, current_step: int,
                              total_steps: int, message: str = "",
                              last_url: Optional[str] = None) -> None:
        """Обновление прогресса задачи"""
        async with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            updates = ["current_step = ?", "steps_total = ?", "progress_message = ?"]
            params: List[Any] = [current_step, total_steps, message]
            if last_url is not None:
                updates.append("last_url = ?")
                params.append(last_url)
            params.append(task_id)

            cursor.execute(f"""
                UPDATE tasks
                SET {', '.join(updates)}
                WHERE id = ?
            """, params)

            conn.commit()
            conn.close()

    async def complete_task(self, task_id: int, steps_total: int, 
                           paywall_reached: bool, error: Optional[str] = None,
                           screenshot_path: Optional[str] = None,
                           log_path: Optional[str] = None,
                           manifest_path: Optional[str] = None,
                           drive_folder_url: Optional[str] = None,
                            last_url: Optional[str] = None,
                            final_status: Optional[TaskStatus] = None,
                            progress_message: Optional[str] = None) -> None:
        """Завершение задачи с результатами"""
        async with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE tasks 
                SET status = ?, completed_at = ?, steps_total = ?, 
                    paywall_reached = ?, error = ?, 
                    screenshot_path = ?, log_path = ?, 
                    manifest_path = ?, drive_folder_url = ?,
                    last_url = ?, progress_message = ?, stop_requested = 0
                WHERE id = ?
            """, (
                (final_status or (TaskStatus.FAILED if error else TaskStatus.COMPLETED)).value,
                datetime.now().isoformat(),
                steps_total,
                paywall_reached,
                error,
                screenshot_path,
                log_path,
                manifest_path,
                drive_folder_url,
                last_url,
                progress_message or "",
                task_id,
            ))

            conn.commit()
            conn.close()

    async def cancel_task(self, task_id: int) -> bool:
        """Совместимость: перенаправляет на мягкую остановку задачи."""
        return await self.request_stop(task_id)

    async def request_stop(self, task_id: int) -> bool:
        """Запрашивает остановку задачи с сохранением текущего состояния."""
        async with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
            row = cursor.fetchone()
            if not row:
                conn.close()
                return False

            status = TaskStatus(row[0])
            now = datetime.now().isoformat()

            if status == TaskStatus.PENDING:
                cursor.execute("""
                    UPDATE tasks
                    SET status = ?, completed_at = ?, stop_requested = 0,
                        stop_requested_at = ?, progress_message = ?
                    WHERE id = ?
                """, (
                    TaskStatus.CANCELLED.value,
                    now,
                    now,
                    "Остановлено до запуска",
                    task_id,
                ))
            elif status == TaskStatus.PROCESSING:
                cursor.execute("""
                    UPDATE tasks
                    SET stop_requested = 1, stop_requested_at = ?, progress_message = ?
                    WHERE id = ?
                """, (
                    now,
                    "Запрошена остановка, ожидается безопасное завершение текущего шага",
                    task_id,
                ))
            else:
                conn.close()
                return False

            affected = cursor.rowcount
            conn.commit()
            conn.close()

            return affected > 0

    async def request_stop_for_user_active_tasks(self, user_id: int) -> List[int]:
        """Запрашивает остановку всех активных задач пользователя."""
        active_tasks = await self.get_tasks_by_statuses(
            user_id,
            [TaskStatus.PENDING, TaskStatus.PROCESSING],
            limit=100,
        )
        stopped_ids: List[int] = []
        for task in active_tasks:
            if await self.request_stop(task.id):
                stopped_ids.append(task.id)
        return stopped_ids

    async def is_stop_requested(self, task_id: int) -> bool:
        """Проверяет, запрошена ли остановка для задачи."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT stop_requested FROM tasks WHERE id = ?", (task_id,))
        row = cursor.fetchone()
        conn.close()
        return bool(row and row[0])

    async def get_active_task_count(self) -> int:
        """Получение количества активных задач"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT COUNT(*) FROM tasks
            WHERE status = ?
        """, (TaskStatus.PROCESSING.value,))

        count = cursor.fetchone()[0]
        conn.close()

        return count

    # ====================
    # Методы для очереди URL
    # ====================

    async def add_urls_to_queue(self, user_id: int, urls: List[str]) -> int:
        """
        Добавление списка URL в очередь
        
        Args:
            user_id: ID пользователя
            urls: Список URL
            
        Returns:
            Количество добавленных URL
        """
        async with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            now = datetime.now().isoformat()
            added_count = 0
            
            for url in urls:
                try:
                    cursor.execute("""
                        INSERT INTO url_queue (user_id, url, created_at)
                        VALUES (?, ?, ?)
                    """, (user_id, url, now))
                    added_count += 1
                except Exception as e:
                    logger.debug(f"Ошибка добавления URL в очередь: {e}")

            conn.commit()
            conn.close()

            return added_count

    async def get_queued_urls_count(self, user_id: int) -> int:
        """Получение количества URL в очереди"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT COUNT(*) FROM url_queue
            WHERE user_id = ?
        """, (user_id,))

        count = cursor.fetchone()[0]
        conn.close()

        return count

    async def pop_queued_urls(self, user_id: int, limit: int) -> List[str]:
        """
        Получение и удаление URL из очереди
        
        Args:
            user_id: ID пользователя
            limit: Максимальное количество URL
            
        Returns:
            Список URL
        """
        async with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Получаем URL
            cursor.execute("""
                SELECT id, url FROM url_queue
                WHERE user_id = ?
                ORDER BY created_at ASC
                LIMIT ?
            """, (user_id, limit))

            rows = cursor.fetchall()
            urls = [row[1] for row in rows]
            ids = [row[0] for row in rows]

            # Удаляем полученные URL
            if ids:
                placeholders = ','.join('?' * len(ids))
                cursor.execute(f"""
                    DELETE FROM url_queue
                    WHERE id IN ({placeholders})
                """, ids)

            conn.commit()
            conn.close()

            return urls

    async def get_all_users_with_queued_urls(self) -> List[int]:
        """Получение списка всех пользователей с URL в очереди"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT DISTINCT user_id FROM url_queue
        """)

        rows = cursor.fetchall()
        user_ids = [row[0] for row in rows]

        conn.close()
        return user_ids

    async def get_all_tasks(self, limit: int = 100) -> List[FunnelTask]:
        """Получение всех задач (для отправки архива)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM tasks
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,))

        rows = cursor.fetchall()
        conn.close()

        return [FunnelTask.from_row(row) for row in rows]
