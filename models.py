"""
Модели данных для Quiz Funnel Runner
"""
import sqlite3
import json
import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any
from pathlib import Path


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
    progress_message: str = ""
    current_step: int = 0

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
            "progress_message": self.progress_message,
            "current_step": self.current_step,
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
            progress_message=row[14] or "",
            current_step=row[15] or 0,
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
                progress_message TEXT DEFAULT '',
                current_step INTEGER DEFAULT 0
            )
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_tasks_user_id ON tasks(user_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)
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
                              total_steps: int, message: str = "") -> None:
        """Обновление прогресса задачи"""
        async with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE tasks 
                SET current_step = ?, steps_total = ?, progress_message = ?
                WHERE id = ?
            """, (current_step, total_steps, message, task_id))

            conn.commit()
            conn.close()

    async def complete_task(self, task_id: int, steps_total: int, 
                           paywall_reached: bool, error: Optional[str] = None,
                           screenshot_path: Optional[str] = None,
                           log_path: Optional[str] = None,
                           manifest_path: Optional[str] = None,
                           drive_folder_url: Optional[str] = None) -> None:
        """Завершение задачи с результатами"""
        async with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE tasks 
                SET status = ?, completed_at = ?, steps_total = ?, 
                    paywall_reached = ?, error = ?, 
                    screenshot_path = ?, log_path = ?, 
                    manifest_path = ?, drive_folder_url = ?
                WHERE id = ?
            """, (
                TaskStatus.FAILED.value if error else TaskStatus.COMPLETED.value,
                datetime.now().isoformat(),
                steps_total,
                paywall_reached,
                error,
                screenshot_path,
                log_path,
                manifest_path,
                drive_folder_url,
                task_id,
            ))

            conn.commit()
            conn.close()

    async def cancel_task(self, task_id: int) -> bool:
        """Отмена задачи"""
        async with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Можно отменить только pending или processing задачи
            cursor.execute("""
                UPDATE tasks 
                SET status = ?, completed_at = ?
                WHERE id = ? AND status IN (?, ?)
            """, (
                TaskStatus.CANCELLED.value,
                datetime.now().isoformat(),
                task_id,
                TaskStatus.PENDING.value,
                TaskStatus.PROCESSING.value,
            ))

            affected = cursor.rowcount
            conn.commit()
            conn.close()

            return affected > 0

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
