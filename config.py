"""
Конфигурация проекта Quiz Funnel Runner
"""

import json
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from dotenv import load_dotenv

# Загрузка переменных окружения из .env
load_dotenv()


@dataclass
class BotConfig:
    """Конфигурация Telegram бота"""

    token: str = ""
    admin_ids: List[int] = field(default_factory=list)
    allowed_users: List[int] = field(default_factory=list)
    use_only_admin: bool = True  # Если True, только admin_ids могут использовать бота


@dataclass
class RunnerConfig:
    """Конфигурация раннера воронок"""

    max_steps: int = 80
    max_funnels: Optional[int] = None
    slow_mo_ms: int = 100
    headless: bool = True
    fill_values: Dict[str, str] = field(
        default_factory=lambda: {
            "name": "John",
            "email": "testuser{ts}@gmail.com",
            "age": "30",
            "height": "170",
            "weight": "70",
            "goal_weight": "60",
            "default_number": "25",
            "date_of_birth": "01/01/1990",
        }
    )


@dataclass
class GoogleDriveConfig:
    """Конфигурация Google Drive"""

    enabled: bool = False
    credentials_file: str = "credentials.json"
    folder_id: str = ""  # ID корневой папки для загрузки


@dataclass
class CaptchaConfig:
    """Конфигурация CAPTCHA solving"""

    enabled: bool = False
    service: str = "2captcha"  # 2captcha, anticaptcha, capsolver
    api_key: str = ""


@dataclass
class AiFallbackConfig:
    """Конфигурация AI fallback для прогресса на зависаниях."""

    enabled: bool = False
    model: str = "gpt-5.4-nano"
    max_calls_per_stuck: int = 2
    max_candidates: int = 16
    text_char_limit: int = 16000
    timeout_seconds: float = 30.0
    max_concurrent_requests: int = 2


@dataclass
class Config:
    """Основная конфигурация"""

    bot: BotConfig = field(default_factory=BotConfig)
    runner: RunnerConfig = field(default_factory=RunnerConfig)
    google_drive: GoogleDriveConfig = field(default_factory=GoogleDriveConfig)
    captcha: CaptchaConfig = field(default_factory=CaptchaConfig)
    ai_fallback: AiFallbackConfig = field(default_factory=AiFallbackConfig)
    funnels: List[str] = field(default_factory=list)

    @classmethod
    def load(cls, path: str = "config.json") -> "Config":
        """Загрузка конфигурации из JSON файла"""
        if not os.path.exists(path):
            # Создаем конфиг по умолчанию
            default_config = cls()
            default_config.save(path)
            return default_config

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        bot_data = data.get("bot", {})

        # Загружаем токен из переменных окружения (имеет приоритет)
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        if not token:
            token = bot_data.get("token", "")

        # Загружаем admin_id из переменных окружения (имеет приоритет)
        admin_id = os.getenv("TELEGRAM_ADMIN_ID", "").strip()
        admin_ids = bot_data.get("admin_ids", [])
        if admin_id:
            try:
                admin_ids = [int(admin_id)]
            except ValueError:
                pass

        runner_data = data.get("runner", {})
        drive_data = data.get("google_drive", {})
        captcha_data = data.get("captcha", {})
        ai_data = data.get("ai_fallback", {})

        # Загружаем Google Drive настройки из .env (имеют приоритет)
        drive_credentials_file = os.getenv("GOOGLE_DRIVE_CREDENTIALS_FILE", "").strip()
        if not drive_credentials_file:
            drive_credentials_file = drive_data.get(
                "credentials_file", "credentials.json"
            )

        drive_folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "").strip()
        if not drive_folder_id:
            drive_folder_id = drive_data.get("folder_id", "")

        return cls(
            bot=BotConfig(
                token=token,
                admin_ids=admin_ids,
                allowed_users=bot_data.get("allowed_users", []),
                use_only_admin=bot_data.get("use_only_admin", True),
            ),
            runner=RunnerConfig(
                max_steps=runner_data.get("max_steps", 80),
                max_funnels=runner_data.get("max_funnels"),
                slow_mo_ms=runner_data.get("slow_mo_ms", 100),
                headless=runner_data.get("headless", True),
                fill_values=runner_data.get(
                    "fill_values",
                    {
                        "name": "John",
                        "email": "testuser{ts}@gmail.com",
                        "age": "30",
                        "height": "170",
                        "weight": "70",
                        "goal_weight": "60",
                        "default_number": "25",
                        "date_of_birth": "01/01/1990",
                    },
                ),
            ),
            google_drive=GoogleDriveConfig(
                enabled=drive_data.get("enabled", False),
                credentials_file=drive_credentials_file,
                folder_id=drive_folder_id,
            ),
            captcha=CaptchaConfig(
                enabled=captcha_data.get("enabled", False),
                service=captcha_data.get("service", "2captcha"),
                api_key=captcha_data.get("api_key", ""),
            ),
            ai_fallback=AiFallbackConfig(
                enabled=ai_data.get("enabled", False),
                model=ai_data.get("model", "gpt-5.4-nano"),
                max_calls_per_stuck=ai_data.get("max_calls_per_stuck", 2),
                max_candidates=ai_data.get("max_candidates", 16),
                text_char_limit=ai_data.get("text_char_limit", 16000),
                timeout_seconds=ai_data.get("timeout_seconds", 30.0),
                max_concurrent_requests=ai_data.get("max_concurrent_requests", 2),
            ),
            funnels=data.get("funnels", []),
        )

    def save(self, path: str = "config.json") -> None:
        """Сохранение конфигурации в JSON файл"""
        data = {
            "bot": {
                "token": self.bot.token,
                "admin_ids": self.bot.admin_ids,
                "allowed_users": self.bot.allowed_users,
                "use_only_admin": self.bot.use_only_admin,
            },
            "runner": {
                "max_steps": self.runner.max_steps,
                "max_funnels": self.runner.max_funnels,
                "slow_mo_ms": self.runner.slow_mo_ms,
                "headless": self.runner.headless,
                "fill_values": self.runner.fill_values,
            },
            "google_drive": {
                "enabled": self.google_drive.enabled,
                "credentials_file": self.google_drive.credentials_file,
                "folder_id": self.google_drive.folder_id,
            },
            "captcha": {
                "enabled": self.captcha.enabled,
                "service": self.captcha.service,
                "api_key": self.captcha.api_key,
            },
            "ai_fallback": {
                "enabled": self.ai_fallback.enabled,
                "model": self.ai_fallback.model,
                "max_calls_per_stuck": self.ai_fallback.max_calls_per_stuck,
                "max_candidates": self.ai_fallback.max_candidates,
                "text_char_limit": self.ai_fallback.text_char_limit,
                "timeout_seconds": self.ai_fallback.timeout_seconds,
                "max_concurrent_requests": self.ai_fallback.max_concurrent_requests,
            },
            "funnels": self.funnels,
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)


# Глобальный экземляр конфигурации
config: Optional[Config] = None


def get_config() -> Config:
    """Получение глобальной конфигурации"""
    global config
    if config is None:
        config = Config.load()
    return config


def init_config(path: str = "config.json") -> Config:
    """Инициализация глобальной конфигурации"""
    global config
    config = Config.load(path)
    return config
