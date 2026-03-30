"""
Конфигурация проекта Quiz Funnel Runner
"""
import json
import os
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional
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
    fill_values: Dict[str, str] = field(default_factory=lambda: {
        "name": "John",
        "email": "testuser{ts}@gmail.com",
        "age": "30",
        "height": "170",
        "weight": "70",
        "goal_weight": "60",
        "default_number": "25",
        "date_of_birth": "01/01/1990",
    })


@dataclass
class GoogleDriveConfig:
    """Конфигурация Google Drive"""
    enabled: bool = False
    credentials_file: str = "credentials.json"
    token_file: str = "token.json"
    folder_id: str = ""  # ID корневой папки для загрузки
    root_folder_name: str = "Quiz Funnel Runner Results"
    max_parallel_uploads: int = 2


@dataclass
class CaptchaConfig:
    """Конфигурация CAPTCHA solving"""
    enabled: bool = False
    service: str = "2captcha"  # 2captcha, anticaptcha, capsolver
    api_key: str = ""


def _parse_bool(value, default: bool) -> bool:
    """Безопасный парсинг bool-значения."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return default


def _normalize_user_ids(raw_value, *, field_name: str) -> List[int]:
    """Нормализует список Telegram user id из строки/числа/массива."""
    if raw_value is None:
        return []

    if isinstance(raw_value, int):
        return [raw_value]

    if isinstance(raw_value, str):
        parts = [part.strip() for part in raw_value.split(",")]
        values: Iterable[str] = [part for part in parts if part]
    elif isinstance(raw_value, (list, tuple, set)):
        values = raw_value
    else:
        return []

    normalized_ids: List[int] = []
    seen_ids = set()

    for value in values:
        text_value = str(value).strip()
        if not text_value:
            continue
        try:
            parsed_id = int(text_value)
        except (TypeError, ValueError):
            raise ValueError(f"{field_name} must contain only integer Telegram user IDs")

        if parsed_id <= 0:
            raise ValueError(f"{field_name} must contain only positive integer Telegram user IDs")

        if parsed_id not in seen_ids:
            normalized_ids.append(parsed_id)
            seen_ids.add(parsed_id)

    return normalized_ids


def _resolve_admin_ids(bot_data: Dict) -> List[int]:
    """Читает список администраторов из .env с обратной совместимостью."""
    admin_ids_env = os.getenv("ADMIN_IDS", "").strip()
    legacy_admin_id_env = os.getenv("TELEGRAM_ADMIN_ID", "").strip()

    if admin_ids_env:
        return _normalize_user_ids(admin_ids_env, field_name="ADMIN_IDS")

    if legacy_admin_id_env:
        return _normalize_user_ids(legacy_admin_id_env, field_name="TELEGRAM_ADMIN_ID")

    admin_ids_value = bot_data.get("admin_ids")
    if admin_ids_value is not None:
        return _normalize_user_ids(admin_ids_value, field_name="bot.admin_ids")

    legacy_admin_id_value = bot_data.get("admin_id")
    if legacy_admin_id_value is not None:
        return _normalize_user_ids(legacy_admin_id_value, field_name="bot.admin_id")

    return []


@dataclass
class Config:
    """Основная конфигурация"""
    bot: BotConfig = field(default_factory=BotConfig)
    runner: RunnerConfig = field(default_factory=RunnerConfig)
    google_drive: GoogleDriveConfig = field(default_factory=GoogleDriveConfig)
    captcha: CaptchaConfig = field(default_factory=CaptchaConfig)
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

        admin_ids = _resolve_admin_ids(bot_data)
        allowed_users = _normalize_user_ids(
            bot_data.get("allowed_users", []),
            field_name="bot.allowed_users",
        )

        runner_data = data.get("runner", {})
        drive_data = data.get("google_drive", {})
        captcha_data = data.get("captcha", {})
        
        # Загружаем Google Drive настройки из .env (имеют приоритет)
        drive_enabled_env = os.getenv("GOOGLE_DRIVE_ENABLED", "").strip().lower()
        if drive_enabled_env in {"1", "true", "yes", "on"}:
            drive_enabled = True
        elif drive_enabled_env in {"0", "false", "no", "off"}:
            drive_enabled = False
        else:
            drive_enabled = drive_data.get("enabled", False)

        drive_credentials_file = os.getenv("GOOGLE_DRIVE_CREDENTIALS_FILE", "").strip()
        if not drive_credentials_file:
            drive_credentials_file = drive_data.get("credentials_file", "credentials.json")

        drive_token_file = os.getenv("GOOGLE_DRIVE_TOKEN_FILE", "").strip()
        if not drive_token_file:
            drive_token_file = drive_data.get("token_file", "token.json")
        
        drive_folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "").strip()
        if not drive_folder_id:
            drive_folder_id = drive_data.get("folder_id", "")

        drive_root_folder_name = os.getenv("GOOGLE_DRIVE_ROOT_FOLDER_NAME", "").strip()
        if not drive_root_folder_name:
            drive_root_folder_name = drive_data.get("root_folder_name", "Quiz Funnel Runner Results")

        drive_max_parallel_uploads_raw = os.getenv("GOOGLE_DRIVE_MAX_PARALLEL_UPLOADS", "").strip()
        if drive_max_parallel_uploads_raw:
            try:
                drive_max_parallel_uploads = max(1, int(drive_max_parallel_uploads_raw))
            except ValueError:
                drive_max_parallel_uploads = max(1, int(drive_data.get("max_parallel_uploads", 2)))
        else:
            drive_max_parallel_uploads = max(1, int(drive_data.get("max_parallel_uploads", 2)))

        return cls(
            bot=BotConfig(
                token=token,
                admin_ids=admin_ids,
                allowed_users=allowed_users,
                use_only_admin=_parse_bool(bot_data.get("use_only_admin", True), True),
            ),
            runner=RunnerConfig(
                max_steps=runner_data.get("max_steps", 80),
                max_funnels=runner_data.get("max_funnels"),
                slow_mo_ms=runner_data.get("slow_mo_ms", 100),
                headless=runner_data.get("headless", True),
                fill_values=runner_data.get("fill_values", {
                    "name": "John",
                    "email": "testuser{ts}@gmail.com",
                    "age": "30",
                    "height": "170",
                    "weight": "70",
                    "goal_weight": "60",
                    "default_number": "25",
                    "date_of_birth": "01/01/1990",
                }),
            ),
            google_drive=GoogleDriveConfig(
                enabled=drive_enabled,
                credentials_file=drive_credentials_file,
                token_file=drive_token_file,
                folder_id=drive_folder_id,
                root_folder_name=drive_root_folder_name,
                max_parallel_uploads=drive_max_parallel_uploads,
            ),
            captcha=CaptchaConfig(
                enabled=captcha_data.get("enabled", False),
                service=captcha_data.get("service", "2captcha"),
                api_key=captcha_data.get("api_key", ""),
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
                "token_file": self.google_drive.token_file,
                "folder_id": self.google_drive.folder_id,
                "root_folder_name": self.google_drive.root_folder_name,
                "max_parallel_uploads": self.google_drive.max_parallel_uploads,
            },
            "captcha": {
                "enabled": self.captcha.enabled,
                "service": self.captcha.service,
                "api_key": self.captcha.api_key,
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
