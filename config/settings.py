import os
from dataclasses import dataclass
from dotenv import load_dotenv
from typing import Optional

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
ENV_PATH = os.path.join(ROOT_DIR, '.env')

# Load environment variables from project root .env if it exists
load_dotenv(dotenv_path=ENV_PATH)


def env_str(key: str, default: Optional[str] = None) -> Optional[str]:
    return os.environ.get(key, default)


def env_int(key: str, default: Optional[int] = None) -> Optional[int]:
    val = os.environ.get(key)
    if val is None:
        return default
    try:
        return int(val)
    except ValueError:
        return default


def env_float(key: str, default: Optional[float] = None) -> Optional[float]:
    val = os.environ.get(key)
    if val is None:
        return default
    try:
        return float(val)
    except ValueError:
        return default

def env_bool(key: str, default: bool = False) -> bool:
    val = os.environ.get(key, str(default)).lower()
    return val in ('true', '1', 't', 'y', 'yes')


@dataclass
class DBConfig:
    host: str
    port: int
    name: str
    user: str
    password: str


@dataclass
class ScrapeConfig:
    user_agent: str
    request_delay_seconds: float
    request_timeout_seconds: int
    use_proxy_rotator: bool


@dataclass
class AppConfig:
    db: DBConfig
    scrape: ScrapeConfig


def load_config() -> AppConfig:
    db = DBConfig(
        host=env_str('DB_HOST', '127.0.0.1'),
        port=env_int('DB_PORT', 3306),
        name=env_str('DB_NAME', 'nyc_apartments'),
        user=env_str('DB_USER', 'nyc_user'),
        password=env_str('DB_PASSWORD', 'nyc_password'),
    )
    scrape = ScrapeConfig(
        user_agent=env_str('USER_AGENT', 'Mozilla/5.0'),
        request_delay_seconds=env_float('REQUEST_DELAY_SECONDS', 2.5),
        request_timeout_seconds=env_int('REQUEST_TIMEOUT_SECONDS', 20),
        use_proxy_rotator=env_bool('USE_PROXY_ROTATOR', False),
    )
    return AppConfig(db=db, scrape=scrape)
