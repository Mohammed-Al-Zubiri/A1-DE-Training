"""Application configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _to_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    db_path: str = "data/books.db"
    source_url: str = "https://books.toscrape.com/"
    request_timeout: int = 15
    scrape_on_start: bool = False
    max_pages_per_category: int = 0
    default_page_size: int = 20

    @property
    def resolved_db_path(self) -> Path:
        path = Path(self.db_path)
        if path.is_absolute():
            return path
        return Path.cwd() / path


def load_settings() -> Settings:
    return Settings(
        app_host=os.getenv("SCRAPING_SERVICE_HOST", "0.0.0.0"),
        app_port=int(os.getenv("SCRAPING_SERVICE_PORT", "8000")),
        db_path=os.getenv("SCRAPING_SERVICE_DB_PATH", "data/books.db"),
        source_url=os.getenv("SCRAPING_SERVICE_SOURCE_URL", "https://books.toscrape.com/"),
        request_timeout=int(os.getenv("SCRAPING_SERVICE_REQUEST_TIMEOUT", "15")),
        scrape_on_start=_to_bool(os.getenv("SCRAPING_SERVICE_SCRAPE_ON_START"), False),
        max_pages_per_category=int(os.getenv("SCRAPING_SERVICE_MAX_PAGES_PER_CATEGORY", "0")),
        default_page_size=int(os.getenv("SCRAPING_SERVICE_DEFAULT_PAGE_SIZE", "20")),
    )
