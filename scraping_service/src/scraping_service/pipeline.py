"""Pipeline orchestration for scrape -> clean -> store."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TypedDict

from .cleaner import clean_books
from .database import upsert_books
from .scraper import scrape_books


class ScrapeResult(TypedDict):
    scraped: int
    cleaned: int
    stored: int
    errors: int
    timestamp: str


def run_scrape(db_path: str, source_url: str, timeout: int, max_pages_per_category: int) -> ScrapeResult:
    raw_books = scrape_books(
        base_url=source_url,
        timeout=timeout,
        max_pages_per_category=max_pages_per_category,
    )
    cleaned_books, errors = clean_books(raw_books)
    stored_count = upsert_books(db_path, cleaned_books)

    return {
        "scraped": len(raw_books),
        "cleaned": len(cleaned_books),
        "stored": stored_count,
        "errors": len(errors),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
