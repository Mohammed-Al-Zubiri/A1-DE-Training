"""Run one scrape cycle and print summary."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from scraping_service.config import load_settings  # noqa: E402
from scraping_service.database import init_db  # noqa: E402
from scraping_service.pipeline import run_scrape  # noqa: E402


if __name__ == "__main__":
    settings = load_settings()
    init_db(str(settings.resolved_db_path))

    result = run_scrape(
        db_path=str(settings.resolved_db_path),
        source_url=settings.source_url,
        timeout=settings.request_timeout,
        max_pages_per_category=settings.max_pages_per_category,
    )
    print(json.dumps(result, indent=2))
