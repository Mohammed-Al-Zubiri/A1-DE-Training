"""Command-line entry points for service operations."""

from __future__ import annotations

import argparse
import json

import uvicorn

from .config import load_settings
from .database import init_db
from .pipeline import run_scrape


def _run_api() -> None:
    settings = load_settings()
    init_db(str(settings.resolved_db_path))
    uvicorn.run("scraping_service.api:app", host=settings.app_host, port=settings.app_port)


def _run_scrape() -> None:
    settings = load_settings()
    init_db(str(settings.resolved_db_path))
    result = run_scrape(
        db_path=str(settings.resolved_db_path),
        source_url=settings.source_url,
        timeout=settings.request_timeout,
        max_pages_per_category=settings.max_pages_per_category,
    )
    print(json.dumps(result, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Books scraping service CLI")
    parser.add_argument("command", choices=["api", "scrape"], help="Action to run")
    args = parser.parse_args()

    if args.command == "api":
        _run_api()
    elif args.command == "scrape":
        _run_scrape()


if __name__ == "__main__":
    main()
