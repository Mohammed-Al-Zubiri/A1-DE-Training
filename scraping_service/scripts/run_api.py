"""Run the FastAPI server with project-local imports."""

from __future__ import annotations

import sys
from pathlib import Path

import uvicorn


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from scraping_service.config import load_settings # noqa: E402


if __name__ == "__main__":
    settings = load_settings()
    uvicorn.run(
        "scraping_service.api:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=False,
    )
