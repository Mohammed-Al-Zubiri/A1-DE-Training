"""FastAPI entrypoint for the scraping service."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query

from .config import Settings, load_settings
from .database import category_stats, count_books, get_book, init_db, list_books, rating_stats
from .pipeline import run_scrape
from .schemas import BookOut, BookPage, CategoryStat, HealthResponse, RatingStat, ScrapeResponse

settings: Settings = load_settings()


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    init_db(str(settings.resolved_db_path))
    if settings.scrape_on_start:
        run_scrape(
            db_path=str(settings.resolved_db_path),
            source_url=settings.source_url,
            timeout=settings.request_timeout,
            max_pages_per_category=settings.max_pages_per_category,
        )
    yield

app = FastAPI(
    title="Books Scraping Service",
    description="Scrape books.toscrape.com, clean records, and expose them through API endpoints.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok", database_path=str(settings.resolved_db_path))


@app.get("/books", response_model=BookPage)
def get_books(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=settings.default_page_size, ge=1, le=200),
    category: str | None = Query(default=None),
    min_rating: int | None = Query(default=None, ge=1, le=5),
) -> BookPage:
    offset = (page - 1) * page_size
    items = list_books(
        db_path=str(settings.resolved_db_path),
        limit=page_size,
        offset=offset,
        category=category,
        min_rating=min_rating,
    )
    total = count_books(str(settings.resolved_db_path), category=category, min_rating=min_rating)

    return BookPage(items=[BookOut(**item) for item in items], total=total, page=page, page_size=page_size)


@app.get("/books/{book_id}", response_model=BookOut)
def get_book_by_id(book_id: str) -> BookOut:
    item = get_book(str(settings.resolved_db_path), book_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Book not found")
    return BookOut(**item)


@app.get("/stats/categories", response_model=list[CategoryStat])
def get_category_stats() -> list[CategoryStat]:
    rows = category_stats(str(settings.resolved_db_path))
    return [CategoryStat(**row) for row in rows]


@app.get("/stats/ratings", response_model=list[RatingStat])
def get_rating_stats() -> list[RatingStat]:
    rows = rating_stats(str(settings.resolved_db_path))
    return [RatingStat(**row) for row in rows]


@app.post("/scrape/trigger", response_model=ScrapeResponse)
def trigger_scrape() -> ScrapeResponse:
    result = run_scrape(
        db_path=str(settings.resolved_db_path),
        source_url=settings.source_url,
        timeout=settings.request_timeout,
        max_pages_per_category=settings.max_pages_per_category,
    )
    return ScrapeResponse(**result)
