"""Pydantic models for API I/O."""

from __future__ import annotations

from pydantic import BaseModel, Field


class BookOut(BaseModel):
    id: str
    title: str
    price: float
    rating: int
    availability: str
    category: str
    product_url: str
    image_url: str | None = None
    scraped_at: str


class BookPage(BaseModel):
    items: list[BookOut]
    total: int
    page: int
    page_size: int = Field(ge=1, le=200)


class CategoryStat(BaseModel):
    category: str
    book_count: int


class RatingStat(BaseModel):
    rating: int
    book_count: int


class ScrapeResponse(BaseModel):
    scraped: int
    cleaned: int
    stored: int
    errors: int
    timestamp: str


class HealthResponse(BaseModel):
    status: str
    database_path: str
