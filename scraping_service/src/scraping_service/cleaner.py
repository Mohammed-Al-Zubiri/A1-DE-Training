"""Data cleaning and normalization helpers."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any

RATING_MAP = {
    "One": 1,
    "Two": 2,
    "Three": 3,
    "Four": 4,
    "Five": 5,
}

PRICE_PATTERN = re.compile(r"([0-9]+(?:\.[0-9]+)?)")


def _clean_text(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(value.split()).strip()


def parse_price(price_text: str) -> float:
    cleaned = _clean_text(price_text).replace(",", "")
    match = PRICE_PATTERN.search(cleaned)
    if not match:
        raise ValueError(f"Unable to parse price from: {price_text!r}")
    return float(match.group(1))


def parse_rating(rating_text: str) -> int:
    rating = RATING_MAP.get(_clean_text(rating_text), 0)
    if rating == 0:
        raise ValueError(f"Unknown rating value: {rating_text!r}")
    return rating


def normalize_availability(value: str) -> str:
    text = _clean_text(value)
    if "In stock" in text:
        return "In stock"
    if "Out of stock" in text:
        return "Out of stock"
    return text


def make_book_id(product_url: str) -> str:
    return hashlib.sha1(product_url.encode("utf-8")).hexdigest()[:16]


def clean_book(raw: dict[str, Any]) -> dict[str, Any]:
    title = _clean_text(raw.get("title"))
    product_url = _clean_text(raw.get("product_url"))
    category = _clean_text(raw.get("category"))

    if not title or not product_url or not category:
        raise ValueError("Missing required fields: title, product_url, or category")

    return {
        "id": make_book_id(product_url),
        "title": title,
        "price": parse_price(str(raw.get("price_text", ""))),
        "rating": parse_rating(str(raw.get("rating_text", ""))),
        "availability": normalize_availability(str(raw.get("availability", ""))),
        "category": category,
        "product_url": product_url,
        "image_url": _clean_text(raw.get("image_url")),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


def clean_books(raw_books: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    seen_urls: set[str] = set()
    cleaned: list[dict[str, Any]] = []
    errors: list[str] = []

    for raw in raw_books:
        try:
            book = clean_book(raw)
            if book["product_url"] in seen_urls:
                continue
            seen_urls.add(book["product_url"])
            cleaned.append(book)
        except ValueError as exc:
            errors.append(str(exc))

    return cleaned, errors
