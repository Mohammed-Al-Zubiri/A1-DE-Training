"""Web scraping module for Books to Scrape."""

from __future__ import annotations

from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


def _attr_as_str(value: object) -> str:
    if isinstance(value, list):
        return str(value[0]) if value else ""
    return str(value) if value is not None else ""


def _fetch_soup(url: str, timeout: int) -> BeautifulSoup:
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def _category_links(base_url: str, timeout: int) -> list[tuple[str, str]]:
    soup = _fetch_soup(base_url, timeout=timeout)
    links: list[tuple[str, str]] = []

    for anchor in soup.select("div.side_categories ul li ul li a"):
        name = " ".join(anchor.get_text(strip=True).split())
        href = _attr_as_str(anchor.get("href"))
        if not href:
            continue
        links.append((name, urljoin(base_url, href)))

    return links


def _parse_book_cards(soup: BeautifulSoup, category: str, page_url: str) -> list[dict[str, Any]]:
    books: list[dict[str, Any]] = []
    for article in soup.select("article.product_pod"):
        title_anchor = article.select_one("h3 a")
        if title_anchor is None:
            continue

        product_href = _attr_as_str(title_anchor.get("href", ""))
        image = article.select_one("img")
        price = article.select_one("p.price_color")
        availability = article.select_one("p.instock.availability")
        rating = article.select_one("p.star-rating")
        image_src = _attr_as_str(image.get("src", "")) if image else ""

        books.append(
            {
                "title": title_anchor.get("title") or title_anchor.get_text(strip=True),
                "price_text": price.get_text(strip=True) if price else "",
                "rating_text": (rating.get("class") or ["", ""])[1] if rating else "",
                "availability": availability.get_text(" ", strip=True) if availability else "",
                "category": category,
                "product_url": urljoin(page_url, product_href),
                "image_url": urljoin(page_url, image_src) if image_src else "",
            }
        )

    return books


def _crawl_category(category: str, start_url: str, timeout: int, max_pages: int) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    page_count = 0
    current_url = start_url

    while current_url:
        page_count += 1
        if max_pages > 0 and page_count > max_pages:
            break

        soup = _fetch_soup(current_url, timeout=timeout)
        results.extend(_parse_book_cards(soup, category=category, page_url=current_url))

        next_anchor = soup.select_one("li.next a")
        if next_anchor is None:
            break

        next_href = _attr_as_str(next_anchor.get("href", ""))
        current_url = urljoin(current_url, next_href)

    return results


def scrape_books(base_url: str, timeout: int = 15, max_pages_per_category: int = 0) -> list[dict[str, Any]]:
    all_results: list[dict[str, Any]] = []
    for category, category_url in _category_links(base_url, timeout=timeout):
        all_results.extend(
            _crawl_category(
                category=category,
                start_url=category_url,
                timeout=timeout,
                max_pages=max_pages_per_category,
            )
        )
    return all_results
