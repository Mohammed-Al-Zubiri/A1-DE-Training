"""SQLite persistence layer."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS books (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    price REAL NOT NULL,
    rating INTEGER NOT NULL,
    availability TEXT NOT NULL,
    category TEXT NOT NULL,
    product_url TEXT NOT NULL UNIQUE,
    image_url TEXT,
    scraped_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_books_category ON books(category);
CREATE INDEX IF NOT EXISTS idx_books_rating ON books(rating);
CREATE INDEX IF NOT EXISTS idx_books_scraped_at ON books(scraped_at);
"""


def _connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def init_db(db_path: str) -> None:
    with _connect(db_path) as connection:
        connection.executescript(SCHEMA_SQL)
        connection.commit()


def upsert_books(db_path: str, books: list[dict[str, Any]]) -> int:
    if not books:
        return 0

    sql = """
    INSERT INTO books (
        id, title, price, rating, availability, category, product_url, image_url, scraped_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(product_url) DO UPDATE SET
        id=excluded.id,
        title=excluded.title,
        price=excluded.price,
        rating=excluded.rating,
        availability=excluded.availability,
        category=excluded.category,
        image_url=excluded.image_url,
        scraped_at=excluded.scraped_at;
    """

    values = [
        (
            book["id"],
            book["title"],
            book["price"],
            book["rating"],
            book["availability"],
            book["category"],
            book["product_url"],
            book.get("image_url"),
            book["scraped_at"],
        )
        for book in books
    ]

    with _connect(db_path) as connection:
        connection.executemany(sql, values)
        connection.commit()

    return len(values)


def list_books(
    db_path: str,
    limit: int,
    offset: int,
    category: str | None = None,
    min_rating: int | None = None,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    if category:
        clauses.append("category = ?")
        params.append(category)
    if min_rating is not None:
        clauses.append("rating >= ?")
        params.append(min_rating)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"""
        SELECT *
        FROM books
        {where_sql}
        ORDER BY scraped_at DESC, title ASC
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])

    with _connect(db_path) as connection:
        rows = connection.execute(sql, params).fetchall()

    return [dict(row) for row in rows]


def count_books(db_path: str, category: str | None = None, min_rating: int | None = None) -> int:
    clauses: list[str] = []
    params: list[Any] = []

    if category:
        clauses.append("category = ?")
        params.append(category)
    if min_rating is not None:
        clauses.append("rating >= ?")
        params.append(min_rating)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"SELECT COUNT(*) AS total FROM books {where_sql}"

    with _connect(db_path) as connection:
        row = connection.execute(sql, params).fetchone()

    return int(row["total"]) if row else 0


def get_book(db_path: str, book_id: str) -> dict[str, Any] | None:
    with _connect(db_path) as connection:
        row = connection.execute("SELECT * FROM books WHERE id = ?", (book_id,)).fetchone()

    return dict(row) if row else None


def category_stats(db_path: str) -> list[dict[str, Any]]:
    sql = """
    SELECT category, COUNT(*) AS book_count
    FROM books
    GROUP BY category
    ORDER BY book_count DESC, category ASC
    """

    with _connect(db_path) as connection:
        rows = connection.execute(sql).fetchall()

    return [dict(row) for row in rows]


def rating_stats(db_path: str) -> list[dict[str, Any]]:
    sql = """
    SELECT rating, COUNT(*) AS book_count
    FROM books
    GROUP BY rating
    ORDER BY rating DESC
    """

    with _connect(db_path) as connection:
        rows = connection.execute(sql).fetchall()

    return [dict(row) for row in rows]
