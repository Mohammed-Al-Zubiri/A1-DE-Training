"""
Utility functions for the Olist ETL pipeline.
- Database connection factories (SQLite source, PostgreSQL target)
- Dimension surrogate key lookups
- Watermark management (etl_control table)
- Location key resolution
- Data cleaning helpers
"""

import sqlite3
import psycopg2
import psycopg2.extras
import logging
from datetime import date, datetime
from config import (
    SOURCE_DB,
    PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD,
    DEFAULT_WATERMARK, LOG_LEVEL
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Connection factories
# ---------------------------------------------------------------------------

def get_src_conn():
    """Return a read-only connection to the SQLite source database."""
    conn = sqlite3.connect(SOURCE_DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def get_pg_conn():
    """Return a connection to the PostgreSQL target warehouse with autocommit off."""
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    conn.autocommit = False
    return conn


# ---------------------------------------------------------------------------
# Watermark management (incremental ETL)
# ---------------------------------------------------------------------------

def get_last_extract(pg_conn, table_name):
    """
    Retrieve the last extracted watermark for a given fact table.
    Returns DEFAULT_WATERMARK if no previous run is recorded.
    """
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT last_extracted FROM etl_control WHERE table_name = %s",
            (table_name,)
        )
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
        return DEFAULT_WATERMARK


def set_last_extract(pg_conn, table_name, watermark):
    """
    Update or insert the watermark for a given fact table.
    Sets last_loaded to the current timestamp and status to 'SUCCESS'.
    """
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_control (table_name, last_extracted, last_loaded, status, row_count)
            VALUES (%s, %s, NOW(), 'SUCCESS', 0)
            ON CONFLICT (table_name) DO UPDATE SET
                last_extracted = EXCLUDED.last_extracted,
                last_loaded   = EXCLUDED.last_loaded,
                status        = EXCLUDED.status
            """,
            (table_name, watermark)
        )
    pg_conn.commit()


# ---------------------------------------------------------------------------
# Dimension surrogate key lookups
# ---------------------------------------------------------------------------

def lookup_dim(pg_conn, table, key_col, id_col, id_val):
    """
    Generic dimension lookup.
    Returns the surrogate key (integer) for a given natural key,
    or -1 if the natural key does not exist.
    """
    with pg_conn.cursor() as cur:
        cur.execute(
            f"SELECT {key_col} FROM {table} WHERE {id_col} = %s LIMIT 1",
            (id_val,)
        )
        row = cur.fetchone()
        return row[0] if row else -1


def lookup_customer_key(pg_conn, customer_unique_id):
    """Return customer_key for a customer_unique_id, or -1."""
    return lookup_dim(pg_conn, "dim_customer", "customer_key", "customer_unique_id", customer_unique_id)


def lookup_seller_key(pg_conn, seller_id):
    """Return seller_key for a seller_id, or -1."""
    return lookup_dim(pg_conn, "dim_seller", "seller_key", "seller_id", seller_id)

def lookup_review_comment_key(pg_conn, title, msg):
    """Return review_comment_key for a given title and message, or -1."""
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT review_comment_key FROM dim_review_comment
            WHERE review_comment_title IS NOT DISTINCT FROM %s
              AND review_comment_message IS NOT DISTINCT FROM %s
            LIMIT 1
            """,
            (title, msg)
        )
        row = cur.fetchone()
        return row[0] if row else -1


def lookup_payment_type_key(pg_conn, payment_type_code):
    """Return payment_type_key for a payment_type_code, or -1."""
    return lookup_dim(pg_conn, "dim_payment_type", "payment_type_key", "payment_type_code", payment_type_code)


def lookup_product_key(pg_conn, product_id, as_of_date):
    """
    Return the product_key that was active on `as_of_date`.
    Uses SCD Type 2 logic: product_id must exist, and as_of_date must fall
    between effective_from_date and COALESCE(effective_to_date, '9999-12-31').
    Returns -1 if no matching version exists.
    """
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT product_key
            FROM dim_product
            WHERE product_id = %s
              AND %s BETWEEN effective_from_date AND COALESCE(effective_to_date, '9999-12-31')
            LIMIT 1
            """,
            (product_id, as_of_date)
        )
        row = cur.fetchone()
        return row[0] if row else -1


def lookup_location_key(pg_conn, zip_code_prefix):
    """
    Return a location_key for a given zip_code_prefix.
    Because multiple (city, state) combos may exist for the same zip, we pick
    the first one (arbitrary but deterministic) to represent the area.
    Returns -1 if not found.
    """
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT location_key FROM dim_location WHERE zip_code_prefix = %s LIMIT 1",
            (zip_code_prefix,)
        )
        row = cur.fetchone()
        return row[0] if row else -1


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def date_to_key(date_val):
    """
    Convert a date/datetime string or object to an integer date_key (YYYYMMDD).
    Handles:
      - 'YYYY-MM-DD'
      - 'YYYY-MM-DD HH:MM:SS'
      - 'YYYYMMDD HH:MM:SS'  (the format seen in the source)
      - date/datetime objects
    Returns -1 if the input is None, empty, or unparseable.
    """
    if not date_val:
        return -1
    if isinstance(date_val, (date, datetime)):
        return int(date_val.strftime("%Y%m%d"))
    # Take only the part before the first space, then remove all non‑digit characters
    date_str = str(date_val).split(" ")[0]
    digits = "".join(ch for ch in date_str if ch.isdigit())
    if len(digits) == 8:
        return int(digits)
    return -1


def days_between(end_val, start_val):
    """
    Calculate the number of days between two date strings/objects.
    Returns None if either date is missing.
    """
    if not end_val or not start_val:
        return None
    if isinstance(end_val, str):
        end_val = date.fromisoformat(end_val[:10])
    if isinstance(start_val, str):
        start_val = date.fromisoformat(start_val[:10])
    return (end_val - start_val).days


# ---------------------------------------------------------------------------
# Data cleaning helpers
# ---------------------------------------------------------------------------

def clean_city_name(raw_city):
    """
    Normalise common ASCII city names to their proper accented forms.
    Extend this dictionary as needed.
    """
    if not raw_city:
        return raw_city
    replacements = {
        "sao paulo": "São Paulo",
        "sao": "São",
    }
    lower = raw_city.strip().lower()
    if lower in replacements:
        return replacements[lower]
    return raw_city.strip()


def safe_float(val):
    """Return float(val) or None if val is None/empty."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def safe_int(val):
    """Return int(val) or None if val is None/empty."""
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def safe_bool(val):
    """Return Python bool for numeric 0/1 or None."""
    if val is None:
        return False
    return bool(int(val))
