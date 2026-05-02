"""
ETL Configuration
-----------------
Central place for all database connection details and pipeline tuning.
Edit these values before running the pipeline.
"""

# ---------------------------------------------------------------------------
# Source database (SQLite OLTP)
# ---------------------------------------------------------------------------
SOURCE_DB = "olist.sqlite"

# ---------------------------------------------------------------------------
# Target PostgreSQL data warehouse
# ---------------------------------------------------------------------------
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "olist_dwh"
PG_USER = "postgres"
PG_PASSWORD = "secret"               # Change to your PostgreSQL password
PG_SCHEMA = "public"

# ---------------------------------------------------------------------------
# ETL behaviour
# ---------------------------------------------------------------------------
# Batch size for bulk inserts (used in executemany)
BATCH_SIZE = 1000

# Date range for generating dim_date (must cover all possible transaction dates)
DIM_DATE_START = "2016-01-01"
DIM_DATE_END   = "2020-12-31"

# First‑run watermark (load all data from the beginning)
DEFAULT_WATERMARK = "1900-01-01"

# Logging level
LOG_LEVEL = "INFO"
