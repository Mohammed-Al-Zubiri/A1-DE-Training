"""
Olist Data Warehouse ETL Pipeline – Main Orchestrator
-------------------------------------------------------
Runs the complete incremental batch load:
  1. Dimensions (static → Type 1 → Type 2)
  2. Fact tables (sales → fulfillment → payments → reviews → leads)

Idempotent: safe to re‑run; only new/changed rows are processed.
"""

import sys
import logging
from config import LOG_LEVEL, PG_HOST, PG_PORT, PG_DB
from utils import get_pg_conn
from load_dimensions import load_all_dimensions
from load_facts import load_all_facts

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("pipeline")

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def run():
    """Execute the full ETL pipeline."""
    logger.info("=" * 60)
    logger.info("Olist DWH ETL pipeline starting.")
    logger.info("Target: PostgreSQL %s:%d/%s", PG_HOST, PG_PORT, PG_DB)

    pg_conn = None
    try:
        pg_conn = get_pg_conn()
        logger.info("Connected to PostgreSQL.")

        # 1. Dimensions
        load_all_dimensions(pg_conn)

        # 2. Facts
        load_all_facts(pg_conn)

        logger.info("Pipeline completed successfully.")

    except Exception as exc:
        logger.error("Pipeline failed: %s", exc, exc_info=True)
        if pg_conn:
            pg_conn.rollback()
        sys.exit(1)

    finally:
        if pg_conn:
            pg_conn.close()
            logger.info("Database connection closed.")
        logger.info("=" * 60)


if __name__ == "__main__":
    run()
