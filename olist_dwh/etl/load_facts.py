"""
Fact Table Loading Module
-------------------------
Incrementally loads all five fact tables using timestamp-based watermarks.
Each function:
  - Reads the last extraction watermark from etl_control.
  - Extracts new rows from the SQLite source.
  - Transforms data: maps natural keys to surrogate keys, handles data quality
    (deduplication of reviews, zero-value flags, orphaned seller mapping, etc.).
  - Bulk inserts into PostgreSQL with idempotent conflict handling.
  - Updates the watermark for the next run.

All functions are designed to be re‑runnable safely.
"""

import logging
from psycopg2.extras import execute_values
from utils import (
    get_src_conn, get_pg_conn,
    get_last_extract, set_last_extract,
    lookup_customer_key, lookup_seller_key, lookup_product_key,
    lookup_location_key, lookup_payment_type_key, lookup_dim, lookup_review_comment_key,
    date_to_key, days_between,
    safe_float, safe_bool,
)
from config import BATCH_SIZE, DEFAULT_WATERMARK

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1. FACT_SALES – Order-Item Grain
# ---------------------------------------------------------------------------
def load_fact_sales(pg_conn):
    """
    Incrementally load fact_sales.
    Watermark column: o.order_purchase_timestamp
    """
    table = "fact_sales"
    last_ts = get_last_extract(pg_conn, table)
    logger.info("Loading %s (watermark > %s)", table, last_ts)

    with get_src_conn() as src:
        cur = src.cursor()
        cur.execute("""
            SELECT
                oi.order_id,
                oi.order_item_id,
                o.order_purchase_timestamp,
                oi.price,
                oi.freight_value,
                o.customer_id,
                c.customer_unique_id,
                oi.seller_id,
                oi.product_id,
                c.customer_zip_code_prefix,
                s.seller_zip_code_prefix
            FROM order_items oi
            JOIN orders o ON oi.order_id = o.order_id
            JOIN customers c ON o.customer_id = c.customer_id
            JOIN sellers s ON oi.seller_id = s.seller_id
            WHERE o.order_purchase_timestamp > ?
            ORDER BY o.order_purchase_timestamp
        """, (last_ts,))
        rows = cur.fetchall()

    if not rows:
        logger.info("No new rows for %s.", table)
        return

    # Pre-load dimension caches for faster lookup (optional)
    # We'll just call lookup functions which are fast enough for this dataset

    to_insert = []
    max_ts = last_ts
    for (order_id, item_id, purch_ts, price, freight,
         cust_id, cust_unique_id, sell_id, prod_id, cust_zip, sell_zip) in rows:

        cust_key = lookup_customer_key(pg_conn, cust_unique_id)
        sell_key = lookup_seller_key(pg_conn, sell_id)
        prod_key = lookup_product_key(pg_conn, prod_id, purch_ts)  # SCD Type2 active at purch_ts
        origin_loc = lookup_location_key(pg_conn, sell_zip)
        dest_loc = lookup_location_key(pg_conn, cust_zip)
        purch_key = date_to_key(purch_ts)

        to_insert.append((
            order_id, item_id, purch_key,
            cust_key, cust_id, sell_key, prod_key,
            origin_loc, dest_loc,
            price, freight
        ))
        # Update the maximum timestamp seen
        if purch_ts > max_ts:
            max_ts = purch_ts

    with pg_conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO fact_sales (
                order_id, order_item_id, purchase_date_key,
                customer_key, order_customer_id, seller_key, product_key,
                origin_location_key, destination_location_key,
                price, freight_value
            )
            VALUES %s
            ON CONFLICT (order_id, order_item_id) DO NOTHING
        """, to_insert, page_size=BATCH_SIZE)

    pg_conn.commit()
    set_last_extract(pg_conn, table, max_ts)
    logger.info("Loaded %d rows into %s (new watermark: %s)", len(to_insert), table, max_ts)


# ---------------------------------------------------------------------------
# 2. FACT_ORDER_FULFILLMENT – Order Grain
# ---------------------------------------------------------------------------
def load_fact_order_fulfillment(pg_conn):
    """
    Load order-level fulfillment facts.
    Watermark column: order_purchase_timestamp
    Only loads orders that are not 'canceled' or 'created' (non-shipped states).
    """
    table = "fact_order_fulfillment"
    last_ts = get_last_extract(pg_conn, table)
    logger.info("Loading %s (watermark > %s)", table, last_ts)

    with get_src_conn() as src:
        cur = src.cursor()
        cur.execute("""
            SELECT
                o.order_id,
                o.order_status,
                o.order_purchase_timestamp,
                o.order_approved_at,
                o.order_delivered_carrier_date,
                o.order_delivered_customer_date,
                o.order_estimated_delivery_date,
                o.customer_id,
                c.customer_unique_id,
                c.customer_zip_code_prefix
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            WHERE o.order_purchase_timestamp > ?
              AND o.order_status NOT IN ('canceled', 'created')
            ORDER BY o.order_purchase_timestamp
        """, (last_ts,))
        rows = cur.fetchall()

    if not rows:
        logger.info("No new rows for %s.", table)
        return

    to_insert = []
    max_ts = last_ts
    for (order_id, status, purch_ts, approved_ts, carrier_ts,
         delivered_ts, estimated_ts, cust_id, cust_unique_id, cust_zip) in rows:

        cust_key = lookup_customer_key(pg_conn, cust_unique_id)
        dest_loc = lookup_location_key(pg_conn, cust_zip)

        purch_key     = date_to_key(purch_ts)
        approved_key  = date_to_key(approved_ts) if approved_ts else -1
        carrier_key   = date_to_key(carrier_ts) if carrier_ts else -1
        delivered_key = date_to_key(delivered_ts) if delivered_ts else -1
        estimated_key = date_to_key(estimated_ts) if estimated_ts else -1

        # Pre-compute delivery metrics
        days_to_carrier = days_between(carrier_ts, approved_ts)
        days_to_customer = days_between(delivered_ts, purch_ts)
        days_late = days_between(delivered_ts, estimated_ts)
        is_delivered = (status == 'delivered')
        is_on_time = (is_delivered and days_late is not None and days_late <= 0)

        to_insert.append((
            order_id, cust_key, dest_loc,
            purch_key, approved_key, carrier_key, delivered_key, estimated_key,
            status,
            days_to_carrier, days_to_customer, days_late,
            is_on_time, is_delivered
        ))
        if purch_ts > max_ts:
            max_ts = purch_ts

    with pg_conn.cursor() as cur:
        # Use ON CONFLICT DO NOTHING for idempotency.
        # In a production environment with late‑arriving updates,
        # you would use DO UPDATE with COALESCE to merge status changes.
        execute_values(cur, """
            INSERT INTO fact_order_fulfillment (
                order_id, customer_key, destination_location_key,
                purchase_date_key, approved_date_key,
                delivered_carrier_date_key, delivered_customer_date_key,
                estimated_delivery_date_key,
                order_status,
                days_to_carrier, days_to_customer, days_late,
                is_on_time, is_delivered
            )
            VALUES %s
            ON CONFLICT (order_id) DO NOTHING
        """, to_insert, page_size=BATCH_SIZE)

    pg_conn.commit()
    set_last_extract(pg_conn, table, max_ts)
    logger.info("Loaded %d rows into %s (new watermark: %s)", len(to_insert), table, max_ts)


# ---------------------------------------------------------------------------
# 3. FACT_PAYMENTS – Payment Transaction Grain
# ---------------------------------------------------------------------------
def load_fact_payments(pg_conn):
    """
    Load payment facts. Because payments have no timestamp, we pull payments
    for orders whose purchase_timestamp is after the watermark.
    This ensures we capture new orders' payments incrementally.
    """
    table = "fact_payments"
    last_ts = get_last_extract(pg_conn, table)
    logger.info("Loading %s (watermark > %s)", table, last_ts)

    with get_src_conn() as src:
        cur = src.cursor()
        cur.execute("""
            SELECT
                op.order_id,
                op.payment_sequential,
                op.payment_type,
                op.payment_installments,
                op.payment_value,
                o.customer_id
            FROM order_payments op
            JOIN orders o ON op.order_id = o.order_id
            WHERE o.order_purchase_timestamp > ?
            ORDER BY o.order_purchase_timestamp, op.payment_sequential
        """, (last_ts,))
        rows = cur.fetchall()

    if not rows:
        logger.info("No new rows for %s.", table)
        return

    to_insert = []
    max_ts = last_ts
    for (order_id, seq, ptype, installments, value, cust_id) in rows:
        cust_key = lookup_customer_key(pg_conn, cust_id)
        pt_key = lookup_payment_type_key(pg_conn, ptype)
        # Normalize installments: 0 → 1
        if installments == 0:
            installments = 1
        is_zero = (value <= 0)

        to_insert.append((
            order_id, seq, cust_key, pt_key,
            installments, value, is_zero
        ))
        # The watermark we track is still the max purchase timestamp.
        # We'll need to fetch the actual purchase timestamp for the order;
        # we don't have it here, so we set the watermark to the max seen later.
        # To avoid a second query, we'll collect order timestamps separately.
        # For simplicity, we'll run a separate query to get the max order timestamp
        # among these orders at the end.

    # Find the max order purchase timestamp among the loaded orders to update watermark
    with pg_conn.cursor() as cur:
        # We need to get the timestamps from source; let's query them directly.
        # A simpler approach: just set watermark to the maximum currently known purchase timestamp
        # from the orders table. Since we loaded all orders later than last_ts, we can do:
        pass

    # Alternative: we can store the max order_purchase_timestamp from the previous step.
    # I'll do a second pass after extracting rows: track the max purchase_ts from a separate query.
    # Better: retrieve o.order_purchase_timestamp in the main query and use it for watermark.
    # We'll adjust the query to include o.order_purchase_timestamp.
    # Let's rewrite the extraction to include purchase_ts.
    with get_src_conn() as src:
        cur = src.cursor()
        cur.execute("""
            SELECT
                op.order_id,
                op.payment_sequential,
                op.payment_type,
                op.payment_installments,
                op.payment_value,
                o.customer_id,
                c.customer_unique_id,
                o.order_purchase_timestamp
            FROM order_payments op
            JOIN orders o ON op.order_id = o.order_id
            JOIN customers c ON o.customer_id = c.customer_id
            WHERE o.order_purchase_timestamp > ?
            ORDER BY o.order_purchase_timestamp, op.payment_sequential
        """, (last_ts,))
        rows = cur.fetchall()

    if not rows:
        logger.info("No new rows for %s.", table)
        return

    to_insert = []
    max_ts = last_ts
    for (order_id, seq, ptype, installments, value, cust_id, cust_unique_id, purch_ts) in rows:
        cust_key = lookup_customer_key(pg_conn, cust_unique_id)
        pt_key = lookup_payment_type_key(pg_conn, ptype)
        if installments == 0:
            installments = 1
        is_zero = (value <= 0)
        to_insert.append((
            order_id, seq, cust_key, pt_key,
            installments, value, is_zero
        ))
        if purch_ts > max_ts:
            max_ts = purch_ts

    with pg_conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO fact_payments (
                order_id, payment_sequential, customer_key,
                payment_type_key, payment_installments, payment_value,
                is_zero_value
            )
            VALUES %s
            ON CONFLICT (order_id, payment_sequential) DO NOTHING
        """, to_insert, page_size=BATCH_SIZE)

    pg_conn.commit()
    set_last_extract(pg_conn, table, max_ts)
    logger.info("Loaded %d rows into %s (new watermark: %s)", len(to_insert), table, max_ts)


# ---------------------------------------------------------------------------
# 4. FACT_REVIEWS – Review Grain (deduplicated)
# ---------------------------------------------------------------------------
def load_fact_reviews(pg_conn):
    """
    Load reviews without deduplication, handling duplicate review_ids mapped to different orders.
    Watermark column: review_creation_date
    """
    table = "fact_reviews"
    last_ts = get_last_extract(pg_conn, table)
    logger.info("Loading %s (watermark > %s)", table, last_ts)

    with get_src_conn() as src:
        cur = src.cursor()
        cur.execute("""
            SELECT
                r.review_id,
                r.order_id,
                r.review_score,
                r.review_comment_title,
                r.review_comment_message,
                r.review_creation_date,
                r.review_answer_timestamp,
                o.customer_id,
                c.customer_unique_id
            FROM order_reviews r
            JOIN orders o ON r.order_id = o.order_id
            JOIN customers c ON o.customer_id = c.customer_id
            WHERE r.review_creation_date > ?
            ORDER BY r.review_creation_date
        """, (last_ts,))
        rows = cur.fetchall()

    if not rows:
        logger.info("No new rows for %s.", table)
        return

    to_insert = []
    max_ts = last_ts
    for (review_id, order_id, score, title, msg,
         creation_date, answer_ts, cust_id, cust_unique_id) in rows:

        cust_key = lookup_customer_key(pg_conn, cust_unique_id)
        creation_key = date_to_key(creation_date)
        answer_key = date_to_key(answer_ts) if answer_ts else -1
        review_comment_key = lookup_review_comment_key(pg_conn, title, msg)

        to_insert.append((
            review_id, order_id, cust_key,
            creation_key, answer_key,
            score, review_comment_key
        ))
        if creation_date > max_ts:
            max_ts = creation_date

    with pg_conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO fact_reviews (
                review_id, order_id, customer_key,
                review_creation_date_key, review_answer_date_key,
                review_score, review_comment_key
            )
            VALUES %s
            ON CONFLICT (review_id, order_id) DO NOTHING
        """, to_insert, page_size=BATCH_SIZE)

    pg_conn.commit()
    set_last_extract(pg_conn, table, max_ts)
    logger.info("Loaded %d rows into %s (new watermark: %s)", len(to_insert), table, max_ts)


# ---------------------------------------------------------------------------
# 5. FACT_SELLER_LEADS – MQL Grain
# ---------------------------------------------------------------------------
def load_fact_seller_leads(pg_conn):
    """
    Load seller leads facts.
    Watermark column: lq.first_contact_date
    Maps orphaned seller_id to -1 (Unknown seller).
    """
    table = "fact_seller_leads"
    last_ts = get_last_extract(pg_conn, table)
    logger.info("Loading %s (watermark > %s)", table, last_ts)

    with get_src_conn() as src:
        cur = src.cursor()
        cur.execute("""
            SELECT
                lq.mql_id,
                lq.first_contact_date,
                lc.won_date,
                lc.seller_id,
                lq.origin,  -- we need lead_key lookup, but we'll get it via mql_id
                lc.declared_product_catalog_size,
                lc.declared_monthly_revenue
            FROM leads_qualified lq
            LEFT JOIN leads_closed lc ON lq.mql_id = lc.mql_id
            WHERE lq.first_contact_date > ?
            ORDER BY lq.first_contact_date
        """, (last_ts,))
        rows = cur.fetchall()

    if not rows:
        logger.info("No new rows for %s.", table)
        return

    to_insert = []
    max_ts = last_ts
    for (mql_id, first_contact, won_date, seller_id, _, cat_size, rev) in rows:
        lead_key = lookup_dim(pg_conn, "dim_lead", "lead_key", "mql_id", mql_id)
        first_key = date_to_key(first_contact)
        won_key = date_to_key(won_date) if won_date else -1
        # If closed, seller_id is not None; resolve seller_key, else -1
        if seller_id:
            seller_key = lookup_seller_key(pg_conn, seller_id)
            if seller_key == -1:
                # Map orphaned seller to Unknown
                seller_key = -1
        else:
            seller_key = -1
        is_closed = (won_date is not None)
        days_close = days_between(won_date, first_contact) if is_closed else None
        
        cat_sz_val = safe_float(cat_size)
        rev_val = safe_float(rev)

        to_insert.append((
            mql_id, lead_key, first_key, won_key, seller_key,
            is_closed, days_close, cat_sz_val, rev_val
        ))
        if first_contact > max_ts:
            max_ts = first_contact

    with pg_conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO fact_seller_leads (
                mql_id, lead_key, first_contact_date_key,
                won_date_key, seller_key,
                is_closed, days_to_close,
                declared_product_catalog_size, declared_monthly_revenue
            )
            VALUES %s
            ON CONFLICT (mql_id) DO NOTHING
        """, to_insert, page_size=BATCH_SIZE)

    pg_conn.commit()
    set_last_extract(pg_conn, table, max_ts)
    logger.info("Loaded %d rows into %s (new watermark: %s)", len(to_insert), table, max_ts)


# ---------------------------------------------------------------------------
# 6. ORCHESTRATOR
# ---------------------------------------------------------------------------
def load_all_facts(pg_conn):
    """
    Load all fact tables in a dependency-conscious order.
    Sales and fulfillment first, then dependent facts.
    """
    logger.info("=== Starting fact table loads ===")
    load_fact_sales(pg_conn)
    load_fact_order_fulfillment(pg_conn)
    load_fact_payments(pg_conn)
    load_fact_reviews(pg_conn)
    load_fact_seller_leads(pg_conn)
    logger.info("=== All fact tables loaded successfully. ===")
