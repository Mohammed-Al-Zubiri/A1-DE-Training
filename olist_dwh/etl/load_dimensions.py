"""
Dimension Loading Module
------------------------
Populates all conformed dimension tables in the correct order:
  1. Static dimensions (SCD Type 0): dim_date, dim_payment_type, dim_location
  2. Type 1 dimensions (overwrite on change): dim_customer, dim_seller, dim_lead
  3. Type 2 dimension (preserve history): dim_product

Every function is idempotent and safe to re‑run.
"""

import logging
from datetime import date, datetime, timedelta
import psycopg2.extras
from utils import (
    get_src_conn, get_pg_conn,
    lookup_location_key, clean_city_name, safe_float, safe_bool, safe_int,
)
from config import DIM_DATE_START, DIM_DATE_END, BATCH_SIZE

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1. STATIC DIMENSIONS (SCD Type 0)
# ---------------------------------------------------------------------------

def load_dim_date(pg_conn):
    """
    Populate dim_date with every day from DIM_DATE_START to DIM_DATE_END.
    Also insert the special 'Unknown' row (date_key = -1) if not present.
    """
    with pg_conn.cursor() as cur:
        # Check if already loaded (count > 0 and contains Unknown row)
        cur.execute("SELECT COUNT(*) FROM dim_date WHERE date_key != -1")
        if cur.fetchone()[0] > 0:
            logger.info("dim_date already populated. Skipping date generation.")
        else:
            logger.info("Generating dim_date from %s to %s", DIM_DATE_START, DIM_DATE_END)
            start = date.fromisoformat(DIM_DATE_START)
            end   = date.fromisoformat(DIM_DATE_END)
            day = start
            rows = []
            while day <= end:
                rows.append((
                    int(day.strftime("%Y%m%d")),
                    day,
                    (day.weekday() + 1) % 7,   # 0 = Sunday
                    day.day,
                    day.month,
                    day.strftime("%B"),
                    (day.month - 1) // 3 + 1,
                    day.year,
                    day.weekday() >= 5         # weekend
                ))
                day += timedelta(days=1)
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO dim_date (date_key, full_date, day_of_week, day_of_month,
                                      month, month_name, quarter, year, is_weekend)
                VALUES %s
                ON CONFLICT (date_key) DO NOTHING
                """,
                rows,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )
            logger.info("Inserted %d dates into dim_date.", len(rows))

        # Ensure Unknown row exists (relies on primary key conflict)
        cur.execute(
            """
            INSERT INTO dim_date (date_key, full_date, month_name)
            VALUES (-1, NULL, 'Unknown')
            ON CONFLICT (date_key) DO NOTHING
            """
        )
    pg_conn.commit()
    logger.info("dim_date load complete.")


def load_dim_payment_type(pg_conn):
    """Extract distinct payment types from source and load into dim_payment_type."""
    with get_src_conn() as src:
        src_cur = src.cursor()
        src_cur.execute("SELECT DISTINCT payment_type FROM order_payments")
        types = [row[0] for row in src_cur.fetchall()]

    with pg_conn.cursor() as cur:
        for code in types:
            cur.execute(
                """
                INSERT INTO dim_payment_type (payment_type_code, payment_type_desc)
                VALUES (%s, %s)
                ON CONFLICT (payment_type_code) DO UPDATE SET
                    payment_type_desc = EXCLUDED.payment_type_desc
                """,
                (code, code.replace('_', ' ').title())
            )
    pg_conn.commit()
    logger.info("Loaded %d payment types.", len(types))


def load_dim_location(pg_conn):
    """
    Build dim_location from distinct (zip, cleaned city, state) of customers and sellers.
    No coordinates – only city/state for regional analysis.
    """
    with get_src_conn() as src:
        src_cur = src.cursor()
        src_cur.execute("""
            SELECT DISTINCT
                customer_zip_code_prefix AS zip_code_prefix,
                customer_city AS city,
                customer_state AS state
            FROM customers
            UNION
            SELECT DISTINCT
                seller_zip_code_prefix,
                seller_city,
                seller_state
            FROM sellers
        """)
        raw_locations = src_cur.fetchall()

    with pg_conn.cursor() as cur:
        for zip_code, city, state in raw_locations:
            clean_city = clean_city_name(city)
            cur.execute(
                """
                INSERT INTO dim_location (zip_code_prefix, city, state)
                VALUES (%s, %s, %s)
                ON CONFLICT (zip_code_prefix, city, state) DO NOTHING
                """,
                (zip_code, clean_city, state)
            )
    # Ensure Unknown location exists (will be inserted by schema script, but just in case)
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dim_location (location_key, zip_code_prefix, city, state)
            VALUES (-1, 0, 'Unknown', 'Unknown')
            ON CONFLICT (location_key) DO NOTHING
            """
        )
        cur.execute("SELECT setval('dim_location_location_key_seq', GREATEST(MAX(location_key), 1)) FROM dim_location")
    pg_conn.commit()
    logger.info("Loaded %d unique locations.", len(raw_locations))


def load_dim_review_comment(pg_conn):
    """
    Extract distinct review titles and messages from order_reviews and update dim_review_comment.
    """
    with get_src_conn() as src:
        src_cur = src.cursor()
        src_cur.execute("""
            SELECT DISTINCT review_comment_title, review_comment_message
            FROM order_reviews
        """)
        reviews = src_cur.fetchall()

    to_insert = []
    for title, msg in reviews:
        has_comment = bool(title or msg)
        to_insert.append((title, msg, has_comment))

    with pg_conn.cursor() as cur:
        # Postgres ON CONFLICT requires a unique constraint. We dont have it on title/message natively,
        # but we can look it up while inserting, or we can just try to insert and avoid duplicates manually.
        # Wait, the instruction says: "Create a new function load_dim_review_comment(pg_conn) that extracts distinct title and message combinations from order_reviews, calculates the has_comment boolean (True if either title or message is not null/empty), and inserts them."
        # If the table doesn't have a unique key, we can use `NOT EXISTS`.
        
        # It's better to just do this:
        for t, m, hc in to_insert:
            cur.execute("""
                INSERT INTO dim_review_comment (review_comment_title, review_comment_message, has_comment)
                SELECT %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM dim_review_comment
                    WHERE review_comment_title IS NOT DISTINCT FROM %s
                      AND review_comment_message IS NOT DISTINCT FROM %s
                )
            """, (t, m, hc, t, m))
            
    pg_conn.commit()
    logger.info("Inserted %d unique review comments.", len(to_insert))


# ---------------------------------------------------------------------------
# 2. TYPE 1 DIMENSIONS (overwrite on change)
# ---------------------------------------------------------------------------

def load_dim_customer(pg_conn):
    """Load dim_customer as SCD Type 1 from source customers table."""
    with get_src_conn() as src:
        src_cur = src.cursor()
        # Group by customer_unique_id and take latest/first zip code
        src_cur.execute("""
            SELECT customer_unique_id, MAX(customer_zip_code_prefix) as customer_zip_code_prefix
            FROM customers
            GROUP BY customer_unique_id
        """)
        customers = src_cur.fetchall()

    with pg_conn.cursor() as cur:
        for unique_id, zip_code in customers:
            loc_key = lookup_location_key(pg_conn, zip_code)
            cur.execute(
                """
                INSERT INTO dim_customer (customer_unique_id, current_location_key)
                VALUES (%s, %s)
                ON CONFLICT (customer_unique_id) DO UPDATE SET
                    current_location_key = EXCLUDED.current_location_key
                """,
                (unique_id, loc_key)
            )
    pg_conn.commit()
    logger.info("Upserted %d customers.", len(customers))


def load_dim_seller(pg_conn):
    """
    Load dim_seller (Type 1) from source sellers and enrich with
    leads_closed business attributes (latest values).
    """
    with get_src_conn() as src:
        src_cur = src.cursor()
        # We collect the latest business attributes per seller from leads_closed.
        src_cur.execute("""
            SELECT s.seller_id, s.seller_zip_code_prefix,
                   lc.business_segment, lc.lead_type, lc.business_type
            FROM sellers s
            LEFT JOIN leads_closed lc ON s.seller_id = lc.seller_id
        """)
        sellers = src_cur.fetchall()

    with pg_conn.cursor() as cur:
        for seller_id, zip_code, seg, ltype, btype in sellers:
            loc_key = lookup_location_key(pg_conn, zip_code)
            cur.execute(
                """
                INSERT INTO dim_seller (seller_id, business_segment, lead_type, business_type, current_location_key)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (seller_id) DO UPDATE SET
                    business_segment = EXCLUDED.business_segment,
                    lead_type = EXCLUDED.lead_type,
                    business_type = EXCLUDED.business_type,
                    current_location_key = EXCLUDED.current_location_key
                """,
                (seller_id, seg, ltype, btype, loc_key)
            )
    # Unknown seller (created by schema, but ensure it's there)
    with pg_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dim_seller (seller_key, seller_id, business_segment, lead_type, business_type, current_location_key)
            VALUES (-1, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', -1)
            ON CONFLICT (seller_key) DO NOTHING
        """)
        cur.execute("SELECT setval('dim_seller_seller_key_seq', GREATEST(MAX(seller_key), 1)) FROM dim_seller")
    pg_conn.commit()
    logger.info("Upserted %d sellers.", len(sellers))


def load_dim_lead(pg_conn):
    """
    Load dim_lead (Type 1) from leads_qualified enriched with leads_closed fields.
    For leads that are not closed, the closed‑related fields remain NULL.
    """
    with get_src_conn() as src:
        src_cur = src.cursor()
        src_cur.execute("""
            SELECT
                lq.mql_id,
                lq.first_contact_date,
                lq.landing_page_id,
                lq.origin,
                lc.business_segment,
                lc.lead_type,
                lc.lead_behaviour_profile,
                lc.has_company,
                lc.has_gtin,
                lc.average_stock,
                lc.business_type,
                lc.declared_product_catalog_size,
                lc.declared_monthly_revenue
            FROM leads_qualified lq
            LEFT JOIN leads_closed lc ON lq.mql_id = lc.mql_id
        """)
        leads = src_cur.fetchall()

    with pg_conn.cursor() as cur:
        for (mql_id, first_contact, landing, origin,
             seg, ltype, profile, has_comp, has_gtin, avg_stock,
             biz_type, cat_size, revenue) in leads:
            
            c_size = safe_float(cat_size)
            if c_size is None:
                cat_band = 'Unknown'
            elif c_size < 50:
                cat_band = '< 50'
            elif c_size <= 500:
                cat_band = '50 - 500'
            else:
                cat_band = '> 500'

            rev = safe_float(revenue)
            if rev is None:
                rev_band = 'Unknown'
            elif rev < 10000:
                rev_band = '< $10k'
            elif rev <= 50000:
                rev_band = '$10k - $50k'
            else:
                rev_band = '> $50k'

            cur.execute(
                """
                INSERT INTO dim_lead (
                    mql_id, landing_page_id, origin,
                    business_segment, lead_type, lead_behaviour_profile,
                    has_company, has_gtin, average_stock, business_type,
                    catalog_size_band, revenue_band
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (mql_id) DO UPDATE SET
                    landing_page_id = EXCLUDED.landing_page_id,
                    origin = EXCLUDED.origin,
                    business_segment = EXCLUDED.business_segment,
                    lead_type = EXCLUDED.lead_type,
                    lead_behaviour_profile = EXCLUDED.lead_behaviour_profile,
                    has_company = EXCLUDED.has_company,
                    has_gtin = EXCLUDED.has_gtin,
                    average_stock = EXCLUDED.average_stock,
                    business_type = EXCLUDED.business_type,
                    catalog_size_band = EXCLUDED.catalog_size_band,
                    revenue_band = EXCLUDED.revenue_band
                """,
                (mql_id, landing, origin,
                 seg, ltype, profile,
                 safe_bool(has_comp), safe_bool(has_gtin), avg_stock, biz_type,
                 cat_band, rev_band)
            )
    pg_conn.commit()
    logger.info("Upserted %d leads.", len(leads))


# ---------------------------------------------------------------------------
# 3. TYPE 2 DIMENSION (history tracking)
# ---------------------------------------------------------------------------

def load_dim_product(pg_conn):
    """
    Load dim_product with SCD Type 2.  
    Missing English category translations are handled with a hard‑coded fallback;
    the source database is never modified.
    """
    # Hard‑coded fallback for categories missing from the source translation table
    FALLBACK_TRANSLATIONS = {
        "pc_gamer": "Gaming PC",
        "portateis_cozinha_e_preparadores_de_alimentos": "Portable Kitchen & Food Preparers",
    }

    with get_src_conn() as src:
        src_cur = src.cursor()
        src_cur.execute("""
            SELECT
                p.product_id,
                p.product_category_name,
                t.product_category_name_english,
                p.product_name_lenght,
                p.product_description_lenght,
                p.product_photos_qty,
                p.product_weight_g,
                p.product_length_cm,
                p.product_height_cm,
                p.product_width_cm
            FROM products p
            LEFT JOIN product_category_name_translation t
                ON p.product_category_name = t.product_category_name
        """)
        products = src_cur.fetchall()

    with pg_conn.cursor() as cur:
        for (prod_id, cat_pt, cat_en, name_len, desc_len, photos,
             weight, length, height, width) in products:

            # Handle NULL Portuguese category
            if not cat_pt:
                cat_pt = 'UNKNOWN'
                cat_en = 'UNKNOWN'
            else:
                # Resolve missing English category (fallback to Portuguese)
                if cat_en is None:
                    cat_en = FALLBACK_TRANSLATIONS.get(cat_pt, cat_pt)

            # Fetch the current active version
            cur.execute(
                """
                SELECT product_key,
                       product_category_name, product_category_name_english,
                       product_name_length, product_description_length,
                       product_photos_qty,
                       product_weight_g, product_length_cm,
                       product_height_cm, product_width_cm
                FROM dim_product
                WHERE product_id = %s AND is_current = TRUE
                """,
                (prod_id,)
            )
            current = cur.fetchone()

            if current is None:
                _insert_product_version(cur, prod_id, cat_pt, cat_en,
                                        name_len, desc_len, photos,
                                        weight, length, height, width)
            else:
                changed = (
                    current[1] != cat_pt or
                    current[2] != cat_en or
                    current[3] != name_len or
                    current[4] != desc_len or
                    current[5] != photos or
                    current[6] != weight or
                    current[7] != length or
                    current[8] != height or
                    current[9] != width
                )
                if changed:
                    cur.execute(
                        "UPDATE dim_product SET effective_to_date = CURRENT_DATE - 1, is_current = FALSE WHERE product_key = %s",
                        (current[0],)
                    )
                    _insert_product_version(cur, prod_id, cat_pt, cat_en,
                                            name_len, desc_len, photos,
                                            weight, length, height, width)
    pg_conn.commit()
    logger.info("SCD Type 2 product load complete.")


def _insert_product_version(cur, prod_id, cat_pt, cat_en,
                            name_len, desc_len, photos,
                            weight, length, height, width,
                            effective_from='2016-01-01'):
    """Insert a current product version, active from a safe early date."""
    dim_complete = (
        weight is not None and weight > 0 and
        length is not None and length > 0 and
        height is not None and height > 0 and
        width is not None and width > 0
    )
    cur.execute(
        """
        INSERT INTO dim_product (
            product_id, product_category_name, product_category_name_english,
            product_name_length, product_description_length, product_photos_qty,
            product_weight_g, product_length_cm, product_height_cm, product_width_cm,
            effective_from_date, is_current, dimensions_complete
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s, TRUE, %s)
        """,
        (prod_id, cat_pt, cat_en,
         safe_float(name_len), safe_float(desc_len), safe_float(photos),
         safe_float(weight), safe_float(length), safe_float(height), safe_float(width),
         effective_from, dim_complete)
    )


# ---------------------------------------------------------------------------
# 4. ORCHESTRATOR
# ---------------------------------------------------------------------------

def load_all_dimensions(pg_conn):
    """
    Load all dimensions in dependency order. This function is called from
    the main pipeline and raises an exception on failure.
    """
    logger.info("=== Starting dimension loads ===")

    # Static dimensions
    load_dim_date(pg_conn)
    load_dim_payment_type(pg_conn)
    load_dim_location(pg_conn)
    load_dim_review_comment(pg_conn)

    # Type 1 dimensions
    load_dim_customer(pg_conn)
    load_dim_seller(pg_conn)
    load_dim_lead(pg_conn)

    # Type 2 dimension
    load_dim_product(pg_conn)

    logger.info("=== All dimensions loaded successfully. ===")
