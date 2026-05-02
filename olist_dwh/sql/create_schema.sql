-- =============================================================================
-- Olist Data Warehouse – Complete Target Schema (PostgreSQL)
-- =============================================================================
-- Architecture: Kimball star schema
-- SCD strategies: Type 0 (static), Type 1 (overwrite), Type 2 (history)
-- Includes indexes, constraints, and special "Unknown" dimension members
-- =============================================================================

SET client_min_messages TO WARNING;

-- =============================================================================
-- 1. CONFORMED DIMENSIONS
-- =============================================================================

-- 1.1 Date Dimension (SCD Type 0) -------------------------------------------
-- Grain: one row per calendar day
-- Role-playing master for all date attributes in fact tables
CREATE TABLE dim_date (
    date_key        INTEGER         PRIMARY KEY,   -- YYYYMMDD format, -1 for Unknown
    full_date       DATE            UNIQUE,         -- NULL for Unknown
    day_of_week     INTEGER,                        -- 0=Sunday .. 6=Saturday (NULL for Unknown)
    day_of_month    INTEGER,
    month           INTEGER,
    month_name      TEXT            NOT NULL,       -- 'January' .. 'December', 'Unknown' for -1
    quarter         INTEGER,
    year            INTEGER,
    is_weekend      BOOLEAN
);

-- 1.2 Location Dimension (SCD Type 0) ----------------------------------------
-- Grain: one row per unique (zip_code_prefix, clean city, state)
-- Source: cleaned distinct locations from customers and sellers
CREATE TABLE dim_location (
    location_key    SERIAL          PRIMARY KEY,
    zip_code_prefix INTEGER         NOT NULL,
    city            TEXT            NOT NULL,
    state           TEXT            NOT NULL,
    UNIQUE (zip_code_prefix, city, state)
);
CREATE INDEX idx_dim_location_zip ON dim_location(zip_code_prefix);

-- 1.3 Customer Dimension (SCD Type 1) ----------------------------------------
-- Grain: one row per customer (latest snapshot)
-- Natural key: customer_unique_id
CREATE TABLE dim_customer (
    customer_key        SERIAL      PRIMARY KEY,
    customer_unique_id  TEXT        UNIQUE NOT NULL,
    current_location_key INTEGER    NOT NULL REFERENCES dim_location(location_key)
);
CREATE INDEX idx_dim_cust_location ON dim_customer(current_location_key);

-- 1.4 Seller Dimension (SCD Type 1) ------------------------------------------
-- Grain: one row per seller (latest snapshot)
-- Natural key: seller_id
-- Enriched with business attributes from leads_closed
CREATE TABLE dim_seller (
    seller_key          SERIAL      PRIMARY KEY,
    seller_id           TEXT        UNIQUE NOT NULL,
    business_segment    TEXT,
    lead_type           TEXT,
    business_type       TEXT,
    current_location_key INTEGER    NOT NULL REFERENCES dim_location(location_key)
);
CREATE INDEX idx_dim_seller_location ON dim_seller(current_location_key);

-- 1.5 Product Dimension (SCD Type 2) -----------------------------------------
-- Grain: one row per product version
-- Tracks attribute changes over time
CREATE TABLE dim_product (
    product_key                 SERIAL      PRIMARY KEY,
    product_id                  TEXT        NOT NULL,
    product_category_name       TEXT,
    product_category_name_english TEXT,
    product_name_length         REAL,
    product_description_length  REAL,
    product_photos_qty          REAL,
    product_weight_g            REAL,
    product_length_cm           REAL,
    product_height_cm           REAL,
    product_width_cm            REAL,
    effective_from_date         DATE        NOT NULL,
    effective_to_date           DATE,       -- NULL for current version
    is_current                  BOOLEAN     NOT NULL DEFAULT TRUE,
    dimensions_complete         BOOLEAN     NOT NULL DEFAULT TRUE
);
CREATE INDEX idx_dim_product_id ON dim_product(product_id);
CREATE INDEX idx_dim_product_current ON dim_product(product_id, is_current) WHERE is_current;

-- 1.6 Payment Type Dimension (SCD Type 0) ------------------------------------
-- Grain: one row per payment method
CREATE TABLE dim_payment_type (
    payment_type_key    SERIAL      PRIMARY KEY,
    payment_type_code   TEXT        UNIQUE NOT NULL,
    payment_type_desc   TEXT
);

-- 1.7 Lead Dimension (SCD Type 1) --------------------------------------------
-- Grain: one row per marketing qualified lead (MQL)
-- Natural key: mql_id
CREATE TABLE dim_lead (
    lead_key                    SERIAL      PRIMARY KEY,
    mql_id                      TEXT        UNIQUE NOT NULL,
    landing_page_id             TEXT,
    origin                      TEXT,
    business_segment            TEXT,
    lead_type                   TEXT,
    lead_behaviour_profile      TEXT,
    has_company                 BOOLEAN,
    has_gtin                    BOOLEAN,
    average_stock               TEXT,
    business_type               TEXT,
    catalog_size_band           TEXT,
    revenue_band                TEXT
);

-- 1.8 Review Comment Dimension (SCD Type 0) ----------------------------------
-- Grain: one row per unique title/message combination
CREATE TABLE dim_review_comment (
    review_comment_key SERIAL PRIMARY KEY,
    review_comment_title TEXT,
    review_comment_message TEXT,
    has_comment BOOLEAN NOT NULL
);

-- =============================================================================
-- 2. FACT TABLES
-- =============================================================================

-- 2.1 Sales Fact – Order-Item Grain ------------------------------------------
-- Grain: one row per item in an order
-- Measures: price, freight_value
CREATE TABLE fact_sales (
    order_id                    TEXT        NOT NULL,
    order_item_id               INTEGER     NOT NULL,
    purchase_date_key           INTEGER     NOT NULL REFERENCES dim_date(date_key),
    customer_key                INTEGER     NOT NULL REFERENCES dim_customer(customer_key),
    order_customer_id           TEXT        NOT NULL,
    seller_key                  INTEGER     NOT NULL REFERENCES dim_seller(seller_key),
    product_key                 INTEGER     NOT NULL REFERENCES dim_product(product_key),
    origin_location_key         INTEGER     NOT NULL REFERENCES dim_location(location_key),
    destination_location_key    INTEGER     NOT NULL REFERENCES dim_location(location_key),
    price                       REAL        NOT NULL,
    freight_value               REAL        NOT NULL,
    PRIMARY KEY (order_id, order_item_id)
);
CREATE INDEX idx_fact_sales_date        ON fact_sales(purchase_date_key);
CREATE INDEX idx_fact_sales_customer    ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product     ON fact_sales(product_key);

-- 2.2 Order Fulfillment Fact – Order Grain -----------------------------------
-- Grain: one row per order (accumulating snapshot)
-- Measures: delivery times, on-time flags
CREATE TABLE fact_order_fulfillment (
    order_id                    TEXT        PRIMARY KEY,
    customer_key                INTEGER     NOT NULL REFERENCES dim_customer(customer_key),
    destination_location_key    INTEGER     NOT NULL REFERENCES dim_location(location_key),
    purchase_date_key           INTEGER     NOT NULL REFERENCES dim_date(date_key),
    approved_date_key           INTEGER     NOT NULL REFERENCES dim_date(date_key),   -- -1 if missing
    delivered_carrier_date_key  INTEGER     REFERENCES dim_date(date_key),            -- nullable until handoff
    delivered_customer_date_key INTEGER     REFERENCES dim_date(date_key),            -- nullable until delivery
    estimated_delivery_date_key INTEGER     NOT NULL REFERENCES dim_date(date_key),
    order_status                TEXT        NOT NULL,
    days_to_carrier             REAL,
    days_to_customer            REAL,
    days_late                   REAL,
    is_on_time                  BOOLEAN     NOT NULL DEFAULT FALSE,
    is_delivered                BOOLEAN     NOT NULL DEFAULT FALSE
);
CREATE INDEX idx_fact_fulfill_customer ON fact_order_fulfillment(customer_key);
CREATE INDEX idx_fact_fulfill_purchase ON fact_order_fulfillment(purchase_date_key);

-- 2.3 Payments Fact – Payment Transaction Grain ------------------------------
-- Grain: one row per payment per order
-- Measures: payment_value, installments, zero-value flag
CREATE TABLE fact_payments (
    order_id                TEXT        NOT NULL,
    payment_sequential      INTEGER     NOT NULL,
    customer_key            INTEGER     NOT NULL REFERENCES dim_customer(customer_key),
    payment_type_key        INTEGER     NOT NULL REFERENCES dim_payment_type(payment_type_key),
    payment_installments    INTEGER     NOT NULL,
    payment_value           REAL        NOT NULL,
    is_zero_value           BOOLEAN     NOT NULL DEFAULT FALSE,
    PRIMARY KEY (order_id, payment_sequential)
);
CREATE INDEX idx_fact_pay_customer ON fact_payments(customer_key);

-- 2.4 Reviews Fact – Review Grain --------------------------------------------
-- Grain: one row per review (deduplicated)
-- Measures: review_score, comment texts
CREATE TABLE fact_reviews (
    review_fact_key             SERIAL      PRIMARY KEY,
    review_id                   TEXT        NOT NULL,
    order_id                    TEXT        NOT NULL,
    customer_key                INTEGER     NOT NULL REFERENCES dim_customer(customer_key),
    review_creation_date_key    INTEGER     NOT NULL REFERENCES dim_date(date_key),
    review_answer_date_key      INTEGER     REFERENCES dim_date(date_key),   -- nullable if no answer
    review_score                INTEGER     NOT NULL,
    review_comment_key          INTEGER     NOT NULL REFERENCES dim_review_comment(review_comment_key)
);
CREATE INDEX idx_fact_rev_customer ON fact_reviews(customer_key);
CREATE INDEX idx_fact_rev_order    ON fact_reviews(order_id);
CREATE INDEX idx_fact_rev_date     ON fact_reviews(review_creation_date_key);

-- 2.5 Seller Leads Fact – MQL Grain ------------------------------------------
-- Grain: one row per marketing qualified lead
-- Measures: is_closed flag, days_to_close
CREATE TABLE fact_seller_leads (
    mql_id                  TEXT        PRIMARY KEY,
    lead_key                INTEGER     NOT NULL REFERENCES dim_lead(lead_key),
    first_contact_date_key  INTEGER     NOT NULL REFERENCES dim_date(date_key),
    won_date_key            INTEGER     REFERENCES dim_date(date_key),   -- nullable if not closed
    seller_key              INTEGER     NOT NULL REFERENCES dim_seller(seller_key),   -- -1 if orphaned
    is_closed               BOOLEAN     NOT NULL DEFAULT FALSE,
    days_to_close           REAL,
    declared_product_catalog_size REAL,
    declared_monthly_revenue REAL
);
CREATE INDEX idx_fact_leads_lead  ON fact_seller_leads(lead_key);
CREATE INDEX idx_fact_leads_date  ON fact_seller_leads(first_contact_date_key);

-- =============================================================================
-- 3. SPECIAL "UNKNOWN" DIMENSION MEMBERS
-- =============================================================================

-- Unknown date (date_key = -1)
INSERT INTO dim_date (date_key, full_date, day_of_week, day_of_month, month, month_name, quarter, year, is_weekend)
VALUES (-1, NULL, NULL, NULL, NULL, 'Unknown', NULL, NULL, NULL)
ON CONFLICT (date_key) DO NOTHING;

-- Unknown location (location_key = -1)
INSERT INTO dim_location (location_key, zip_code_prefix, city, state)
VALUES (-1, 0, 'Unknown', 'Unknown')
ON CONFLICT (location_key) DO NOTHING;
-- Reset the autoincrement sequence so the next generated location_key starts after the max
SELECT setval('dim_location_location_key_seq', GREATEST(MAX(location_key), 1)) FROM dim_location;

-- Unknown seller (seller_key = -1) – for orphaned sellers in leads
INSERT INTO dim_seller (seller_key, seller_id, business_segment, lead_type, business_type, current_location_key)
VALUES (-1, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', -1)
ON CONFLICT (seller_key) DO NOTHING;
SELECT setval('dim_seller_seller_key_seq', GREATEST(MAX(seller_key), 1)) FROM dim_seller;

-- Unknown product (product_key = -1)
INSERT INTO dim_product (
    product_key, product_id, product_category_name, product_category_name_english,
    effective_from_date, is_current, dimensions_complete
)
VALUES (-1, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', '2016-01-01', FALSE, FALSE)
ON CONFLICT (product_key) DO NOTHING;
SELECT setval('dim_product_product_key_seq', GREATEST(MAX(product_key), 1))
FROM dim_product;

-- Unknown review comment (review_comment_key = -1)
INSERT INTO dim_review_comment (review_comment_key, review_comment_title, review_comment_message, has_comment)
VALUES (-1, 'Unknown', 'No Comment', FALSE)
ON CONFLICT (review_comment_key) DO NOTHING;
SELECT setval('dim_review_comment_review_comment_key_seq', GREATEST(MAX(review_comment_key), 1)) FROM dim_review_comment;

-- =============================================================================
-- 4. ETL CONTROL TABLE
-- =============================================================================
-- Tracks incremental extraction watermarks and load status
CREATE TABLE etl_control (
    table_name      TEXT        PRIMARY KEY,
    last_extracted  TEXT,                   -- timestamp or date of last extracted record
    last_loaded     TIMESTAMPTZ,
    status          TEXT,
    row_count       INTEGER
);

-- =============================================================================
-- End of schema
-- =============================================================================
