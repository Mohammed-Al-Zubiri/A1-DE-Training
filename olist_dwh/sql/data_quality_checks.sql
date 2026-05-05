-- =============================================================================
-- Olist Data Warehouse – Data Quality Checks
-- =============================================================================
-- Run these queries after each ETL load to verify:
--   - Referential integrity
--   - Uniqueness of primary keys
--   - Expected row counts
--   - Business rule compliance (flags, ranges)
-- =============================================================================

-- -------------------------------------------------------------------------
-- 1. PRIMARY KEY UNIQUENESS
-- -------------------------------------------------------------------------

-- 1.1 fact_sales: (order_id, order_item_id) must be unique
SELECT 'fact_sales - duplicate PK' AS check_name, order_id, order_item_id, COUNT(*)
FROM fact_sales
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1;

-- 1.2 fact_order_fulfillment: order_id must be unique
SELECT 'fact_order_fulfillment - duplicate PK' AS check_name, order_id, COUNT(*)
FROM fact_order_fulfillment
GROUP BY order_id
HAVING COUNT(*) > 1;

-- 1.3 fact_payments: (order_id, payment_sequential) must be unique
SELECT 'fact_payments - duplicate PK' AS check_name, order_id, payment_sequential, COUNT(*)
FROM fact_payments
GROUP BY order_id, payment_sequential
HAVING COUNT(*) > 1;

-- 1.4 fact_reviews: (review_id, order_id) must be unique
SELECT 'fact_reviews - duplicate PK' AS check_name, review_id, order_id, COUNT(*)
FROM fact_reviews
GROUP BY review_id, order_id
HAVING COUNT(*) > 1;

-- 1.5 fact_seller_leads: mql_id must be unique
SELECT 'fact_seller_leads - duplicate PK' AS check_name, mql_id, COUNT(*)
FROM fact_seller_leads
GROUP BY mql_id
HAVING COUNT(*) > 1;

-- -------------------------------------------------------------------------
-- 2. REFERENTIAL INTEGRITY
-- -------------------------------------------------------------------------

-- 2.1 fact_sales: invalid date keys
SELECT 'fact_sales - orphan date_key' AS check_name, COUNT(*)
FROM fact_sales f
LEFT JOIN dim_date d ON f.purchase_date_key = d.date_key
WHERE d.date_key IS NULL;

-- 2.2 fact_sales: invalid customer_key
SELECT 'fact_sales - orphan customer_key' AS check_name, COUNT(*)
FROM fact_sales f
LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

-- 2.3 fact_sales: invalid seller_key
SELECT 'fact_sales - orphan seller_key' AS check_name, COUNT(*)
FROM fact_sales f
LEFT JOIN dim_seller s ON f.seller_key = s.seller_key
WHERE s.seller_key IS NULL;

-- 2.4 fact_sales: invalid product_key
SELECT 'fact_sales - orphan product_key' AS check_name, COUNT(*)
FROM fact_sales f
LEFT JOIN dim_product p ON f.product_key = p.product_key
WHERE p.product_key IS NULL;

-- 2.5 fact_sales: invalid location keys
SELECT 'fact_sales - orphan origin_location_key' AS check_name, COUNT(*)
FROM fact_sales f
LEFT JOIN dim_location l ON f.origin_location_key = l.location_key
WHERE l.location_key IS NULL;

SELECT 'fact_sales - orphan dest_location_key' AS check_name, COUNT(*)
FROM fact_sales f
LEFT JOIN dim_location l ON f.destination_location_key = l.location_key
WHERE l.location_key IS NULL;

-- 2.6 fact_order_fulfillment: orphan references
SELECT 'fact_order_fulfillment - orphan date_key' AS check_name, COUNT(*)
FROM fact_order_fulfillment f
LEFT JOIN dim_date d ON f.purchase_date_key = d.date_key
WHERE d.date_key IS NULL;

SELECT 'fact_order_fulfillment - orphan customer_key' AS check_name, COUNT(*)
FROM fact_order_fulfillment f
LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

SELECT 'fact_order_fulfillment - orphan dest_location_key' AS check_name, COUNT(*)
FROM fact_order_fulfillment f
LEFT JOIN dim_location l ON f.destination_location_key = l.location_key
WHERE l.location_key IS NULL;

-- 2.7 fact_payments: orphan references
SELECT 'fact_payments - orphan customer_key' AS check_name, COUNT(*)
FROM fact_payments f
LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

SELECT 'fact_payments - orphan payment_type_key' AS check_name, COUNT(*)
FROM fact_payments f
LEFT JOIN dim_payment_type pt ON f.payment_type_key = pt.payment_type_key
WHERE pt.payment_type_key IS NULL;

-- 2.8 fact_reviews: orphan references
SELECT 'fact_reviews - orphan customer_key' AS check_name, COUNT(*)
FROM fact_reviews f
LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

SELECT 'fact_reviews - orphan order_id' AS check_name, COUNT(*)
FROM fact_reviews r
LEFT JOIN fact_order_fulfillment o ON r.order_id = o.order_id
WHERE o.order_id IS NULL;

-- 2.9 fact_seller_leads: orphan references
SELECT 'fact_seller_leads - orphan lead_key' AS check_name, COUNT(*)
FROM fact_seller_leads f
LEFT JOIN dim_lead l ON f.lead_key = l.lead_key
WHERE l.lead_key IS NULL;

SELECT 'fact_seller_leads - orphan date_key' AS check_name, COUNT(*)
FROM fact_seller_leads f
LEFT JOIN dim_date d ON f.first_contact_date_key = d.date_key
WHERE d.date_key IS NULL;

-- -------------------------------------------------------------------------
-- 3. BUSINESS RULE VALIDATIONS
-- -------------------------------------------------------------------------

-- 3.1 All delivered orders in fulfillment fact must have is_delivered = TRUE
SELECT 'fact_order_fulfillment - delivered flag mismatch' AS check_name, COUNT(*)
FROM fact_order_fulfillment
WHERE order_status = 'delivered' AND is_delivered = FALSE;

-- 3.2 All on-time orders must have days_late <= 0 (or NULL if not delivered)
SELECT 'fact_order_fulfillment - on_time flag mismatch' AS check_name, COUNT(*)
FROM fact_order_fulfillment
WHERE is_on_time AND days_late > 0;

-- 3.3 Review scores must be between 1 and 5
SELECT 'fact_reviews - invalid review_score' AS check_name, COUNT(*)
FROM fact_reviews
WHERE review_score NOT BETWEEN 1 AND 5;

-- 3.4 Payment values should not be negative
SELECT 'fact_payments - negative payment_value' AS check_name, COUNT(*)
FROM fact_payments
WHERE payment_value < 0;

-- 3.5 Zero-value payments must have is_zero_value = TRUE
SELECT 'fact_payments - zero_value flag missing' AS check_name, COUNT(*)
FROM fact_payments
WHERE payment_value = 0 AND is_zero_value = FALSE;

-- 3.6 Payment instalments should be >= 1 (already normalised in ETL)
SELECT 'fact_payments - zero instalments' AS check_name, COUNT(*)
FROM fact_payments
WHERE payment_installments = 0;

-- 3.7 Seller leads: if is_closed, then days_to_close and won_date must not be null
SELECT 'fact_seller_leads - closed but no won_date' AS check_name, COUNT(*)
FROM fact_seller_leads
WHERE is_closed AND won_date_key IS NULL;

SELECT 'fact_seller_leads - closed but no days_to_close' AS check_name, COUNT(*)
FROM fact_seller_leads
WHERE is_closed AND days_to_close IS NULL;

-- 3.8 Orphaned sellers mapped correctly (seller_key = -1 or valid)
SELECT 'fact_seller_leads - invalid seller_key range' AS check_name, COUNT(*)
FROM fact_seller_leads
WHERE seller_key < -1;

-- -------------------------------------------------------------------------
-- 4. DIMENSION CONSISTENCY
-- -------------------------------------------------------------------------

-- 4.1 dim_customer: must have no duplicate customer_unique_id
SELECT 'dim_customer - duplicate customer_unique_id' AS check_name, customer_unique_id, COUNT(*)
FROM dim_customer
GROUP BY customer_unique_id
HAVING COUNT(*) > 1;

-- 4.2 dim_seller: must have no duplicate seller_id
SELECT 'dim_seller - duplicate seller_id' AS check_name, seller_id, COUNT(*)
FROM dim_seller
GROUP BY seller_id
HAVING COUNT(*) > 1;

-- 4.3 dim_product: only one current version per product_id at any time
SELECT 'dim_product - multiple current versions' AS check_name, product_id, COUNT(*)
FROM dim_product
WHERE is_current
GROUP BY product_id
HAVING COUNT(*) > 1;

-- 4.4 dim_product: effective_to_date must be > effective_from_date
SELECT 'dim_product - invalid date range' AS check_name, COUNT(*)
FROM dim_product
WHERE effective_to_date IS NOT NULL AND effective_to_date <= effective_from_date;

-- 4.5 dim_product: current versions must have NULL effective_to_date
SELECT 'dim_product - current with end date' AS check_name, COUNT(*)
FROM dim_product
WHERE is_current AND effective_to_date IS NOT NULL;

-- -------------------------------------------------------------------------
-- 5. ETL WATERMARK VALIDATION
-- -------------------------------------------------------------------------

-- 5.1 Ensure etl_control has one row per expected table
SELECT 'etl_control - missing entries' AS check_name, COUNT(*)
FROM (VALUES ('fact_sales'), ('fact_order_fulfillment'), ('fact_payments'),
             ('fact_reviews'), ('fact_seller_leads')) AS expected(name)
LEFT JOIN etl_control c ON expected.name = c.table_name
WHERE c.table_name IS NULL;

-- -------------------------------------------------------------------------
-- 6. ROW COUNT SANITY (optional, after initial full load)
-- -------------------------------------------------------------------------

-- Compare fact_sales row count with source order_items (should roughly match)
-- You would need cross-database access; here we just output target counts
SELECT 'fact_sales - row count' AS check_name, COUNT(*) AS cnt FROM fact_sales
UNION ALL
SELECT 'fact_payments - row count', COUNT(*) FROM fact_payments
UNION ALL
SELECT 'fact_reviews - row count', COUNT(*) FROM fact_reviews
UNION ALL
SELECT 'fact_seller_leads - row count', COUNT(*) FROM fact_seller_leads;

-- =============================================================================
-- End of data quality checks
-- =============================================================================
