-- =============================================================================
-- Olist Data Warehouse – Data Quality Checks
-- =============================================================================
-- Run after each ETL load to verify integrity and business rules.
-- All checks should return ZERO rows (or expected counts where noted).

-- -------------------------------------------------------------------------
-- 1. PRIMARY KEY & UNIQUENESS
-- -------------------------------------------------------------------------
-- fact_sales (order_id, order_item_id)
SELECT 'fact_sales - duplicate PK' AS check_name, order_id, order_item_id, COUNT(*)
FROM fact_sales
GROUP BY order_id, order_item_id HAVING COUNT(*) > 1;

-- fact_order_fulfillment (order_id)
SELECT 'fact_ord_ful - duplicate PK' AS check_name, order_id, COUNT(*)
FROM fact_order_fulfillment
GROUP BY order_id HAVING COUNT(*) > 1;

-- fact_payments (order_id, payment_sequential)
SELECT 'fact_pay - duplicate PK' AS check_name, order_id, payment_sequential, COUNT(*)
FROM fact_payments
GROUP BY order_id, payment_sequential HAVING COUNT(*) > 1;

-- fact_reviews (review_id must still be unique – no duplicates allowed)
SELECT 'fact_rev - duplicate review_id' AS check_name, review_id, COUNT(*)
FROM fact_reviews
GROUP BY review_id HAVING COUNT(*) > 1;

-- fact_seller_leads (mql_id)
SELECT 'fact_leads - duplicate PK' AS check_name, mql_id, COUNT(*)
FROM fact_seller_leads
GROUP BY mql_id HAVING COUNT(*) > 1;

-- dim_customer (customer_unique_id)
SELECT 'dim_cust - duplicate customer_unique_id' AS check_name, customer_unique_id, COUNT(*)
FROM dim_customer
GROUP BY customer_unique_id HAVING COUNT(*) > 1;

-- dim_product (only one current version per product_id)
SELECT 'dim_prod - multiple current versions' AS check_name, product_id, COUNT(*)
FROM dim_product
WHERE is_current
GROUP BY product_id HAVING COUNT(*) > 1;

-- dim_review_comment: check for unintended duplicate combinations
SELECT 'dim_rev_comm - duplicate title/message' AS check_name, review_comment_title, review_comment_message, COUNT(*)
FROM dim_review_comment
WHERE review_comment_key != -1
GROUP BY review_comment_title, review_comment_message HAVING COUNT(*) > 1;

-- -------------------------------------------------------------------------
-- 2. REFERENTIAL INTEGRITY
-- -------------------------------------------------------------------------
-- 2.1 fact_sales
SELECT 'fact_sales - orphan purchase_date_key' AS issue, COUNT(*)
FROM fact_sales f LEFT JOIN dim_date d ON f.purchase_date_key = d.date_key WHERE d.date_key IS NULL;

SELECT 'fact_sales - orphan customer_key' AS issue, COUNT(*)
FROM fact_sales f LEFT JOIN dim_customer c ON f.customer_key = c.customer_key WHERE c.customer_key IS NULL;

SELECT 'fact_sales - orphan seller_key' AS issue, COUNT(*)
FROM fact_sales f LEFT JOIN dim_seller s ON f.seller_key = s.seller_key WHERE s.seller_key IS NULL;

SELECT 'fact_sales - orphan product_key' AS issue, COUNT(*)
FROM fact_sales f LEFT JOIN dim_product p ON f.product_key = p.product_key WHERE p.product_key IS NULL;

SELECT 'fact_sales - orphan origin_location_key' AS issue, COUNT(*)
FROM fact_sales f LEFT JOIN dim_location l ON f.origin_location_key = l.location_key WHERE l.location_key IS NULL;

SELECT 'fact_sales - orphan destination_location_key' AS issue, COUNT(*)
FROM fact_sales f LEFT JOIN dim_location l ON f.destination_location_key = l.location_key WHERE l.location_key IS NULL;

-- 2.2 fact_order_fulfillment
SELECT 'fact_ord_ful - orphan customer_key' AS issue, COUNT(*)
FROM fact_order_fulfillment f LEFT JOIN dim_customer c ON f.customer_key = c.customer_key WHERE c.customer_key IS NULL;

SELECT 'fact_ord_ful - orphan dest_location_key' AS issue, COUNT(*)
FROM fact_order_fulfillment f LEFT JOIN dim_location l ON f.destination_location_key = l.location_key WHERE l.location_key IS NULL;

SELECT 'fact_ord_ful - orphan purchase_date_key' AS issue, COUNT(*)
FROM fact_order_fulfillment f LEFT JOIN dim_date d ON f.purchase_date_key = d.date_key WHERE d.date_key IS NULL;

SELECT 'fact_ord_ful - orphan approved_date_key' AS issue, COUNT(*)
FROM fact_order_fulfillment f LEFT JOIN dim_date d ON f.approved_date_key = d.date_key
WHERE d.date_key IS NULL AND f.approved_date_key != -1;

-- 2.3 fact_payments
SELECT 'fact_pay - orphan customer_key' AS issue, COUNT(*)
FROM fact_payments f LEFT JOIN dim_customer c ON f.customer_key = c.customer_key WHERE c.customer_key IS NULL;

SELECT 'fact_pay - orphan payment_type_key' AS issue, COUNT(*)
FROM fact_payments f LEFT JOIN dim_payment_type pt ON f.payment_type_key = pt.payment_type_key WHERE pt.payment_type_key IS NULL;

-- 2.4 fact_reviews
SELECT 'fact_rev - orphan customer_key' AS issue, COUNT(*)
FROM fact_reviews f LEFT JOIN dim_customer c ON f.customer_key = c.customer_key WHERE c.customer_key IS NULL;

SELECT 'fact_rev - orphan review_creation_date_key' AS issue, COUNT(*)
FROM fact_reviews f LEFT JOIN dim_date d ON f.review_creation_date_key = d.date_key WHERE d.date_key IS NULL;

SELECT 'fact_rev - orphan order_id' AS issue, COUNT(*)
FROM fact_reviews r LEFT JOIN fact_order_fulfillment o ON r.order_id = o.order_id WHERE o.order_id IS NULL;

SELECT 'fact_rev - orphan review_comment_key' AS issue, COUNT(*)
FROM fact_reviews f LEFT JOIN dim_review_comment c ON f.review_comment_key = c.review_comment_key WHERE c.review_comment_key IS NULL;

-- 2.5 fact_seller_leads
SELECT 'fact_leads - orphan lead_key' AS issue, COUNT(*)
FROM fact_seller_leads f LEFT JOIN dim_lead l ON f.lead_key = l.lead_key WHERE l.lead_key IS NULL;

SELECT 'fact_leads - orphan first_contact_date_key' AS issue, COUNT(*)
FROM fact_seller_leads f LEFT JOIN dim_date d ON f.first_contact_date_key = d.date_key WHERE d.date_key IS NULL;

SELECT 'fact_leads - orphan seller_key' AS issue, COUNT(*)
FROM fact_seller_leads f LEFT JOIN dim_seller s ON f.seller_key = s.seller_key WHERE s.seller_key IS NULL;

-- -------------------------------------------------------------------------
-- 3. BUSINESS RULE VALIDATIONS
-- -------------------------------------------------------------------------
-- 3.1 fact_order_fulfillment: is_delivered flag must match order_status
SELECT 'fact_ord_ful - delivered flag mismatch' AS issue, COUNT(*)
FROM fact_order_fulfillment
WHERE order_status = 'delivered' AND NOT is_delivered;

-- 3.2 on‑time orders must have days_late <= 0 (or NULL if not delivered)
SELECT 'fact_ord_ful - on_time mismatch' AS issue, COUNT(*)
FROM fact_order_fulfillment
WHERE is_on_time AND days_late > 0;

-- 3.3 Review scores must be between 1 and 5
SELECT 'fact_rev - score out of range' AS issue, COUNT(*)
FROM fact_reviews
WHERE review_score NOT BETWEEN 1 AND 5;

-- 3.4 Payment values must not be negative
SELECT 'fact_pay - negative value' AS issue, COUNT(*)
FROM fact_payments
WHERE payment_value < 0;

-- 3.5 Zero‑value payments must be flagged
SELECT 'fact_pay - zero value not flagged' AS issue, COUNT(*)
FROM fact_payments
WHERE payment_value = 0 AND NOT is_zero_value;

-- 3.6 Instalments must be >= 1 (already normalised in ETL)
SELECT 'fact_pay - zero instalments' AS issue, COUNT(*)
FROM fact_payments
WHERE payment_installments = 0;

-- 3.7 Seller leads: if is_closed, then won_date_key and days_to_close must not be null
SELECT 'fact_leads - closed without won_date' AS issue, COUNT(*)
FROM fact_seller_leads
WHERE is_closed AND won_date_key IS NULL;

SELECT 'fact_leads - closed without days_to_close' AS issue, COUNT(*)
FROM fact_seller_leads
WHERE is_closed AND days_to_close IS NULL;

-- 3.8 Orphaned sellers must map to seller_key = -1 (the Unknown row)
SELECT 'fact_leads - orphan seller not -1' AS issue, COUNT(*)
FROM fact_seller_leads
WHERE seller_key = -1 AND seller_key NOT IN (SELECT seller_key FROM dim_seller);
-- (expect 0 because -1 exists in dim_seller)

-- 3.9 dim_review_comment: has_comment must be TRUE when either title or message is non‑empty
SELECT 'dim_rev_comm - has_comment mismatch' AS issue, COUNT(*)
FROM dim_review_comment
WHERE (review_comment_title IS NOT NULL AND review_comment_title != '')
   OR (review_comment_message IS NOT NULL AND review_comment_message != '')
   AND NOT has_comment;

-- -------------------------------------------------------------------------
-- 4. DIMENSION CONSISTENCY
-- -------------------------------------------------------------------------
-- 4.1 dim_product: effective_to_date must be > effective_from_date (if not null)
SELECT 'dim_prod - invalid date range' AS issue, COUNT(*)
FROM dim_product
WHERE effective_to_date IS NOT NULL AND effective_to_date <= effective_from_date;

-- 4.2 dim_product: current versions must have NULL effective_to_date
SELECT 'dim_prod - current has end date' AS issue, COUNT(*)
FROM dim_product
WHERE is_current AND effective_to_date IS NOT NULL;

-- 4.3 All current products should have non‑NULL English category name
SELECT 'dim_prod - missing english category' AS issue, COUNT(*)
FROM dim_product
WHERE is_current AND (product_category_name_english IS NULL OR product_category_name_english = '');

-- 4.4 fact_sales: verify order_customer_id consistency with customer_key
-- For a given customer_key, all order_customer_id values should eventually map
-- to the same customer_unique_id in dim_customer (to detect mismatches).
SELECT 'fact_sales - customer_id mismatch' AS issue, COUNT(DISTINCT fs.order_customer_id)
FROM fact_sales fs
JOIN dim_customer c ON fs.customer_key = c.customer_key
GROUP BY c.customer_key, c.customer_unique_id
HAVING COUNT(DISTINCT fs.order_customer_id) > 1;
-- (If this returns rows, it means one dim_customer row is linked to multiple
-- distinct order_customer_id values – acceptable if a customer has multiple
-- accounts, but worth noting.)

-- -------------------------------------------------------------------------
-- 5. WATERMARK STATUS
-- -------------------------------------------------------------------------
SELECT '--- WATERMARK STATUS ---' AS info;
SELECT table_name, last_extracted, status FROM etl_control ORDER BY table_name;

-- -------------------------------------------------------------------------
-- 6. ROW COUNT SUMMARY (optional)
-- -------------------------------------------------------------------------
SELECT 'fact_sales' AS table_name, COUNT(*) AS cnt FROM fact_sales
UNION ALL
SELECT 'fact_order_fulfillment', COUNT(*) FROM fact_order_fulfillment
UNION ALL
SELECT 'fact_payments', COUNT(*) FROM fact_payments
UNION ALL
SELECT 'fact_reviews', COUNT(*) FROM fact_reviews
UNION ALL
SELECT 'fact_seller_leads', COUNT(*) FROM fact_seller_leads;

-- =============================================================================
-- End of data quality checks
-- =============================================================================
