-- =============================================================================
-- Olist Data Warehouse – Sample Analytical Queries
-- =============================================================================
-- These queries demonstrate how the star schema answers business questions.
-- All queries are written for the PostgreSQL target schema (create_schema.sql).
-- =============================================================================

-- -------------------------------------------------------------------------
-- Q1: How are sales trending over time?
-- -------------------------------------------------------------------------
-- Monthly revenue trend (product price only, excluding freight)
SELECT
    d.year,
    d.month_name,
    SUM(f.price) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS order_count,
    COUNT(*) AS items_sold
FROM fact_sales f
JOIN dim_date d ON f.purchase_date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- Daily revenue with 7‑day moving average
SELECT
    d.full_date,
    SUM(f.price) AS daily_revenue,
    AVG(SUM(f.price)) OVER (ORDER BY d.full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7d
FROM fact_sales f
JOIN dim_date d ON f.purchase_date_key = d.date_key
GROUP BY d.full_date
ORDER BY d.full_date;

-- -------------------------------------------------------------------------
-- Q2: Who are the most valuable customers? (by customer_unique_id)
-- -------------------------------------------------------------------------
-- Top 10 customers by lifetime value (total spend + freight)
SELECT
    c.customer_unique_id,
    COUNT(DISTINCT f.order_id) AS order_count,
    SUM(f.price + f.freight_value) AS lifetime_value,
    SUM(f.price) AS total_product_spend,
    ROUND(AVG(f.price + f.freight_value), 2) AS avg_order_value
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_unique_id
ORDER BY lifetime_value DESC
LIMIT 10;

-- Customer segmentation by order frequency
SELECT
    order_count_bucket,
    COUNT(*) AS customer_count,
    SUM(lifetime_value) AS total_revenue_in_bucket
FROM (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT f.order_id) AS order_count,
        SUM(f.price + f.freight_value) AS lifetime_value,
        CASE WHEN COUNT(DISTINCT f.order_id) = 1 THEN 'One-time'
             WHEN COUNT(DISTINCT f.order_id) BETWEEN 2 AND 5 THEN 'Repeat (2-5)'
             ELSE 'Loyal (6+)'
        END AS order_count_bucket
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_key = c.customer_key
    GROUP BY c.customer_unique_id
) sub
GROUP BY order_count_bucket
ORDER BY customer_count DESC;

-- -------------------------------------------------------------------------
-- Q3: What affects delivery performance?
-- -------------------------------------------------------------------------
-- Monthly on‑time delivery rate and average delay
SELECT
    d.year,
    d.month_name,
    COUNT(*) AS delivered_orders,
    ROUND(AVG(CASE WHEN fo.is_on_time THEN 1.0 ELSE 0.0 END) * 100, 2) AS on_time_pct,
    AVG(fo.days_late) AS avg_days_late,
    AVG(fo.days_to_customer) AS avg_delivery_days
FROM fact_order_fulfillment fo
JOIN dim_date d ON fo.purchase_date_key = d.date_key
WHERE fo.is_delivered
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- Correlation between delivery delay and review score
SELECT
    CASE
        WHEN fo.days_late <= 0 THEN 'On time / Early'
        WHEN fo.days_late BETWEEN 1 AND 5 THEN 'Late 1-5 days'
        WHEN fo.days_late BETWEEN 6 AND 10 THEN 'Late 6-10 days'
        ELSE 'Late >10 days'
    END AS delay_category,
    COUNT(*) AS review_count,
    ROUND(AVG(r.review_score), 2) AS avg_review_score,
    ROUND(AVG(fo.days_late), 1) AS avg_days_late
FROM fact_reviews r
JOIN fact_order_fulfillment fo ON r.order_id = fo.order_id
WHERE fo.is_delivered
GROUP BY delay_category
ORDER BY avg_days_late;

-- Delivery performance by destination state (using location dimension)
SELECT
    l.state,
    COUNT(*) AS orders,
    ROUND(AVG(CASE WHEN fo.is_on_time THEN 1.0 ELSE 0.0 END) * 100, 2) AS on_time_pct,
    AVG(fo.days_to_customer) AS avg_delivery_days
FROM fact_order_fulfillment fo
JOIN dim_location l ON fo.destination_location_key = l.location_key
WHERE fo.is_delivered
GROUP BY l.state
ORDER BY orders DESC
LIMIT 10;

-- -------------------------------------------------------------------------
-- Q4: Which products/categories drive revenue?
-- -------------------------------------------------------------------------
-- Top 10 categories by revenue
SELECT
    p.product_category_name_english AS category,
    COUNT(DISTINCT f.order_id) AS orders,
    COUNT(*) AS items_sold,
    ROUND(SUM(f.price), 2) AS total_revenue,
    ROUND(SUM(f.price) / NULLIF(SUM(SUM(f.price)) OVER (), 0) * 100, 2) AS revenue_pct
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_category_name_english
ORDER BY total_revenue DESC
LIMIT 10;

-- Top 10 individual products by revenue
SELECT
    p.product_id,
    p.product_category_name_english AS category,
    COUNT(*) AS items_sold,
    ROUND(SUM(f.price), 2) AS revenue
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_id, p.product_category_name_english
ORDER BY revenue DESC
LIMIT 10;

-- -------------------------------------------------------------------------
-- Q5: Payment method distribution and instalment behaviour
-- -------------------------------------------------------------------------
-- Payment type usage
SELECT
    pt.payment_type_code,
    COUNT(*) AS transaction_count,
    COUNT(DISTINCT fp.order_id) AS orders,
    ROUND(SUM(fp.payment_value), 2) AS total_amount,
    ROUND(AVG(fp.payment_installments), 1) AS avg_installments,
    ROUND(AVG(fp.payment_value), 2) AS avg_transaction_value
FROM fact_payments fp
JOIN dim_payment_type pt ON fp.payment_type_key = pt.payment_type_key
GROUP BY pt.payment_type_code
ORDER BY total_amount DESC;

-- Instalment distribution for credit card payments
SELECT
    payment_installments,
    COUNT(*) AS transaction_count,
    ROUND(SUM(payment_value), 2) AS total_value,
    ROUND(AVG(payment_value), 2) AS avg_value
FROM fact_payments
WHERE payment_type_key = (SELECT payment_type_key FROM dim_payment_type WHERE payment_type_code = 'credit_card')
GROUP BY payment_installments
ORDER BY payment_installments;

-- -------------------------------------------------------------------------
-- Q6: Customer satisfaction trends (reviews)
-- -------------------------------------------------------------------------
-- Average review score by month
SELECT
    d.year,
    d.month_name,
    COUNT(*) AS review_count,
    ROUND(AVG(r.review_score), 2) AS avg_score,
    ROUND(SUM(CASE WHEN r.review_score >= 4 THEN 1.0 ELSE 0.0 END) / COUNT(*) * 100, 2) AS positive_pct
FROM fact_reviews r
JOIN dim_date d ON r.review_creation_date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- Score distribution
SELECT
    review_score,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM fact_reviews
GROUP BY review_score
ORDER BY review_score DESC;

-- Top 10 sellers with worst review scores (join to fact_sales)
SELECT
    s.seller_id,
    COUNT(*) AS reviews,
    ROUND(AVG(r.review_score), 2) AS avg_score,
    COUNT(DISTINCT f.order_id) AS orders_fulfilled
FROM fact_reviews r
JOIN fact_sales f ON r.order_id = f.order_id
JOIN dim_seller s ON f.seller_key = s.seller_key
WHERE s.seller_id != 'UNKNOWN'
GROUP BY s.seller_id
HAVING COUNT(*) >= 5
ORDER BY avg_score ASC
LIMIT 10;

-- -------------------------------------------------------------------------
-- Q7: Seller acquisition funnel metrics
-- -------------------------------------------------------------------------
-- Conversion rate by lead origin
SELECT
    l.origin,
    COUNT(*) AS leads_qualified,
    SUM(CASE WHEN fl.is_closed THEN 1 ELSE 0 END) AS leads_closed,
    ROUND(AVG(CASE WHEN fl.is_closed THEN 1.0 ELSE 0.0 END) * 100, 2) AS conversion_pct,
    AVG(fl.days_to_close) AS avg_days_to_close
FROM fact_seller_leads fl
JOIN dim_lead l ON fl.lead_key = l.lead_key
GROUP BY l.origin
ORDER BY leads_qualified DESC;

-- Revenue generated by acquired sellers (join to fact_sales via seller_key)
SELECT
    l.origin,
    COUNT(DISTINCT fl.seller_key) AS sellers_acquired,
    ROUND(SUM(fs.price), 2) AS total_revenue_generated
FROM fact_seller_leads fl
JOIN dim_lead l ON fl.lead_key = l.lead_key
JOIN fact_sales fs ON fl.seller_key = fs.seller_key
WHERE fl.is_closed
  AND fl.seller_key != -1
GROUP BY l.origin
ORDER BY total_revenue_generated DESC;

-- Average sales cycle length by business segment
SELECT
    l.business_segment,
    COUNT(*) AS closed_leads,
    ROUND(AVG(fl.days_to_close), 1) AS avg_days_to_close,
    ROUND(AVG(fl.declared_monthly_revenue), 2) AS avg_declared_monthly_revenue
FROM fact_seller_leads fl
JOIN dim_lead l ON fl.lead_key = l.lead_key
WHERE fl.is_closed
  AND l.business_segment IS NOT NULL
GROUP BY l.business_segment
ORDER BY closed_leads DESC;

-- -------------------------------------------------------------------------
-- Q8: SCD Type2 demonstration: revenue by category uses the version that
--     was active at the time of sale
-- -------------------------------------------------------------------------
SELECT
    p.product_category_name_english AS category_at_sale_time,
    d.year,
    d.month_name,
    ROUND(SUM(f.price), 2) AS revenue
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_date d ON f.purchase_date_key = d.date_key
WHERE p.product_category_name_english = 'Health & Beauty'
GROUP BY p.product_category_name_english, d.year, d.month_name
ORDER BY d.year, d.month_name;

-- =============================================================================
-- End of analytical queries
-- =============================================================================