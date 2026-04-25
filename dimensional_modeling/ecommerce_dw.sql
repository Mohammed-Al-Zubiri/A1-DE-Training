-- ================================================================================
-- E-commerce Data Warehouse:  Schema, Indexes, Semantic Layer and Reporting Views
-- ================================================================================
CREATE DATABASE ecommerce_dw;

-- ==========================================
-- LAYER 1: DIMENSION TABLES
-- ==========================================

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,            -- Format: YYYYMMDD
    full_date DATE NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL DEFAULT FALSE
);

-- Denormalized Product/Brand/Category (SCD Type 2)
CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,      -- Surrogate Key
    product_id INT NOT NULL,             -- Natural Key
    product_name VARCHAR(200) NOT NULL,
    brand_name VARCHAR(100) NOT NULL,
    country_of_origin VARCHAR(50),       
    category_name VARCHAR(100) NOT NULL,
    parent_category_name VARCHAR(100),   
    
    effective_start_date TIMESTAMP NOT NULL,
    effective_end_date TIMESTAMP         -- NULL means this is the current active record
);

-- Customer/User (SCD Type 2)
CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,     
    user_id INT NOT NULL,                
    full_name VARCHAR(150) NOT NULL,
    email VARCHAR(100) NOT NULL,
    phone VARCHAR(20),                   
    address TEXT,                        
    preferred_currency_code VARCHAR(3),
    
    effective_start_date TIMESTAMP NOT NULL,
    effective_end_date TIMESTAMP         
);

-- Branch (SCD Type 2)
CREATE TABLE dim_branch (
    branch_key SERIAL PRIMARY KEY,       
    branch_id INT NOT NULL,              
    branch_name VARCHAR(100) NOT NULL,
    city VARCHAR(50) NOT NULL,
    location_details TEXT,               
    manager_name VARCHAR(100),
    
    effective_start_date TIMESTAMP NOT NULL,
    effective_end_date TIMESTAMP         
);

-- Payment Method (SCD Type 1)
CREATE TABLE dim_payment_method (
    payment_method_key SERIAL PRIMARY KEY,
    method_id INT NOT NULL,
    method_name VARCHAR(50) NOT NULL,
    is_active BOOLEAN NOT NULL
);

-- Currency (SCD Type 1)
CREATE TABLE dim_currency (
    currency_key SERIAL PRIMARY KEY,
    currency_id INT NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    currency_name VARCHAR(50) NOT NULL
);

-- ==========================================
-- LAYER 2: FACT TABLES
-- ==========================================

-- Fact table for Sales Transactions (Grain: Individual Order Line/Item)

CREATE TABLE fact_sales (
    sales_key SERIAL PRIMARY KEY,
    
    -- Dimension Keys
    date_key INT NOT NULL REFERENCES dim_date(date_key),
    customer_key INT NOT NULL REFERENCES dim_customer(customer_key),
    product_key INT NOT NULL REFERENCES dim_product(product_key),
    branch_key INT NOT NULL REFERENCES dim_branch(branch_key),
    currency_key INT NOT NULL REFERENCES dim_currency(currency_key),
    payment_method_key INT NOT NULL REFERENCES dim_payment_method(payment_method_key),
    
    -- Degenerate Dimensions (Transactional context)
    order_id INT NOT NULL,               
    order_status VARCHAR(50) NOT NULL,
    
    -- Core Facts (Measures)
    quantity INT NOT NULL,
    
    -- Local Currency Amounts (As transacted)
    unit_sale_price_local DECIMAL(10, 2) NOT NULL,
    unit_purchase_price_local DECIMAL(10, 2) NOT NULL,
    sales_amount_local DECIMAL(10, 2) NOT NULL,   
    cost_amount_local DECIMAL(10, 2) NOT NULL,    
    net_profit_local DECIMAL(10, 2) NOT NULL,     
    
    -- Allocated Facts
    allocated_tax_amount_local DECIMAL(10, 2) NOT NULL, 
    
    -- Base Currency Amounts (Standardized to SAR via ETL)
    sales_amount_sar DECIMAL(10, 2) NOT NULL,
    cost_amount_sar DECIMAL(10, 2) NOT NULL,
    net_profit_sar DECIMAL(10, 2) NOT NULL
);


-- Periodic snapshot fact table for inventory
-- Grain: One row per product, per day (End-of-Day snapshot)
CREATE TABLE fact_inventory_snapshot (
    inventory_snapshot_key SERIAL PRIMARY KEY,
    
    -- Dimension Keys
    date_key INT NOT NULL REFERENCES dim_date(date_key),
    product_key INT NOT NULL REFERENCES dim_product(product_key),
    
    -- Core Facts (Measures)
    stock_quantity INT NOT NULL,
    min_stock_level INT NOT NULL,
    
    -- Derived Facts
    is_below_min_stock BOOLEAN NOT NULL
);

-- ==========================================
-- LAYER 3: PERFORMANCE INDEXING
-- ==========================================

CREATE INDEX idx_dim_product_current ON dim_product (product_id) WHERE effective_end_date IS NULL;
CREATE INDEX idx_dim_customer_current ON dim_customer (user_id) WHERE effective_end_date IS NULL;
CREATE INDEX idx_dim_branch_current ON dim_branch (branch_id) WHERE effective_end_date IS NULL;

CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_branch ON fact_sales(branch_key);

-- ==========================================
-- LAYER 4: THE SEMANTIC LAYER (Business Facade)
-- ==========================================

-- Creating views to abstract the SCD logic and provide a clean interface for reporting and analysis

CREATE VIEW vw_dim_product AS 
SELECT 
    product_key,
    product_id,
    product_name,
    brand_name,
    country_of_origin,                   
    category_name,
    parent_category_name,
    effective_start_date,
    effective_end_date,
    CASE WHEN effective_end_date IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM dim_product;

CREATE VIEW vw_dim_customer AS 
SELECT 
    customer_key,
    user_id,
    full_name,
    email,
    phone,                               
    address,                             
    preferred_currency_code,
    effective_start_date,
    effective_end_date,
    CASE WHEN effective_end_date IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM dim_customer;

CREATE VIEW vw_dim_branch AS 
SELECT 
    branch_key,
    branch_id,
    branch_name,
    city,
    location_details,                    
    manager_name,
    effective_start_date,
    effective_end_date,
    CASE WHEN effective_end_date IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM dim_branch;

CREATE VIEW vw_fact_sales AS
SELECT * FROM fact_sales;

CREATE VIEW vw_fact_inventory_snapshot AS
SELECT * FROM fact_inventory_snapshot;

-- ==========================================
-- LAYER 5: REPORTING VIEW
-- ==========================================

CREATE VIEW vw_rpt_sales_profit AS
SELECT 
    f.order_id,
    d.full_date AS order_date,
    SUM(f.sales_amount_sar) AS total_sales_sar,
    SUM(f.cost_amount_sar) AS total_cost_sar,
    SUM(f.net_profit_sar) AS total_net_profit_sar,
    SUM(f.allocated_tax_amount_local) AS total_tax_collected
FROM vw_fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.order_status != 'cancelled'
GROUP BY 
    f.order_id, 
    d.full_date;

-- This view allows the business to instantly see what needs to be ordered today
CREATE VIEW vw_rpt_low_stock_alerts AS
SELECT 
    d.full_date AS snapshot_date,
    p.product_id,
    p.product_name,
    p.category_name,
    f.stock_quantity,
    f.min_stock_level,
FROM vw_fact_inventory_snapshot f
JOIN vw_dim_product p ON f.product_key = p.product_key
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.is_below_min_stock = TRUE
AND p.is_current = TRUE;
