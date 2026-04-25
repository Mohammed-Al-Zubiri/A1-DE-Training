import pandas as pd
from sqlalchemy import create_engine, text
import holidays
import logging

# Set up logging for the pipeline
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EcommerceETLPipeline:
    def __init__(self, source_db_uri, target_dw_uri):
        """Initialize database connections and state dictionaries."""
        logging.info("Initializing ETL Pipeline...")
        self.source_engine = create_engine(source_db_uri)
        self.dw_engine = create_engine(target_dw_uri)
        
        # Dictionary to hold extracted operational data
        self.raw_data = {}

    # ==========================================
    # PHASE 1: EXTRACTION
    # ==========================================
    def extract_source_data(self):
        """Extract all required tables from the source OLTP database."""
        logging.info("Extracting data from source database...")
        tables_to_extract = [
            'currencies', 'brands', 'branches', 'categories', 
            'users', 'products', 'payment_methods', 
            'orders', 'payments', 'order_items'
        ]
        
        for table in tables_to_extract:
            self.raw_data[table] = pd.read_sql(f"SELECT * FROM {table}", self.source_engine)
            logging.info(f"Extracted {len(self.raw_data[table])} rows from {table}")

    # ==========================================
    # PHASE 2: TRANSFORM & LOAD DIMENSIONS
    # ==========================================
    def process_dim_date(self, start_date='2024-01-01', end_date='2030-12-31'):
        """Generate the Date dimension dynamically in Python."""
        logging.info("Processing dim_date...")

        count = pd.read_sql("SELECT COUNT(*) FROM dim_date", self.dw_engine).iloc[0, 0]
        if count > 0:
            logging.info("dim_date is already populated. Skipping...")
            return
        
        # Generate date range
        date_range = pd.date_range(start=start_date, end=end_date)
        df_date = pd.DataFrame(date_range, columns=['full_date'])
        
        # Saudi Arabia holidays lookup
        sa_holidays = holidays.SA(years=range(2024, 2031))
        
        # Extract attributes
        df_date['date_key'] = df_date['full_date'].dt.strftime('%Y%m%d').astype(int)
        df_date['day_name'] = df_date['full_date'].dt.day_name()
        df_date['month_name'] = df_date['full_date'].dt.month_name()
        df_date['quarter'] = df_date['full_date'].dt.quarter
        df_date['year'] = df_date['full_date'].dt.year
        
        # Business rules
        df_date['is_weekend'] = df_date['day_name'].isin(['Friday', 'Saturday'])
        df_date['is_holiday'] = df_date['full_date'].apply(lambda x: x in sa_holidays)
        
        # Reorder to match DW schema exactly and load
        cols = ['date_key', 'full_date', 'day_name', 'month_name', 'quarter', 'year', 'is_weekend', 'is_holiday']
        self._load_to_dw(df_date[cols], 'dim_date')

    def process_scd_type_1_dims(self):
        """Process Type 1 (Overwrite) Dimensions: Currencies and Payment Methods"""
        logging.info("Processing SCD Type 1 dimensions...")
        
        # 1. Currency
        df_currency = self.raw_data['currencies'][['currency_id', 'currency_code', 'currency_name']].copy()
        self._load_to_dw(df_currency, 'dim_currency')
        
        # 2. Payment Method
        df_payment = self.raw_data['payment_methods'][['method_id', 'method_name', 'is_active']].copy()

        unknown_payment = pd.DataFrame([{
            'method_id': -1, 
            'method_name': 'Unknown / No Payment', 
            'is_active': False
        }])
        df_payment = pd.concat([unknown_payment, df_payment], ignore_index=True)

        self._load_to_dw(df_payment, 'dim_payment_method')

    def process_scd_type_2_dims(self):
        """Process Type 2 (Historical) Dimensions for Initial Load."""
        logging.info("Processing SCD Type 2 dimensions...")
        now = pd.Timestamp.now()
        
        # 1. Customer
        df_customer = self.raw_data['users'].copy()
        # Merge preferred currency code (required in Target)
        df_customer = df_customer.merge(self.raw_data['currencies'], left_on='preferred_currency_id', right_on='currency_id', how='left')
        df_customer.rename(columns={'currency_code': 'preferred_currency_code'}, inplace=True)
        
        # Add SCD2 Columns
        df_customer['effective_start_date'] = now
        df_customer['effective_end_date'] = pd.NaT
        
        cols_cust = ['user_id', 'full_name', 'email', 'phone', 'address', 'preferred_currency_code', 'effective_start_date', 'effective_end_date']
        self._load_to_dw(df_customer[cols_cust], 'dim_customer')
        
        # 2. Branch
        df_branch = self.raw_data['branches'].copy()
        df_branch['effective_start_date'] = now
        df_branch['effective_end_date'] = pd.NaT
        
        cols_branch = ['branch_id', 'branch_name', 'city', 'location_details', 'manager_name', 'effective_start_date', 'effective_end_date']
        self._load_to_dw(df_branch[cols_branch], 'dim_branch')

        # 3. Product (Requires Denormalization / Flattening)
        df_prod = self.raw_data['products'].copy()
        df_brand = self.raw_data['brands'].copy()
        df_cat = self.raw_data['categories'].copy()

        df_cat['category_id'] = pd.to_numeric(df_cat['category_id'], errors='coerce').astype('Int64')
        df_cat['parent_category_id'] = pd.to_numeric(df_cat['parent_category_id'], errors='coerce').astype('Int64')
        
        # Flatten Categories (Self-Join to get parent name)
        df_cat_merged = df_cat.merge(df_cat, left_on='parent_category_id', right_on='category_id', suffixes=('', '_parent'), how='left')
        df_cat_merged.rename(columns={'category_name_parent': 'parent_category_name'}, inplace=True)
        
        # Join Product to Brand and Category
        df_prod = df_prod.merge(df_brand, on='brand_id', how='left')
        df_prod = df_prod.merge(df_cat_merged[['category_id', 'category_name', 'parent_category_name']], on='category_id', how='left')
        
        df_prod['effective_start_date'] = now
        df_prod['effective_end_date'] = pd.NaT
        
        cols_prod = ['product_id', 'product_name', 'brand_name', 'country_of_origin', 'category_name', 'parent_category_name', 'effective_start_date', 'effective_end_date']
        self._load_to_dw(df_prod[cols_prod], 'dim_product')

    # ==========================================
    # PHASE 3: TRANSFORM & LOAD FACTS
    # ==========================================
    def process_fact_sales(self):
        """Process the main transactional fact table."""
        logging.info("Processing fact_sales...")
        
        # 1. Base Join: Order Items + Orders + Payments
        df_items = self.raw_data['order_items'].copy()
        df_orders = self.raw_data['orders'].copy()
        df_payments = self.raw_data['payments'].copy()
        df_curr = self.raw_data['currencies'].copy()
        
        # Build the denormalized fact DataFrame
        df_fact = df_items.merge(df_orders, on='order_id', how='inner')
        # Assuming 1 Payment per order
        df_fact = df_fact.merge(df_payments[['order_id', 'method_id']], on='order_id', how='left')
        df_fact = df_fact.merge(df_curr[['currency_id', 'exchange_rate_to_sar']], on='currency_id', how='left')

        df_fact['method_id'] = df_fact['method_id'].fillna(-1)
        
        # 2. Extract Foreign Keys via Database Lookup (Getting Surrogate Keys)
        # Fetch current active keys from the Data Warehouse
        dim_cust_map = pd.read_sql("SELECT customer_key, user_id FROM dim_customer WHERE effective_end_date IS NULL", self.dw_engine)
        dim_prod_map = pd.read_sql("SELECT product_key, product_id FROM dim_product WHERE effective_end_date IS NULL", self.dw_engine)
        dim_branch_map = pd.read_sql("SELECT branch_key, branch_id FROM dim_branch WHERE effective_end_date IS NULL", self.dw_engine)
        dim_pay_map = pd.read_sql("SELECT payment_method_key, method_id FROM dim_payment_method", self.dw_engine)
        dim_curr_map = pd.read_sql("SELECT currency_key, currency_id FROM dim_currency", self.dw_engine)
        
        # Map natural keys to surrogate keys
        df_fact['date_key'] = df_fact['order_date'].dt.strftime('%Y%m%d').astype(int)
        df_fact = df_fact.merge(dim_cust_map, on='user_id', how='left')
        df_fact = df_fact.merge(dim_prod_map, on='product_id', how='left')
        df_fact = df_fact.merge(dim_branch_map, on='branch_id', how='left')
        df_fact = df_fact.merge(dim_pay_map, on='method_id', how='left')
        df_fact = df_fact.merge(dim_curr_map, on='currency_id', how='left')
        
        # 3. Calculate Core Facts
        df_fact['sales_amount_local'] = df_fact['quantity'] * df_fact['unit_sale_price']
        df_fact['cost_amount_local'] = df_fact['quantity'] * df_fact['unit_purchase_price']
        df_fact['net_profit_local'] = df_fact['sales_amount_local'] - df_fact['cost_amount_local']
        
        # 4. Calculate Allocated Tax (Handle div by 0 safely)
        df_fact['allocated_tax_amount_local'] = 0.0
        mask = df_fact['subtotal'] > 0
        df_fact.loc[mask, 'allocated_tax_amount_local'] = (df_fact['sales_amount_local'] / df_fact['subtotal']) * df_fact['tax_amount']
        
        # 5. Convert to Base Currency (SAR)
        # Assuming exchange rate is local_amount / rate = SAR_amount
        df_fact['sales_amount_sar'] = df_fact['sales_amount_local'] / df_fact['exchange_rate_to_sar']
        df_fact['cost_amount_sar'] = df_fact['cost_amount_local'] / df_fact['exchange_rate_to_sar']
        df_fact['net_profit_sar'] = df_fact['net_profit_local'] / df_fact['exchange_rate_to_sar']
        
        # Rename transactional columns to match Degenerate Dimensions
        df_fact.rename(columns={'status': 'order_status'}, inplace=True)
        
        # Clean and Load
        cols_fact = [
            'date_key', 'customer_key', 'product_key', 'branch_key', 'currency_key', 'payment_method_key',
            'order_id', 'order_status', 'quantity', 'unit_sale_price', 'unit_purchase_price', 
            'sales_amount_local', 'cost_amount_local', 'net_profit_local', 'allocated_tax_amount_local',
            'sales_amount_sar', 'cost_amount_sar', 'net_profit_sar'
        ]
        
        # Rename unit columns to match schema
        df_fact.rename(columns={
            'unit_sale_price': 'unit_sale_price_local', 
            'unit_purchase_price': 'unit_purchase_price_local'
        }, inplace=True)
        
        cols_fact_final = [c if c not in ['unit_sale_price', 'unit_purchase_price'] else f"{c}_local" for c in cols_fact]
        
        self._load_to_dw(df_fact[cols_fact_final], 'fact_sales')

    def process_fact_inventory(self):
        """Process the periodic snapshot fact table."""
        logging.info("Processing fact_inventory_snapshot...")
        
        df_prod = self.raw_data['products'].copy()
        
        # Snapshot Context
        today_key = int(pd.Timestamp.now().strftime('%Y%m%d'))
        df_prod['date_key'] = today_key
        
        # Lookup Product Key
        dim_prod_map = pd.read_sql("SELECT product_key, product_id FROM dim_product WHERE effective_end_date IS NULL", self.dw_engine)
        df_inv = df_prod.merge(dim_prod_map, on='product_id', how='inner')
        
        # Derived Boolean Fact
        df_inv['is_below_min_stock'] = df_inv['stock_quantity'] <= df_inv['min_stock_level']
        
        cols_inv = ['date_key', 'product_key', 'stock_quantity', 'min_stock_level', 'is_below_min_stock']
        self._load_to_dw(df_inv[cols_inv], 'fact_inventory_snapshot')

    # ==========================================
    # UTILITY METHODS
    # ==========================================
    def _load_to_dw(self, df, table_name):
        """Helper function to load dataframes to PostgreSQL."""
        try:
            # We use if_exists='append' to simulate real ETL runs. 
            # (In production, we'd use MERGE/UPSERT logic for updates)
            df.to_sql(table_name, self.dw_engine, if_exists='append', index=False)
            logging.info(f"Successfully loaded {len(df)} rows into {table_name}")
        except Exception as e:
            logging.error(f"Failed to load {table_name}: {str(e)}")

    def clean_target_tables(self):
        """Truncate target tables to prevent duplicate appends (Full Refresh Pattern)."""
        logging.info("Truncating DW tables for a clean run...")
        
        # We DO NOT truncate dim_date because it is a static, generated dimension
        truncate_sql = text("""
            TRUNCATE TABLE 
                fact_inventory_snapshot, 
                fact_sales, 
                dim_product, 
                dim_branch, 
                dim_customer, 
                dim_payment_method, 
                dim_currency 
            RESTART IDENTITY CASCADE;
        """)
        
        try:
            with self.dw_engine.begin() as conn:
                conn.execute(truncate_sql)
            logging.info("Successfully truncated target tables.")
        except Exception as e:
            logging.error(f"Failed to truncate tables: {str(e)}")

    def run(self):
        """Master orchestration method."""
        logging.info("--- Starting E-commerce ETL Pipeline ---")

        self.clean_target_tables()
        
        # Phase 1
        self.extract_source_data()
        
        # Phase 2 (Order matters! Dependencies first)
        self.process_dim_date()
        self.process_scd_type_1_dims()
        self.process_scd_type_2_dims()
        
        # Phase 3
        self.process_fact_sales()
        self.process_fact_inventory()
        
        logging.info("--- ETL Pipeline Completed Successfully ---")

# ==========================================
# EXECUTION
# ==========================================
if __name__ == "__main__":
    # Replace with your actual database credentials
    SOURCE_URI = "postgresql://postgres:7710@localhost:5432/ecommerce_db"
    TARGET_URI = "postgresql://postgres:7710@localhost:5432/ecommerce_dw"
    
    pipeline = EcommerceETLPipeline(SOURCE_URI, TARGET_URI)
    pipeline.run()
