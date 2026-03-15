-- Table for etl_api_pipeline.py
CREATE TABLE IF NOT EXISTS api_data (
    id INT PRIMARY KEY,
    userId INT,
    title VARCHAR(255),
    body TEXT,
    processed_at TIMESTAMP
);

-- Table for ecommerce_full_pipeline.py (orders)
CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    price DECIMAL(10,2),
    category VARCHAR(100),
    order_date DATE,
    quantity INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for ecommerce_full_pipeline.py (reports)
CREATE TABLE IF NOT EXISTS daily_reports (
    id INT AUTO_INCREMENT PRIMARY KEY,
    category VARCHAR(100),
    total_sales DECIMAL(10,2),
    total_quantity INT,
    report_date DATE,
    processed_at TIMESTAMP
);

-- Table for customer_review_sentiment_pipeline.py
CREATE TABLE IF NOT EXISTS customer_sentiment_results (
    review_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    review_date DATE,
    sentiment VARCHAR(50),
    rating INT
);

-- Table for bot_exchange_rate_pipeline.py
CREATE TABLE IF NOT EXISTS bot_exchange_rates (
    period DATE,
    currency_code VARCHAR(10),
    currency_name VARCHAR(255),
    buying_rate DECIMAL(10,4),
    selling_rate DECIMAL(10,4),
    mid_rate DECIMAL(10,4),
    updated_at TIMESTAMP,
    PRIMARY KEY (period, currency_code)
);

-- Table for exchange_rate_pipeline.py
CREATE TABLE IF NOT EXISTS global_exchange_rates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    rate_date DATE,
    currency_code VARCHAR(10),
    rate DECIMAL(10,4),
    updated_at TIMESTAMP,
    period DATE,
    currency VARCHAR(10)
);