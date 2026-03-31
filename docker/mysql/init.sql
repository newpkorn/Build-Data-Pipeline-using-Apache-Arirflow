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

-- Table for weather_api_pipeline.py
CREATE TABLE IF NOT EXISTS weather_observations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    province VARCHAR(120) NOT NULL,
    region VARCHAR(40) NOT NULL,
    city VARCHAR(120) NOT NULL,
    country_code VARCHAR(8) NOT NULL DEFAULT 'TH',
    weather_description VARCHAR(255),
    temperature_celsius DECIMAL(8,2),
    feels_like_celsius DECIMAL(8,2),
    minimum_temp_celsius DECIMAL(8,2),
    maximum_temp_celsius DECIMAL(8,2),
    pressure INT,
    humidity INT,
    wind_speed DECIMAL(8,2),
    observed_at_local DATETIME NOT NULL,
    observed_date DATE NOT NULL,
    sunrise_local DATETIME,
    sunset_local DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    snapshot_date DATE NOT NULL,
    UNIQUE KEY uniq_weather_province_snapshot_day (province, snapshot_date),
    KEY idx_weather_observed_date (observed_date)
);
