-- Migration for existing weather_observations tables on long-lived MySQL volumes.
-- Keep docker/mysql/init.sql as the schema source of truth for new environments.
-- Run this manually only on existing databases that already have weather_observations.

SET @schema_name = DATABASE();

SET @sql = IF(
    EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = @schema_name
          AND table_name = 'weather_observations'
          AND column_name = 'province'
    ),
    'SELECT 1',
    'ALTER TABLE weather_observations ADD COLUMN province VARCHAR(120) NULL AFTER id'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
    EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = @schema_name
          AND table_name = 'weather_observations'
          AND column_name = 'region'
    ),
    'SELECT 1',
    'ALTER TABLE weather_observations ADD COLUMN region VARCHAR(40) NULL AFTER province'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
    EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = @schema_name
          AND table_name = 'weather_observations'
          AND column_name = 'country_code'
    ),
    'SELECT 1',
    'ALTER TABLE weather_observations ADD COLUMN country_code VARCHAR(8) NOT NULL DEFAULT ''TH'' AFTER city'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE weather_observations
SET province = COALESCE(NULLIF(province, ''), city)
WHERE province IS NULL OR province = '';

UPDATE weather_observations
SET region = 'Other'
WHERE region IS NULL OR region = '';

UPDATE weather_observations
SET country_code = 'TH'
WHERE country_code IS NULL OR country_code = '';

ALTER TABLE weather_observations
    MODIFY COLUMN province VARCHAR(120) NOT NULL,
    MODIFY COLUMN region VARCHAR(40) NOT NULL,
    MODIFY COLUMN city VARCHAR(120) NOT NULL,
    MODIFY COLUMN country_code VARCHAR(8) NOT NULL DEFAULT 'TH';

SET @sql = IF(
    EXISTS (
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = @schema_name
          AND table_name = 'weather_observations'
          AND index_name = 'uniq_weather_city_observed'
    ),
    'ALTER TABLE weather_observations DROP INDEX uniq_weather_city_observed',
    'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
    EXISTS (
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = @schema_name
          AND table_name = 'weather_observations'
          AND index_name = 'uniq_weather_province_observed'
    ),
    'SELECT 1',
    'ALTER TABLE weather_observations ADD UNIQUE INDEX uniq_weather_province_observed (province, observed_at_local)'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
