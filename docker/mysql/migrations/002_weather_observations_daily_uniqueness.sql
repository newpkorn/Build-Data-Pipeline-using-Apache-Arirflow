-- Migration to enforce one weather observation per province per local day.
-- Run this manually on existing databases with historical weather_observations data.

SET @schema_name = DATABASE();

SET @sql = IF(
    EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = @schema_name
          AND table_name = 'weather_observations'
          AND column_name = 'observed_date'
    ),
    'SELECT 1',
    'ALTER TABLE weather_observations ADD COLUMN observed_date DATE NULL AFTER observed_at_local'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE weather_observations
SET observed_date = DATE(observed_at_local)
WHERE observed_date IS NULL;

-- Keep only the latest row per province/day before adding the new unique index.
DELETE w1
FROM weather_observations w1
JOIN weather_observations w2
  ON w1.province = w2.province
 AND DATE(w1.observed_at_local) = DATE(w2.observed_at_local)
 AND (
      w1.observed_at_local < w2.observed_at_local
      OR (w1.observed_at_local = w2.observed_at_local AND w1.id < w2.id)
 );

UPDATE weather_observations
SET observed_date = DATE(observed_at_local)
WHERE observed_date IS NULL;

ALTER TABLE weather_observations
    MODIFY COLUMN observed_date DATE NOT NULL;

SET @sql = IF(
    EXISTS (
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = @schema_name
          AND table_name = 'weather_observations'
          AND index_name = 'uniq_weather_province_observed'
    ),
    'ALTER TABLE weather_observations DROP INDEX uniq_weather_province_observed',
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
          AND index_name = 'uniq_weather_province_observed_day'
    ),
    'SELECT 1',
    'ALTER TABLE weather_observations ADD UNIQUE INDEX uniq_weather_province_observed_day (province, observed_date)'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
