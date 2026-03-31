-- Migration to stabilize daily weather snapshots by pipeline run date.
-- This allows observed_date (true observation date) to differ from the DAG run date.

SET @schema_name = DATABASE();

SET @sql = IF(
    EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = @schema_name
          AND table_name = 'weather_observations'
          AND column_name = 'snapshot_date'
    ),
    'SELECT 1',
    'ALTER TABLE weather_observations ADD COLUMN snapshot_date DATE NULL AFTER created_at'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE weather_observations
SET snapshot_date = DATE(created_at)
WHERE snapshot_date IS NULL;

ALTER TABLE weather_observations
    MODIFY COLUMN snapshot_date DATE NOT NULL;

SET @sql = IF(
    EXISTS (
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = @schema_name
          AND table_name = 'weather_observations'
          AND index_name = 'uniq_weather_province_observed_day'
    ),
    'ALTER TABLE weather_observations DROP INDEX uniq_weather_province_observed_day',
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
          AND index_name = 'uniq_weather_province_snapshot_day'
    ),
    'SELECT 1',
    'ALTER TABLE weather_observations ADD UNIQUE INDEX uniq_weather_province_snapshot_day (province, snapshot_date)'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

