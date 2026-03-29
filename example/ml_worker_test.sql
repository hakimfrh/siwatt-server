-- SQL test script for ML worker queue (MySQL)
-- Tested for queue table with schema like: id, user_id, device_id, type, status, params, progress, result, error_message, created_at, started_at, finished_at

USE siwatt_final;

-- 1) Verify prediction queue table candidates
SHOW TABLES LIKE '%predictions%';

-- 2) Set queue table name (choose one that exists)
-- Example: siwatt_predictions OR predictions
SET @queue_table = 'siwatt_predictions';

-- 3) Pick a device to test (change this)
SET @device_id = 1;

-- 4) Auto-derive user_id from selected device
SELECT @user_id := user_id FROM devices WHERE id = @device_id LIMIT 1;

-- 5) Validate selected device and history availability
SELECT d.id, d.user_id, d.device_code, d.device_name
FROM devices d
WHERE d.id = @device_id;

SELECT device_id, COUNT(*) AS hourly_rows, MIN(datetime) AS first_dt, MAX(datetime) AS last_dt
FROM data_hourly
WHERE device_id = @device_id;

-- 6) Build params JSON (aligned with notebook-style smart_fill)
SET @params_hourly = JSON_OBJECT(
  'horizon', 24,
  'history_hours', 3000,
  'fill_method', 'smart_fill',
  'smart_fill_weeks', 6
);

SET @params_daily = JSON_OBJECT(
  'horizon', 14,
  'history_hours', 4000,
  'fill_method', 'smart_fill',
  'smart_fill_weeks', 6,
  'allow_partial_daily', FALSE
);

-- Optional alternative: explicit history range from 00:00 to 00:00
-- SET @params_hourly = JSON_OBJECT(
--   'horizon', 24,
--   'history_start', '2026-03-20T00:00:00',
--   'history_end', '2026-03-27T00:00:00',
--   'fill_method', 'smart_fill',
--   'smart_fill_weeks', 6
-- );

-- 7) Insert one pending hourly job
SET @sql_insert_hourly = CONCAT(
  'INSERT INTO ', @queue_table,
  ' (user_id, device_id, type, status, params, progress, created_at) ',
  'VALUES (?, ?, ''hourly'', ''pending'', ?, ''{"percentage":0,"info":"pending"}'', NOW())'
);
PREPARE stmt_insert_hourly FROM @sql_insert_hourly;
EXECUTE stmt_insert_hourly USING @user_id, @device_id, @params_hourly;
DEALLOCATE PREPARE stmt_insert_hourly;

-- 8) Insert one pending daily job
SET @sql_insert_daily = CONCAT(
  'INSERT INTO ', @queue_table,
  ' (user_id, device_id, type, status, params, progress, created_at) ',
  'VALUES (?, ?, ''daily'', ''pending'', ?, ''{"percentage":0,"info":"pending"}'', NOW())'
);
PREPARE stmt_insert_daily FROM @sql_insert_daily;
EXECUTE stmt_insert_daily USING @user_id, @device_id, @params_daily;
DEALLOCATE PREPARE stmt_insert_daily;

-- 9) Monitor latest queue status (run repeatedly while worker is running)
SET @sql_monitor = CONCAT(
  'SELECT id, type, status, ',
  'JSON_EXTRACT(progress, ''$.percentage'') AS progress_percentage, ',
  'JSON_EXTRACT(progress, ''$.info'') AS progress_info, ',
  'error_message, created_at, started_at, finished_at ',
  'FROM ', @queue_table, ' ORDER BY id DESC LIMIT 20'
);
PREPARE stmt_monitor FROM @sql_monitor;
EXECUTE stmt_monitor;
DEALLOCATE PREPARE stmt_monitor;

-- 10) Inspect latest result payload for this device
SET @sql_result = CONCAT(
  'SELECT id, type, status, JSON_PRETTY(result) AS result_json ',
  'FROM ', @queue_table, ' WHERE device_id = ? ORDER BY id DESC LIMIT 5'
);
PREPARE stmt_result FROM @sql_result;
EXECUTE stmt_result USING @device_id;
DEALLOCATE PREPARE stmt_result;

-- 11) Optional: retry one failed job
-- SET @retry_job_id = 123;
-- SET @sql_retry = CONCAT(
--   'UPDATE ', @queue_table,
--   ' SET status = ''pending'', progress = ''{"percentage":0,"info":"pending"}'', error_message = NULL, started_at = NULL, finished_at = NULL ',
--   'WHERE id = ?'
-- );
-- PREPARE stmt_retry FROM @sql_retry;
-- EXECUTE stmt_retry USING @retry_job_id;
-- DEALLOCATE PREPARE stmt_retry;

-- 12) Optional: delete recent test jobs for selected device (be careful)
-- SET @sql_delete_test = CONCAT(
--   'DELETE FROM ', @queue_table,
--   ' WHERE device_id = ? AND created_at >= NOW() - INTERVAL 1 DAY'
-- );
-- PREPARE stmt_delete_test FROM @sql_delete_test;
-- EXECUTE stmt_delete_test USING @device_id;
-- DEALLOCATE PREPARE stmt_delete_test;
