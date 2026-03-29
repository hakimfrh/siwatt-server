-- Quick checks for retrain status/result logs

USE siwatt4;

SELECT train_id, model_type, status, path, train_time, epoch, error_message
FROM train_log
ORDER BY train_id DESC
LIMIT 20;

-- Monitor retrain progress stage (current_task) from details JSON
SELECT
  train_id,
  model_type,
  status,
  JSON_UNQUOTE(JSON_EXTRACT(details, '$.current_task')) AS current_task,
  JSON_UNQUOTE(JSON_EXTRACT(details, '$.updated_at')) AS updated_at,
  train_time
FROM train_log
ORDER BY train_id DESC
LIMIT 20;

SELECT
  train_id,
  model_type,
  status,
  JSON_EXTRACT(train_result, '$.mae') AS mae,
  JSON_EXTRACT(train_result, '$.rmse') AS rmse,
  JSON_EXTRACT(train_result, '$.mape') AS mape,
  JSON_EXTRACT(details, '$.train_samples') AS train_samples,
  JSON_EXTRACT(details, '$.validation_samples') AS validation_samples,
  train_time
FROM train_log
WHERE status = 'done'
ORDER BY train_id DESC
LIMIT 20;
