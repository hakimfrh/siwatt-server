-- Schema for auto-retrain logs
-- Run this in your MySQL database before enabling ML_ENABLE_RETRAIN=true

USE siwatt4;

CREATE TABLE IF NOT EXISTS train_log (
  train_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  model_type ENUM('hourly','daily') NOT NULL,
  status ENUM('running','done','error') NOT NULL DEFAULT 'running',
  path TEXT,
  train_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  epoch INT,
  details JSON,
  train_result JSON,
  error_message TEXT,
  INDEX idx_model_status_time (model_type, status, train_time)
);

-- If you already have old table structure, run ALTERs below as needed.
-- ALTER TABLE train_log ADD COLUMN model_type ENUM('hourly','daily') NOT NULL AFTER train_id;
-- ALTER TABLE train_log ADD COLUMN status ENUM('running','done','error') NOT NULL DEFAULT 'running' AFTER model_type;
-- ALTER TABLE train_log ADD COLUMN details JSON NULL;
-- ALTER TABLE train_log ADD COLUMN train_result JSON NULL;
-- ALTER TABLE train_log ADD COLUMN error_message TEXT NULL;
-- CREATE INDEX idx_model_status_time ON train_log (model_type, status, train_time);
