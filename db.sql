CREATE DATABASE IF NOT EXISTS logs

USE logs

CREATE TABLE IF NOT EXISTS logs.switchlogs
(
    `ts_local` DateTime,
    `sw_name` String,
    `sw_ip` String,
    `ts_remote` DateTime,
    `facility` UInt32,
    `severity` UInt32,
    `priority` UInt32,
    `log_time` String,
    `log_event_number` String,
    `log_module` String,
    `log_msg` String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ts_local)
ORDER BY ts_local
