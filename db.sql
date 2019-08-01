CREATE TABLE switchlogs
(
    `log_id` UInt32,
    `ts_local` DateTime,
    `sw_name` String,
    `sw_ip` String,
    `ts_remote` DateTime,
    `facility` UInt32,
    `severity` UInt32,
    `priority` UInt32,
    `log_time` String,
    `log_msg` String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ts_local)
PRIMARY KEY log_id
ORDER BY ts_local

