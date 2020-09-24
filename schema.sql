USE logs;
CREATE TABLE IF NOT EXISTS logs.switchlogs (
  `ts_local` DateTime,
  `sw_name` String,
  `sw_ip` IPv4,
  `ts_remote` DateTime,
  `facility` UInt8,
  `severity` UInt8,
  `priority` UInt8,
  `log_msg` String
) ENGINE = MergeTree() PARTITION BY toYYYYMMDD(ts_local)
ORDER BY ts_local