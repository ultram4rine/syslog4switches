USE logs;
CREATE TABLE IF NOT EXISTS logs.nginx (
    `hostname` String,
    `timestamp` DateTime,
    `facility` UInt8,
    `severity` UInt8,
    `priority` UInt8,
    `content` String
) ENGINE = MergeTree() PARTITION BY toYYYYMMDD(timestamp)
ORDER BY timestamp