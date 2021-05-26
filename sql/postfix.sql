USE logs;
CREATE TABLE IF NOT EXISTS logs.postfix (
    `daemon` String,
    `timestamp` DateTime,
    `message` String
) ENGINE = MergeTree() PARTITION BY toYYYYMMDD(timestamp)
ORDER BY timestamp