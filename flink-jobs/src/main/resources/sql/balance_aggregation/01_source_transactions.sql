-- ==============================================================================
-- WHAT: Source table definition for transaction events from Kafka
-- WHY: Defines schema and connector configuration for reading transaction data
-- HOW: Creates a Kafka source table with Avro deserialization and watermarks
-- ==============================================================================

CREATE TABLE transactions (
    id STRING,
    transaction_type_id INT,
    account_id STRING,
    bank_id STRING,
    amount_transferred DOUBLE,
    reason STRING,
    timestamp BIGINT,
    ts AS TO_TIMESTAMP_LTZ(timestamp, 3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '${balance.source.topic}',
    'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
    'properties.group.id' = '${balance.consumer.group.id}',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${schema.registry.url}',
    'scan.startup.mode' = 'earliest-offset'
);
