-- ==============================================================================
-- WHAT: Sink table definition for account balances to Kafka
-- WHY: Defines output schema and upsert-kafka connector for balance updates
-- HOW: Creates an upsert-kafka sink that updates balances by account_id key
-- ==============================================================================

CREATE TABLE account_balances (
    account_id STRING,
    total_balance DOUBLE,
    last_updated TIMESTAMP(3),
    PRIMARY KEY (account_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = '${balance.sink.topic}',
    'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
    'key.format' = 'avro-confluent',
    'key.avro-confluent.url' = '${schema.registry.url}',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${schema.registry.url}'
);
