package com.shahar.flink;

import com.shahar.bank.*;
import com.shahar.flink.config.FlinkConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * ==============================================================================
 * WHAT: Test data generator for bank events
 * WHY: Produces realistic transaction and fraud report events for testing
 * HOW: Kafka producer with Avro serialization and Schema Registry
 * ==============================================================================
 * 
 * LEARNING POINT: This is a standalone application that generates test data.
 * Run this BEFORE starting your Flink job to populate the input topic.
 * 
 * Usage:
 * mvn exec:java -Dexec.mainClass="com.shahar.flink.TestDataGenerator"
 */
public class TestDataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(TestDataGenerator.class);
    private static final Random RANDOM = new Random();

    // Test data configuration
    private static final int NUM_ACCOUNTS = 10;
    private static final String[] ACCOUNT_IDS = generateAccountIds();
    private static final String BANK_ID = UUID.randomUUID().toString();

    public static void main(String[] args) throws Exception {

        FlinkConfig config = FlinkConfig.getInstance();

        LOG.info("=".repeat(70));
        LOG.info("Bank Event Test Data Generator");
        LOG.info("=".repeat(70));
        LOG.info("Kafka: {}", config.getKafkaBootstrapServers());
        LOG.info("Topic: {}", config.getInputTopic());
        LOG.info("Schema Registry: {}", config.getSchemaRegistryUrl());
        LOG.info("=".repeat(70));

        // Create Kafka producer
        KafkaProducer<String, BankEvent> producer = createProducer(config);

        try {
            // Generate events continuously
            int eventCount = 0;
            while (true) {

                // Generate a batch of events
                for (int i = 0; i < 5; i++) {
                    BankEvent event = generateEvent();

                    ProducerRecord<String, BankEvent> record = new ProducerRecord<>(
                            config.getInputTopic(),
                            getAccountId(event), // Key for partitioning
                            event);

                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            LOG.error("Failed to send event", exception);
                        } else {
                            LOG.debug("Sent event to partition {} offset {}",
                                    metadata.partition(), metadata.offset());
                        }
                    });

                    eventCount++;
                }

                LOG.info("Generated {} events (total: {})", 5, eventCount);

                // Sleep between batches
                TimeUnit.SECONDS.sleep(2);
            }

        } finally {
            producer.close();
            LOG.info("Producer closed");
        }
    }

    /**
     * Create Kafka producer with Avro serialization
     */
    private static KafkaProducer<String, BankEvent> createProducer(FlinkConfig config) {
        Properties props = new Properties();

        // Kafka configuration
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        // Schema Registry configuration
        props.put("schema.registry.url", config.getSchemaRegistryUrl());

        // Producer performance settings
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Wait for all replicas
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);

        return new KafkaProducer<>(props);
    }

    /**
     * Generate a random bank event (Transaction or FraudReport)
     */
    private static BankEvent generateEvent() {
        // 90% transactions, 10% fraud reports
        if (RANDOM.nextDouble() < 0.9) {
            return generateTransactionEvent();
        } else {
            return generateFraudReportEvent();
        }
    }

    /**
     * Generate a transaction event
     */
    private static BankEvent generateTransactionEvent() {
        String accountId = ACCOUNT_IDS[RANDOM.nextInt(ACCOUNT_IDS.length)];

        Transaction transaction = Transaction.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setTransactionTypeId(RANDOM.nextInt(4) + 1) // 1-4
                .setAccountId(accountId)
                .setBankId(BANK_ID)
                .setAmountTransferred(generateAmount())
                .setReason(generateReason())
                .setTimestamp(System.currentTimeMillis())
                .build();

        BankEvent event = BankEvent.newBuilder()
                .setMetadata(EventMetadata.newBuilder()
                        .setEventType(EventType.Transaction)
                        .build())
                .setBody(transaction)
                .build();

        LOG.debug("Generated Transaction: Account={}, Amount=${}",
                accountId, transaction.getAmountTransferred());

        return event;
    }

    /**
     * Generate a fraud report event
     */
    private static BankEvent generateFraudReportEvent() {
        String accountId = ACCOUNT_IDS[RANDOM.nextInt(ACCOUNT_IDS.length)];

        FraudReport fraudReport = FraudReport.newBuilder()
                .setReportId(UUID.randomUUID().toString())
                .setFraudChainId(UUID.randomUUID().toString())
                .setTransactionId(UUID.randomUUID().toString())
                .setAccountId(accountId)
                .setBankId(BANK_ID)
                .setRiskLevel(generateRiskLevel())
                .setTimestamp(System.currentTimeMillis())
                .build();

        BankEvent event = BankEvent.newBuilder()
                .setMetadata(EventMetadata.newBuilder()
                        .setEventType(EventType.FraudReport)
                        .build())
                .setBody(fraudReport)
                .build();

        LOG.info("Generated FraudReport: Account={}, Risk={}",
                accountId, fraudReport.getRiskLevel());

        return event;
    }

    /**
     * Generate random transaction amount
     * Occasionally generate large amounts to trigger fraud detection
     */
    private static double generateAmount() {
        // 80% normal transactions ($10-$1000)
        // 15% medium transactions ($1000-$5000)
        // 5% large transactions ($5000-$20000) - should trigger alerts
        double rand = RANDOM.nextDouble();
        if (rand < 0.80) {
            return 10 + RANDOM.nextDouble() * 990;
        } else if (rand < 0.95) {
            return 1000 + RANDOM.nextDouble() * 4000;
        } else {
            return 5000 + RANDOM.nextDouble() * 15000;
        }
    }

    /**
     * Generate random transaction reason
     */
    private static String generateReason() {
        String[] reasons = {
                "Online purchase",
                "ATM withdrawal",
                "Bill payment",
                "Transfer to savings",
                "Salary deposit",
                "Refund",
                "Subscription payment"
        };
        return reasons[RANDOM.nextInt(reasons.length)];
    }

    /**
     * Generate random risk level
     */
    private static String generateRiskLevel() {
        String[] levels = { "LOW", "MEDIUM", "HIGH", "CRITICAL" };
        return levels[RANDOM.nextInt(levels.length)];
    }

    /**
     * Extract account ID from event for partitioning
     */
    private static String getAccountId(BankEvent event) {
        if (event.getBody() instanceof Transaction) {
            return ((Transaction) event.getBody()).getAccountId();
        } else if (event.getBody() instanceof FraudReport) {
            return ((FraudReport) event.getBody()).getAccountId();
        }
        return "unknown";
    }

    /**
     * Generate test account IDs
     */
    private static String[] generateAccountIds() {
        String[] ids = new String[NUM_ACCOUNTS];
        for (int i = 0; i < NUM_ACCOUNTS; i++) {
            ids[i] = UUID.randomUUID().toString();
        }
        return ids;
    }
}
