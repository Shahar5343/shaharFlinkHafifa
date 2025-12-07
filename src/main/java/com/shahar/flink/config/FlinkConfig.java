package com.shahar.flink.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * ==============================================================================
 * WHAT: Configuration management for Flink streaming job
 * WHY: Centralizes all configuration, enables environment-specific settings
 * HOW: Loads from application.properties, provides type-safe getters
 * ==============================================================================
 * 
 * LEARNING POINT: Externalizing configuration is a production best practice.
 * It allows you to deploy the same JAR to dev/staging/prod with different
 * configurations without rebuilding.
 * 
 * LEARNING POINT: Implements Serializable because Flink may capture this object
 * in closures/lambdas that get serialized for distributed execution.
 */
public class FlinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private final Properties properties;

    // Singleton pattern for configuration
    private static FlinkConfig instance;

    /**
     * Private constructor - loads configuration from classpath
     */
    private FlinkConfig() {
        properties = new Properties();
        try (InputStream input = getClass().getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new RuntimeException("Unable to find application.properties");
            }
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load configuration", e);
        }
    }

    /**
     * Get singleton instance
     */
    public static synchronized FlinkConfig getInstance() {
        if (instance == null) {
            instance = new FlinkConfig();
        }
        return instance;
    }

    // ========================================================================
    // KAFKA CONFIGURATION
    // ========================================================================

    /**
     * WHAT: Kafka bootstrap servers
     * WHY: Connection string for Kafka cluster
     * EXAMPLE: "localhost:9092" or "broker1:9092,broker2:9092,broker3:9092"
     */
    public String getKafkaBootstrapServers() {
        return properties.getProperty("kafka.bootstrap.servers", "localhost:9092");
    }

    /**
     * WHAT: Input topic name for bank events
     * WHY: Where to read Transaction and FraudReport events from
     */
    public String getInputTopic() {
        return properties.getProperty("kafka.input.topic", "bank-events");
    }

    /**
     * WHAT: Output topic name for alerts
     * WHY: Where to write generated fraud alerts
     */
    public String getOutputTopic() {
        return properties.getProperty("kafka.output.topic", "alerts");
    }

    /**
     * WHAT: Kafka consumer group ID
     * WHY: Identifies this Flink job as a consumer group for offset management
     * LEARNING POINT: Consumer groups enable parallel consumption and offset
     * tracking
     */
    public String getConsumerGroupId() {
        return properties.getProperty("kafka.consumer.group.id", "fraud-detection-consumer");
    }

    // ========================================================================
    // SCHEMA REGISTRY CONFIGURATION
    // ========================================================================

    /**
     * WHAT: Schema Registry URL
     * WHY: Where to fetch/register Avro schemas
     * LEARNING POINT: This should point to your organization's Schema Registry
     * in production, not localhost
     */
    public String getSchemaRegistryUrl() {
        return properties.getProperty("schema.registry.url", "http://localhost:8081");
    }

    // ========================================================================
    // FLINK CHECKPOINTING CONFIGURATION
    // ========================================================================

    /**
     * WHAT: Checkpoint interval in milliseconds
     * WHY: How often Flink saves state snapshots for fault tolerance
     * LEARNING POINT: Lower interval = more overhead but faster recovery
     * Higher interval = less overhead but slower recovery
     * Typical production: 30s - 5min
     */
    public long getCheckpointInterval() {
        return Long.parseLong(properties.getProperty("checkpoint.interval.ms", "60000"));
    }

    /**
     * WHAT: Checkpoint storage directory
     * WHY: Where to persist state snapshots
     * LEARNING POINT: In production, use distributed storage (S3, HDFS, etc.)
     */
    public String getCheckpointDir() {
        return properties.getProperty("checkpoint.dir", "file:///tmp/flink-checkpoints");
    }

    // ========================================================================
    // WINDOWING CONFIGURATION
    // ========================================================================

    /**
     * WHAT: Window size in minutes
     * WHY: Time period for aggregating transactions per account
     * LEARNING POINT: Tumbling windows are non-overlapping, fixed-size time
     * intervals
     */
    public int getWindowSizeMinutes() {
        return Integer.parseInt(properties.getProperty("window.size.minutes", "5"));
    }

    /**
     * WHAT: Maximum allowed event lateness in seconds
     * WHY: Handles out-of-order events in streaming
     * LEARNING POINT: Watermarks = event_time - max_out_of_orderness
     * Events arriving later than watermark are dropped
     */
    public int getWatermarkMaxOutOfOrdernessSeconds() {
        return Integer.parseInt(properties.getProperty("watermark.max.out.of.orderness.seconds", "10"));
    }

    // ========================================================================
    // FRAUD DETECTION THRESHOLDS
    // ========================================================================

    /**
     * WHAT: Transaction count threshold for medium urgency alert
     * WHY: More than this many transactions in a window is suspicious
     */
    public int getFraudThresholdTransactionCount() {
        return Integer.parseInt(properties.getProperty("fraud.threshold.transaction.count", "10"));
    }

    /**
     * WHAT: Total amount threshold for high urgency alert
     * WHY: More than this total amount in a window is suspicious
     */
    public double getFraudThresholdTotalAmount() {
        return Double.parseDouble(properties.getProperty("fraud.threshold.total.amount", "10000.0"));
    }

    // ========================================================================
    // UTILITY METHODS
    // ========================================================================

    /**
     * Get raw property value (for custom properties)
     */
    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    /**
     * Get all properties (for debugging)
     */
    public Properties getAllProperties() {
        return (Properties) properties.clone();
    }
}
