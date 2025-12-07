package com.shahar.flink.constants;

/**
 * Central repository for all constants used in the Flink streaming job.
 * 
 * BENEFITS:
 * - Eliminates magic strings and numbers scattered throughout codebase
 * - Single source of truth for configuration values
 * - Easier to maintain and update
 * - Type-safe and compile-time checked
 */
public final class FlinkConstants {

    // Prevent instantiation
    private FlinkConstants() {
        throw new AssertionError("Cannot instantiate constants class");
    }

    // =========================================================================
    // OPERATOR NAMES - Used for monitoring and debugging in Flink UI
    // =========================================================================

    public static final class OperatorNames {
        public static final String KAFKA_SOURCE = "Kafka Source (bank-events)";
        public static final String EVENT_ROUTER = "Event Router";
        public static final String TRANSACTION_LOGGER = "Transaction Logger";
        public static final String FRAUD_REPORT_LOGGER = "FraudReport Logger";
        public static final String FRAUD_DETECTOR = "Fraud Detector";
        public static final String KAFKA_SINK = "Kafka Sink (alerts)";

        private OperatorNames() {
        }
    }

    // =========================================================================
    // CHECKPOINT CONFIGURATION
    // =========================================================================

    public static final class Checkpointing {
        public static final long MIN_PAUSE_BETWEEN_CHECKPOINTS_MS = 30_000L; // 30 seconds
        public static final long CHECKPOINT_TIMEOUT_MS = 600_000L; // 10 minutes
        public static final int MAX_CONCURRENT_CHECKPOINTS = 1;

        private Checkpointing() {
        }
    }

    // =========================================================================
    // SCHEMA REGISTRY CONFIGURATION
    // =========================================================================

    public static final class SchemaRegistry {
        public static final int CACHE_SIZE = 100;
        public static final String SPECIFIC_AVRO_READER_KEY = "specific.avro.reader";
        public static final String AUTO_REGISTER_SCHEMAS_KEY = "auto.register.schemas";
        public static final String SUBJECT_NAME_STRATEGY_KEY = "value.subject.name.strategy";
        public static final String TOPIC_NAME_STRATEGY = "io.confluent.kafka.serializers.subject.TopicNameStrategy";

        private SchemaRegistry() {
        }
    }

    // =========================================================================
    // KAFKA CONSUMER CONFIGURATION
    // =========================================================================

    public static final class KafkaConsumer {
        public static final String ENABLE_AUTO_COMMIT_KEY = "enable.auto.commit";
        public static final String ENABLE_AUTO_COMMIT_VALUE = "false"; // Flink manages offsets

        private KafkaConsumer() {
        }
    }

    // =========================================================================
    // FRAUD DETECTION - Magic numbers eliminated
    // =========================================================================

    public static final class FraudDetection {
        // Urgency levels
        public static final int URGENCY_MEDIUM = 2;
        public static final int URGENCY_HIGH = 3;
        public static final int URGENCY_CRITICAL = 4;

        // Alert type IDs
        public static final int ALERT_TYPE_HIGH_FREQUENCY = 1;
        public static final int ALERT_TYPE_LARGE_AMOUNT = 2;
        public static final int ALERT_TYPE_COMBINED = 3;

        // Alert reason templates
        public static final String REASON_HIGH_FREQUENCY = "MEDIUM: High transaction frequency (%d transactions) detected in %d-minute window";
        public static final String REASON_LARGE_AMOUNT = "HIGH: Large total amount ($%.2f) detected in %d-minute window";
        public static final String REASON_CRITICAL = "CRITICAL: High frequency (%d transactions) AND large amount ($%.2f) detected in %d-minute window";

        private FraudDetection() {
        }
    }

    // =========================================================================
    // SIDE OUTPUT TAGS
    // =========================================================================

    public static final class SideOutputs {
        public static final String FRAUD_REPORT_TAG_NAME = "fraud-reports";

        private SideOutputs() {
        }
    }

    // =========================================================================
    // LOGGING MESSAGES
    // =========================================================================

    public static final class LogMessages {
        public static final String JOB_STARTING = "Starting Bank Fraud Detection Streaming Job";
        public static final String JOB_EXECUTING = "Executing Flink job: Bank Fraud Detection";
        public static final String CONFIG_LOADED = "Configuration: {}";
        public static final String FRAUD_ALERT_EMITTED = "FRAUD ALERT: Account {} - Urgency {} - {}";

        private LogMessages() {
        }
    }
}
