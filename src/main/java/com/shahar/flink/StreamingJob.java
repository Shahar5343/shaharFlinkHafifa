package com.shahar.flink;

import com.shahar.bank.BankEvent;
import com.shahar.bank.FraudReport;
import com.shahar.bank.Transaction;
import com.shahar.flink.Alert;
import com.shahar.flink.config.FlinkConfig;
import com.shahar.flink.functions.EventRouter;
import com.shahar.flink.functions.FraudDetector;
import com.shahar.flink.serialization.AvroDeserializationSchema;
import com.shahar.flink.serialization.AvroSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * ==============================================================================
 * WHAT: Main Flink streaming job for bank fraud detection
 * WHY: Orchestrates the entire data pipeline from Kafka to fraud alerts
 * HOW: Connects source, transformations, and sink with proper configuration
 * ==============================================================================
 * 
 * LEARNING POINT: This class demonstrates production-grade Flink job structure:
 * 1. Environment configuration (checkpointing, state backend, restart strategy)
 * 2. Source configuration (Kafka with Schema Registry)
 * 3. Stream transformations (routing, windowing, aggregation)
 * 4. Sink configuration (Kafka with Schema Registry)
 * 5. Execution
 * 
 * PIPELINE FLOW:
 * ```
 * Kafka Source (bank-events)
 * ↓
 * Deserialize BankEvent (Schema Registry)
 * ↓
 * Assign Timestamps & Watermarks (event-time processing)
 * ↓
 * Route by Event Type (ProcessFunction with side outputs)
 * ├─→ Transaction Stream
 * │ ↓
 * │ Key by account_id
 * │ ↓
 * │ Window (5-minute tumbling)
 * │ ↓
 * │ Detect Fraud Patterns
 * │ ↓
 * │ Generate Alerts
 * │
 * └─→ FraudReport Stream (future enhancement: correlate with transactions)
 * ↓
 * Serialize Alert (Schema Registry)
 * ↓
 * Kafka Sink (alerts)
 * ```
 */
public class StreamingJob {

        private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

        public static void main(String[] args) throws Exception {

                // Load configuration
                FlinkConfig config = FlinkConfig.getInstance();
                LOG.info("Starting Bank Fraud Detection Streaming Job");
                LOG.info("Configuration: {}", config.getAllProperties());

                // ====================================================================
                // STEP 1: Create Streaming Environment
                // ====================================================================
                // WHAT: Entry point for Flink streaming applications
                // WHY: Provides APIs for sources, transformations, sinks, and execution
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

                // ====================================================================
                // STEP 2: Configure Checkpointing
                // ====================================================================
                // WHAT: Periodic state snapshots for fault tolerance
                // WHY: Enables exactly-once processing and recovery from failures
                // LEARNING POINT: Checkpointing is THE key feature for production Flink

                env.enableCheckpointing(config.getCheckpointInterval());

                CheckpointConfig checkpointConfig = env.getCheckpointConfig();

                // Checkpoint mode: EXACTLY_ONCE vs AT_LEAST_ONCE
                // LEARNING POINT: EXACTLY_ONCE guarantees no duplicates but has overhead
                // AT_LEAST_ONCE is faster but may process records multiple times
                // checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

                // Minimum pause between checkpoints (prevents checkpoint storms)
                checkpointConfig.setMinPauseBetweenCheckpoints(30000); // 30 seconds

                // Checkpoint timeout (fail if checkpoint takes too long)
                checkpointConfig.setCheckpointTimeout(600000); // 10 minutes

                // Maximum concurrent checkpoints
                checkpointConfig.setMaxConcurrentCheckpoints(1);

                // Retain checkpoints on cancellation (for debugging)
                // LEARNING POINT: In production, you might want to retain for manual recovery
                // checkpointConfig.setExternalizedCheckpointCleanup(
                                // CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

                // Checkpoint storage
                // LEARNING POINT: In production, use distributed storage (S3, HDFS, etc.)
                // checkpointConfig.setCheckpointStorage(config.getCheckpointDir());

                // ====================================================================
                // STEP 3: Configure State Backend
                // ====================================================================
                // WHAT: How Flink stores state (in-memory, on-disk, etc.)
                // WHY: Different backends have different performance characteristics
                // LEARNING POINT: State backend choices:
                // - HashMapStateBackend: Fast, in-memory (good for small state)
                // - EmbeddedRocksDBStateBackend: Disk-based (good for large state)
                // env.setStateBackend(new HashMapStateBackend());
                // LEARNING POINT: Flink's new Kafka connector (FLIP-27) provides:
                // - Better performance
                // - Improved exactly-once semantics
                // - Cleaner API

                KafkaSource<BankEvent> kafkaSource = KafkaSource.<BankEvent>builder()
                                .setBootstrapServers(config.getKafkaBootstrapServers())
                                .setTopics(config.getInputTopic())
                                .setGroupId(config.getConsumerGroupId())

                                // Start reading from earliest offset
                                // LEARNING POINT: Offset initialization strategies:
                                // - earliest(): Read from beginning (for reprocessing)
                                // - latest(): Read only new messages
                                // - committedOffsets(): Resume from last checkpoint
                                .setStartingOffsets(OffsetsInitializer.earliest())

                                // Deserialization with Schema Registry
                                .setValueOnlyDeserializer(
                                                new AvroDeserializationSchema(config.getSchemaRegistryUrl()))

                                // Consumer properties
                                .setProperty("enable.auto.commit", "false") // Flink manages offsets

                                .build();

                // ====================================================================
                // STEP 6: Create Source Stream
                // ====================================================================
                // WHAT: DataStream of BankEvent records
                // WHY: Entry point for transformations
                DataStream<BankEvent> sourceStream = env.fromSource(
                                kafkaSource,
                                WatermarkStrategy
                                                .<BankEvent>forBoundedOutOfOrderness(
                                                                Duration.ofSeconds(config
                                                                                .getWatermarkMaxOutOfOrdernessSeconds()))
                                                .withTimestampAssigner((event, timestamp) -> {
                                                        // Extract timestamp from event body
                                                        // LEARNING POINT: Event time vs Processing time
                                                        // - Event time: When event actually occurred (use this!)
                                                        // - Processing time: When Flink processes it (not
                                                        // deterministic)
                                                        if (event.getBody() instanceof Transaction) {
                                                                return ((Transaction) event.getBody()).getTimestamp();
                                                        } else if (event.getBody() instanceof FraudReport) {
                                                                return ((FraudReport) event.getBody()).getTimestamp();
                                                        }
                                                        return System.currentTimeMillis(); // Fallback
                                                }),
                                "Kafka Source (bank-events)");

                // ====================================================================
                // STEP 7: Route Events by Type
                // ====================================================================
                // WHAT: Split polymorphic BankEvent into Transaction and FraudReport streams
                // WHY: Different processing logic for each type
                // LEARNING POINT: Side outputs provide type-safe stream splitting

                SingleOutputStreamOperator<Transaction> routedStream = sourceStream
                                .process(new EventRouter())
                                .name("Event Router");

                // Extract side outputs
                DataStream<Transaction> transactionStream = routedStream;
                DataStream<FraudReport> fraudReportStream = routedStream
                                .getSideOutput(EventRouter.FRAUD_REPORT_TAG);

                // Log stream sizes for monitoring
                transactionStream.map(t -> {
                        LOG.debug("Transaction: {} - Account: {} - Amount: {}",
                                        t.getId(), t.getAccountId(), t.getAmountTransferred());
                        return t;
                }).name("Transaction Logger");

                fraudReportStream.map(fr -> {
                        LOG.info("FraudReport: {} - Account: {} - Risk: {}",
                                        fr.getReportId(), fr.getAccountId(), fr.getRiskLevel());
                        return fr;
                }).name("FraudReport Logger");

                // ====================================================================
                // STEP 8: Windowed Fraud Detection
                // ====================================================================
                // WHAT: Aggregate transactions per account in 5-minute windows
                // WHY: Detect fraud patterns across multiple transactions
                // LEARNING POINT: Window types:
                // - Tumbling: Non-overlapping, fixed-size (we use this)
                // - Sliding: Overlapping windows
                // - Session: Dynamic size based on inactivity gap

                DataStream<Alert> alertStream = transactionStream
                                .keyBy(Transaction::getAccountId) // Partition by account
                                .window(TumblingEventTimeWindows.of(
                                                Duration.ofMinutes(config.getWindowSizeMinutes())))
                                .process(new FraudDetector())
                                .name("Fraud Detector");

                // ====================================================================
                // STEP 9: Create Kafka Sink
                // ====================================================================
                // WHAT: Write Alert records to Kafka
                // WHY: Output alerts for downstream consumers
                // LEARNING POINT: Kafka sink with exactly-once guarantees requires:
                // - Transactional writes
                // - Proper checkpoint configuration

                KafkaSink<Alert> kafkaSink = KafkaSink.<Alert>builder()
                                .setBootstrapServers(config.getKafkaBootstrapServers())
                                .setRecordSerializer(
                                                KafkaRecordSerializationSchema.builder()
                                                                .setTopic(config.getOutputTopic())
                                                                .setValueSerializationSchema(
                                                                                new AvroSerializationSchema(
                                                                                                config.getSchemaRegistryUrl(),
                                                                                                config.getOutputTopic()))
                                                                .build())

                                // Delivery guarantee: EXACTLY_ONCE vs AT_LEAST_ONCE
                                // LEARNING POINT: EXACTLY_ONCE uses Kafka transactions
                                .setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.EXACTLY_ONCE)

                                // Transactional ID prefix (required for exactly-once)
                                .setTransactionalIdPrefix("fraud-detection-")

                                .build();

                // ====================================================================
                // STEP 10: Connect Sink
                // ====================================================================
                alertStream.sinkTo(kafkaSink).name("Kafka Sink (alerts)");

                // ====================================================================
                // STEP 11: Execute Job
                // ====================================================================
                // WHAT: Submit job to Flink cluster
                // WHY: Start processing
                // LEARNING POINT: This is a blocking call in local mode,
                // but returns immediately when submitting to a cluster

                LOG.info("Executing Flink job: Bank Fraud Detection");
                env.execute("Bank Fraud Detection");
        }
}
