package com.shahar.flink;

import com.shahar.bank.BankEvent;
import com.shahar.bank.FraudReport;
import com.shahar.bank.Transaction;
import com.shahar.flink.config.FlinkConfig;
import com.shahar.flink.functions.EventRouter;
import com.shahar.flink.serialization.AvroDeserializationSchema;
import com.shahar.flink.serialization.TransactionSerializationSchema;
import com.shahar.flink.serialization.FraudReportSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class StreamingJob {

        private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

        public static void main(String[] args) throws Exception {

                FlinkConfig config = FlinkConfig.getInstance();
                LOG.info("Starting Bank Event Router");
                LOG.info("Configuration: {}", config.getAllProperties());

                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

                env.enableCheckpointing(config.getCheckpointInterval());

                CheckpointConfig checkpointConfig = env.getCheckpointConfig();
                checkpointConfig.setMinPauseBetweenCheckpoints(30000);
                checkpointConfig.setCheckpointTimeout(600000);
                checkpointConfig.setMaxConcurrentCheckpoints(1);

                KafkaSource<BankEvent> kafkaSource = KafkaSource.<BankEvent>builder()
                                .setBootstrapServers(config.getKafkaBootstrapServers())
                                .setTopics(config.getInputTopic())
                                .setGroupId(config.getConsumerGroupId())
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setValueOnlyDeserializer(new AvroDeserializationSchema(config.getSchemaRegistryUrl()))
                                .setProperty("enable.auto.commit", "false")
                                .build();

                DataStream<BankEvent> sourceStream = env.fromSource(
                                kafkaSource,
                                WatermarkStrategy
                                                .<BankEvent>forBoundedOutOfOrderness(
                                                                Duration.ofSeconds(config
                                                                                .getWatermarkMaxOutOfOrdernessSeconds()))
                                                .withTimestampAssigner((event, timestamp) -> {
                                                        if (event.getBody() instanceof Transaction) {
                                                                return ((Transaction) event.getBody()).getTimestamp();
                                                        } else if (event.getBody() instanceof FraudReport) {
                                                                return ((FraudReport) event.getBody()).getTimestamp();
                                                        }
                                                        return System.currentTimeMillis();
                                                }),
                                "Kafka Source");

                SingleOutputStreamOperator<Transaction> routedStream = sourceStream
                                .process(new EventRouter())
                                .name("Event Router");

                DataStream<Transaction> transactionStream = routedStream;
                DataStream<FraudReport> fraudReportStream = routedStream.getSideOutput(EventRouter.FRAUD_REPORT_TAG);

                KafkaSink<Transaction> transactionSink = KafkaSink.<Transaction>builder()
                                .setBootstrapServers(config.getKafkaBootstrapServers())
                                .setRecordSerializer(
                                                KafkaRecordSerializationSchema.builder()
                                                                .setTopic(config.getTransactionTopic())
                                                                .setValueSerializationSchema(
                                                                                new TransactionSerializationSchema(
                                                                                                config.getSchemaRegistryUrl(),
                                                                                                config.getTransactionTopic()))
                                                                .build())
                                .setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE)
                                .build();

                KafkaSink<FraudReport> fraudReportSink = KafkaSink.<FraudReport>builder()
                                .setBootstrapServers(config.getKafkaBootstrapServers())
                                .setRecordSerializer(
                                                KafkaRecordSerializationSchema.builder()
                                                                .setTopic(config.getFraudReportTopic())
                                                                .setValueSerializationSchema(
                                                                                new FraudReportSerializationSchema(
                                                                                                config.getSchemaRegistryUrl(),
                                                                                                config.getFraudReportTopic()))
                                                                .build())
                                .setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE)
                                .build();

                transactionStream.sinkTo(transactionSink).name("Transaction Sink");
                fraudReportStream.sinkTo(fraudReportSink).name("FraudReport Sink");

                LOG.info("Executing Bank Event Router - Transactions: {}, Fraud Reports: {}",
                                config.getTransactionTopic(), config.getFraudReportTopic());

                env.execute("Bank Event Router");
        }
}
