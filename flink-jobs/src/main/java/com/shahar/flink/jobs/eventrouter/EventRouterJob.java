package com.shahar.flink.jobs.eventrouter;

import com.shahar.bank.BankEvent;
import com.shahar.bank.FraudReport;
import com.shahar.bank.Transaction;
import com.shahar.flink.common.base.AbstractFlinkJob;
import com.shahar.flink.common.connectors.KafkaSinkBuilder;
import com.shahar.flink.common.connectors.KafkaSourceBuilder;
import com.shahar.flink.common.serde.AvroDeserializationSchemaFactory;
import com.shahar.flink.common.serde.AvroSerializationSchemaFactory;
import com.shahar.flink.common.utils.WatermarkStrategyFactory;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class EventRouterJob extends AbstractFlinkJob<EventRouterConfig> {

        public EventRouterJob(EventRouterConfig config) {
                super(config);
        }

        @Override
        protected void buildPipeline(StreamExecutionEnvironment env) {
                log.info("Building pipeline with Kafka bootstrap servers: {}", config.getKafkaBootstrapServers());
                log.info("Input topic: {}", config.getInputTopic());
                log.info("Consumer group ID: {}", config.getConsumerGroupId());

                KafkaSource<BankEvent> source = KafkaSourceBuilder.<BankEvent>create()
                                .bootstrapServers(config.getKafkaBootstrapServers())
                                .topic(config.getInputTopic())
                                .groupId(config.getConsumerGroupId())
                                .deserializationSchema(AvroDeserializationSchemaFactory.create(
                                                config.getSchemaRegistryUrl(), BankEvent.class))
                                .build();

                DataStream<BankEvent> sourceStream = env.fromSource(
                                source,
                                WatermarkStrategyFactory.forBoundedOutOfOrderness(
                                                Duration.ofSeconds(config.getWatermarkMaxOutOfOrdernessSeconds()),
                                                (event, timestamp) -> {
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
                DataStream<FraudReport> fraudReportStream = routedStream
                                .getSideOutput(EventRouter.FRAUD_REPORT_TAG);

                KafkaSink<Transaction> transactionSink = KafkaSinkBuilder.<Transaction>create()
                                .bootstrapServers(config.getKafkaBootstrapServers())
                                .topic(config.getTransactionTopic())
                                .serializationSchema(AvroSerializationSchemaFactory.create(
                                                config.getSchemaRegistryUrl(), config.getTransactionTopic(),
                                                Transaction.class))
                                .build();

                KafkaSink<FraudReport> fraudReportSink = KafkaSinkBuilder.<FraudReport>create()
                                .bootstrapServers(config.getKafkaBootstrapServers())
                                .topic(config.getFraudReportTopic())
                                .serializationSchema(AvroSerializationSchemaFactory.create(
                                                config.getSchemaRegistryUrl(), config.getFraudReportTopic(),
                                                FraudReport.class))
                                .build();

                transactionStream.sinkTo(transactionSink).name("Transaction Sink");
                fraudReportStream.sinkTo(fraudReportSink).name("FraudReport Sink");

                log.info("Configured Kafka sinks - Transactions: {}, Fraud Reports: {}",
                                config.getTransactionTopic(), config.getFraudReportTopic());
        }

        @Override
        protected String getJobName() {
                return "Bank Event Router";
        }

        public static void main(String[] args) throws Exception {
                EventRouterConfig config = EventRouterConfig.getInstance();
                EventRouterJob job = new EventRouterJob(config);
                job.execute();
        }
}
