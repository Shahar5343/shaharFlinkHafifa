package com.shahar.flink.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

public class FlinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private final Properties properties;
    private static FlinkConfig instance;

    private FlinkConfig() {
        this.properties = new Properties();
        try (InputStream input = getClass().getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (input == null) {
                System.out.println("WARNING: application.properties not found. Using defaults.");
            } else {
                properties.load(input);
                System.out.println("Configuration loaded from application.properties");
            }
        } catch (IOException e) {
            System.err.println("ERROR: Failed to load application.properties: " + e.getMessage());
        }
    }

    public static synchronized FlinkConfig getInstance() {
        if (instance == null) {
            instance = new FlinkConfig();
        }
        return instance;
    }

    public String getKafkaBootstrapServers() {
        return properties.getProperty("kafka.bootstrap.servers", "kafka:29092");
    }

    public String getInputTopic() {
        return properties.getProperty("kafka.input.topic", "bank-events");
    }

    public String getOutputTopic() {
        return properties.getProperty("kafka.output.topic", "alerts");
    }

    public String getTransactionTopic() {
        return properties.getProperty("kafka.transaction.topic", "transactions");
    }

    public String getFraudReportTopic() {
        return properties.getProperty("kafka.fraud-report.topic", "fraud-reports");
    }

    public String getConsumerGroupId() {
        return properties.getProperty("kafka.consumer.group.id", "fraud-detection-consumer");
    }

    public String getSchemaRegistryUrl() {
        return properties.getProperty("schema.registry.url", "http://schema-registry:8081");
    }

    public long getCheckpointInterval() {
        return Long.parseLong(properties.getProperty("checkpoint.interval.ms", "60000"));
    }

    public String getCheckpointDir() {
        return properties.getProperty("checkpoint.dir", "file:///tmp/flink-checkpoints");
    }

    public int getWindowSizeMinutes() {
        return Integer.parseInt(properties.getProperty("window.size.minutes", "5"));
    }

    public int getWatermarkMaxOutOfOrdernessSeconds() {
        return Integer.parseInt(properties.getProperty("watermark.max.out.of.orderness.seconds", "10"));
    }

    public int getFraudThresholdTransactionCount() {
        return Integer.parseInt(properties.getProperty("fraud.threshold.transaction.count", "10"));
    }

    public double getFraudThresholdTotalAmount() {
        return Double.parseDouble(properties.getProperty("fraud.threshold.total.amount", "10000.0"));
    }

    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public Properties getAllProperties() {
        return (Properties) properties.clone();
    }
}
