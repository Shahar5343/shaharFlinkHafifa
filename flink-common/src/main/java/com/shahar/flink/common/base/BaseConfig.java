package com.shahar.flink.common.base;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

public abstract class BaseConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    protected final Properties properties;

    protected BaseConfig(String propertiesFile) {
        this.properties = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(propertiesFile)) {
            if (input == null) {
                System.err.println("WARNING: " + propertiesFile + " not found. Using defaults.");
            } else {
                properties.load(input);
                System.out.println("INFO: Successfully loaded " + propertiesFile);
                System.out.println(
                        "INFO: kafka.bootstrap.servers = " + properties.getProperty("kafka.bootstrap.servers"));
            }
        } catch (IOException e) {
            System.err.println("ERROR: Failed to load " + propertiesFile + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public int getIntProperty(String key, int defaultValue) {
        return Integer.parseInt(properties.getProperty(key, String.valueOf(defaultValue)));
    }

    public long getLongProperty(String key, long defaultValue) {
        return Long.parseLong(properties.getProperty(key, String.valueOf(defaultValue)));
    }

    public double getDoubleProperty(String key, double defaultValue) {
        return Double.parseDouble(properties.getProperty(key, String.valueOf(defaultValue)));
    }

    public Properties getAllProperties() {
        return (Properties) properties.clone();
    }

    public String getKafkaBootstrapServers() {
        return getProperty("kafka.bootstrap.servers", "localhost:9092");
    }

    public String getSchemaRegistryUrl() {
        return getProperty("schema.registry.url", "http://localhost:8081");
    }

    public String getConsumerGroupId() {
        return getProperty("kafka.consumer.group.id", "flink-consumer");
    }

    public long getCheckpointInterval() {
        return getLongProperty("checkpoint.interval.ms", 60000);
    }

    public int getWatermarkMaxOutOfOrdernessSeconds() {
        return getIntProperty("watermark.max.out.of.orderness.seconds", 10);
    }
}
