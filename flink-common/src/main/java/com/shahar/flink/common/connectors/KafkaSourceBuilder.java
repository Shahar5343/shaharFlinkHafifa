package com.shahar.flink.common.connectors;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class KafkaSourceBuilder<T> {

    private String bootstrapServers;
    private String topic;
    private String groupId;
    private DeserializationSchema<T> deserializationSchema;
    private OffsetsInitializer offsetsInitializer = OffsetsInitializer.earliest();

    public KafkaSourceBuilder<T> bootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    public KafkaSourceBuilder<T> topic(String topic) {
        this.topic = topic;
        return this;
    }

    public KafkaSourceBuilder<T> groupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public KafkaSourceBuilder<T> deserializationSchema(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        return this;
    }

    public KafkaSourceBuilder<T> offsetsInitializer(OffsetsInitializer offsetsInitializer) {
        this.offsetsInitializer = offsetsInitializer;
        return this;
    }

    public KafkaSource<T> build() {
        System.out.println("INFO: Building KafkaSource with bootstrapServers = " + bootstrapServers);

        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            throw new IllegalArgumentException("Bootstrap servers cannot be null or empty");
        }

        return KafkaSource.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(offsetsInitializer)
                .setValueOnlyDeserializer(deserializationSchema)
                .setProperty("enable.auto.commit", "false")
                .build();
    }

    public static <T> KafkaSourceBuilder<T> create() {
        return new KafkaSourceBuilder<>();
    }
}
