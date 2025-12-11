package com.shahar.flink.common.connectors;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

public class KafkaSinkBuilder<T> {

    private String bootstrapServers;
    private String topic;
    private SerializationSchema<T> serializationSchema;
    private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;

    public KafkaSinkBuilder<T> bootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    public KafkaSinkBuilder<T> topic(String topic) {
        this.topic = topic;
        return this;
    }

    public KafkaSinkBuilder<T> serializationSchema(SerializationSchema<T> serializationSchema) {
        this.serializationSchema = serializationSchema;
        return this;
    }

    public KafkaSinkBuilder<T> deliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        this.deliveryGuarantee = deliveryGuarantee;
        return this;
    }

    public KafkaSink<T> build() {
        return KafkaSink.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(serializationSchema)
                                .build())
                .setDeliveryGuarantee(deliveryGuarantee)
                .build();
    }

    public static <T> KafkaSinkBuilder<T> create() {
        return new KafkaSinkBuilder<>();
    }
}
