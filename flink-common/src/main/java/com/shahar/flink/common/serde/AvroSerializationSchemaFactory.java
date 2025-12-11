package com.shahar.flink.common.serde;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class AvroSerializationSchemaFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSerializationSchemaFactory.class);

    public static <T> SerializationSchema<T> create(String schemaRegistryUrl, String topic, Class<T> type) {
        return new AvroSerializationSchemaImpl<>(schemaRegistryUrl, topic, type);
    }

    private static class AvroSerializationSchemaImpl<T> implements SerializationSchema<T> {

        private final String schemaRegistryUrl;
        private final String topic;
        private final Class<T> type;
        private transient KafkaAvroSerializer serializer;
        private transient SchemaRegistryClient schemaRegistryClient;

        public AvroSerializationSchemaImpl(String schemaRegistryUrl, String topic, Class<T> type) {
            this.schemaRegistryUrl = schemaRegistryUrl;
            this.topic = topic;
            this.type = type;
        }

        @Override
        public void open(InitializationContext context) throws Exception {
            schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);

            Map<String, Object> config = new HashMap<>();
            config.put("schema.registry.url", schemaRegistryUrl);
            config.put("value.subject.name.strategy", "io.confluent.kafka.serializers.subject.TopicNameStrategy");
            config.put("auto.register.schemas", true);

            serializer = new KafkaAvroSerializer(schemaRegistryClient, config);

            LOG.info("Initialized AvroSerializationSchema for {} on topic '{}' with Schema Registry: {}",
                    type.getSimpleName(), topic, schemaRegistryUrl);
        }

        @Override
        public byte[] serialize(T element) {
            if (element == null) {
                return null;
            }

            try {
                return serializer.serialize(topic, element);
            } catch (Exception e) {
                LOG.error("Failed to serialize {}: {}", type.getSimpleName(), element, e);
                throw new RuntimeException("Serialization failed", e);
            }
        }
    }
}
