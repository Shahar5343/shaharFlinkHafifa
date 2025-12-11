package com.shahar.flink.common.serde;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AvroDeserializationSchemaFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AvroDeserializationSchemaFactory.class);

    public static <T> DeserializationSchema<T> create(String schemaRegistryUrl, Class<T> type) {
        return new AvroDeserializationSchemaImpl<>(schemaRegistryUrl, type);
    }

    private static class AvroDeserializationSchemaImpl<T> implements DeserializationSchema<T> {

        private final String schemaRegistryUrl;
        private final Class<T> type;
        private transient KafkaAvroDeserializer deserializer;
        private transient SchemaRegistryClient schemaRegistryClient;

        public AvroDeserializationSchemaImpl(String schemaRegistryUrl, Class<T> type) {
            this.schemaRegistryUrl = schemaRegistryUrl;
            this.type = type;
        }

        @Override
        public void open(InitializationContext context) throws Exception {
            schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);

            Map<String, Object> config = new HashMap<>();
            config.put("schema.registry.url", schemaRegistryUrl);
            config.put("specific.avro.reader", true);

            deserializer = new KafkaAvroDeserializer(schemaRegistryClient, config);

            LOG.info("Initialized AvroDeserializationSchema for {} with Schema Registry: {}",
                    type.getSimpleName(), schemaRegistryUrl);
        }

        @Override
        public T deserialize(byte[] message) throws IOException {
            if (message == null) {
                return null;
            }

            try {
                Object deserialized = deserializer.deserialize(null, message);

                if (type.isInstance(deserialized)) {
                    return type.cast(deserialized);
                } else {
                    LOG.warn("Deserialized object is not a {}: {}",
                            type.getSimpleName(), deserialized != null ? deserialized.getClass() : "null");
                    return null;
                }
            } catch (Exception e) {
                LOG.error("Failed to deserialize message, skipping", e);
                return null;
            }
        }

        @Override
        public boolean isEndOfStream(T nextElement) {
            return false;
        }

        @Override
        public TypeInformation<T> getProducedType() {
            return TypeInformation.of(type);
        }
    }
}
