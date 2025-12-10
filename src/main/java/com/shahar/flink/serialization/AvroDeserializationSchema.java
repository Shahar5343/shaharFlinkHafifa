package com.shahar.flink.serialization;

import com.shahar.bank.BankEvent;
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

public class AvroDeserializationSchema implements DeserializationSchema<BankEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(AvroDeserializationSchema.class);

    private final String schemaRegistryUrl;
    private transient KafkaAvroDeserializer deserializer;
    private transient SchemaRegistryClient schemaRegistryClient;

    public AvroDeserializationSchema(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);

        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", schemaRegistryUrl);
        config.put("specific.avro.reader", true);

        deserializer = new KafkaAvroDeserializer(schemaRegistryClient, config);

        LOG.info("Initialized AvroDeserializationSchema with Schema Registry: {}", schemaRegistryUrl);
    }

    @Override
    public BankEvent deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }

        try {
            Object deserialized = deserializer.deserialize(null, message);

            if (deserialized instanceof BankEvent) {
                return (BankEvent) deserialized;
            } else {
                LOG.warn("Deserialized object is not a BankEvent: {}",
                        deserialized != null ? deserialized.getClass() : "null");
                return null;
            }
        } catch (Exception e) {
            LOG.error("Failed to deserialize message, skipping", e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(BankEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<BankEvent> getProducedType() {
        return TypeInformation.of(BankEvent.class);
    }
}
