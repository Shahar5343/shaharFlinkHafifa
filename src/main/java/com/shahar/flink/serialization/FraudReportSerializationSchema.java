package com.shahar.flink.serialization;

import com.shahar.bank.FraudReport;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class FraudReportSerializationSchema implements SerializationSchema<FraudReport> {

    private static final Logger LOG = LoggerFactory.getLogger(FraudReportSerializationSchema.class);

    private final String schemaRegistryUrl;
    private final String topic;
    private transient KafkaAvroSerializer serializer;
    private transient SchemaRegistryClient schemaRegistryClient;

    public FraudReportSerializationSchema(String schemaRegistryUrl, String topic) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.topic = topic;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);

        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", schemaRegistryUrl);
        config.put("value.subject.name.strategy", "io.confluent.kafka.serializers.subject.TopicNameStrategy");
        config.put("auto.register.schemas", true);

        serializer = new KafkaAvroSerializer(schemaRegistryClient, config);

        LOG.info("Initialized FraudReportSerializationSchema for topic '{}' with Schema Registry: {}",
                topic, schemaRegistryUrl);
    }

    @Override
    public byte[] serialize(FraudReport element) {
        if (element == null) {
            return null;
        }

        try {
            return serializer.serialize(topic, element);
        } catch (Exception e) {
            LOG.error("Failed to serialize FraudReport: {}", element, e);
            throw new RuntimeException("Serialization failed", e);
        }
    }
}
