package com.shahar.flink.serialization;

import com.shahar.flink.Alert;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * ==============================================================================
 * WHAT: Flink serialization schema for Alert to Kafka
 * WHY: Converts Alert objects to Kafka message bytes with Schema Registry
 * HOW: Uses Confluent's KafkaAvroSerializer to register schema and serialize
 * ==============================================================================
 * 
 * LEARNING POINT: This class handles the output side of Schema Registry
 * integration:
 * 1. Registers Alert schema with Schema Registry (if not already registered)
 * 2. Serializes Alert object to bytes
 * 3. Prepends schema ID to message (5-byte header)
 * 
 * This ensures consumers can deserialize the message by looking up the schema.
 */
public class AvroSerializationSchema implements SerializationSchema<Alert> {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSerializationSchema.class);

    private final String schemaRegistryUrl;
    private final String topic;
    private transient KafkaAvroSerializer serializer;
    private transient SchemaRegistryClient schemaRegistryClient;

    /**
     * Constructor
     * 
     * @param schemaRegistryUrl URL of Schema Registry
     * @param topic             Kafka topic name (used for subject naming strategy)
     */
    public AvroSerializationSchema(String schemaRegistryUrl, String topic) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.topic = topic;
    }

    /**
     * WHAT: Initialize serializer (called once per task)
     * WHY: Lazy initialization for non-serializable objects
     * LEARNING POINT: Similar to deserialization, we use 'transient' for
     * objects that can't be serialized by Flink's distribution mechanism.
     */
    @Override
    public void open(InitializationContext context) throws Exception {
        // Create Schema Registry client with caching
        schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);

        // Configure Kafka Avro Serializer
        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", schemaRegistryUrl);

        // LEARNING POINT: Subject naming strategy
        // - TopicNameStrategy (default): subject = "<topic>-value"
        // - RecordNameStrategy: subject = "<namespace>.<name>"
        // - TopicRecordNameStrategy: subject = "<topic>-<namespace>.<name>"
        //
        // We're using default (TopicNameStrategy), so schema will be registered as:
        // "alerts-value" (for topic "alerts")
        config.put("value.subject.name.strategy",
                "io.confluent.kafka.serializers.subject.TopicNameStrategy");

        // Auto-register schema if it doesn't exist
        // LEARNING POINT: In production, you might want to disable this and
        // require manual schema registration for governance
        config.put("auto.register.schemas", true);

        serializer = new KafkaAvroSerializer(schemaRegistryClient, config);

        LOG.info("Initialized AvroSerializationSchema for topic '{}' with Schema Registry: {}",
                topic, schemaRegistryUrl);
    }

    /**
     * WHAT: Serialize a single Alert object
     * WHY: Called for each output record
     * HOW: Delegates to KafkaAvroSerializer which handles schema registration
     * 
     * @param element Alert object to serialize
     * @return Byte array with schema ID header + Avro-encoded data
     * 
     *         LEARNING POINT: Output message format:
     *         - Byte 0: Magic byte (0x00)
     *         - Bytes 1-4: Schema ID (registered in Schema Registry)
     *         - Bytes 5+: Avro-encoded Alert data
     * 
     *         The schema ID allows consumers to look up the schema and deserialize
     *         correctly.
     */
    @Override
    public byte[] serialize(Alert element) {
        if (element == null) {
            return null;
        }

        try {
            // Serialize using Schema Registry
            // LEARNING POINT: On first call, this will:
            // 1. Extract schema from Alert class
            // 2. Register schema with Schema Registry (if auto-register enabled)
            // 3. Get schema ID back
            // 4. Serialize object to bytes
            // 5. Prepend schema ID to bytes
            //
            // Subsequent calls use cached schema ID for performance
            return serializer.serialize(topic, element);
        } catch (Exception e) {
            // LEARNING POINT: Serialization errors are usually fatal
            // Unlike deserialization (where we might skip bad input),
            // we can't skip our own output - it indicates a bug
            LOG.error("Failed to serialize Alert: {}", element, e);
            throw new RuntimeException("Serialization failed", e);
        }
    }
}
