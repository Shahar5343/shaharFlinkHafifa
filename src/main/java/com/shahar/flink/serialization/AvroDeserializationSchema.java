package com.shahar.flink.serialization;

import com.shahar.bank.BankEvent;
import com.shahar.flink.config.FlinkConfig;
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

/**
 * ==============================================================================
 * WHAT: Flink deserialization schema for BankEvent from Kafka
 * WHY: Converts Kafka message bytes to BankEvent objects using Schema Registry
 * HOW: Uses Confluent's KafkaAvroDeserializer with schema caching
 * ==============================================================================
 * 
 * LEARNING POINT: This class bridges Flink's DeserializationSchema interface
 * with Confluent's Schema Registry client. It handles:
 * 1. Reading schema ID from message header (first 5 bytes)
 * 2. Fetching schema from registry (cached locally)
 * 3. Deserializing bytes using the schema
 * 4. Returning typed Java object (BankEvent)
 */
public class AvroDeserializationSchema implements DeserializationSchema<BankEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(AvroDeserializationSchema.class);

    private final String schemaRegistryUrl;
    private transient KafkaAvroDeserializer deserializer;
    private transient SchemaRegistryClient schemaRegistryClient;

    /**
     * Constructor
     * 
     * @param schemaRegistryUrl URL of Schema Registry
     */
    public AvroDeserializationSchema(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    /**
     * WHAT: Initialize deserializer (called once per task)
     * WHY: Lazy initialization because Flink serializes this object
     * LEARNING POINT: Flink distributes this object to task managers via
     * serialization.
     * Non-serializable objects (like HTTP clients) must be created lazily using
     * 'transient'.
     */
    @Override
    public void open(InitializationContext context) throws Exception {
        // Create Schema Registry client with caching
        // LEARNING POINT: Cache size of 100 means it will cache up to 100 schemas
        // locally
        // This avoids network calls to Schema Registry for every message
        schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);

        // Configure Kafka Avro Deserializer
        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", schemaRegistryUrl);
        config.put("specific.avro.reader", true); // Use generated classes, not GenericRecord

        deserializer = new KafkaAvroDeserializer(schemaRegistryClient, config);

        LOG.info("Initialized AvroDeserializationSchema with Schema Registry: {}", schemaRegistryUrl);
    }

    /**
     * WHAT: Deserialize a single message
     * WHY: Called for each Kafka message
     * HOW: Delegates to KafkaAvroDeserializer which handles schema ID lookup
     * 
     * @param message Raw bytes from Kafka (includes schema ID in header)
     * @return Deserialized BankEvent object
     * 
     *         LEARNING POINT: Message format from Schema Registry:
     *         - Byte 0: Magic byte (0x00)
     *         - Bytes 1-4: Schema ID (big-endian int)
     *         - Bytes 5+: Avro-encoded data
     */
    @Override
    public BankEvent deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }

        try {
            // Deserialize using Schema Registry
            // LEARNING POINT: The deserializer automatically:
            // 1. Reads schema ID from message header
            // 2. Fetches schema from registry (or cache)
            // 3. Deserializes bytes using that schema
            Object deserialized = deserializer.deserialize(null, message);

            if (deserialized instanceof BankEvent) {
                return (BankEvent) deserialized;
            } else {
                LOG.warn("Deserialized object is not a BankEvent: {}",
                        deserialized != null ? deserialized.getClass() : "null");
                return null;
            }
        } catch (Exception e) {
            // LEARNING POINT: Error handling strategy
            // Option 1: Return null (skip bad messages) - used here
            // Option 2: Throw exception (fail job) - for strict requirements
            // Option 3: Send to dead-letter queue - for production
            LOG.error("Failed to deserialize message, skipping", e);
            return null;
        }
    }

    /**
     * WHAT: Indicates if this schema produces null values
     * WHY: Flink uses this for optimization
     * HOW: We can produce nulls for malformed messages
     */
    @Override
    public boolean isEndOfStream(BankEvent nextElement) {
        return false; // Streaming never ends
    }

    /**
     * WHAT: Type information for Flink's type system
     * WHY: Flink needs to know the output type for serialization and operators
     * LEARNING POINT: Flink's type system enables optimizations like:
     * - Efficient serialization (Flink's own format, not Java serialization)
     * - Memory management (off-heap storage)
     * - Code generation for operators
     */
    @Override
    public TypeInformation<BankEvent> getProducedType() {
        return TypeInformation.of(BankEvent.class);
    }
}
