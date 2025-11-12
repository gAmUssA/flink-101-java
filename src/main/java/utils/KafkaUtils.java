package utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Kafka Utilities for Educational Flink Lessons
 *
 * This utility class provides common Kafka configuration methods for both producers and consumers
 * when connecting to Confluent Cloud. Designed for educational clarity and code reuse across
 * multiple Flink lessons.
 *
 * Key Features:
 * - Unified environment variable handling with fallback values
 * - Separate producer and consumer Kafka property configurations
 * - Confluent Cloud SASL_SSL authentication setup
 * - Educational logging and debugging support
 * - Consistent configuration across all lessons
 *
 * Usage Examples:
 * 
 * For Kafka Consumers (Flink applications):
 * ```java
 * String apiKey = KafkaUtils.getEnvVar("CNFL_KC_API_KEY", "your-api-key");
 * String apiSecret = KafkaUtils.getEnvVar("CNFL_KC_API_SECRET", "your-api-secret");
 * Properties props = KafkaUtils.createConsumerKafkaProperties(apiKey, apiSecret);
 * ```
 * 
 * For Kafka Producers (data generators):
 * ```java
 * String bootstrapServers = KafkaUtils.getEnvVar("CNFL_KAFKA_BROKER", "your-broker:9092");
 * String apiKey = KafkaUtils.getEnvVar("CNFL_KC_API_KEY", "your-api-key");
 * String apiSecret = KafkaUtils.getEnvVar("CNFL_KC_API_SECRET", "your-api-secret");
 * Properties props = KafkaUtils.createProducerKafkaProperties(bootstrapServers, apiKey, apiSecret);
 * ```
 *
 * Educational Benefits:
 * - Eliminates code duplication across lessons
 * - Provides consistent Kafka configuration patterns
 * - Centralizes Confluent Cloud authentication logic
 * - Makes it easy to update configurations across all examples
 * - Demonstrates proper utility class design patterns
 */
public class KafkaUtils {

    /**
     * Helper method to get environment variables with fallback values
     *
     * This method reads configuration from environment variables, which should
     * be loaded from your .env file. If a variable is not found, it uses the
     * provided default value and logs a warning.
     *
     * Educational Usage:
     * - Load Confluent Cloud API keys and broker information
     * - Provide sensible defaults for development and testing
     * - Give clear feedback about missing environment variables
     *
     * @param envVarName   Name of the environment variable to read
     * @param defaultValue Default value if environment variable is not set
     * @return The environment variable value or default value
     */
    public static String getEnvVar(String envVarName, String defaultValue) {
        String value = System.getenv(envVarName);
        if (value == null || value.trim().isEmpty()) {
            System.out.println("⚠️  Environment variable " + envVarName + " not found, using default: " + defaultValue);
            return defaultValue;
        }
        System.out.println("✓ Loaded " + envVarName + " from environment");
        return value;
    }

    /**
     * Creates Kafka consumer properties for Confluent Cloud connection
     *
     * This method sets up the necessary configuration for Flink Kafka consumers
     * to securely connect to Confluent Cloud using SASL_SSL authentication.
     * 
     * Key Configuration Details:
     * - Does NOT set deserializer properties (Flink handles deserialization internally)
     * - Configures SASL_SSL security for Confluent Cloud
     * - Sets consumer behavior for educational clarity
     * - Includes session and heartbeat settings for stability
     *
     * Educational Notes:
     * - Consumer properties are different from producer properties
     * - Flink manages deserialization through its own mechanisms
     * - Settings are optimized for learning, not production performance
     *
     * @param apiKey    Confluent Cloud API key for authentication
     * @param apiSecret Confluent Cloud API secret for authentication
     * @return Properties object configured for Kafka consumer with Confluent Cloud
     */
    public static Properties createConsumerKafkaProperties(String apiKey, String apiSecret) {
        Properties props = new Properties();

        // NOTE: We don't set deserializer properties here because Flink handles
        // deserialization through the deserializer specified in KafkaSource builder
        // Setting deserializers here would conflict with Flink's internal deserialization

        // Confluent Cloud security configuration
        props.setProperty("security.protocol", "SASL_SSL");
        props.setProperty("sasl.mechanism", "PLAIN");
        props.setProperty("sasl.jaas.config", String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"%s\" password=\"%s\";", apiKey, apiSecret));

        // Consumer behavior settings for educational clarity
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        // Session and heartbeat settings for stable connections
        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");

        System.out.println("✓ Kafka consumer properties configured for Confluent Cloud");
        return props;
    }

    /**
     * Creates Kafka producer properties for Confluent Cloud connection
     *
     * This method sets up the necessary configuration for Kafka producers
     * to securely connect to Confluent Cloud using SASL_SSL authentication.
     * 
     * Key Configuration Details:
     * - Sets bootstrap servers, key/value serializers for producers
     * - Configures SASL_SSL security for Confluent Cloud
     * - Includes producer-specific settings (acks, retries, idempotence)
     * - Optimizes batching settings for better throughput
     *
     * Educational Notes:
     * - Producer properties include serialization configuration
     * - Settings balance reliability with performance for learning
     * - Idempotence prevents duplicate messages
     * - Batching improves throughput while maintaining low latency
     *
     * @param bootstrapServers Confluent Cloud bootstrap servers (with port)
     * @param apiKey          Confluent Cloud API key for authentication
     * @param apiSecret       Confluent Cloud API secret for authentication
     * @return Properties object configured for Kafka producer with Confluent Cloud
     */
    public static Properties createProducerKafkaProperties(String bootstrapServers, String apiKey, String apiSecret) {
        Properties props = new Properties();

        // Basic producer configuration
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Confluent Cloud security configuration
        props.setProperty("security.protocol", "SASL_SSL");
        props.setProperty("sasl.mechanism", "PLAIN");
        props.setProperty("sasl.jaas.config", String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"%s\" password=\"%s\";", apiKey, apiSecret));

        // Producer behavior settings for educational clarity and reliability
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // Wait for all replicas
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // Batching settings for better throughput while maintaining low latency
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10");
        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        System.out.println("✓ Kafka producer properties configured for Confluent Cloud");
        return props;
    }
}