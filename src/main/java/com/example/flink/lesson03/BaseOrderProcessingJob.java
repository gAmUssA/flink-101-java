package com.example.flink.lesson03;

import java.io.IOException;
import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.ObjectMapper;

import shared.data.generators.Order;
import utils.KafkaUtils;
import utils.FlinkEnvironmentConfig;

/**
 * Base class for Order Processing Jobs
 * <p> 
 * This class provides common Kafka integration setup and utilities that are shared
 * across all order processing jobs. It eliminates code duplication while allowing
 * each job to focus on its specific processing logic.
 * <p> 
 * Educational Benefits:
 * - Demonstrates proper code organization and reuse
 * - Shows how to create base classes for common functionality
 * - Separates concerns between infrastructure and business logic
 * - Makes individual jobs simpler and more focused
 */
public abstract class BaseOrderProcessingJob {

    protected static final String TOPIC_NAME = "orders";

    /**
     * Creates and configures the Flink execution environment
     * with educational settings optimized for learning and Web UI enabled
     */
    protected static StreamExecutionEnvironment createExecutionEnvironment() {
        return FlinkEnvironmentConfig.createEnvironmentWithUI();
    }

    /**
     * Creates a Kafka source for Order objects with Confluent Cloud integration
     * 
     * This method handles all the common Kafka configuration including:
     * - Environment variable loading
     * - Confluent Cloud authentication
     * - JSON deserialization
     * - Consumer group configuration
     */
    protected static DataStream<Order> createOrderStream(StreamExecutionEnvironment env, String consumerGroupId) {
        System.out.println("Connecting to Confluent Cloud Kafka for order data...");

        // Load Confluent Cloud configuration from environment variables
        String bootstrapServers = KafkaUtils.getEnvVar("CNFL_KAFKA_BROKER", "your-kafka-broker:9092");
        String apiKey = KafkaUtils.getEnvVar("CNFL_KC_API_KEY", "your-api-key");
        String apiSecret = KafkaUtils.getEnvVar("CNFL_KC_API_SECRET", "your-api-secret");

        System.out.println("Bootstrap servers: " + bootstrapServers);
        System.out.println("Using topic: " + TOPIC_NAME);
        System.out.println("Using consumer group: " + consumerGroupId);

        // Configure Kafka source with JSON deserialization
        KafkaSource<Order> kafkaSource = KafkaSource.<Order>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(TOPIC_NAME)
            .setGroupId(consumerGroupId)
            .setStartingOffsets(OffsetsInitializer.latest()) // Start from latest messages for real-time processing
            .setValueOnlyDeserializer(new OrderJsonDeserializer())
            .setProperties(KafkaUtils.createConsumerKafkaProperties(apiKey, apiSecret))
            .build();

        // Create data stream from Kafka source
        return env.fromSource(
            kafkaSource,
            WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((order, timestamp) -> order.timestamp),
            "Kafka Order Source"
        );
    }

    /**
     * Prints common startup information for all jobs
     */
    protected static void printJobHeader(String jobName, String description) {
        System.out.println("=== " + jobName + " ===");
        System.out.println(description);
        System.out.println();
        
        // Print Web UI access instructions
        FlinkEnvironmentConfig.printWebUIInstructions();
    }

    /**
     * Executes the job with proper error handling and educational messaging
     */
    protected static void executeJob(StreamExecutionEnvironment env, String jobName) throws Exception {
        System.out.println("Starting " + jobName + "... Press Ctrl+C to stop.");
        env.execute(jobName);
    }

    /**
     * JSON Deserializer for Order objects from Kafka
     * Handles conversion from JSON strings to Order objects
     */
    public static class OrderJsonDeserializer extends AbstractDeserializationSchema<Order> {

        private static final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public Order deserialize(byte[] message) throws IOException {
            try {
                String jsonString = new String(message);
                Order order = objectMapper.readValue(message, Order.class);
                return order;
            } catch (Exception e) {
                System.err.println("[ERROR] Failed to deserialize message: " + new String(message));
                System.err.println("[ERROR] Deserialization error: " + e.getMessage());
                e.printStackTrace();
                throw e;
            }
        }

        @Override
        public TypeInformation<Order> getProducedType() {
            return TypeInformation.of(Order.class);
        }
    }
}