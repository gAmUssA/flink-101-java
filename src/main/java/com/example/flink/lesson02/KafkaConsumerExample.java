package com.example.flink.lesson02;

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
import shared.utils.KafkaUtils;
import utils.FlinkEnvironmentConfig;

/**
 * Lesson 2: Kafka Integration with Confluent Cloud (Order Processing)
 * <p>
 * This lesson demonstrates how to connect Apache Flink to Confluent Cloud Kafka
 * using secure SASL_SSL authentication to consume and process Order objects in real-time.
 * You'll learn how to deserialize JSON messages into Java objects and process them
 * with Flink's DataStream API.
 * <p>
 * What you'll learn:
 * - How to configure Kafka source with Confluent Cloud
 * - Setting up secure SASL_SSL authentication
 * - Using environment variables for API keys and configuration
 * - JSON deserialization of Order objects from Kafka messages
 * - Implementing watermark strategies for event time processing
 * - Processing structured data (Order objects) instead of raw strings
 * - Extracting and transforming order information
 * <p>
 * Prerequisites:
 * - Confluent Cloud account with Kafka cluster
 * - API keys configured in .env file
 * - Topic 'orders' created in your Kafka cluster
 * - KafkaOrderProducer running to generate test data
 * <p>
 * Expected Output:
 * You should see Order objects from your Kafka topic being processed in real-time:
 * Processing order: Order{id=order_00001, customer=customer_002, amount=156.78, category=Electronics}
 * Processed Orders> [1721058123456] Order processed: order_00001 (Customer: customer_002, Amount: $156.78, Category: Electronics)
 * Processing order: Order{id=order_00002, customer=customer_001, amount=89.99, category=Books}
 * Processed Orders> [1721058126789] Order processed: order_00002 (Customer: customer_001, Amount: $89.99, Category: Books)
 *
 * Try this:
 * 1. Run KafkaOrderProducer to generate continuous test data
 * 2. Modify the processing logic to filter orders by category or amount
 * 3. Add aggregations to calculate customer spending totals
 * 4. Experiment with different consumer group IDs
 * 5. Add alerts for high-value orders (e.g., amount > $400)
 * 6. Create different processing branches for different customer types
 */
public class KafkaConsumerExample {

  public static void main(String[] args) throws Exception {

    // Step 1: Create the execution environment with Web UI enabled
    StreamExecutionEnvironment env = FlinkEnvironmentConfig.createEnvironmentWithUI();

    System.out.println("=== Flink Lesson 2: Kafka Integration ===");
    System.out.println("Connecting to Confluent Cloud Kafka...");
    System.out.println();
    
    // Print Web UI access instructions
    FlinkEnvironmentConfig.printWebUIInstructions();

    // Step 2: Load Confluent Cloud configuration from environment variables
    // These should be set in your .env file and loaded into the environment
    String bootstrapServers = KafkaUtils.getEnvVar("CNFL_KAFKA_BROKER", "your-kafka-broker:9092");
    String apiKey = KafkaUtils.getEnvVar("CNFL_KC_API_KEY", "your-api-key");
    String apiSecret = KafkaUtils.getEnvVar("CNFL_KC_API_SECRET", "your-api-secret");
    String topicName = "orders"; // You can change this to any topic in your cluster

    System.out.println("Bootstrap servers: " + bootstrapServers);
    System.out.println("Using topic: " + topicName);

    // Step 3: Configure Kafka source with Confluent Cloud settings
    KafkaSource<Order> kafkaSource = KafkaSource.<Order>builder()
        .setBootstrapServers(bootstrapServers)
        .setTopics(topicName)
        .setGroupId("flink-lesson02-consumer-group")
        .setStartingOffsets(OffsetsInitializer.latest()) // Start from latest messages
        .setValueOnlyDeserializer(new OrderJsonDeserializer())
        .setProperties(KafkaUtils.createConsumerKafkaProperties(apiKey, apiSecret))
        .build();

    // Step 4: Create data st ream from Kafka source
    // This creates a continuous stream of Order objects from your Kafka topic
    DataStream<Order> kafkaStream = env
        .fromSource(
            kafkaSource,
            WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((order, timestamp) -> order.timestamp),
            "Kafka Source"
        );

    // Step 5: Process the stream
    // For this lesson, we'll process each Order object and extract useful information
    // In real applications, you'd apply transformations, aggregations, etc.
    kafkaStream
        .map(order -> {
          // Add processing timestamp for educational purposes
          long processingTime = System.currentTimeMillis();
          String processedOrder = String.format("[%d] Order processed: %s (Customer: %s, Amount: $%.2f, Category: %s)",
                                                processingTime, order.orderId, order.customerId, order.amount, order.category);
          System.out.println("Processing order: " + order);
          return processedOrder;
        })
        .print("Processed Orders");

    // Step 6: Execute the program
    System.out.println("Starting Kafka consumer... Press Ctrl+C to stop.");
    env.execute("Lesson 2: Kafka Consumer with Confluent Cloud");
  }


  /**
   * JSON Deserializer for Order objects from Kafka
   * Handles conversion from JSON strings to Order objects
   */
  public static class OrderJsonDeserializer extends AbstractDeserializationSchema<Order> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Order deserialize(byte[] message) throws IOException {
      return objectMapper.readValue(message, Order.class);
    }

    @Override
    public TypeInformation<Order> getProducedType() {
      return TypeInformation.of(Order.class);
    }
  }
}