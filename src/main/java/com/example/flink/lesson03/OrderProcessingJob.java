package com.example.flink.lesson03;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Duration;

import shared.data.generators.Order;
import shared.utils.KafkaUtils;

/**
 * Lesson 3: Advanced Stream Processing with Kafka Integration
 * <p>
 * This lesson demonstrates advanced Apache Flink stream processing concepts including
 * stateful processing, keyed streams, and complex transformations using real-time data
 * from Confluent Cloud Kafka. You'll learn how to build applications that maintain
 * state across multiple events from streaming data sources.
 * <p>
 * What you'll learn:
 * - Stateful processing with keyed streams and managed state
 * - Using ValueState to track information across events
 * - Rich functions with lifecycle methods (open, close)
 * - Complex event processing with business logic
 * - Checkpointing configuration for fault tolerance
 * - Kafka integration with JSON deserialization
 * - Confluent Cloud connectivity and authentication
 * - Order processing with realistic business scenarios
 * <p>
 * Prerequisites:
 * - Confluent Cloud account with Kafka cluster
 * - API keys configured in .env file
 * - Topic 'orders' with JSON order data
 * - Environment variables loaded from .env file
 * <p>
 * Business Scenario:
 * We're processing an e-commerce order stream from Kafka to:
 * - Track customer order totals and counts (stateful processing)
 * - Detect VIP customers based on spending patterns
 * - Calculate running averages per customer
 * - Monitor order frequency and patterns
 * - Analyze spending by product category
 * <p>
 * Expected Output:
 * Connecting to Confluent Cloud Kafka for order data...
 * ✓ Loaded CNFL_KAFKA_BROKER from environment
 * ✓ Loaded CNFL_KC_API_KEY from environment
 * ✓ Loaded CNFL_KC_API_SECRET from environment
 * Bootstrap servers: pkc-rgm37.us-west-2.aws.confluent.cloud:9092
 * Using topic: orders
 * ✓ Kafka properties configured for Confluent Cloud
 * Customer Totals> (customer_002, 156.78, 1)
 * VIP Status> customer_002: REGULAR (total: 156.78, orders: 1)
 * Order Frequency> customer_002: 1 orders in session (NEW CUSTOMER)
 * Category Analysis> Category Electronics: total=156.78, orders=1, avg=156.78, max=156.78
 * <p>
 * Try this:
 * 1. Modify the VIP customer threshold and observe different classifications
 * 2. Add new state variables to track additional metrics
 * 3. Send test orders to your Kafka topic using Confluent Cloud Console
 * 4. Add filtering logic for specific customer segments
 * 5. Implement alerts for unusual spending patterns
 * 6. Experiment with different consumer group IDs
 */
public class OrderProcessingJob {

  public static void main(String[] args) throws Exception {

    // Step 1: Create an execution environment with checkpointing
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Configure for educational clarity
    env.setParallelism(1);
    env.disableOperatorChaining();

    // Enable checkpointing for fault tolerance
    // In production, you'd typically use shorter intervals (e.g., 30 seconds)
    env.enableCheckpointing(60000); // Checkpoint every 60 seconds

    System.out.println("=== Flink Lesson 3: Advanced Stream Processing ===");
    System.out.println("Processing order stream with stateful operations...");
    System.out.println("Connecting to Confluent Cloud Kafka for order data...");

    // Step 2: Load Confluent Cloud configuration from environment variables
    String bootstrapServers = KafkaUtils.getEnvVar("CNFL_KAFKA_BROKER", "your-kafka-broker:9092");
    String apiKey = KafkaUtils.getEnvVar("CNFL_KC_API_KEY", "your-api-key");
    String apiSecret = KafkaUtils.getEnvVar("CNFL_KC_API_SECRET", "your-api-secret");

    String topicName = "orders"; // Topic containing JSON order data

    System.out.println("Bootstrap servers: " + bootstrapServers);
    System.out.println("Using topic: " + topicName);

    // Step 3: Configure Kafka source with JSON deserialization
    String consumerGroupId = "flink-lesson03-order-processing"; // Consistent group ID for proper offset management
    System.out.println("Using consumer group: " + consumerGroupId);

    KafkaSource<Order> kafkaSource = KafkaSource.<Order>builder()
        .setBootstrapServers(bootstrapServers)
        .setTopics(topicName)
        .setGroupId(consumerGroupId)
        .setStartingOffsets(OffsetsInitializer.latest()) // Start from latest messages for real-time processing
        .setValueOnlyDeserializer(new OrderJsonDeserializer())
        .setProperties(KafkaUtils.createConsumerKafkaProperties(apiKey, apiSecret))
        .build();

    // Step 4: Create data stream from Kafka source
    DataStream<Order> orderStream = env
        .fromSource(
            kafkaSource,
            WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((order, timestamp) -> order.timestamp),
            "Kafka Order Source"
        );

    // Step 3: Stateful Processing - Track customer order totals
    // This demonstrates how to maintain state across multiple events
    DataStream<Tuple3<String, Double, Integer>> customerTotals = orderStream
        .keyBy(order -> order.customerId)
        .process(new CustomerOrderTracker());

    customerTotals.print("Customer Totals");

    // Debug: Add logging to see if orders are reaching the processing functions
    orderStream.map(order -> {
      System.out.println("[DEBUG] Order reached processing pipeline: " + order);
      return order;
    });

    // Step 4: VIP Customer Detection using Rich Function with State
    // This shows how to use RichMapFunction with state management
    DataStream<String> vipStatus = orderStream
        .keyBy(order -> order.customerId)
        .map(new VIPCustomerDetector());

    vipStatus.print("VIP Status");

    // Step 5: Order Frequency Tracking
    // Track how many orders each customer has placed
    DataStream<String> orderFrequency = orderStream
        .keyBy(order -> order.customerId)
        .process(new OrderFrequencyTracker());

    orderFrequency.print("Order Frequency");

    // Step 6: Category-based Analysis
    // Analyze spending patterns by product category
    DataStream<String> categoryAnalysis = orderStream
        .keyBy(order -> order.category)
        .process(new CategorySpendingAnalyzer());

    categoryAnalysis.print("Category Analysis");

    // Step 7: Execute the program
    System.out.println("Starting advanced stream processing... Press Ctrl+C to stop.");
    env.execute("Lesson 3: Advanced Order Processing with State");
  }


  /**
   * Stateful function to track customer order totals and counts
   * Demonstrates how to use managed state in Flink with KeyedProcessFunction
   */
  public static class CustomerOrderTracker extends KeyedProcessFunction<String, Order, Tuple3<String, Double, Integer>> {

    private transient ValueState<Tuple2<Double, Integer>> customerState;

    @Override
    public void processElement(Order order, Context ctx, Collector<Tuple3<String, Double, Integer>> out) throws Exception {
      // Lazy state initialization - only initialize once per function instance
      if (customerState == null) {
        ValueStateDescriptor<Tuple2<Double, Integer>> descriptor = new ValueStateDescriptor<>(
            "customer-totals",
            TypeInformation.of(new TypeHint<Tuple2<Double, Integer>>() {
            })
        );
        customerState = getRuntimeContext().getState(descriptor);
      }

      // Get current state (total amount, order count) for this specific customer key
      Tuple2<Double, Integer> current = customerState.value();
      if (current == null) {
        // This is the first order for this customer
        current = new Tuple2<>(0.0, 0);
      }

      // Update state with new order
      double newTotal = current.f0 + order.amount;
      int newCount = current.f1 + 1;

      customerState.update(new Tuple2<>(newTotal, newCount));

      // Emit updated totals
      out.collect(new Tuple3<>(order.customerId, newTotal, newCount));

      // Educational debugging
      System.out.println("Updated customer " + order.customerId +
                         ": total=" + String.format("%.2f", newTotal) + ", orders=" + newCount);
    }
  }

  /**
   * Rich function to detect VIP customers based on spending patterns
   * Demonstrates RichMapFunction with state management
   */
  public static class VIPCustomerDetector extends RichMapFunction<Order, String> {

    private static final double VIP_THRESHOLD = 500.0;
    private static final double PREMIUM_THRESHOLD = 1000.0;

    private transient ValueState<Tuple2<Double, Integer>> customerSpendingState;

    public void open(Configuration parameters) throws Exception {
      // Initialize state for tracking customer spending
      ValueStateDescriptor<Tuple2<Double, Integer>> descriptor = new ValueStateDescriptor<>(
          "customer-spending",
          TypeInformation.of(new TypeHint<Tuple2<Double, Integer>>() {
          })
      );
      customerSpendingState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public String map(Order order) throws Exception {
      // Get current spending state
      Tuple2<Double, Integer> current = customerSpendingState.value();
      if (current == null) {
        current = new Tuple2<>(0.0, 0);
      }

      // Update spending totals
      double newTotal = current.f0 + order.amount;
      int newCount = current.f1 + 1;

      customerSpendingState.update(new Tuple2<>(newTotal, newCount));

      // Determine VIP status
      String status;
      if (newTotal >= PREMIUM_THRESHOLD) {
        status = "PREMIUM";
      } else if (newTotal >= VIP_THRESHOLD) {
        status = "VIP";
      } else {
        status = "REGULAR";
      }

      return String.format("%s: %s (total: %.2f, orders: %d)",
                           order.customerId, status, newTotal, newCount);
    }
  }

  /**
   * Function to track order frequency per customer
   * Demonstrates stateful processing for behavioral analysis
   */
  public static class OrderFrequencyTracker extends KeyedProcessFunction<String, Order, String> {

    private transient ValueState<Integer> orderCountState;

    public void open(Configuration parameters) throws Exception {
      ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
          "order-count",
          Integer.class
      );
      orderCountState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Order order, Context ctx, Collector<String> out) throws Exception {
      // Get current order count
      Integer currentCount = orderCountState.value();
      if (currentCount == null) {
        currentCount = 0;
      }

      // Increment count
      int newCount = currentCount + 1;
      orderCountState.update(newCount);

      // Emit frequency information
      String frequency;
      if (newCount == 1) {
        frequency = "NEW CUSTOMER";
      } else if (newCount <= 5) {
        frequency = "OCCASIONAL";
      } else if (newCount <= 15) {
        frequency = "REGULAR";
      } else {
        frequency = "FREQUENT";
      }

      out.collect(String.format("%s: %d orders in session (%s)",
                                order.customerId, newCount, frequency));
    }
  }

  /**
   * Function to analyze spending patterns by product category
   * Demonstrates keyed state for category-based analytics
   */
  public static class CategorySpendingAnalyzer extends KeyedProcessFunction<String, Order, String> {

    private transient ValueState<Tuple3<Double, Integer, Double>> categoryState;

    public void open(Configuration parameters) throws Exception {
      // State: (total amount, order count, max single order)
      ValueStateDescriptor<Tuple3<Double, Integer, Double>> descriptor = new ValueStateDescriptor<>(
          "category-stats",
          TypeInformation.of(new TypeHint<Tuple3<Double, Integer, Double>>() {
          })
      );
      categoryState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Order order, Context ctx, Collector<String> out) throws Exception {
      // Get current category statistics
      Tuple3<Double, Integer, Double> current = categoryState.value();
      if (current == null) {
        current = new Tuple3<>(0.0, 0, 0.0);
      }

      // Update statistics
      double newTotal = current.f0 + order.amount;
      int newCount = current.f1 + 1;
      double newMax = Math.max(current.f2, order.amount);

      categoryState.update(new Tuple3<>(newTotal, newCount, newMax));

      // Calculate average
      double average = newTotal / newCount;

      // Emit category analysis
      out.collect(String.format("Category %s: total=%.2f, orders=%d, avg=%.2f, max=%.2f",
                                order.category, newTotal, newCount, average, newMax));
    }
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
        //System.out.println("[DEBUG] Deserializing message: " + jsonString);
        Order order = objectMapper.readValue(message, Order.class);
        //System.out.println("[DEBUG] Successfully deserialized: " + order);
        return order;
      } catch (Exception e) {
//        System.err.println("[ERROR] Failed to deserialize message: " + new String(message));
//        System.err.println("[ERROR] Deserialization error: " + e.getMessage());
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