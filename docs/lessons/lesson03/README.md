# Lesson 3: Advanced Stream Processing with Kafka Integration

## Overview

This lesson demonstrates advanced Apache Flink stream processing concepts including stateful processing, keyed streams, and complex transformations using real-time data from Confluent Cloud Kafka. You'll learn how to build applications that maintain state across multiple events and implement sophisticated business logic for real-time analytics.

## Learning Objectives

By the end of this lesson, you will understand:

- ✅ Stateful processing with keyed streams and managed state
- ✅ Using ValueState to track information across events
- ✅ KeyedProcessFunction and RichMapFunction lifecycle methods
- ✅ Complex event processing with business logic
- ✅ Checkpointing configuration for fault tolerance
- ✅ Kafka integration with JSON deserialization
- ✅ Confluent Cloud connectivity and authentication
- ✅ Real-time customer analytics and VIP detection
- ✅ Order frequency tracking and behavioral analysis
- ✅ Category-based spending analysis

## Prerequisites

### Required Setup
1. **Confluent Cloud Account**: Active account with Kafka cluster
2. **API Keys**: Configured in your `.env` file
3. **Kafka Topic**: 'orders' topic created in your cluster
4. **Environment Variables**: Loaded from `.env` file
5. **KafkaOrderProducer**: Running to generate test data (recommended)

### Environment Variables Required
Make sure your `.env` file contains:
```bash
export CNFL_KAFKA_BROKER=your-broker-endpoint:9092
export CNFL_KC_API_KEY=your-kafka-api-key
export CNFL_KC_API_SECRET=your-kafka-api-secret
```

### Data Generator Setup
For continuous testing, it's recommended to run the KafkaOrderProducer:
```bash
# In a separate terminal
source .env
./gradlew runKafkaProducer
```

## Key Concepts

### 1. Stateful Processing with KeyedProcessFunction
```java
public static class CustomerOrderTracker extends KeyedProcessFunction<String, Order, Tuple3<String, Double, Integer>> {
    private transient ValueState<Tuple2<Double, Integer>> customerState;
    
    @Override
    public void processElement(Order order, Context ctx, Collector<...> out) throws Exception {
        // State management logic
        Tuple2<Double, Integer> current = customerState.value();
        // Update and emit results
    }
}
```

### 2. JSON Deserialization from Kafka
```java
KafkaSource<Order> kafkaSource = KafkaSource.<Order>builder()
    .setBootstrapServers(bootstrapServers)
    .setTopics("orders")
    .setValueOnlyDeserializer(new OrderJsonDeserializer())
    .setProperties(KafkaUtils.createConsumerKafkaProperties(apiKey, apiSecret))
    .build();
```

### 3. Managed State with ValueState
```java
// State initialization (lazy)
if (customerState == null) {
    ValueStateDescriptor<Tuple2<Double, Integer>> descriptor = new ValueStateDescriptor<>(
        "customer-totals",
        TypeInformation.of(new TypeHint<Tuple2<Double, Integer>>() {})
    );
    customerState = getRuntimeContext().getState(descriptor);
}
```

### 4. Checkpointing for Fault Tolerance
```java
// Enable checkpointing every 60 seconds
env.enableCheckpointing(60000);
```

## Business Scenario

We're processing an e-commerce order stream from Kafka to implement real-time customer analytics:

- **Customer Order Tracking**: Maintain running totals and order counts per customer
- **VIP Customer Detection**: Classify customers based on spending patterns (REGULAR, VIP, PREMIUM)
- **Order Frequency Analysis**: Track customer behavior (NEW, OCCASIONAL, REGULAR, FREQUENT)
- **Category Analytics**: Analyze spending patterns by product category
- **Real-time Insights**: Provide immediate feedback on customer activity

## Running the Example

### Step 1: Load Environment Variables
```bash
# Load environment variables from .env file
source .env
```

### Step 2: Start Data Generator (Recommended)
```bash
# In a separate terminal, start the order producer
source .env
./gradlew runKafkaProducer
```

This will continuously generate realistic order data:
```
Sent Order> Order{id=order_00001, customer=customer_002, amount=156.78, category=Electronics}
Sent Order> Order{id=order_00002, customer=customer_001, amount=89.99, category=Books}
```

### Step 3: Run the Advanced Processing Job
```bash
# Using Gradle task
./gradlew runLesson03

# Or compile and run directly
./gradlew build
java -cp build/libs/flink-demo.jar com.example.flink.lesson03.OrderProcessingJob
```

### Step 4: Observe Real-time Processing
Watch the console output for real-time analytics as orders are processed.

## Expected Output

When running successfully with the KafkaOrderProducer, you should see:

```
=== Flink Lesson 3: Advanced Stream Processing ===
Processing order stream with stateful operations...
Connecting to Confluent Cloud Kafka for order data...
✓ Loaded CNFL_KAFKA_BROKER from environment
✓ Loaded CNFL_KC_API_KEY from environment
✓ Loaded CNFL_KC_API_SECRET from environment
Bootstrap servers: pkc-rgm37.us-west-2.aws.confluent.cloud:9092
Using topic: orders
Using consumer group: flink-lesson03-order-processing
✓ Kafka consumer properties configured for Confluent Cloud
Starting advanced stream processing... Press Ctrl+C to stop.

Updated customer customer_002: total=156.78, orders=1
Customer Totals> (customer_002,156.78,1)
VIP Status> customer_002: REGULAR (total: 156.78, orders: 1)
Order Frequency> customer_002: 1 orders in session (NEW CUSTOMER)
Category Analysis> Category Electronics: total=156.78, orders=1, avg=156.78, max=156.78

Updated customer customer_001: total=89.99, orders=1
Customer Totals> (customer_001,89.99,1)
VIP Status> customer_001: REGULAR (total: 89.99, orders: 1)
Order Frequency> customer_001: 1 orders in session (NEW CUSTOMER)
Category Analysis> Category Books: total=89.99, orders=1, avg=89.99, max=89.99

Updated customer customer_002: total=312.56, orders=2
Customer Totals> (customer_002,312.56,2)
VIP Status> customer_002: REGULAR (total: 312.56, orders: 2)
Order Frequency> customer_002: 2 orders in session (OCCASIONAL)
```

## Code Structure Explanation

### Main Components

1. **OrderProcessingJob**: Main class with execution environment setup
2. **CustomerOrderTracker**: KeyedProcessFunction for tracking customer totals
3. **VIPCustomerDetector**: RichMapFunction for customer classification
4. **OrderFrequencyTracker**: KeyedProcessFunction for behavioral analysis
5. **CategorySpendingAnalyzer**: KeyedProcessFunction for category analytics
6. **OrderJsonDeserializer**: JSON deserialization for Order objects

### Processing Pipeline

```
Kafka Topic 'orders' → JSON Deserialization → Keyed Streams → Stateful Processing → Analytics Output
                                                    ↓
                                            [customer_001] → CustomerOrderTracker → (customer_001, 156.78, 1)
                                            [customer_001] → VIPCustomerDetector → "customer_001: REGULAR"
                                            [customer_001] → OrderFrequencyTracker → "1 orders (NEW CUSTOMER)"
                                            [Electronics] → CategorySpendingAnalyzer → "Category Electronics: ..."
```

### Key Processing Functions

- **CustomerOrderTracker**: Maintains running totals and counts using ValueState
- **VIPCustomerDetector**: Classifies customers based on spending thresholds:
  - REGULAR: < $500
  - VIP: $500 - $999
  - PREMIUM: ≥ $1000
- **OrderFrequencyTracker**: Analyzes customer behavior patterns
- **CategorySpendingAnalyzer**: Tracks category-level metrics (total, count, average, max)

## Troubleshooting

### Common Issues

#### 1. No Output Visible
**Issue**: Consumer starts but doesn't process any messages
**Solutions**:
- Ensure KafkaOrderProducer is running and generating data
- Check that the 'orders' topic exists in Confluent Cloud
- Verify consumer group is not conflicting with previous runs
- Check offset strategy (latest vs earliest)

#### 2. State Aggregation Issues
**Issue**: Customers always show orders=1 instead of accumulating
**Solutions**:
- Verify consumer group ID is consistent (not timestamp-based)
- Check that KeyedProcessFunction state is properly initialized
- Ensure messages aren't being reprocessed due to offset issues
- Use consistent consumer group across runs

#### 3. Authentication Errors
**Error**: `Authentication failed`
**Solutions**:
- Verify API keys in `.env` file
- Ensure `source .env` was executed
- Check API key permissions in Confluent Cloud
- Verify bootstrap server URL includes port (:9092)

#### 4. JSON Deserialization Errors
**Error**: `Failed to deserialize JSON`
**Solutions**:
- Ensure Order class structure matches JSON format
- Check Jackson annotations are correct
- Verify KafkaOrderProducer is sending valid JSON
- Check for schema evolution issues

#### 5. Consumer Lag Issues
**Issue**: Processing seems delayed or inconsistent
**Solutions**:
- Monitor consumer lag in Confluent Cloud Console
- Check network connectivity and latency
- Verify watermark strategy configuration
- Consider adjusting consumer properties

### Debugging Tips

1. **Enable Debug Logging**: Monitor message flow and state updates
2. **Check Consumer Groups**: Use Confluent Cloud Console to monitor consumer groups
3. **Verify Topic Messages**: Confirm messages exist in the 'orders' topic
4. **Test Connection**: Use Confluent CLI to test connectivity
5. **Monitor State**: Add logging to track state initialization and updates

## Experimentation Ideas

### 1. Modify VIP Thresholds
```java
private static final double VIP_THRESHOLD = 300.0;      // Lower threshold
private static final double PREMIUM_THRESHOLD = 800.0;  // Adjust premium level
```

### 2. Add New State Variables
```java
// Track customer's favorite category
private transient ValueState<Map<String, Integer>> categoryPreferences;

// Track order timing patterns
private transient ValueState<List<Long>> orderTimestamps;
```

### 3. Implement Custom Alerts
```java
// Alert for high-value orders
if (order.amount > 400.0) {
    out.collect("ALERT: High-value order: " + order);
}

// Alert for frequent orders
if (timeSinceLastOrder < Duration.ofMinutes(5).toMillis()) {
    out.collect("ALERT: Rapid ordering detected for " + order.customerId);
}
```

### 4. Add Filtering Logic
```java
// Process only specific categories
orderStream
    .filter(order -> order.category.equals("Electronics"))
    .keyBy(order -> order.customerId)
    .process(new ElectronicsSpecialistTracker());
```

### 5. Implement Time-based Windows
```java
// Hourly customer analytics
orderStream
    .keyBy(order -> order.customerId)
    .window(TumblingEventTimeWindows.of(Time.hours(1)))
    .aggregate(new HourlyCustomerAggregator());
```

### 6. Create Customer Segments
```java
// Segment customers by behavior
public enum CustomerSegment {
    NEW_CUSTOMER,      // First order
    PRICE_SENSITIVE,   // Low average order value
    HIGH_VALUE,        // High total spending
    FREQUENT_BUYER,    // Many orders
    CATEGORY_FOCUSED   // Shops in specific categories
}
```

## Performance Considerations

### State Management
- **Memory Usage**: Monitor state size for high-cardinality keys
- **Checkpointing**: Balance frequency vs. performance impact
- **State Backend**: Consider RocksDB for large state
- **TTL**: Implement state TTL for inactive customers

### Kafka Configuration
- **Consumer Groups**: Use consistent group IDs for proper offset management
- **Parallelism**: Scale processing based on topic partitions
- **Batch Size**: Optimize for throughput vs. latency
- **Watermarks**: Configure appropriate out-of-orderness bounds

## Next Steps

After completing this lesson, you should be able to:
- Implement stateful stream processing applications
- Handle complex business logic with managed state
- Integrate with Kafka for real-time data processing
- Build customer analytics and behavioral tracking systems
- Debug and troubleshoot stateful processing issues

**Ready for Lesson 4?** Next, we'll explore materialized views and how to create persistent, queryable views of streaming data for analytics dashboards.

## Additional Resources

- [Apache Flink State Management](https://flink.apache.org/docs/stable/dev/stream/state/)
- [KeyedProcessFunction Documentation](https://flink.apache.org/docs/stable/dev/stream/operators/process_function.html)
- [Flink Kafka Connector](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/)
- [Confluent Cloud Documentation](https://docs.confluent.io/cloud/current/overview.html)
- [Flink Checkpointing](https://flink.apache.org/docs/stable/dev/stream/state/checkpointing.html)

## Success Indicators

You've successfully completed this lesson when you can:

- ✅ Run the OrderProcessingJob and see real-time customer analytics
- ✅ Understand how state is maintained across multiple events
- ✅ Explain the difference between KeyedProcessFunction and RichMapFunction
- ✅ Modify VIP thresholds and observe different customer classifications
- ✅ Add new stateful processing functions to the pipeline
- ✅ Troubleshoot common state management and Kafka connectivity issues
- ✅ Implement custom business logic for real-time analytics

**Congratulations!** You've mastered advanced stream processing with Apache Flink! 🎉