# Kafka Order Producer Setup Guide

## Overview

The Kafka Order Producer continuously generates realistic e-commerce order data and sends it to a Kafka topic as JSON messages. 
This enables continuous testing of Flink streaming applications like OrderProcessingJob and KafkaConsumerExample.

## Components

### 1. KafkaOrderProducer
- **Location**: `shared/data-generators/KafkaOrderProducer.java`
- **Purpose**: Continuously generates and sends Order JSON messages to Kafka
- **Features**: 
  - Confluent Cloud integration with SASL_SSL authentication
  - Realistic order data generation (customers, categories, amounts)
  - Configurable generation intervals
  - Graceful shutdown handling
  - Educational logging and debugging

### 2. Updated KafkaConsumerExample
- **Location**: `src/main/java/com/example/flink/lesson02/KafkaConsumerExample.java`
- **Purpose**: Consumes and processes Order objects from Kafka
- **Features**:
  - JSON deserialization of Order objects
  - Event time processing with watermarks
  - Order-specific transformations and analysis
  - Educational output formatting

### 3. OrderProcessingJob
- **Location**: `src/main/java/com/example/flink/lesson03/OrderProcessingJob.java`
- **Purpose**: Advanced stateful processing of Order streams
- **Features**:
  - Customer analytics and VIP detection
  - Category-based spending analysis
  - Order frequency tracking
  - Stateful processing with managed state

## Usage Instructions

### Step 1: Environment Setup

Ensure your `.env` file contains the required Confluent Cloud credentials:

```bash
export CNFL_KAFKA_BROKER=your-broker-endpoint:9092
export CNFL_KC_API_KEY=your-kafka-api-key
export CNFL_KC_API_SECRET=your-kafka-api-secret
```

Load the environment variables:
```bash
source .env
```

### Step 2: Create Kafka Topic

Create the 'orders' topic in your Confluent Cloud Console or use the CLI:
```bash
confluent kafka topic create orders --partitions 3 --replication-factor 3
```

### Step 3: Start the Kafka Producer

Run the producer to generate continuous order data:
```bash
./gradlew runKafkaProducer
```

**Expected Output:**
```
=== Kafka Order Producer ===
Connecting to Confluent Cloud Kafka...
✓ Loaded CNFL_KAFKA_BROKER from environment
✓ Loaded CNFL_KC_API_KEY from environment
✓ Loaded CNFL_KC_API_SECRET from environment
✓ Kafka producer configured for Confluent Cloud
Starting continuous order generation... Press Ctrl+C to stop.
Sent Order> Order{id=order_00001, customer=customer_002, amount=156.78, category=Electronics}
  ✓ Delivered to partition 1 at offset 12345
Sent Order> Order{id=order_00002, customer=customer_001, amount=89.99, category=Books}
  ✓ Delivered to partition 0 at offset 12346
```

### Step 4: Run Flink Applications

#### Option A: KafkaConsumerExample (Basic Order Processing)
```bash
./gradlew runLesson02
```

**Expected Output:**
```
=== Flink Lesson 2: Kafka Integration ===
Processing order: Order{id=order_00001, customer=customer_002, amount=156.78, category=Electronics}
Processed Orders> [1721058123456] Order processed: order_00001 (Customer: customer_002, Amount: $156.78, Category: Electronics)
```

#### Option B: OrderProcessingJob (Advanced Stateful Processing)
```bash
./gradlew runLesson03
```

**Expected Output:**
```
=== Flink Lesson 3: Advanced Stream Processing ===
Customer Totals> (customer_002, 156.78, 1)
VIP Status> customer_002: REGULAR (total: 156.78, orders: 1)
Order Frequency> customer_002: 1 orders in session (NEW CUSTOMER)
Category Analysis> Category Electronics: total=156.78, orders=1, avg=156.78, max=156.78
```

## Data Flow Architecture

```
KafkaOrderProducer → Kafka Topic 'orders' → Flink Applications
                                          ├── KafkaConsumerExample
                                          └── OrderProcessingJob
```

1. **KafkaOrderProducer** generates Order objects and serializes them to JSON
2. **Kafka Topic** stores the JSON messages with customer-based partitioning
3. **Flink Applications** consume and process the Order objects in real-time

## Order Data Structure

```json
{
  "orderId": "order_00001",
  "customerId": "customer_002", 
  "amount": 156.78,
  "timestamp": 1721058123456,
  "category": "Electronics"
}
```

## Configuration Options

### Producer Configuration
- **Generation Interval**: 2-4 seconds between orders (configurable in code)
- **Customer Pool**: 5 predefined customers (expandable)
- **Categories**: Electronics, Clothing, Books, Home, Sports
- **Price Range**: $10-$500
- **Partitioning**: By customer ID for ordered processing

### Consumer Configuration
- **Consumer Group**: Configurable per application
- **Starting Offset**: Latest (configurable)
- **Watermark Strategy**: 5-second bounded out-of-orderness
- **Event Time**: Uses order timestamp

## Troubleshooting

### Common Issues

#### 1. Producer Connection Errors
**Error**: `Authentication failed`
**Solution**: 
- Verify API keys in `.env` file
- Ensure `source .env` was executed
- Check API key permissions in Confluent Cloud

#### 2. Topic Not Found
**Error**: `Topic 'orders' not found`
**Solution**:
- Create the topic in Confluent Cloud Console
- Verify topic name matches exactly

#### 3. Consumer Lag
**Issue**: Consumer not receiving messages
**Solution**:
- Check producer is running and sending messages
- Verify consumer group ID is unique
- Monitor consumer lag in Confluent Cloud Console

#### 4. JSON Deserialization Errors
**Error**: `Failed to deserialize JSON`
**Solution**:
- Ensure Order class structure matches JSON format
- Check Jackson annotations are correct
- Verify no schema evolution issues

### Monitoring

#### Producer Monitoring
- Check console output for delivery confirmations
- Monitor partition distribution
- Watch for error messages and retries

#### Consumer Monitoring
- Use Confluent Cloud Console to monitor consumer groups
- Check consumer lag metrics
- Monitor processing throughput in Flink Web UI

#### Flink Monitoring
- Access Flink Web UI at http://localhost:8081
- Monitor job execution graphs
- Check checkpoint status and metrics

## Experimentation Ideas

### Producer Experiments
1. **Multiple Producers**: Run multiple producer instances for higher throughput
2. **Custom Data**: Modify customer lists, categories, or price ranges
3. **Burst Patterns**: Implement time-based order bursts (e.g., flash sales)
4. **Error Simulation**: Add occasional malformed messages for error handling testing

### Consumer Experiments
1. **Filtering**: Add filters for specific categories or price ranges
2. **Aggregations**: Calculate real-time customer spending totals
3. **Alerts**: Implement alerts for high-value orders
4. **Multiple Consumers**: Run multiple consumer groups for different use cases

### Integration Experiments
1. **End-to-End Testing**: Run producer + multiple consumers simultaneously
2. **Failure Recovery**: Test behavior during network interruptions
3. **Scaling**: Increase parallelism and observe performance
4. **State Management**: Experiment with different state backends

## Best Practices

### Production Considerations
- Use appropriate batch sizes and linger settings for throughput
- Implement proper error handling and retry logic
- Monitor resource usage and adjust memory settings
- Use incremental checkpointing for large state
- Implement proper logging and monitoring

### Development Tips
- Start with single parallelism for debugging
- Use educational logging to understand data flow
- Test with small datasets before scaling up
- Monitor Kafka topic metrics during development
- Use consistent naming conventions across components

## Next Steps

After successfully running the complete system:

1. **Experiment** with different data patterns and processing logic
2. **Scale** the system by increasing parallelism and throughput
3. **Extend** with additional data sources and sinks
4. **Monitor** performance and optimize bottlenecks
5. **Deploy** to production environments with proper configuration

This setup provides a complete foundation for learning and experimenting with real-time stream processing using Apache Flink and Kafka.