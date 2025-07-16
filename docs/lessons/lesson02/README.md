# Lesson 2: Kafka Integration with Confluent Cloud

## Overview

This lesson demonstrates how to connect Apache Flink to Confluent Cloud Kafka. 
You'll learn to consume real-time data streams from Kafka topics and process them with Flink's DataStream API.

## Learning Objectives

By the end of this lesson, you will understand:

- How to configure a Kafka source with Confluent Cloud
- Setting up secure SASL_SSL authentication
- Using environment variables for API keys and configuration
- Implementing watermark strategies for event time processing
- Handling consumer lag and monitoring
- Basic stream processing with Kafka data

## Prerequisites

### Required Setup
1. **Confluent Cloud Account**: Active account with Kafka cluster
2. **API Keys**: Configured in your `.env` file
3. **Kafka Topic**: At least one topic created (we'll use 'orders' topic)
4. **Environment Variables**: Loaded from `.env` file

### Environment Variables Required
Make sure your `.env` file contains:
```bash
export CNFL_KAFKA_BROKER=your-broker-endpoint
export CNFL_KC_API_KEY=your-kafka-api-key
export CNFL_KC_API_SECRET=your-kafka-api-secret
```

## Key Concepts

### 1. Kafka Source Configuration
```java
KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
    .setBootstrapServers(bootstrapServers)
    .setTopics(topicName)
    .setGroupId("flink-lesson02-consumer-group")
    .setStartingOffsets(OffsetsInitializer.latest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .setProperties(createKafkaProperties(apiKey, apiSecret))
    .build();
```

### 2. SASL_SSL Authentication
Confluent Cloud requires secure authentication:
```java
props.setProperty("security.protocol", "SASL_SSL");
props.setProperty("sasl.mechanism", "PLAIN");
props.setProperty("sasl.jaas.config", String.format(
    "org.apache.kafka.common.security.plain.PlainLoginModule required " +
    "username=\"%s\" password=\"%s\";", apiKey, apiSecret));
```

### 3. Watermark Strategy
For event time processing:
```java
WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis())
```

## Running the Example

### Step 1: Load Environment Variables
```bash
# Load environment variables from .env file
source .env
```

### Step 2: Create Test Topic (Optional)
If you don't have an 'orders' topic, create one in Confluent Cloud Console or modify the topic name in the code.

### Step 3: Run the Application
```bash
# Using Gradle task
./gradlew runLesson02

# Or compile and run directly
./gradlew build
java -cp build/libs/flink-demo.jar com.example.flink.lesson02.KafkaConsumerExample
```

### Step 4: Send Test Messages
Use Confluent Cloud Console to send test messages to your topic:
```json
{"orderId": "12345", "customerId": "cust_001", "amount": 99.99}
{"orderId": "12346", "customerId": "cust_002", "amount": 149.50}
```

## Expected Output

When running successfully, you should see:
```
=== Flink Lesson 2: Kafka Integration ===
Connecting to Confluent Cloud Kafka...
✓ Loaded CNFL_KAFKA_BROKER from environment
✓ Loaded CNFL_KC_API_KEY from environment
✓ Loaded CNFL_KC_API_SECRET from environment
Bootstrap servers: pkc-rgm37.us-west-2.aws.confluent.cloud:9092
Using topic: orders
✓ Kafka properties configured for Confluent Cloud
Starting Kafka consumer... Press Ctrl+C to stop.
Processing message: {"orderId": "12345", "customerId": "cust_001", "amount": 99.99}
Kafka Messages> [1721058123456] {"orderId": "12345", "customerId": "cust_001", "amount": 99.99}
```

## Troubleshooting

### Common Issues

#### 1. Authentication Errors
**Error**: `Authentication failed`
**Solution**: 
- Verify API keys in `.env` file
- Ensure environment variables are loaded: `source .env`
- Check API key permissions in Confluent Cloud

#### 2. Connection Timeout
**Error**: `Connection timeout`
**Solution**:
- Verify bootstrap server URL
- Check network connectivity
- Ensure firewall allows outbound connections on port 9092

#### 3. Topic Not Found
**Error**: `Topic 'orders' not found`
**Solution**:
- Create the topic in Confluent Cloud Console
- Or modify the topic name in the code to use an existing topic

#### 4. Environment Variables Not Found
**Error**: `Environment variable CNFL_KC_API_KEY not found`
**Solution**:
- Run `source .env` before executing the program
- Verify `.env` file exists and contains required variables
- Check variable names match exactly

### Debugging Tips

1. **Enable Debug Logging**: Add to your log4j2.properties:
   ```properties
   logger.kafka.name = org.apache.kafka
   logger.kafka.level = DEBUG
   ```

2. **Check Consumer Group**: Monitor your consumer group in Confluent Cloud Console

3. **Verify Topic Messages**: Use Confluent Cloud Console to verify messages exist in your topic

4. **Test Connection**: Use Confluent CLI to test connectivity:
   ```bash
   confluent kafka topic consume orders --from-beginning
   ```

## Experimentation Ideas

### 1. Different Topics
Modify the topic name to consume from different topics:
```java
String topicName = "user-events"; // Change this line
```

### 2. JSON Processing
Add JSON parsing to extract specific fields:
```java
kafkaStream
    .map(message -> {
        // Parse JSON and extract orderId
        // Add your JSON parsing logic here
        return message;
    })
```

### 3. Filtering
Add filtering logic to process only specific messages:
```java
kafkaStream
    .filter(message -> message.contains("VIP"))
    .print("VIP Orders");
```

### 4. Consumer Groups
Experiment with different consumer group IDs:
```java
.setGroupId("my-custom-consumer-group")
```

### 5. Offset Management
Try different starting offset strategies:
```java
.setStartingOffsets(OffsetsInitializer.earliest()) // Start from beginning
.setStartingOffsets(OffsetsInitializer.latest())   // Start from latest
```

## Next Steps

After completing this lesson, you should be able to:
- Connect Flink to any Kafka cluster
- Handle authentication and security
- Process real-time streaming data
- Monitor consumer lag and performance

**Ready for Lesson 3?** Next, we'll explore advanced stream processing with stateful operations, windowing, and complex event processing.

## Additional Resources

- [Confluent Cloud Documentation](https://docs.confluent.io/cloud/current/overview.html)
- [Flink Kafka Connector Documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/)
- [Kafka Consumer Configuration](https://kafka.apache.org/documentation/#consumerconfigs)
- [SASL_SSL Authentication Guide](https://docs.confluent.io/platform/current/kafka/authentication_sasl/authentication_sasl_plain.html)