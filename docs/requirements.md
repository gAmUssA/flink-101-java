# Apache Flink 2.0 Demo Suite PRD: 5-Lesson Developer Course

## Executive Summary

This Product Requirements Document outlines a comprehensive 5-lesson Apache Flink 2.0 demo suite designed for developers new to stream processing. 
The course leverages Docker for local deployment, Confluent Cloud for Kafka services, and follows a progressive, hands-on learning approach with 30-minute code-driven lessons.

**Key innovations in Flink 2.0** include disaggregated state management, materialized tables, enhanced stream-batch unification, and cloud-native architecture. The course capitalizes on these features while providing a solid foundation in both DataStream and Table API approaches.

## Technical Architecture

### Infrastructure Components

**Local Development Stack:**
- **Apache Flink 2.0**: Latest version with Java 17 support
- **Docker**: Container orchestration for Flink cluster
- **Confluent Cloud**: Managed Kafka and Schema Registry services
- **Schema Registry**: Avro serialization and schema evolution

**Supported Environments:**
- Docker Desktop with 4GB+ memory allocation
- Java 17 (minimum Java 11)
- Gradle 8.0+ with Kotlin DSL
- IDE with Flink plugin support

### Project Structure

```
flink-demo-suite/
├── docker-compose.yml
├── config/
│   ├── flink-conf.yaml
│   └── log4j2.properties
├── lessons/
│   ├── lesson01-datastream-memory/
│   ├── lesson02-kafka-consumption/
│   ├── lesson03-data-processing/
│   ├── lesson04-materialized-views/
│   └── lesson05-table-api-sql/
├── shared/
│   ├── data-generators/
│   ├── utils/
│   └── test-harness/
├── docs/
│   ├── setup-guide.md
│   ├── troubleshooting.md
│   └── confluent-cloud-setup.md
└── README.md
```

## Lesson Plans

### Lesson 1: Data Stream API with In-Memory Data

**Duration:** 30 minutes  
**Complexity:** Beginner  
**Focus:** DataStream API fundamentals

#### Learning Objectives
- Implement basic DataStream transformations using in-memory data sources
- Configure StreamExecutionEnvironment for local development
- Apply map, filter, and window operations to streaming data

#### Technical Prerequisites
- Basic Java programming knowledge
- Understanding of collections and iterators
- Docker installation and basic container concepts

#### Implementation Details

**Phase 1: Environment Setup (5 minutes)**

```java
// StreamExecutionEnvironment configuration
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1); // Single parallelism for learning
env.disableOperatorChaining(); // Enable step-by-step debugging
```

**Phase 2: Core Implementation (20 minutes)**
```java
public class StreamingWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Create in-memory data source
        DataStreamSource<String> textLines = env.fromElements(
            "apache flink streaming",
            "real time data processing",
            "flink datastream api"
        );
        
        // Apply transformations
        DataStream<Tuple2<String, Integer>> wordCounts = textLines
            .flatMap(new Tokenizer())
            .keyBy(value -> value.f0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .sum(1);
        
        wordCounts.print();
        env.execute("In-Memory Word Count");
    }
}
```

**Phase 3: Validation (5 minutes)**
- Test with different data inputs
- Verify windowing behavior
- Observe parallelism effects

#### Sample Code Structure
```java
// Tokenizer implementation
public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
        for (String word : value.toLowerCase().split("\\W+")) {
            if (word.length() > 0) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
```

#### Expected Outcomes
- Successful execution of streaming job
- Understanding of DataStream transformation pipeline
- Basic debugging and monitoring capability

#### Docker Configuration
```yaml
# lesson01-docker-compose.yml
services:
  flink-jobmanager:
    image: flink:2.0.0-scala_2.12-java17
    ports:
      - "8081:8081"
    environment:
      - FLINK_PROPERTIES=jobmanager.rpc.address: flink-jobmanager
    volumes:
      - ./config:/opt/flink/conf
      - ./lesson01:/workspace

  flink-taskmanager:
    image: flink:2.0.0-scala_2.12-java17
    depends_on:
      - flink-jobmanager
    environment:
      - FLINK_PROPERTIES=jobmanager.rpc.address: flink-jobmanager
    volumes:
      - ./config:/opt/flink/conf
      - ./lesson01:/workspace
```

### Lesson 2: Consuming Data from Kafka

**Duration:** 30 minutes  
**Complexity:** Intermediate  
**Focus:** Kafka integration and external data consumption

#### Learning Objectives
- Configure Kafka source connectors with Confluent Cloud
- Implement proper authentication and security for cloud Kafka
- Handle real-time data ingestion with watermarking strategies

#### Technical Prerequisites
- Completion of Lesson 1
- Confluent Cloud account with Kafka cluster
- Basic understanding of message queues

#### Implementation Details

**Phase 1: Confluent Cloud Setup (8 minutes)**
```java
// Kafka configuration for Confluent Cloud
Properties props = new Properties();
props.setProperty("bootstrap.servers", "your-cluster.us-west-2.aws.confluent.cloud:9092");
props.setProperty("security.protocol", "SASL_SSL");
props.setProperty("sasl.mechanism", "PLAIN");
props.setProperty("sasl.jaas.config", 
    "org.apache.kafka.common.security.plain.PlainLoginModule required " +
    "username=\"" + System.getenv("KAFKA_API_KEY") + "\" " +
    "password=\"" + System.getenv("KAFKA_API_SECRET") + "\";");
props.setProperty("group.id", "flink-demo-consumer");
```

**Phase 2: Kafka Source Implementation (15 minutes)**
```java
public class KafkaConsumerExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("your-cluster.us-west-2.aws.confluent.cloud:9092")
            .setTopics("demo-events")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setProperties(getKafkaProps())
            .build();
        
        // Create watermark strategy
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
            .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis());
        
        // Process stream
        DataStream<String> stream = env.fromSource(source, watermarkStrategy, "Kafka Source");
        
        stream
            .map(new EventProcessor())
            .print();
        
        env.execute("Kafka Consumer Demo");
    }
}
```

**Phase 3: Monitoring and Debugging (7 minutes)**
- Monitor Kafka consumer lag
- Validate message consumption rates
- Test with different offset strategies

#### Confluent Cloud Integration Details
```bash
# Environment variables setup
export KAFKA_API_KEY="your-api-key"
export KAFKA_API_SECRET="your-api-secret"
export KAFKA_BOOTSTRAP_SERVERS="your-cluster.us-west-2.aws.confluent.cloud:9092"

# Topic creation
confluent kafka topic create demo-events --partitions 3 --cluster lkc-cluster-id
```

#### Expected Outcomes
- Successful connection to Confluent Cloud Kafka
- Real-time data consumption from external source
- Understanding of watermarking and event time processing

### Lesson 3: Data Processing

**Duration:** 30 minutes  
**Complexity:** Intermediate  
**Focus:** Advanced stream processing patterns

#### Learning Objectives
- Implement stateful processing with keyed streams
- Apply window operations for time-based aggregations
- Handle late-arriving data and out-of-order events

#### Technical Prerequisites
- Completion of Lessons 1-2
- Understanding of event time vs processing time
- Basic knowledge of windowing concepts

#### Implementation Details

**Phase 1: Stateful Processing Setup (10 minutes)**
```java
public class OrderProcessingJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure checkpointing for fault tolerance
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        
        // Order event stream
        DataStream<OrderEvent> orderStream = env
            .fromSource(getKafkaSource(), getWatermarkStrategy(), "Orders")
            .map(new OrderEventDeserializer());
        
        // Process orders by customer
        DataStream<CustomerOrderSummary> customerSummaries = orderStream
            .keyBy(OrderEvent::getCustomerId)
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .aggregate(new OrderAggregator(), new SummaryWindowFunction());
        
        customerSummaries.print();
        env.execute("Order Processing");
    }
}
```

**Phase 2: Window Operations and Aggregations (15 minutes)**
```java
// Custom aggregation function
public static class OrderAggregator implements AggregateFunction<OrderEvent, OrderAccumulator, OrderSummary> {
    @Override
    public OrderAccumulator createAccumulator() {
        return new OrderAccumulator();
    }
    
    @Override
    public OrderAccumulator add(OrderEvent order, OrderAccumulator accumulator) {
        accumulator.totalAmount += order.getAmount();
        accumulator.orderCount++;
        return accumulator;
    }
    
    @Override
    public OrderSummary getResult(OrderAccumulator accumulator) {
        return new OrderSummary(accumulator.orderCount, accumulator.totalAmount);
    }
    
    @Override
    public OrderAccumulator merge(OrderAccumulator a, OrderAccumulator b) {
        return new OrderAccumulator(a.orderCount + b.orderCount, a.totalAmount + b.totalAmount);
    }
}
```

**Phase 3: Late Data Handling (5 minutes)**
```java
// Configure allowed lateness
DataStream<CustomerOrderSummary> results = orderStream
    .keyBy(OrderEvent::getCustomerId)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .allowedLateness(Time.minutes(2))
    .sideOutputLateData(lateDataTag)
    .aggregate(new OrderAggregator());

// Handle late data
DataStream<OrderEvent> lateOrders = results.getSideOutput(lateDataTag);
lateOrders.print("Late Data");
```

#### Expected Outcomes
- Functioning stateful stream processing application
- Understanding of windowing and aggregation patterns
- Capability to handle late-arriving data

### Lesson 4: Creating Materialized Views

**Duration:** 30 minutes  
**Complexity:** Advanced  
**Focus:** Materialized tables and unified stream-batch processing

#### Learning Objectives
- Implement materialized tables using Flink 2.0 features
- Configure automatic refresh strategies
- Integrate with persistent storage systems

#### Technical Prerequisites
- Completion of Lessons 1-3
- Understanding of SQL and table concepts
- Basic knowledge of database operations

#### Implementation Details

**Phase 1: Confluent Cloud Materialized Table Creation (10 minutes)**
```sql
-- In Confluent Cloud, Kafka topics automatically appear as queryable tables
-- First, verify your topic exists and is accessible
SHOW TABLES;

-- Since Confluent Cloud automatically maps Kafka topics to Flink tables,
-- you can directly query your order_events topic (if it exists)
DESCRIBE order_events;

-- Create a materialized view using Confluent Cloud Flink SQL
-- Note: Confluent Cloud handles the infrastructure automatically
CREATE TABLE customer_metrics (
    customer_id STRING,
    total_orders BIGINT,
    total_amount DECIMAL(10,2),
    avg_order_amount DECIMAL(10,2),
    last_order_time TIMESTAMP(3),
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'changelog.mode' = 'upsert'
);

-- Populate the materialized view with continuous query
INSERT INTO customer_metrics
SELECT 
    customer_id,
    COUNT(*) as total_orders,
    SUM(amount) as total_amount,
    AVG(amount) as avg_order_amount,
    MAX(order_time) as last_order_time
FROM order_events
GROUP BY customer_id;

-- Query the materialized view
SELECT * FROM customer_metrics WHERE total_amount > 1000;
```

**Phase 2: Confluent Cloud Table Environment Setup (15 minutes)**
```java
public class ConfluentCloudMaterializedViewExample {
    public static void main(String[] args) throws Exception {
        // For local Flink connecting to Confluent Cloud
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        // Configure Confluent Cloud connection
        tableEnv.executeSql("""
            CREATE CATALOG confluent_cloud WITH (
                'type' = 'confluent-cloud',
                'bootstrap.servers' = '%s',
                'properties.security.protocol' = 'SASL_SSL',
                'properties.sasl.mechanism' = 'PLAIN',
                'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";'
            )
            """.formatted(
                System.getenv("KAFKA_BOOTSTRAP_SERVERS"),
                System.getenv("KAFKA_API_KEY"),
                System.getenv("KAFKA_API_SECRET")
            ));
        
        // Use Confluent Cloud catalog
        tableEnv.executeSql("USE CATALOG confluent_cloud");
        
        // Show available databases (Kafka clusters) and tables (topics)
        tableEnv.executeSql("SHOW DATABASES").print();
        tableEnv.executeSql("USE DATABASE lkc_cluster_id"); // Your cluster ID
        tableEnv.executeSql("SHOW TABLES").print();
        
        // Query materialized view - leveraging automatic topic-to-table mapping
        Table results = tableEnv.sqlQuery(
            "SELECT customer_id, total_orders, total_amount " +
            "FROM customer_metrics " +
            "WHERE total_amount > 1000"
        );
        
        // Convert to DataStream for further processing
        DataStream<Row> resultStream = tableEnv.toDataStream(results);
        resultStream.print();
        
        env.execute("Confluent Cloud Materialized View Demo");
    }
}
```

**Phase 3: Cross-Environment Queries (5 minutes)**
```java
// Demonstrate Confluent Cloud's cross-environment query capability
Table crossEnvResults = tableEnv.sqlQuery(
    "SELECT o.customer_id, o.total_amount, p.product_name " +
    "FROM `production-env`.`main-cluster`.customer_metrics o " +
    "JOIN `catalog-env`.`product-cluster`.products p " +
    "ON o.customer_id = p.customer_id " +
    "WHERE o.total_amount > 1000"
);

// This showcases Confluent Cloud's unique ability to query across 
// environments and clusters within the same region
crossEnvResults.execute().print();
```

#### Expected Outcomes
- Functioning materialized view with automatic data sync between Kafka topics and Flink tables
- Understanding of Confluent Cloud's unified metadata management
- Capability to perform cross-environment queries within the same region
- Experience with Confluent Cloud's serverless, auto-scaling Flink service

### Lesson 5: Table API and SQL Approach

**Duration:** 30 minutes  
**Complexity:** Intermediate  
**Focus:** Comparative implementation using Table API/SQL

#### Learning Objectives
- Recreate previous lesson functionality using Table API and SQL
- Compare DataStream vs Table API approaches
- Implement complex analytical queries using SQL

#### Technical Prerequisites
- Completion of Lessons 1-4
- SQL query writing experience
- Understanding of relational data concepts

#### Implementation Details

**Phase 1: Table API Implementation (12 minutes)**
```java
public class TableAPIExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        // Create source table
        tableEnv.executeSql(getSourceTableDDL());
        
        // Table API transformations
        Table sourceTable = tableEnv.from("order_events");
        Table processedTable = sourceTable
            .select($("customer_id"), $("amount"), $("order_time"))
            .filter($("amount").isGreater(100))
            .groupBy($("customer_id"))
            .select($("customer_id"), 
                   $("amount").sum().as("total_amount"),
                   $("amount").count().as("order_count"));
        
        // Execute query
        TableResult result = processedTable.execute();
        result.print();
    }
}
```

**Phase 2: Confluent Cloud Flink SQL Implementation (13 minutes)**
```sql
-- Confluent Cloud automatically maps Kafka topics to Flink tables
-- Show available catalogs (environments), databases (clusters), and tables (topics)
SHOW CATALOGS;
USE CATALOG `my-environment`;
SHOW DATABASES;
USE DATABASE `lkc-cluster-id`;
SHOW TABLES;

-- Confluent Cloud Flink SQL with time window functions
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_order_amount,
    -- Use Confluent Cloud's built-in window functions
    window_start,
    window_end
FROM TABLE(
    TUMBLE(TABLE order_events, DESCRIPTOR($rowtime), INTERVAL '5' MINUTE)
)
WHERE amount > 50
GROUP BY customer_id, window_start, window_end
HAVING COUNT(*) > 2
ORDER BY total_amount DESC;

-- Advanced analytics with Confluent Cloud features
SELECT 
    customer_id,
    total_amount,
    -- Ranking within partition
    ROW_NUMBER() OVER (PARTITION BY DATE_FORMAT($rowtime, 'yyyy-MM-dd') ORDER BY total_amount DESC) as daily_rank,
    -- Percentile calculations
    PERCENT_RANK() OVER (ORDER BY total_amount) as amount_percentile,
    -- Cross-environment join (unique to Confluent Cloud)
    p.product_category
FROM customer_metrics c
JOIN `catalog-env`.`product-cluster`.products p 
    ON c.customer_id = p.customer_id
WHERE c.total_amount > 1000;
```

**Phase 3: Performance Comparison (5 minutes)**
```java
// Performance optimization settings
tableEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
tableEnv.getConfig().set("table.exec.mini-batch.allow-latency", "5s");
tableEnv.getConfig().set("table.exec.mini-batch.size", "1000");
tableEnv.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
```

#### Expected Outcomes
- Equivalent functionality using Table API/SQL with Confluent Cloud integration
- Understanding of when to use DataStream API vs Table API/SQL approaches
- Experience with Confluent Cloud's cross-environment query capabilities
- Performance optimization awareness for cloud-native Flink deployments

## Docker Configuration Requirements

### Base Configuration
```yaml
# docker-compose.yml
services:
  jobmanager:
    image: flink:2.0.0-scala_2.12-java17
    ports:
      - "8081:8081"
      - "6123:6123"
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        rest.address: jobmanager
        rest.port: 8081
        rest.flamegraph.enabled: true
        taskmanager.numberOfTaskSlots: 4
        parallelism.default: 1
        execution.checkpointing.interval: 60s
        state.backend: rocksdb
        state.backend.incremental: true
    volumes:
      - ./config:/opt/flink/conf
      - ./lib:/opt/flink/lib
      - ./data:/data
      - ./checkpoints:/checkpoints
    networks:
      - flink-demo

  taskmanager:
    image: flink:2.0.0-scala_2.12-java17
    depends_on:
      - jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 4
        taskmanager.memory.process.size: 2g
        taskmanager.memory.managed.fraction: 0.1
    volumes:
      - ./config:/opt/flink/conf
      - ./lib:/opt/flink/lib
      - ./data:/data
      - ./checkpoints:/checkpoints
    networks:
      - flink-demo
    scale: 2

networks:
  flink-demo:
    driver: bridge
```

### Memory Configuration
```yaml
# Memory settings for development
taskmanager:
  memory:
    process:
      size: 2g
    managed:
      fraction: 0.1
  numberOfTaskSlots: 4

jobmanager:
  memory:
    process:
      size: 1g
    heap:
      size: 768m
```

## Confluent Cloud Integration

### Confluent Cloud Flink SQL Integration

Since the demo suite uses Confluent Cloud for Kafka and Schema Registry, we'll leverage Confluent Cloud's unique Flink SQL capabilities that provide seamless integration between Kafka topics and Flink tables.

#### Key Confluent Cloud Flink SQL Features
- **Automatic Topic-to-Table Mapping**: Kafka topics automatically appear as queryable Flink tables
- **Cross-Environment Queries**: Query data across different environments within the same region
- **Unified Metadata Management**: No need to manually create table definitions for existing topics
- **Serverless Execution**: Auto-scaling compute pools eliminate infrastructure management

#### Environment Variables Setup
```bash
# Confluent Cloud Flink SQL connection
export CONFLUENT_CLOUD_ENVIRONMENT_ID="env-12345"
export CONFLUENT_CLOUD_CLUSTER_ID="lkc-cluster-id"  
export FLINK_COMPUTE_POOL_ID="lfcp-pool-id"
export KAFKA_BOOTSTRAP_SERVERS="your-cluster.us-west-2.aws.confluent.cloud:9092"
export KAFKA_API_KEY="your-kafka-api-key"
export KAFKA_API_SECRET="your-kafka-api-secret"
```

#### Topic Creation with Schema Registry
```bash
# Create topics with Avro schemas for the demo
confluent kafka topic create demo-events --partitions 3 --cluster ${CONFLUENT_CLOUD_CLUSTER_ID}
confluent kafka topic create order-events --partitions 6 --cluster ${CONFLUENT_CLOUD_CLUSTER_ID}
confluent kafka topic create customer-metrics --partitions 3 --cluster ${CONFLUENT_CLOUD_CLUSTER_ID}

# Register Avro schemas
confluent schema-registry schema create --subject order-events-value --schema order-event-schema.avsc
```

### Gradle Build and Execution Commands

```bash
# Build the project
./gradlew build

# Run specific lesson
./gradlew :lesson01-datastream-memory:run

# Create fat JAR for Flink submission
./gradlew shadowJar

# Submit to Flink cluster
docker exec flink-jobmanager flink run /workspace/build/libs/flink-demo.jar

# Run tests
./gradlew test

# Clean and rebuild
./gradlew clean build
```

### Confluent Cloud Flink SQL Examples

Throughout the lessons, we'll use Confluent Cloud's specific Flink SQL dialect that provides enhanced features:

#### Bounded vs Unbounded Processing
```sql
-- Bounded query (snapshot) - processes finite data
SELECT customer_id, COUNT(*) as order_count 
FROM order_events /*+ OPTIONS('scan.bounded.mode'='latest-offset') */
GROUP BY customer_id;

-- Unbounded query (streaming) - continuous processing
SELECT customer_id, COUNT(*) as order_count 
FROM order_events
GROUP BY customer_id;
```

#### Time-based Operations
```sql
-- Window operations with event time
SELECT 
    customer_id,
    COUNT(*) as orders_per_window,
    window_start,
    window_end
FROM TABLE(
    TUMBLE(TABLE order_events, DESCRIPTOR($rowtime), INTERVAL '1' HOUR)
)
GROUP BY customer_id, window_start, window_end;
```

#### Cross-Environment Queries
```sql
-- Query across different environments (unique Confluent Cloud feature)
SELECT 
    o.customer_id,
    o.total_amount,
    c.customer_name
FROM `prod-env`.`main-cluster`.customer_metrics o
JOIN `customer-env`.`crm-cluster`.customer_details c
    ON o.customer_id = c.id
WHERE o.total_amount > 1000;
```

## Testing Approaches

### Unit Testing Framework
```kotlin
// Test dependencies in build.gradle.kts
dependencies {
    testImplementation("org.apache.flink:flink-test-utils:$flinkVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    testImplementation("org.testcontainers:kafka:1.19.1")
    testImplementation("org.testcontainers:junit-jupiter:1.19.1")
}

// Example unit test
@Test
public void testOrderProcessor() throws Exception {
    ProcessFunction<OrderEvent, OrderSummary> processor = new OrderProcessor();
    
    OneInputStreamOperatorTestHarness<OrderEvent, OrderSummary> testHarness = 
        ProcessFunctionTestHarnesses.forProcessFunction(processor);
    
    testHarness.open();
    testHarness.processElement(new OrderEvent("customer1", 100.0), 1000);
    
    assertEquals(1, testHarness.getOutput().size());
}
```

### Integration Testing
```java
// Docker-based integration test
@Test
public void testKafkaIntegration() {
    KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));
    kafka.start();
    
    // Test Flink job with real Kafka
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // Test implementation
}
```

## Performance Considerations

### Local Development Optimization
```yaml
# Performance settings
taskmanager:
  memory:
    process:
      size: 2g
    network:
      fraction: 0.1
  numberOfTaskSlots: 4

execution:
  checkpointing:
    interval: 60s
    timeout: 10min
  buffer-timeout: 100ms

pipeline:
  auto-watermark-interval: 100ms
```

### Monitoring Configuration
```yaml
# Metrics configuration
metrics:
  reporters: prom
  reporter:
    prom:
      factory:
        class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
      port: 9249
```

## Common Beginner Pitfalls and Solutions

### Authentication Issues
**Problem:** SASL authentication failures with Confluent Cloud
**Solution:**
```java
// Ensure proper JAAS configuration
props.setProperty("sasl.jaas.config", 
    "org.apache.kafka.common.security.plain.PlainLoginModule required " +
    "username=\"API_KEY\" password=\"API_SECRET\";");
```

### State Management Confusion
**Problem:** Not understanding exactly-once semantics
**Solution:**
```java
// Enable checkpointing for fault tolerance
env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
```

### Memory Configuration Errors
**Problem:** Out of memory errors during execution
**Solution:**
```yaml
# Proper memory allocation
taskmanager:
  memory:
    process:
      size: 2g
    managed:
      fraction: 0.4  # For stateful jobs
```

### Serialization Issues
**Problem:** Slow performance due to Kryo serialization
**Solution:**
```java
// Use POJO or register custom serializers
env.getConfig().registerTypeWithKryoSerializer(MyClass.class, MySerializer.class);

// Or configure in build.gradle.kts for compile-time optimization
tasks.compileJava {
    options.compilerArgs.addAll(listOf(
        "-parameters", // Enable parameter names for better serialization
        "-Xlint:unchecked"
    ))
}
```

## Project Dependencies

## Implementation Timeline

### Phase 1: Infrastructure Setup (Week 1)
- Docker environment configuration
- Confluent Cloud setup and authentication
- Base project structure creation

### Phase 2: Core Lessons Development (Weeks 2-3)
- Lesson 1-3 implementation and testing
- Basic documentation and troubleshooting guides

### Phase 3: Advanced Features (Week 4)
- Lessons 4-5 implementation
- Performance optimization and monitoring

### Phase 4: Testing and Validation (Week 5)
- Comprehensive testing across all lessons
- Documentation finalization
- User acceptance testing

## Success Metrics

### Technical Metrics
- **Completion Rate**: 90% of learners complete all 5 lessons
- **Code Execution Success**: 95% successful job submissions
- **Performance Benchmarks**: Sub-second startup times for demo applications

### Educational Metrics
- **Comprehension Assessment**: 85% pass rate on lesson assessments
- **Practical Application**: 80% successful completion of hands-on exercises
- **Knowledge Retention**: 75% retention rate after 30 days

## Conclusion

This PRD provides a comprehensive foundation for implementing an Apache Flink 2.0 demo suite that effectively teaches stream processing concepts through progressive, hands-on learning. The integration with Confluent Cloud's managed Flink service eliminates infrastructure complexity while showcasing enterprise-grade capabilities like cross-environment queries, automatic topic-to-table mapping, and serverless auto-scaling.

The combination of local Flink development using Docker and Confluent Cloud integration provides learners with both foundational DataStream API knowledge and practical experience with cloud-native stream processing. The course design leverages Flink 2.0's latest features including disaggregated state management and unified stream-batch processing, while maintaining accessibility for beginners through clear learning objectives, comprehensive error handling, and extensive documentation.

Key benefits of this approach:
- **Hybrid Architecture**: Local development with cloud data services
- **Real-world Relevance**: Enterprise patterns using managed services  
- **Progressive Learning**: From basic concepts to advanced SQL analytics
- **Practical Experience**: Both programmatic APIs and declarative SQL approaches
- **Cloud-Native Skills**: Exposure to serverless stream processing concepts