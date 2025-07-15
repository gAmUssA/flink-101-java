# Lesson 1: DataStream API with In-Memory Data

## Overview

Welcome to your first Apache Flink lesson! 
This lesson introduces you to the fundamentals of stream processing using Flink's DataStream API with simple, in-memory data sources. 
No external dependencies are required, it is perfect for getting started.

## Learning Objectives

By the end of this lesson, you will understand:

- ✅ How to set up a `StreamExecutionEnvironment`
- ✅ Creating data streams from in-memory collections
- ✅ Applying basic transformations (`flatMap`, `keyBy`, `window`, `sum`)
- ✅ Understanding parallelism and operator chaining
- ✅ Observing stream processing in the Flink Web UI
- ✅ Reading and interpreting streaming job output

## Prerequisites

- Java 17 installed
- Docker Desktop running
- Basic understanding of Java programming
- Familiarity with collections and iterators

## Step-by-Step Execution Guide

### Step 1: Start the Flink Cluster

First, let's start our local Flink cluster using Docker:

```bash
# Navigate to the project root directory
cd /path/to/flink-101-java

# Start the Flink cluster (JobManager + TaskManager)
docker-compose up -d

# Verify the cluster is running
docker-compose ps
```

**Expected Output:**
```
NAME                    COMMAND                  SERVICE             STATUS
flink-jobmanager        "/docker-entrypoint.…"   jobmanager          running
flink-taskmanager       "/docker-entrypoint.…"   taskmanager         running
```

### Step 2: Access the Flink Web UI

Open your browser and navigate to: http://localhost:8081

You should see the Flink Dashboard with:
- **JobManager** status: Running
- **TaskManager** count: 1
- **Available Task Slots**: 2
- **Running Jobs**: 0

### Step 3: Build the Project

```bash
# Build the project and create the JAR file
./gradlew build

# Create the shadow JAR for Flink submission
./gradlew shadowJar
```

**Expected Output:**
```
BUILD SUCCESSFUL in 30s
```

### Step 4: Run the Lesson (Method 1: Direct Gradle Execution)

```bash
# Run Lesson 1 directly using Gradle
./gradlew runLesson01
```

**Expected Output:**
```
=== Flink Lesson 1: Streaming Word Count ===
Starting stream processing with in-memory data...

Word Count Results> (apache,1)
Word Count Results> (flink,2)
Word Count Results> (streaming,1)
Word Count Results> (data,1)
Word Count Results> (processing,1)
Word Count Results> (real,1)
Word Count Results> (time,1)
Word Count Results> (analytics,1)
...

Stream processing completed!
```

### Step 5: Run the Lesson (Method 2: Submit to Flink Cluster)

```bash
# Submit the job to the running Flink cluster
docker exec flink-jobmanager flink run /opt/flink/usrlib/flink-demo.jar
```

**What to observe:**
1. In the Web UI, you'll see a new job appear under "Running Jobs"
2. Click on the job to see the execution graph
3. Notice the individual operators (Source → FlatMap → KeyBy → Window → Sum → Sink)
4. Observe the parallelism settings and data flow

### Step 6: Understanding the Output

The program processes these input sentences:
```
"apache flink streaming data processing"
"real time analytics with apache flink"
"flink datastream api tutorial"
"streaming word count example"
"apache flink lesson one"
```

And produces word counts like:
```
(apache, 3)    // appears in 3 sentences
(flink, 4)     // appears in 4 sentences
(streaming, 2) // appears in 2 sentences
(data, 1)      // appears in 1 sentence
...
```

## Code Structure Explanation

### Main Components

1. **StreamExecutionEnvironment**: The entry point for all Flink programs
2. **DataStreamSource**: Creates a stream from in-memory data
3. **Tokenizer**: Splits text lines into individual words
4. **Transformations**: Chain of operations (flatMap → keyBy → window → sum)
5. **Output**: Prints results to console

### Key Concepts Demonstrated

- **Parallelism**: Set to 1 for educational clarity
- **Operator Chaining**: Disabled to see individual steps in Web UI
- **Windowing**: 5-second tumbling windows for aggregation
- **KeyBy**: Groups data by word for counting
- **Stateful Processing**: Maintains word counts across time windows

## Experimentation Ideas

### 1. Change the Input Data

Modify the `fromElements()` call in `StreamingWordCount.java`:

```java
// Try different datasets
DataStreamSource<String> textLines = env.fromElements(
    "your custom sentence here",
    "another sentence with different words",
    "experiment with various inputs"
);
```

### 2. Use Sample Data Generator

```java
// Import the data generator
import shared.data.generators.SampleDataGenerator;

// Replace fromElements with generated data
List<String> data = SampleDataGenerator.getDataset("ecommerce");
DataStreamSource<String> textLines = env.fromCollection(data);
```

Available datasets:
- `"basic"`: Simple Flink-related sentences
- `"extended"`: More variety with repeated words
- `"ecommerce"`: Business-themed data
- `"iot"`: IoT sensor data simulation
- `"mixed"`: Various edge cases and complexity
- `"random"`: Randomly generated sentences

### 3. Modify Window Size

```java
// Change from 5 seconds to 10 seconds
.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))

// Or try different window types
.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
```

### 4. Experiment with Parallelism

```java
// Try different parallelism settings
env.setParallelism(2);  // or 4, 8, etc.

// Re-enable operator chaining for performance
// env.disableOperatorChaining(); // Comment this out
```

### 5. Add Filtering

Modify the Tokenizer to filter out common words:

```java
// Add this to the Tokenizer class
private static final Set<String> STOP_WORDS = Set.of(
    "the", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with", "by"
);

// In the flatMap method, add filtering
if (word.length() > 0 && !STOP_WORDS.contains(word)) {
    out.collect(new Tuple2<>(word, 1));
}
```

## Troubleshooting

### Common Issues

**1. "Port 8081 already in use"**
```bash
# Check what's using the port
lsof -i :8081

# Stop any existing Flink clusters
docker-compose down
```

**2. "Java version mismatch"**
```bash
# Check your Java version
java -version

# Should be Java 11 or higher, preferably Java 17
```

**3. "Docker not running"**
```bash
# Start Docker Desktop
# On Mac: Open Docker Desktop application
# On Linux: sudo systemctl start docker
```

**4. "Build failed"**
```bash
# Clean and rebuild
./gradlew clean build

# Check for compilation errors in the output
```

**5. "No output visible"**
```bash
# Check Docker logs
docker-compose logs jobmanager
docker-compose logs taskmanager

# Or run with direct output
./gradlew runLesson01
```

### Performance Tips

- **Single Parallelism**: Good for learning, but try higher values for performance
- **Operator Chaining**: Disabled for visibility, enable for better performance
- **Window Size**: Smaller windows = more frequent output, larger windows = better throughput

## Next Steps

Once you've successfully completed this lesson:

1. ✅ Experiment with different datasets and parameters
2. ✅ Observe the job execution in the Flink Web UI
3. ✅ Try modifying the Tokenizer logic
4. ✅ Test with different window sizes and parallelism settings
5. ➡️ **Move to Lesson 2**: Kafka Integration

## Additional Resources

- [Apache Flink DataStream API Documentation](https://flink.apache.org/docs/stable/dev/datastream_api.html)
- [Flink Web UI Guide](https://flink.apache.org/docs/stable/ops/monitoring/web_ui.html)
- [Understanding Flink's Windowing](https://flink.apache.org/docs/stable/dev/stream/operators/windows.html)

## Success Indicators

You've successfully completed this lesson when you can:

- ✅ Start the Flink cluster and access the Web UI
- ✅ Run the word count example and see expected output
- ✅ Understand each step in the processing pipeline
- ✅ Modify the code and observe different results
- ✅ Explain what each transformation does
- ✅ Navigate the Flink Web UI to monitor job execution

**Congratulations!** You've taken your first steps into the world of Apache Flink stream processing! 🎉