# Flink Web UI Guide

## Overview

All Java-based lessons now support the Flink Web UI, allowing you to visualize and monitor your streaming pipelines in real-time. This is an essential tool for understanding how Flink processes data and debugging your applications.

## Accessing the Web UI

When you run any lesson, you'll see instructions like this:

```
╔════════════════════════════════════════════════════════════╗
║           Flink Web UI Access Instructions                ║
╠════════════════════════════════════════════════════════════╣
║  Open your browser and navigate to:                       ║
║  → http://localhost:8081                                   ║
║                                                            ║
║  In the Web UI you can:                                    ║
║  • View the execution graph of your streaming pipeline     ║
║  • Monitor job status and metrics in real-time             ║
║  • Check task manager resources and performance            ║
║  • View checkpoints and savepoints                         ║
║  • Debug issues using logs and flame graphs                ║
╚════════════════════════════════════════════════════════════╝
```

Simply open your browser and go to **http://localhost:8081**

## What You Can See

### 1. Job Overview
- **Running Jobs**: See all currently executing Flink jobs
- **Completed Jobs**: View history of finished jobs
- **Job Status**: Monitor job health and execution state

### 2. Execution Graph
- **Visual Pipeline**: See your data flow as a directed graph
- **Operators**: Each transformation step is visualized
- **Parallelism**: Understand how tasks are distributed
- **Operator Chaining**: See which operators are chained together (disabled in lessons for clarity)

### 3. Metrics & Performance
- **Records Processed**: Track throughput in real-time
- **Backpressure**: Identify bottlenecks in your pipeline
- **Latency**: Monitor processing delays
- **Resource Usage**: CPU, memory, and network metrics

### 4. Checkpoints
- **Checkpoint History**: View all completed checkpoints
- **State Size**: Monitor how much state your job maintains
- **Duration**: Track checkpoint completion times
- **Failures**: Identify checkpoint issues

### 5. Task Managers
- **Available Slots**: See task execution capacity
- **Resource Allocation**: Monitor CPU and memory usage
- **Task Distribution**: Understand how work is distributed

## Educational Benefits

### For Lesson 1 (DataStream API)
- See how `flatMap`, `keyBy`, and `sum` operators are connected
- Understand data flow through the pipeline
- Observe operator chaining (disabled for clarity)

### For Lesson 2 (Kafka Integration)
- Monitor Kafka source connector performance
- Track records consumed from Kafka
- View deserialization metrics

### For Lesson 3 (Data Processing Patterns)
- Compare execution graphs of different jobs
- Understand windowing operations visually
- Monitor state size for stateful operations

### For Lessons 4 & 5 (Table API/SQL)
- See how SQL queries are translated to DataStream operations
- Understand query optimization
- Monitor materialized view updates

## Tips for Using the Web UI

1. **Start Simple**: Begin with Lesson 1 to understand basic pipeline visualization
2. **Operator Chaining**: Notice that chaining is disabled in lessons - this makes each step visible
3. **Parallelism**: Set to 1 for clarity - increase it to see parallel execution
4. **Refresh**: The UI updates automatically, but you can manually refresh for latest data
5. **Flame Graphs**: Use flame graphs to identify performance hotspots (enabled by default)

## Troubleshooting

### Port Already in Use
If port 8081 is already in use:
```bash
# Find the process using port 8081
lsof -i :8081

# Kill the process if needed
kill -9 <PID>
```

### Web UI Not Loading
1. Ensure your lesson is running (don't stop the Java process)
2. Check that port 8081 is accessible
3. Try accessing from `http://127.0.0.1:8081` instead

### No Jobs Visible
- The job only appears while the lesson is running
- For streaming jobs, they run continuously until you stop them (Ctrl+C)
- For batch-like jobs (Lesson 1), they may complete quickly

## Running Lessons with Web UI

All lesson run commands now automatically enable the Web UI:

```bash
# Lesson 1: DataStream API
./gradlew runLesson01

# Lesson 2: Kafka Integration
./gradlew runLesson02

# Lesson 3: Order Processing Jobs
./gradlew runLesson03A  # Customer Order Tracking
./gradlew runLesson03B  # VIP Customer Detection
./gradlew runLesson03C  # Order Frequency Analysis
./gradlew runLesson03D  # Category Spending Analysis

# Lesson 4: Materialized Views
./gradlew runLesson04

# Lesson 5: Table API and SQL
./gradlew runLesson05
```

## Docker Cluster vs Local Execution

### Local Execution with Web UI (Current Setup)
- ✅ Runs directly from your IDE or Gradle
- ✅ Web UI at http://localhost:8081
- ✅ Easy debugging with IDE breakpoints
- ✅ Fast iteration and development
- ✅ No Docker required

### Docker Cluster Execution (Alternative)
If you want to submit jobs to the Docker cluster instead:

```bash
# Start the Flink cluster
docker-compose up -d

# Build the shadow JAR
./gradlew shadowJar

# Submit to cluster
docker exec flink-jobmanager flink run /opt/flink/usrlib/flink-demo.jar
```

The Docker cluster also provides a Web UI at http://localhost:8081

## Next Steps

1. **Run Lesson 1**: Start with the basics
   ```bash
   ./gradlew runLesson01
   ```

2. **Open Web UI**: Navigate to http://localhost:8081

3. **Explore**: Click through the tabs and explore the execution graph

4. **Experiment**: Modify lesson code and see how the graph changes

5. **Monitor**: Watch metrics update in real-time as data flows through

## Additional Resources

- [Apache Flink Web UI Documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/monitoring/web_ui/)
- [Flink Metrics System](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/)
- [Debugging Flink Applications](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/debugging/)
