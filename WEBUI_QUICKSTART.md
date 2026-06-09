# Flink Web UI - Quick Start

## 🚀 Get Started in 3 Steps

### 1. Run Any Lesson
```bash
./gradlew runLesson01
```

### 2. Open Your Browser
Navigate to: **http://localhost:8081**

### 3. Explore!
- Click "Running Jobs" to see your pipeline
- View the execution graph
- Monitor metrics in real-time

---

## 📊 What You'll See

### Execution Graph
Visual representation of your data pipeline showing:
- Source operators (where data comes from)
- Transformation operators (map, filter, keyBy, etc.)
- Sink operators (where data goes)

### Real-Time Metrics
- Records processed per second
- Bytes sent/received
- Backpressure indicators
- Task status

### Checkpoints
- Checkpoint history
- State size
- Duration
- Success/failure status

---

## 🎓 Educational Value

### Lesson 1: DataStream API
See how `flatMap`, `keyBy`, and `sum` connect together

### Lesson 2: Kafka Integration
Monitor Kafka source performance and throughput

### Lesson 3: Order Processing
Compare execution graphs of different processing patterns

### Lessons 4 & 5: Table API/SQL
Understand how SQL translates to DataStream operations

---

## 🛠️ Available Commands

```bash
# DataStream API basics
./gradlew runLesson01

# Kafka integration
./gradlew runLesson02

# Order processing jobs
./gradlew runLesson03A  # Customer tracking
./gradlew runLesson03B  # VIP detection
./gradlew runLesson03C  # Frequency analysis
./gradlew runLesson03D  # Category spending

# Advanced topics
./gradlew runLesson04   # Materialized views
./gradlew runLesson05   # Table API & SQL
```

---

## 🔍 Troubleshooting

### Port Already in Use?
```bash
# Find what's using port 8081
lsof -i :8081

# Kill the process
kill -9 <PID>
```

### Web UI Not Loading?
1. Ensure the lesson is still running (don't stop it!)
2. Try http://127.0.0.1:8081 instead
3. Check firewall settings

### No Jobs Visible?
- Streaming jobs run continuously - keep the process running
- Batch-like jobs (Lesson 1) may complete quickly
- Check the "Completed Jobs" tab

---

## 📚 Learn More

- **Detailed Guide**: [docs/WEB_UI_GUIDE.md](docs/WEB_UI_GUIDE.md)
- **Implementation Details**: [docs/WEBUI_IMPLEMENTATION.md](docs/WEBUI_IMPLEMENTATION.md)
- **Flink Documentation**: https://flink.apache.org/

---

## ✨ Pro Tips

1. **Disable Operator Chaining**: Already done! Each step is visible separately
2. **Parallelism = 1**: Makes it easier to follow data flow
3. **Flame Graphs**: Enabled by default for performance profiling
4. **Multiple Jobs**: Run different lessons to compare execution patterns
5. **Checkpoints**: Watch state grow as your job processes data

---

**Happy Learning! 🎉**

For questions or issues, check the documentation or open an issue on GitHub.
