#!/bin/bash

# Test script to verify Flink Web UI is enabled for lessons

echo "========================================="
echo "Flink Web UI Test Script"
echo "========================================="
echo ""
echo "This script will:"
echo "1. Build the project"
echo "2. Start Lesson 1 in the background"
echo "3. Wait for Web UI to be available"
echo "4. Test the Web UI endpoint"
echo "5. Clean up"
echo ""

# Build the project
echo "Building project..."
./gradlew build -x test -q
if [ $? -ne 0 ]; then
    echo "❌ Build failed"
    exit 1
fi
echo "✅ Build successful"
echo ""

# Start Lesson 1 in background
echo "Starting Lesson 1..."
./gradlew runLesson01 > /tmp/flink-lesson01.log 2>&1 &
LESSON_PID=$!
echo "Started with PID: $LESSON_PID"
echo ""

# Wait for Web UI to be available (max 30 seconds)
echo "Waiting for Web UI to be available..."
MAX_WAIT=30
COUNTER=0
while [ $COUNTER -lt $MAX_WAIT ]; do
    if curl -s http://localhost:8081/config > /dev/null 2>&1; then
        echo "✅ Web UI is available!"
        break
    fi
    sleep 1
    COUNTER=$((COUNTER + 1))
    echo -n "."
done
echo ""

if [ $COUNTER -eq $MAX_WAIT ]; then
    echo "❌ Web UI did not become available within $MAX_WAIT seconds"
    echo "Check logs at /tmp/flink-lesson01.log"
    kill $LESSON_PID 2>/dev/null
    exit 1
fi

# Test the Web UI
echo ""
echo "Testing Web UI endpoints..."
echo ""

# Test config endpoint
echo "1. Testing /config endpoint..."
if curl -s http://localhost:8081/config | grep -q "flink-version"; then
    echo "   ✅ Config endpoint working"
else
    echo "   ❌ Config endpoint failed"
fi

# Test overview endpoint
echo "2. Testing /overview endpoint..."
if curl -s http://localhost:8081/overview | grep -q "taskmanagers"; then
    echo "   ✅ Overview endpoint working"
else
    echo "   ❌ Overview endpoint failed"
fi

# Test jobs endpoint
echo "3. Testing /jobs endpoint..."
if curl -s http://localhost:8081/jobs | grep -q "jobs"; then
    echo "   ✅ Jobs endpoint working"
else
    echo "   ❌ Jobs endpoint failed"
fi

echo ""
echo "========================================="
echo "Test Complete!"
echo "========================================="
echo ""
echo "Web UI is accessible at: http://localhost:8081"
echo ""
echo "To view the running job:"
echo "  1. Open http://localhost:8081 in your browser"
echo "  2. Click on 'Running Jobs' to see Lesson 1"
echo "  3. Click on the job to see the execution graph"
echo ""
echo "Press Ctrl+C to stop the lesson and close this script"
echo ""

# Keep the script running and show logs
tail -f /tmp/flink-lesson01.log &
TAIL_PID=$!

# Cleanup on exit
cleanup() {
    echo ""
    echo "Cleaning up..."
    kill $LESSON_PID 2>/dev/null
    kill $TAIL_PID 2>/dev/null
    echo "✅ Cleanup complete"
    exit 0
}

trap cleanup INT TERM

# Wait for user to stop
wait $LESSON_PID
