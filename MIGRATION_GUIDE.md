# Migration Guide: Flink 2.0 Source API

## Overview

This guide explains the migration from the deprecated `SourceFunction` API to the new FLIP-27 `Source` API in Apache Flink 2.0.

## What Changed?

### Deprecated API (Flink 1.x)
```java
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;

public class OrderDataGenerator implements SourceFunction<Order> {
    private volatile boolean running = true;
    
    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
        while (running) {
            Order order = generateOrder();
            ctx.collect(order);
            Thread.sleep(2000);
        }
    }
    
    @Override
    public void cancel() {
        running = false;
    }
}

// Usage
DataStream<Order> orders = env.addSource(new OrderDataGenerator());
```

### New API (Flink 2.0)
```java
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

public class OrderDataGenerator {
    public static Source<Order, ?, ?> createSource(long numberOfOrders) {
        GeneratorFunction<Long, Order> generatorFunction = index -> {
            // Generate order based on index
            return new Order(...);
        };
        
        return new DataGeneratorSource<>(
            generatorFunction,
            numberOfOrders,
            TypeInformation.of(Order.class)
        );
    }
}

// Usage
DataStream<Order> orders = env.fromSource(
    OrderDataGenerator.createUnboundedSource(),
    WatermarkStrategy.noWatermarks(),
    "Order Source"
);
```

## Key Differences

### 1. **Static Factory Methods Instead of Instantiation**
- **Old**: `new OrderDataGenerator()`
- **New**: `OrderDataGenerator.createSource(...)` or `OrderDataGenerator.createUnboundedSource()`

### 2. **Using `fromSource()` Instead of `addSource()`**
- **Old**: `env.addSource(sourceFunction)`
- **New**: `env.fromSource(source, watermarkStrategy, sourceName)`

### 3. **Index-Based Generation**
The new `GeneratorFunction` receives a `Long` index parameter, making it easier to generate deterministic data:
```java
GeneratorFunction<Long, Order> generatorFunction = index -> {
    String orderId = "order_" + String.format("%05d", index);
    // ... generate order
};
```

### 4. **Built-in Parallelism Support**
The new API automatically handles parallelism - each parallel instance gets a different range of indices.

### 5. **No Manual Thread Management**
- **Old**: Required `Thread.sleep()` and `volatile boolean running`
- **New**: No manual threading - the framework handles timing and cancellation

## Migration Steps for OrderDataGenerator

### Step 1: Update Imports
```java
// Remove
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;

// Add
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
```

### Step 2: Convert to Static Factory Pattern
```java
public class OrderDataGenerator {
    // Static factory methods
    public static Source<Order, ?, ?> createSource(long numberOfOrders) { ... }
    public static Source<Order, ?, ?> createUnboundedSource() { ... }
    public static Source<Order, ?, ?> createBoundedSource(long numberOfOrders) { ... }
}
```

### Step 3: Implement GeneratorFunction
```java
private static class OrderGeneratorFunction implements GeneratorFunction<Long, Order> {
    private static final long serialVersionUID = 1L;
    private transient Random random;

    @Override
    public Order map(Long index) throws Exception {
        if (random == null) {
            random = new Random();
        }
        // Generate order using index
        return new Order(...);
    }
}
```

### Step 4: Update Usage in Your Code
```java
// Old way
DataStream<Order> orders = env.addSource(new OrderDataGenerator());

// New way
DataStream<Order> orders = env.fromSource(
    OrderDataGenerator.createUnboundedSource(),
    WatermarkStrategy.noWatermarks(),
    "Order Generator"
);
```

## Benefits of the New API

1. **Better Performance**: The new API is more efficient and scalable
2. **Unified Batch & Streaming**: Same API works for both bounded and unbounded sources
3. **Built-in Checkpointing**: Better integration with Flink's checkpointing mechanism
4. **Cleaner Code**: No manual thread management or cancellation logic
5. **Type Safety**: Better type inference and compile-time checking

## Available Factory Methods

### `createSource(long numberOfOrders)`
Creates a source that generates a specific number of orders.

### `createUnboundedSource()`
Creates a source that generates orders continuously (Long.MAX_VALUE).

### `createBoundedSource(long numberOfOrders)`
Creates a bounded source with a specific number of orders.

## Example: Complete Migration

### Before (Flink 1.x)
```java
public class OrderProcessingJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStream<Order> orders = env.addSource(new OrderDataGenerator())
            .name("Order Source");
        
        // Process orders...
        
        env.execute("Order Processing");
    }
}
```

### After (Flink 2.0)
```java
public class OrderProcessingJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStream<Order> orders = env.fromSource(
            OrderDataGenerator.createUnboundedSource(),
            WatermarkStrategy.noWatermarks(),
            "Order Source"
        );
        
        // Process orders...
        
        env.execute("Order Processing");
    }
}
```

## Troubleshooting

### Issue: "Cannot find symbol: SourceFunction"
**Solution**: You're using the old API. Update imports and use the new `Source` API.

### Issue: "addSource is deprecated"
**Solution**: Replace `env.addSource()` with `env.fromSource()` and provide a `WatermarkStrategy`.

### Issue: "DataGeneratorSource not found"
**Solution**: Ensure you have the correct Flink dependencies in your `build.gradle.kts`:
```kotlin
implementation("org.apache.flink:flink-connector-datagen:$flinkVersion")
```

## Additional Resources

- [FLIP-27: Refactor Source Interface](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface)
- [Flink 2.0 DataStream API Documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/sources/)
- [DataGeneratorSource JavaDoc](https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/connector/datagen/source/DataGeneratorSource.html)
