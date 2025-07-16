package shared.data.generators;

import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;

import java.util.Random;

/**
 * Order Data Generator for Educational Flink Lessons
 * <p>
 * This utility generates realistic e-commerce order data for stream processing demonstrations.
 * Designed for educational clarity - creates orders with realistic patterns and timing.
 * <p>
 * Usage Examples:
 * - Use in Kafka producers to generate test data
 * - Experiment with different order patterns and frequencies
 * - Test various customer and product scenarios
 */
public class OrderDataGenerator implements SourceFunction<Order> {
    
    private volatile boolean running = true;
    private final Random random = new Random();
    private final String[] customers = {"customer_001", "customer_002", "customer_003", "customer_004", "customer_005"};
    private final String[] categories = {"Electronics", "Clothing", "Books", "Home", "Sports"};

    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
        int orderCounter = 1;
        
        while (running) {
            // Generate random order
            String orderId = "order_" + String.format("%05d", orderCounter++);
            String customerId = customers[random.nextInt(customers.length)];
            double amount = 10.0 + (random.nextDouble() * 490.0); // $10-$500
            String category = categories[random.nextInt(categories.length)];
            long timestamp = System.currentTimeMillis();
            
            Order order = new Order(orderId, customerId, amount, timestamp, category);
            ctx.collect(order);
            
            // Educational debugging
            System.out.println("Generated Order> " + order);
            
            // Wait between orders (2-4 seconds for better observation)
            Thread.sleep(2000 + random.nextInt(2000));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}