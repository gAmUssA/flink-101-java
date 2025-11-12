package shared.data.generators;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * Order Data Generator for Educational Flink Lessons
 * <p>
 * This utility generates realistic e-commerce order data for stream processing demonstrations.
 * Designed for educational clarity - creates orders with realistic patterns and timing.
 * <p>
 * Updated to use Flink 2.0 FLIP-27 Source API (DataGeneratorSource) instead of deprecated SourceFunction.
 * <p>
 * Usage Examples:
 * - Use in Kafka producers to generate test data
 * - Experiment with different order patterns and frequencies
 * - Test various customer and product scenarios
 */
public class OrderDataGenerator {
    
    private static final String[] CUSTOMERS = {"customer_001", "customer_002", "customer_003", "customer_004", "customer_005"};
    private static final String[] CATEGORIES = {"Electronics", "Clothing", "Books", "Home", "Sports"};

    /**
     * Creates a DataGeneratorSource that generates Order objects.
     * 
     * @param numberOfOrders Maximum number of orders to generate (use Long.MAX_VALUE for unbounded)
     * @return A Source that generates Order objects
     */
    public static Source<Order, ?, ?> createSource(long numberOfOrders) {
        GeneratorFunction<Long, Order> generatorFunction = new OrderGeneratorFunction();
        
        return new DataGeneratorSource<>(
            generatorFunction,
            numberOfOrders,
            TypeInformation.of(Order.class)
        );
    }

    /**
     * Creates an unbounded source that generates orders continuously.
     * 
     * @return A Source that generates orders continuously
     */
    public static Source<Order, ?, ?> createUnboundedSource() {
        return createSource(Long.MAX_VALUE);
    }

    /**
     * Creates a bounded source with a specific number of orders.
     * 
     * @param numberOfOrders Number of orders to generate
     * @return A Source that generates the specified number of orders
     */
    public static Source<Order, ?, ?> createBoundedSource(long numberOfOrders) {
        return createSource(numberOfOrders);
    }

    /**
     * GeneratorFunction implementation that creates Order objects.
     * Thread-safe as each parallel instance gets its own Random instance.
     */
    private static class OrderGeneratorFunction implements GeneratorFunction<Long, Order> {
        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(OrderGeneratorFunction.class);
        
        // Thread-local Random for thread safety in parallel execution
        private transient Random random;

        @Override
        public Order map(Long index) throws Exception {
            if (random == null) {
                random = new Random();
            }
            
            // Generate random order based on index
            String orderId = "order_" + String.format("%05d", index);
            String customerId = CUSTOMERS[random.nextInt(CUSTOMERS.length)];
            double amount = 10.0 + (random.nextDouble() * 490.0); // $10-$500
            String category = CATEGORIES[random.nextInt(CATEGORIES.length)];
            long timestamp = System.currentTimeMillis();
            
            Order order = new Order(orderId, customerId, amount, timestamp, category);
            
            // Educational debugging at debug level
            LOG.debug("Generated Order> {}", order);
            
            return order;
        }
    }
}