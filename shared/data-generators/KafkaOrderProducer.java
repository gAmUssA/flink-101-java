package shared.data.generators;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

import shared.utils.KafkaUtils;

/**
 * Kafka Order Producer for Educational Flink Lessons
 *
 * This utility continuously generates realistic e-commerce order data and sends it
 * to a Kafka topic as JSON messages. Designed for educational demonstrations of
 * real-time stream processing with Apache Flink.
 *
 * Usage Examples:
 * - Run continuously to provide data for OrderProcessingJob
 * - Generate test data for Kafka consumer examples
 * - Experiment with different order patterns and frequencies
 * - Test various customer and product scenarios
 *
 * Prerequisites:
 * - Confluent Cloud account with Kafka cluster
 * - API keys configured in .env file
 * - Topic 'orders' created in your Kafka cluster
 * - Environment variables loaded from .env file
 *
 * Expected Output:
 * === Kafka Order Producer ===
 * Connecting to Confluent Cloud Kafka...
 * ✓ Loaded CNFL_KAFKA_BROKER from environment
 * ✓ Loaded CNFL_KC_API_KEY from environment
 * ✓ Loaded CNFL_KC_API_SECRET from environment
 * ✓ Kafka producer configured for Confluent Cloud
 * Starting continuous order generation... Press Ctrl+C to stop.
 * Sent Order> Order{id=order_00001, customer=customer_002, amount=156.78, category=Electronics}
 * Sent Order> Order{id=order_00002, customer=customer_001, amount=89.99, category=Books}
 *
 * Try this:
 * 1. Modify the order generation frequency by changing sleep intervals
 * 2. Add new customers or product categories to the arrays
 * 3. Experiment with different price ranges and patterns
 * 4. Run multiple producers to simulate high-volume scenarios
 * 5. Monitor the Kafka topic in Confluent Cloud Console
 */
public class KafkaOrderProducer {

    private static final String TOPIC_NAME = "orders";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private final KafkaProducer<String, String> producer;
    private final Random random = new Random();
    private final String[] customers = {"customer_001", "customer_002", "customer_003", "customer_004", "customer_005"};
    private final String[] categories = {"Electronics", "Clothing", "Books", "Home", "Sports"};
    
    private volatile boolean running = true;

    public KafkaOrderProducer(String bootstrapServers, String apiKey, String apiSecret) {
        this.producer = new KafkaProducer<>(KafkaUtils.createProducerKafkaProperties(bootstrapServers, apiKey, apiSecret));
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== Kafka Order Producer ===");
        System.out.println("Connecting to Confluent Cloud Kafka...");

        // Load Confluent Cloud configuration from environment variables
        String bootstrapServers = KafkaUtils.getEnvVar("CNFL_KAFKA_BROKER", "your-kafka-broker:9092");
        String apiKey = KafkaUtils.getEnvVar("CNFL_KC_API_KEY", "your-api-key");
        String apiSecret = KafkaUtils.getEnvVar("CNFL_KC_API_SECRET", "your-api-secret");

        System.out.println("Bootstrap servers: " + bootstrapServers);
        System.out.println("Using topic: " + TOPIC_NAME);

        // Create and start the producer
        KafkaOrderProducer orderProducer = new KafkaOrderProducer(bootstrapServers, apiKey, apiSecret);
        
        // Add shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down Kafka Order Producer...");
            orderProducer.stop();
        }));

        System.out.println("Starting continuous order generation... Press Ctrl+C to stop.");
        orderProducer.startProducing();
    }

    /**
     * Starts continuous order generation and sending to Kafka
     */
    public void startProducing() throws Exception {
        int orderCounter = 1;
        
        while (running) {
            try {
                // Generate random order
                String orderId = "order_" + String.format("%05d", orderCounter++);
                String customerId = customers[random.nextInt(customers.length)];
                double amount = 10.0 + (random.nextDouble() * 490.0); // $10-$500
                String category = categories[random.nextInt(categories.length)];
                long timestamp = System.currentTimeMillis();
                
                Order order = new Order(orderId, customerId, amount, timestamp, category);
                
                // Convert order to JSON
                String orderJson = objectMapper.writeValueAsString(order);
                
                // Send to Kafka topic
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    TOPIC_NAME, 
                    order.customerId, // Use customerId as key for partitioning
                    orderJson
                );
                
                Future<RecordMetadata> future = producer.send(record);
                
                // Educational debugging - show what was sent
                System.out.println("Sent Order> " + order);
                
                // Optional: Wait for acknowledgment (educational purposes)
                RecordMetadata metadata = future.get();
                System.out.println("  ✓ Delivered to partition " + metadata.partition() + 
                                 " at offset " + metadata.offset());
                
                // Wait between orders (2-4 seconds for better observation)
                Thread.sleep(2000 + random.nextInt(2000));
                
            } catch (Exception e) {
                System.err.println("Error sending order: " + e.getMessage());
                e.printStackTrace();
                // Continue running even if individual messages fail
            }
        }
    }

    /**
     * Stops the producer gracefully
     */
    public void stop() {
        running = false;
        if (producer != null) {
            producer.flush(); // Ensure all messages are sent
            producer.close();
            System.out.println("✓ Kafka producer closed gracefully");
        }
    }

}