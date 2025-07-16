package com.example.flink.lesson03;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import shared.data.generators.Order;

/**
 * Lesson 3C: Order Frequency Analysis Job
 * 
 * This job demonstrates behavioral analysis by tracking customer ordering frequency
 * and patterns using KeyedProcessFunction with managed state. It shows how to analyze
 * customer behavior and classify customers based on their ordering habits.
 * 
 * What you'll learn:
 * - Behavioral analysis with stream processing
 * - Customer segmentation based on ordering patterns
 * - State management for frequency tracking
 * - Business logic for customer behavior classification
 * - Real-time customer insights generation
 * 
 * Business Use Case:
 * Analyze customer ordering behavior for:
 * - Customer retention strategies
 * - Personalized marketing campaigns
 * - Inventory planning and demand forecasting
 * - Customer lifecycle management
 * - Churn prediction and prevention
 * 
 * Frequency Classifications:
 * - NEW CUSTOMER: First order (1 order)
 * - OCCASIONAL: 2-5 orders
 * - REGULAR: 6-15 orders
 * - FREQUENT: 16+ orders
 * 
 * Expected Output:
 * Order Frequency> customer_002: 1 orders in session (NEW CUSTOMER)
 * Order Frequency> customer_001: 1 orders in session (NEW CUSTOMER)
 * Order Frequency> customer_002: 2 orders in session (OCCASIONAL)
 * Order Frequency> customer_001: 2 orders in session (OCCASIONAL)
 * Order Frequency> customer_002: 3 orders in session (OCCASIONAL)
 * Order Frequency> customer_001: 6 orders in session (REGULAR)
 * Order Frequency> customer_002: 16 orders in session (FREQUENT)
 * 
 * Try this:
 * 1. Run KafkaOrderProducer to generate continuous test data
 * 2. Observe how customers progress through frequency categories
 * 3. Modify the frequency thresholds to see different classifications
 * 4. Add time-based frequency analysis (orders per hour/day)
 * 5. Implement alerts for customers becoming frequent buyers
 * 6. Add customer reactivation detection (returning after inactivity)
 */
public class OrderFrequencyAnalysisJob extends BaseOrderProcessingJob {

    public static void main(String[] args) throws Exception {
        
        printJobHeader("Flink Lesson 3C: Order Frequency Analysis", 
                      "Analyzing customer ordering behavior and frequency patterns");

        // Step 1: Create execution environment
        StreamExecutionEnvironment env = createExecutionEnvironment();

        // Step 2: Create order stream from Kafka
        DataStream<Order> orderStream = createOrderStream(env, "flink-lesson03c-frequency-analysis");

        // Step 3: Analyze customer ordering frequency
        DataStream<String> orderFrequency = orderStream
            .keyBy(order -> order.customerId)
            .process(new OrderFrequencyTracker());

        // Step 4: Output results
        orderFrequency.print("Order Frequency");

        // Step 5: Execute the job
        executeJob(env, "Lesson 3C: Order Frequency Analysis");
    }

    /**
     * Function to track order frequency per customer
     * 
     * This KeyedProcessFunction demonstrates:
     * - State initialization using open() lifecycle method
     * - Simple integer state management
     * - Business logic for behavioral classification
     * - Educational frequency categorization
     * 
     * Key Features:
     * - Tracks order count per customer session
     * - Classifies customers based on ordering frequency
     * - Provides insights into customer engagement levels
     * - Simple state structure for easy understanding
     * 
     * State Structure:
     * - Key: customerId (String)
     * - State: Integer (orderCount)
     * - Output: String (formatted frequency message)
     */
    public static class OrderFrequencyTracker extends KeyedProcessFunction<String, Order, String> {

        private transient ValueState<Integer> orderCountState;

        public void open(Configuration parameters) throws Exception {
            // Initialize state for tracking order count per customer
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                "order-count",
                Integer.class
            );
            orderCountState = getRuntimeContext().getState(descriptor);
            
            System.out.println("OrderFrequencyTracker initialized for parallel instance");
        }

        @Override
        public void processElement(Order order, Context ctx, Collector<String> out) throws Exception {
            // Lazy state initialization - only initialize once per function instance
            if (orderCountState == null) {
                ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                    "order-count",
                    Integer.class
                );
                orderCountState = getRuntimeContext().getState(descriptor);
            }
            
            // Get current order count for this customer
            Integer currentCount = orderCountState.value();
            if (currentCount == null) {
                // First order for this customer
                currentCount = 0;
            }

            // Increment count
            int newCount = currentCount + 1;
            orderCountState.update(newCount);

            // Classify customer based on ordering frequency
            String frequency = classifyFrequency(newCount);
            
            // Create detailed frequency message
            String frequencyMessage = String.format("%s: %d orders in session (%s)", 
                                                   order.customerId, newCount, frequency);

            // Educational debugging - show frequency changes
            if (isNewFrequencyCategory(currentCount, newCount)) {
                System.out.println("📈 FREQUENCY CHANGE: " + order.customerId + " is now " + frequency + 
                                 " (order #" + newCount + ")");
            }

            out.collect(frequencyMessage);
        }

        /**
         * Classify customer based on order frequency
         */
        private String classifyFrequency(int orderCount) {
            if (orderCount == 1) {
                return "NEW CUSTOMER";
            } else if (orderCount <= 5) {
                return "OCCASIONAL";
            } else if (orderCount <= 15) {
                return "REGULAR";
            } else {
                return "FREQUENT";
            }
        }

        /**
         * Check if a customer moved to a new frequency category
         */
        private boolean isNewFrequencyCategory(Integer oldCount, int newCount) {
            if (oldCount == null) oldCount = 0;
            return !classifyFrequency(oldCount).equals(classifyFrequency(newCount));
        }
    }
}