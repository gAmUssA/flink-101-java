package com.example.flink.lesson03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import shared.data.generators.Order;

/**
 * Lesson 3B: VIP Customer Detection Job
 * 
 * This job demonstrates customer classification based on spending patterns using
 * RichMapFunction with managed state. It shows how to categorize customers into
 * different tiers (REGULAR, VIP, PREMIUM) based on their cumulative spending.
 * 
 * What you'll learn:
 * - RichMapFunction vs KeyedProcessFunction for stateful processing
 * - State initialization using the open() lifecycle method
 * - Customer segmentation and classification logic
 * - Business rule implementation in stream processing
 * - Real-time customer tier management
 * 
 * Business Use Case:
 * Classify customers in real-time for:
 * - Personalized marketing campaigns
 * - Dynamic pricing strategies
 * - Customer service prioritization
 * - Loyalty program management
 * - Revenue optimization
 * 
 * Classification Rules:
 * - REGULAR: Total spending < $500
 * - VIP: Total spending $500 - $999
 * - PREMIUM: Total spending ≥ $1000
 * 
 * Expected Output:
 * VIP Status> customer_002: REGULAR (total: 156.78, orders: 1)
 * VIP Status> customer_001: REGULAR (total: 89.99, orders: 1)
 * VIP Status> customer_002: REGULAR (total: 312.56, orders: 2)
 * VIP Status> customer_002: VIP (total: 567.34, orders: 3)
 * VIP Status> customer_001: VIP (total: 623.45, orders: 4)
 * VIP Status> customer_002: PREMIUM (total: 1123.78, orders: 5)
 * 
 * Try this:
 * 1. Run KafkaOrderProducer to generate continuous test data
 * 2. Observe how customers progress through tiers as they spend more
 * 3. Modify the thresholds to see different classification behavior
 * 4. Add new customer tiers (e.g., PLATINUM for $2000+)
 * 5. Implement time-based tier degradation
 * 6. Add alerts when customers reach new tiers
 */
public class VIPCustomerDetectionJob extends BaseOrderProcessingJob {

    public static void main(String[] args) throws Exception {
        
        printJobHeader("Flink Lesson 3B: VIP Customer Detection", 
                      "Real-time customer classification based on spending patterns");

        // Step 1: Create execution environment
        StreamExecutionEnvironment env = createExecutionEnvironment();

        // Step 2: Create order stream from Kafka
        String consumerGroupId = "flink-lesson03b-vip-detection-" + System.currentTimeMillis();
        DataStream<Order> orderStream = createOrderStream(env, consumerGroupId);

        // Step 3: Classify customers based on spending patterns
        DataStream<String> vipStatus = orderStream
            .keyBy(order -> order.customerId)
            .map(new VIPCustomerDetector());

        // Step 4: Output results
        vipStatus.print("VIP Status");

        // Step 5: Execute the job
        executeJob(env, "Lesson 3B: VIP Customer Detection");
    }

    /**
     * Rich function to detect VIP customers based on spending patterns
     * 
     * This RichMapFunction demonstrates:
     * - State initialization using open() lifecycle method
     * - Customer classification business logic
     * - Threshold-based tier management
     * - Educational output formatting
     * 
     * Key Differences from KeyedProcessFunction:
     * - Uses open() method for state initialization (not lazy)
     * - Simpler processing model (map vs processElement)
     * - Good for straightforward transformations with state
     * - Less control over timing and context
     * 
     * State Structure:
     * - Key: customerId (String) - automatically handled by keyBy()
     * - State: Tuple2<Double, Integer> (totalSpending, orderCount)
     * - Output: String (formatted customer status message)
     */
    public static class VIPCustomerDetector extends RichMapFunction<Order, String> {

        // Business rules for customer classification
        private static final double VIP_THRESHOLD = 500.0;
        private static final double PREMIUM_THRESHOLD = 1000.0;

        private transient ValueState<Tuple2<Double, Integer>> customerSpendingState;

        public void open(Configuration parameters) throws Exception {
            // Initialize state for tracking customer spending
            // This is called once per parallel instance when the function starts
            ValueStateDescriptor<Tuple2<Double, Integer>> descriptor = new ValueStateDescriptor<>(
                "customer-spending",
                TypeInformation.of(new TypeHint<Tuple2<Double, Integer>>() {})
            );
            customerSpendingState = getRuntimeContext().getState(descriptor);
            
            System.out.println("VIPCustomerDetector initialized for parallel instance");
        }

        @Override
        public String map(Order order) throws Exception {
            // Lazy state initialization - fallback if open() wasn't called
            if (customerSpendingState == null) {
                ValueStateDescriptor<Tuple2<Double, Integer>> descriptor = new ValueStateDescriptor<>(
                    "customer-spending",
                    TypeInformation.of(new TypeHint<Tuple2<Double, Integer>>() {})
                );
                customerSpendingState = getRuntimeContext().getState(descriptor);
            }
            
            // Get current spending state for this customer
            Tuple2<Double, Integer> current = customerSpendingState.value();
            if (current == null) {
                // First order for this customer
                current = new Tuple2<>(0.0, 0);
            }

            // Update spending totals
            double newTotal = current.f0 + order.amount;
            int newCount = current.f1 + 1;

            // Save updated state
            customerSpendingState.update(new Tuple2<>(newTotal, newCount));

            // Determine VIP status based on business rules
            String status = classifyCustomer(newTotal);
            
            // Create detailed status message
            String statusMessage = String.format("%s: %s (total: %.2f, orders: %d)", 
                                                order.customerId, status, newTotal, newCount);

            // Educational debugging - show tier changes
            if (isNewTier(current.f0, newTotal)) {
                System.out.println("🎉 TIER CHANGE: " + order.customerId + " upgraded to " + status + 
                                 " (previous: " + classifyCustomer(current.f0) + ")");
            }

            return statusMessage;
        }

        /**
         * Classify customer based on total spending
         */
        private String classifyCustomer(double totalSpending) {
            if (totalSpending >= PREMIUM_THRESHOLD) {
                return "PREMIUM";
            } else if (totalSpending >= VIP_THRESHOLD) {
                return "VIP";
            } else {
                return "REGULAR";
            }
        }

        /**
         * Check if customer moved to a new tier
         */
        private boolean isNewTier(double oldTotal, double newTotal) {
            return !classifyCustomer(oldTotal).equals(classifyCustomer(newTotal));
        }
    }
}