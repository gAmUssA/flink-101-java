package com.example.flink.lesson03;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import shared.data.generators.Order;

/**
 * Lesson 3A: Customer Order Tracking Job
 * 
 * This job demonstrates basic stateful processing by tracking customer order totals
 * and counts using Flink's managed state. It shows how to maintain running totals
 * across multiple events for the same customer key.
 * 
 * What you'll learn:
 * - Basic stateful processing with KeyedProcessFunction
 * - Using ValueState to maintain customer totals and counts
 * - Lazy state initialization patterns
 * - Keyed streams for customer-based processing
 * - Real-time customer analytics fundamentals
 * 
 * Business Use Case:
 * Track real-time customer spending totals and order counts for:
 * - Customer lifetime value calculation
 * - Order volume monitoring
 * - Customer activity tracking
 * - Basic customer analytics
 * 
 * Expected Output:
 * Customer Totals> (customer_002,156.78,1)
 * Updated customer customer_002: total=156.78, orders=1
 * Customer Totals> (customer_001,89.99,1)  
 * Updated customer customer_001: total=89.99, orders=1
 * Customer Totals> (customer_002,312.56,2)
 * Updated customer customer_002: total=312.56, orders=2
 * 
 * Try this:
 * 1. Run KafkaOrderProducer to generate continuous test data
 * 2. Observe how customer totals accumulate over time
 * 3. Test with multiple customers to see independent state management
 * 4. Modify the output format to include additional metrics
 * 5. Add filtering for specific customer segments
 */
public class CustomerOrderTrackingJob extends BaseOrderProcessingJob {

    public static void main(String[] args) throws Exception {
        
        printJobHeader("Flink Lesson 3A: Customer Order Tracking", 
                      "Tracking customer order totals and counts with stateful processing");

        // Step 1: Create execution environment
        StreamExecutionEnvironment env = createExecutionEnvironment();

        // Step 2: Create order stream from Kafka
        DataStream<Order> orderStream = createOrderStream(env, "flink-lesson03a-customer-tracking");

        // Step 3: Track customer order totals using stateful processing
        DataStream<Tuple3<String, Double, Integer>> customerTotals = orderStream
            .keyBy(order -> order.customerId)
            .process(new CustomerOrderTracker());

        // Step 4: Output results
        customerTotals.print("Customer Totals");

        // Step 5: Execute the job
        executeJob(env, "Lesson 3A: Customer Order Tracking");
    }

    /**
     * Stateful function to track customer order totals and counts
     * 
     * This KeyedProcessFunction demonstrates:
     * - Lazy state initialization for better performance
     * - State management for running totals and counts
     * - Educational debugging output
     * - Proper state descriptor configuration
     * 
     * State Structure:
     * - Key: customerId (String)
     * - State: Tuple2<Double, Integer> (totalAmount, orderCount)
     * - Output: Tuple3<String, Double, Integer> (customerId, totalAmount, orderCount)
     */
    public static class CustomerOrderTracker extends KeyedProcessFunction<String, Order, Tuple3<String, Double, Integer>> {

        private transient ValueState<Tuple2<Double, Integer>> customerState;

        @Override
        public void processElement(Order order, Context ctx, Collector<Tuple3<String, Double, Integer>> out) throws Exception {
            // Lazy state initialization - only initialize once per function instance
            if (customerState == null) {
                ValueStateDescriptor<Tuple2<Double, Integer>> descriptor = new ValueStateDescriptor<>(
                    "customer-totals",
                    TypeInformation.of(new TypeHint<Tuple2<Double, Integer>>() {})
                );
                customerState = getRuntimeContext().getState(descriptor);
            }

            // Get current state (total amount, order count) for this specific customer key
            Tuple2<Double, Integer> current = customerState.value();
            if (current == null) {
                // This is the first order for this customer
                current = new Tuple2<>(0.0, 0);
            }

            // Update state with new order
            double newTotal = current.f0 + order.amount;
            int newCount = current.f1 + 1;

            // Save updated state
            customerState.update(new Tuple2<>(newTotal, newCount));

            // Emit updated totals
            out.collect(new Tuple3<>(order.customerId, newTotal, newCount));

            // Educational debugging - show what's happening
            System.out.println("Updated customer " + order.customerId + 
                             ": total=" + String.format("%.2f", newTotal) + ", orders=" + newCount);
        }
    }
}