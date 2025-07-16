package com.example.flink.lesson03;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import shared.data.generators.Order;

/**
 * Lesson 3D: Category Spending Analysis Job
 * 
 * This job demonstrates category-based analytics by tracking spending patterns
 * across different product categories using KeyedProcessFunction with complex state.
 * It shows how to analyze business metrics at the category level for inventory
 * and sales insights.
 * 
 * What you'll learn:
 * - Category-based stream processing and analytics
 * - Complex state management with multiple metrics
 * - Business intelligence through real-time aggregation
 * - Statistical calculations (total, count, average, max)
 * - Product category performance monitoring
 * 
 * Business Use Case:
 * Analyze category performance for:
 * - Inventory management and restocking decisions
 * - Product category performance evaluation
 * - Sales trend analysis and forecasting
 * - Marketing budget allocation by category
 * - Pricing strategy optimization
 * - Seasonal demand pattern analysis
 * 
 * Metrics Tracked:
 * - Total revenue per category
 * - Order count per category
 * - Average order value per category
 * - Maximum single order per category
 * 
 * Expected Output:
 * Category Analysis> Category Electronics: total=156.78, orders=1, avg=156.78, max=156.78
 * Category Analysis> Category Books: total=89.99, orders=1, avg=89.99, max=89.99
 * Category Analysis> Category Electronics: total=312.56, orders=2, avg=156.28, max=156.78
 * Category Analysis> Category Clothing: total=245.67, orders=1, avg=245.67, max=245.67
 * Category Analysis> Category Electronics: total=567.34, orders=3, avg=189.11, max=254.78
 * 
 * Try this:
 * 1. Run KafkaOrderProducer to generate continuous test data
 * 2. Observe how different categories perform over time
 * 3. Identify which categories have higher average order values
 * 4. Add alerts for categories with declining performance
 * 5. Implement category ranking based on revenue
 * 6. Add time-based category analysis (hourly/daily trends)
 */
public class CategorySpendingAnalysisJob extends BaseOrderProcessingJob {

    public static void main(String[] args) throws Exception {
        
        printJobHeader("Flink Lesson 3D: Category Spending Analysis", 
                      "Analyzing spending patterns and performance by product category");

        // Step 1: Create execution environment
        StreamExecutionEnvironment env = createExecutionEnvironment();

        // Step 2: Create order stream from Kafka
        DataStream<Order> orderStream = createOrderStream(env, "flink-lesson03d-category-analysis");

        // Step 3: Analyze spending patterns by product category
        DataStream<String> categoryAnalysis = orderStream
            .keyBy(order -> order.category)
            .process(new CategorySpendingAnalyzer());

        // Step 4: Output results
        categoryAnalysis.print("Category Analysis");

        // Step 5: Execute the job
        executeJob(env, "Lesson 3D: Category Spending Analysis");
    }

    /**
     * Function to analyze spending patterns by product category
     * 
     * This KeyedProcessFunction demonstrates:
     * - Complex state management with multiple metrics
     * - Statistical calculations in stream processing
     * - Category-based business analytics
     * - Real-time performance monitoring
     * 
     * Key Features:
     * - Tracks multiple metrics per category simultaneously
     * - Calculates running averages and maximums
     * - Provides comprehensive category insights
     * - Educational business intelligence patterns
     * 
     * State Structure:
     * - Key: category (String)
     * - State: Tuple3<Double, Integer, Double> (totalAmount, orderCount, maxSingleOrder)
     * - Output: String (formatted category analysis message)
     */
    public static class CategorySpendingAnalyzer extends KeyedProcessFunction<String, Order, String> {

        private transient ValueState<Tuple3<Double, Integer, Double>> categoryState;

        public void open(Configuration parameters) throws Exception {
            // Initialize state for tracking category statistics
            // State: (total amount, order count, max single order)
            ValueStateDescriptor<Tuple3<Double, Integer, Double>> descriptor = new ValueStateDescriptor<>(
                "category-stats",
                TypeInformation.of(new TypeHint<Tuple3<Double, Integer, Double>>() {})
            );
            categoryState = getRuntimeContext().getState(descriptor);
            
            System.out.println("CategorySpendingAnalyzer initialized for parallel instance");
        }

        @Override
        public void processElement(Order order, Context ctx, Collector<String> out) throws Exception {
            // Lazy state initialization - only initialize once per function instance
            if (categoryState == null) {
                ValueStateDescriptor<Tuple3<Double, Integer, Double>> descriptor = new ValueStateDescriptor<>(
                    "category-stats",
                    TypeInformation.of(new TypeHint<Tuple3<Double, Integer, Double>>() {})
                );
                categoryState = getRuntimeContext().getState(descriptor);
            }
            
            // Get current category statistics
            Tuple3<Double, Integer, Double> current = categoryState.value();
            if (current == null) {
                // First order for this category
                current = new Tuple3<>(0.0, 0, 0.0);
            }

            // Update statistics
            double newTotal = current.f0 + order.amount;
            int newCount = current.f1 + 1;
            double newMax = Math.max(current.f2, order.amount);

            // Save updated state
            categoryState.update(new Tuple3<>(newTotal, newCount, newMax));

            // Calculate derived metrics
            double average = newTotal / newCount;

            // Create comprehensive category analysis message
            String analysisMessage = String.format("Category %s: total=%.2f, orders=%d, avg=%.2f, max=%.2f", 
                                                  order.category, newTotal, newCount, average, newMax);

            // Educational debugging - show significant category milestones
            if (isSignificantMilestone(current, newTotal, newCount)) {
                System.out.println("🏆 CATEGORY MILESTONE: " + order.category + 
                                 " reached " + getCategoryMilestone(newTotal, newCount));
            }

            out.collect(analysisMessage);
        }

        /**
         * Check if category reached a significant milestone
         */
        private boolean isSignificantMilestone(Tuple3<Double, Integer, Double> previous, double newTotal, int newCount) {
            if (previous.f0 == 0.0) return true; // First order is always significant
            
            // Revenue milestones
            if (crossedThreshold(previous.f0, newTotal, 1000.0) ||
                crossedThreshold(previous.f0, newTotal, 5000.0) ||
                crossedThreshold(previous.f0, newTotal, 10000.0)) {
                return true;
            }
            
            // Order count milestones
            if (crossedThreshold(previous.f1, newCount, 10) ||
                crossedThreshold(previous.f1, newCount, 50) ||
                crossedThreshold(previous.f1, newCount, 100)) {
                return true;
            }
            
            return false;
        }

        /**
         * Check if a value crossed a threshold
         */
        private boolean crossedThreshold(double oldValue, double newValue, double threshold) {
            return oldValue < threshold && newValue >= threshold;
        }

        /**
         * Get milestone description for category
         */
        private String getCategoryMilestone(double total, int count) {
            if (total >= 10000) return "$10K+ revenue milestone!";
            if (total >= 5000) return "$5K+ revenue milestone!";
            if (total >= 1000) return "$1K+ revenue milestone!";
            if (count >= 100) return "100+ orders milestone!";
            if (count >= 50) return "50+ orders milestone!";
            if (count >= 10) return "10+ orders milestone!";
            return "first order milestone!";
        }
    }
}