package com.example.flink.lesson05;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import shared.data.generators.Order;
import utils.FlinkEnvironmentConfig;

import static com.example.flink.lesson05.queries.CategoryPerformance.runCategoryPerformance;
import static com.example.flink.lesson05.queries.CustomerSegmentation.runCustomerSegmentation;
import static com.example.flink.lesson05.queries.CustomerSpendingSummary.runCustomerSpendingSummary;
import static com.example.flink.lesson05.queries.HourlySalesMetrics.runHourlySalesMetrics;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;
import static shared.data.generators.OrderDataGenerator.createBoundedSource;

/**
 * Lesson 5: Table API and SQL Implementation
 * <p>
 * This lesson demonstrates Apache Flink's Table API and SQL capabilities, showing how to
 * bridge between DataStream API and SQL for complex analytical queries. We implement
 * the same use cases from Lesson 4 using Table API and SQL instead of DataStream API.
 * <p>
 * What you'll learn:
 * - Converting DataStream to Table and vice versa
 * - Writing SQL queries on streaming data
 * - Using window functions (TUMBLE) for time-based aggregations
 * - Customer segmentation with SQL CASE statements
 * - Category performance analysis
 * - Hourly sales metrics with tumbling windows
 * <p>
 * Key Concepts:
 * <p>
 * 1. TABLE API BENEFITS
 * - Declarative SQL syntax for complex analytics
 * - Automatic query optimization by Flink planner
 * - Seamless conversion between DataStream and Table
 * - Rich set of built-in aggregation functions
 * <p>
 * 2. IMPLEMENTED USE CASES (from lesson04 SQL files)
 * - Customer Spending Summary: Total spending, order count, avg order value
 * - Customer Segmentation: VIP/Premium/Regular/New classification
 * - Category Performance: Revenue and order count by product category
 * - Hourly Sales Metrics: Time-windowed aggregations with TUMBLE
 * <p>
 * 3. SQL WINDOW FUNCTIONS
 * - TUMBLE: Non-overlapping time windows for aggregations
 * - Window start/end timestamps for time-series analysis
 * - GROUP BY with window functions
 * <p>
 * Expected Output:
 * <p>
 * === Customer Spending Summary ===
 * +I[cust_001, 450.50, 3, 150.17, 200.00, 2]
 * +I[cust_002, 890.25, 5, 178.05, 350.00, 3]
 * <p>
 * === Customer Segmentation ===
 * +I[cust_001, 450.50, 3, 150.17, 2, Regular, Multi-Category]
 * +I[cust_002, 890.25, 5, 178.05, 3, Premium, Diverse]
 * <p>
 * === Category Performance ===
 * +I[Electronics, 5, 2450.75, 490.15]
 * +I[Books, 8, 340.50, 42.56]
 * <p>
 * === Hourly Sales Metrics ===
 * +I[2024-11-12 10:00:00, 2024-11-12 11:00:00, 15, 1250.50, 83.37, 8, 3, 250.00, 25.50]
 * <p>
 *
 * Try this:
 * 1. Modify the window size in hourly sales metrics
 * 2. Add new customer segments with different thresholds
 * 3. Filter categories by minimum revenue
 * 4. Experiment with different aggregation functions
 */
public class TableAPIExample {

  public static void main(String[] args) throws Exception {

    System.out.println("=== Flink Lesson 5: Table API and SQL ===");
    System.out.println("Implementing lesson04 use cases with Table API\n");

    // Step 1: Create a streaming execution environment with Web UI enabled
    StreamExecutionEnvironment env = FlinkEnvironmentConfig.createEnvironmentWithUI();

    // Print Web UI access instructions
    FlinkEnvironmentConfig.printWebUIInstructions();

    System.out.println("✓ Streaming environment created with Web UI enabled");
    System.out.println("✓ Checkpointing enabled for fault tolerance\n");

    // Step 2: Create Table Environment
    System.out.println("=== Step 1: Creating Table Environment ===");
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    System.out.println("✓ Table environment created for SQL queries\n");

    // Step 3: Generate sample order data
    // TODO: replace with Kafka consumer
    System.out.println("=== Step 2: Generating Sample Order Data ===");
    DataStream<Order> orderStream = env.fromSource(
        createBoundedSource(1000),
        noWatermarks(),
        "Order Data Generator"
    );
    System.out.println("✓ Generated 100 sample orders\n");

    // Step 4: Convert DataStream to Table
    System.out.println("=== Step 3: Converting DataStream to Table ===");
    
    // Register the table for SQL queries
    tableEnv.createTemporaryView("orders", orderStream);
    System.out.println("✓ Converted DataStream to Table 'orders'\n");

    // Step 5: Customer Spending Summary (from customer-spending-summary.flink.sql)
   // runCustomerSpendingSummary(tableEnv);

    // Step 6: Customer Segmentation (from customer-segmentation.flink.sql)
   runCustomerSegmentation(tableEnv);

    // Step 7: Category Performance (from category-performance.flink.sql)
    //runCategoryPerformance(tableEnv);

    // Step 8: Hourly Sales Metrics (from hourly-sales-metrics.flink.sql)
   // runHourlySalesMetrics(tableEnv);

    System.out.println("\n=== Lesson Complete ===");
    System.out.println("All Table API queries executed successfully!");
    System.out.println("Check the Web UI at http://localhost:8081 to see the execution graph\n");

    // Execute the job
    env.execute("Lesson 5: Table API and SQL");
  }
}