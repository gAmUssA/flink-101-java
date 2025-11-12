package com.example.flink.lesson05;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import shared.data.generators.Order;
import utils.FlinkEnvironmentConfig;

import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;
import static org.apache.flink.table.api.Expressions.$;
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

//        Table ordersTable = tableEnv.fromDataStream(
//                orderStream,
//                $("orderId"),
//                $("customerId"),
//                $("amount"),
//                $("timestamp").as("order_timestamp"),
//                $("category")
//        );

    // Register the table for SQL queries
    tableEnv.createTemporaryView("orders", orderStream);
    System.out.println("✓ Converted DataStream to Table 'orders'\n");

    // Step 5: Customer Spending Summary (from customer-spending-summary.flink.sql)
    //runCustomerSpendingSummary(tableEnv);

    // Step 6: Customer Segmentation (from customer-segmentation.flink.sql)
    runCustomerSegmentation(tableEnv);

    // Step 7: Category Performance (from category-performance.flink.sql)
    //runCategoryPerformance(tableEnv);

    // Step 8: Hourly Sales Metrics (from hourly-sales-metrics.flink.sql)
    //runHourlySalesMetrics(tableEnv);

    System.out.println("\n=== Lesson Complete ===");
    System.out.println("All Table API queries executed successfully!");
    System.out.println("Check the Web UI at http://localhost:8081 to see the execution graph\n");

    // Execute the job
    env.execute("Lesson 5: Table API and SQL");
  }

  /**
   * Customer Spending Summary
   * Implements: customer-spending-summary.flink.sql
   * <p>
   * Aggregates order data to provide insights into:
   * - Total spending per customer
   * - Order frequency and patterns
   * - Average order values
   * - Purchase diversity across categories
   */
  private static void runCustomerSpendingSummary(StreamTableEnvironment tableEnv) {
    System.out.println("=== Step 4: Customer Spending Summary ===");

//        String query = """
//            SELECT 
//                customerId,
//                SUM(amount) as total_spent,
//                COUNT(*) as order_count,
//                AVG(amount) as avg_order_value,
//                MAX(amount) as max_order_value,
//                COUNT(DISTINCT category) as categories_purchased
//            FROM orders
//            GROUP BY customerId
//            """;
    // System.out.println("SQL Query:\n" + query);
    //Table resultTable = tableEnv.sqlQuery(query);
    final Table orders = tableEnv.from("orders");
    final Table resultTable = orders.
        groupBy($("customerId")).
        select(
            $("customerId"),
            $("amount").sum().as("total_spent"),
            $("customerId").count().as("order_count"),
            $("amount").avg().as("avg_order_value"),
            $("amount").max().as("max_order_value"),
            $("category").count().distinct().as("categories_purchased")
        );

    //System.out.println(resultTable.explain(ExplainDetail.JSON_EXECUTION_PLAN));

    // Convert to DataStream and print results
    DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
    resultStream.print("Customer Spending");

    System.out.println("✓ Customer spending summary query registered\n");
  }

  /**
   * Customer Segmentation
   * Implements: customer-segmentation.flink.sql
   *
   * Creates customer classifications based on spending patterns:
   * - VIP: $1000+ spent AND 10+ orders
   * - Premium: $500+ spent OR 5+ orders
   * - Regular: $100+ spent OR 2+ orders
   * - New: Everyone else
   *
   * Also classifies purchase behavior:
   * - Diverse: 4+ categories
   * - Multi-Category: 2-3 categories
   * - Single-Category: 1 category
   */
  private static void runCustomerSegmentation(StreamTableEnvironment tableEnv) {
    System.out.println("=== Step 5: Customer Segmentation ===");

    String query = """
        SELECT 
            customerId,
            SUM(amount) as total_spent,
            COUNT(*) as order_count,
            AVG(amount) as avg_order_value,
            COUNT(DISTINCT category) as categories_purchased,
            CASE 
                WHEN SUM(amount) >= 1000 AND COUNT(*) >= 10 THEN 'VIP'
                WHEN SUM(amount) >= 500 OR COUNT(*) >= 5 THEN 'Premium'
                WHEN SUM(amount) >= 100 OR COUNT(*) >= 2 THEN 'Regular'
                ELSE 'New'
            END as customer_segment,
            CASE
                WHEN COUNT(DISTINCT category) >= 4 THEN 'Diverse'
                WHEN COUNT(DISTINCT category) >= 2 THEN 'Multi-Category'
                ELSE 'Single-Category'
            END as purchase_behavior
        FROM orders
        GROUP BY customerId
        """;

    System.out.println("SQL Query:");
    System.out.println(query);

    Table resultTable = tableEnv.sqlQuery(query);

    // Convert to DataStream and print results
    DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
    resultStream.print("Customer Segmentation");

    System.out.println("✓ Customer segmentation query registered\n");
  }

  /**
   * Category Performance Analysis
   * Implements: category-performance.flink.sql
   *
   * Provides real-time analytics on product category performance:
   * - Order volume and revenue by category
   * - Average order values per category
   * - Time-based filtering for recent performance (last 24 hours)
   *
   * Note: For educational purposes, we're showing all data instead of filtering by time
   */
  private static void runCategoryPerformance(StreamTableEnvironment tableEnv) {
    System.out.println("=== Step 6: Category Performance Analysis ===");

    // Simplified version without time filtering for educational clarity
    String query = """
        SELECT 
            category,
            COUNT(*) as order_count,
            SUM(amount) as total_revenue,
            AVG(amount) as avg_order_value
        FROM orders
        GROUP BY category
        """;

    System.out.println("SQL Query:");
    System.out.println(query);
    System.out.println("Note: Original query filters by last 24 hours using:");
    System.out.println("WHERE TO_TIMESTAMP_LTZ(`timestamp`, 3) >= CURRENT_TIMESTAMP - INTERVAL '1' DAY\n");

    Table resultTable = tableEnv.sqlQuery(query);

    // Convert to DataStream and print results
    DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
    resultStream.print("Category Performance");

    System.out.println("✓ Category performance query registered\n");
  }

  /**
   * Hourly Sales Metrics with Tumbling Windows
   * Implements: hourly-sales-metrics.flink.sql
   *
   * Creates time-windowed analytics for sales performance tracking:
   * - Total orders and revenue per hour
   * - Average order values
   * - Customer and category diversity metrics
   * - Min/max order amounts for outlier detection
   *
   * Uses TUMBLE window function for non-overlapping 1-hour windows
   *
   * Note: This requires event-time processing with watermarks.
   * For educational purposes, we're using a simplified version with processing time.
   */
  private static void runHourlySalesMetrics(StreamTableEnvironment tableEnv) {
    System.out.println("=== Step 7: Hourly Sales Metrics (Tumbling Windows) ===");

    // First, we need to create a table with proper time attributes
    // For simplicity in this educational example, we'll use processing time
    String createTableQuery = """
        CREATE TEMPORARY VIEW orders_with_time AS
        SELECT 
            orderId,
            customerId,
            amount,
            category,
            TO_TIMESTAMP_LTZ(order_timestamp, 3) as event_time,
            PROCTIME() as proc_time
        FROM orders
        """;

    tableEnv.executeSql(createTableQuery);
    System.out.println("✓ Created view with time attributes\n");

    // Now run the tumbling window query
    // Using processing time for simplicity (in production, use event time with watermarks)
    String query = """
        SELECT 
            TUMBLE_START(proc_time, INTERVAL '1' HOUR) as hour_start,
            TUMBLE_END(proc_time, INTERVAL '1' HOUR) as hour_end,
            COUNT(*) as total_orders,
            SUM(amount) as total_revenue,
            AVG(amount) as avg_order_value,
            COUNT(DISTINCT customerId) as unique_customers,
            COUNT(DISTINCT category) as categories_sold,
            MAX(amount) as max_order_amount,
            MIN(amount) as min_order_amount
        FROM orders_with_time
        GROUP BY TUMBLE(proc_time, INTERVAL '1' HOUR)
        """;

    System.out.println("SQL Query:");
    System.out.println(query);
    System.out.println("Note: Using processing time for educational clarity.");
    System.out.println("In production, use event time with watermarks:\n");
    System.out.println("TUMBLE(event_time, INTERVAL '1' HOUR) with WATERMARK definition\n");

    Table resultTable = tableEnv.sqlQuery(query);

    // Convert to DataStream and print results
    DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
    resultStream.print("Hourly Sales");

    System.out.println("✓ Hourly sales metrics query registered\n");
  }

}