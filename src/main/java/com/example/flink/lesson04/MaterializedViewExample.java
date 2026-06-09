package com.example.flink.lesson04;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import shared.data.generators.Order;
import utils.FlinkEnvironmentConfig;

import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;
import static shared.data.generators.OrderDataGenerator.createBoundedSource;

/**
 * Lesson 4: Materialized Views with Flink SQL
 * <p>
 * This lesson demonstrates materialized views using Apache Flink's Table API and SQL.
 * A materialized view is a query whose results are continuously maintained as new data
 * arrives — exactly what a streaming {@code CREATE VIEW} over an unbounded table gives you.
 * <p>
 * Unlike Lesson 5 (which builds the same use cases with the programmatic Table API),
 * this lesson uses raw SQL DDL ({@code CREATE VIEW ... AS SELECT ...}) so you can see the
 * SQL-first authoring style used by Confluent Cloud Flink SQL.
 * <p>
 * It runs against an in-memory bounded order source so you can execute it locally and watch
 * the job in the Flink Web UI at http://localhost:8081. The same SQL runs unchanged against a
 * Kafka-backed {@code orders} table on Confluent Cloud (see the DDL printed at startup and the
 * {@code .flink.sql} files under {@code src/main/resources/lesson04/}).
 * <p>
 * What you'll learn:
 * - Bridging DataStream and SQL with {@link StreamTableEnvironment}
 * - Authoring materialized views with {@code CREATE VIEW}
 * - Customer spending summaries, segmentation, and category performance
 * - How the same SQL maps to a Kafka topic-to-table definition on Confluent Cloud
 * <p>
 * The materialized views executed below:
 * - customer_spending_summary  (customer-spending-summary.flink.sql)
 * - customer_segmentation      (customer-segmentation.flink.sql)
 * - category_performance       (category-performance.flink.sql)
 * <p>
 * Time-windowed views (hourly-sales-metrics, top-performers-leaderboard) require an event-time
 * attribute and watermark on the source table; see those {@code .flink.sql} files and Lesson 5's
 * {@code HourlySalesMetrics} for the windowed variant.
 * <p>
 * Try this:
 * 1. Add new aggregation metrics to a view (e.g. MIN/MAX amount)
 * 2. Adjust the segmentation thresholds and re-run
 * 3. Point the {@code orders} table at a Kafka topic using the DDL printed at startup
 */
public class MaterializedViewExample {

    public static void main(String[] args) throws Exception {

        System.out.println("=== Flink Lesson 4: Materialized Views with Flink SQL ===");
        System.out.println("Authoring materialized views with CREATE VIEW over a streaming table\n");

        // Step 1: Create a streaming execution environment with the Web UI enabled
        StreamExecutionEnvironment env = FlinkEnvironmentConfig.createEnvironmentWithUI();
        FlinkEnvironmentConfig.printWebUIInstructions();
        System.out.println("✓ Streaming environment created (Web UI on http://localhost:8081)\n");

        // Step 2: Create the Table Environment that bridges DataStream and SQL
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        System.out.println("✓ Table environment created\n");

        // Step 3: Register an in-memory bounded order source as the 'orders' table.
        //         On Confluent Cloud this 'orders' table would instead be a Kafka topic
        //         (see the DDL below); the materialized-view SQL stays identical.
        DataStream<Order> orderStream = env.fromSource(
            createBoundedSource(1000),
            noWatermarks(),
            "Order Data Generator"
        );
        tableEnv.createTemporaryView("orders", orderStream);
        System.out.println("✓ Registered 'orders' table from a bounded order source (1000 orders)\n");

        printKafkaTableDdl();

        // Step 4: Create the materialized views with SQL DDL
        System.out.println("=== Creating materialized views ===");

        tableEnv.executeSql("""
            CREATE VIEW customer_spending_summary AS
            SELECT
                customerId,
                SUM(amount)              AS total_spent,
                COUNT(*)                 AS order_count,
                AVG(amount)              AS avg_order_value,
                MAX(amount)              AS max_order_value,
                COUNT(DISTINCT category) AS categories_purchased
            FROM orders
            GROUP BY customerId
            """);
        System.out.println("✓ customer_spending_summary");

        tableEnv.executeSql("""
            CREATE VIEW customer_segmentation AS
            SELECT
                customerId,
                SUM(amount)              AS total_spent,
                COUNT(*)                 AS order_count,
                AVG(amount)              AS avg_order_value,
                COUNT(DISTINCT category) AS categories_purchased,
                CASE
                    WHEN SUM(amount) >= 1000 AND COUNT(*) >= 10 THEN 'VIP'
                    WHEN SUM(amount) >= 500  OR  COUNT(*) >= 5  THEN 'Premium'
                    WHEN SUM(amount) >= 100  OR  COUNT(*) >= 2  THEN 'Regular'
                    ELSE 'New'
                END AS customer_segment,
                CASE
                    WHEN COUNT(DISTINCT category) >= 4 THEN 'Diverse'
                    WHEN COUNT(DISTINCT category) >= 2 THEN 'Multi-Category'
                    ELSE 'Single-Category'
                END AS purchase_behavior
            FROM orders
            GROUP BY customerId
            """);
        System.out.println("✓ customer_segmentation");

        tableEnv.executeSql("""
            CREATE VIEW category_performance AS
            SELECT
                category,
                COUNT(*)    AS order_count,
                SUM(amount) AS total_revenue,
                AVG(amount) AS avg_order_value
            FROM orders
            GROUP BY category
            """);
        System.out.println("✓ category_performance\n");

        // Step 5: Query the materialized views. Each print() submits a job and runs it
        //         to completion against the bounded source; watch them in the Web UI.
        //         The output is a changelog (+I/-U/+U): you can see each view's aggregates
        //         being continuously updated as orders are processed — the essence of a
        //         materialized view. (Streaming SQL doesn't support ORDER BY on a
        //         non-time attribute, so we print the views unsorted.)
        System.out.println("=== customer_spending_summary ===");
        tableEnv.sqlQuery("SELECT * FROM customer_spending_summary").execute().print();

        System.out.println("\n=== customer_segmentation ===");
        tableEnv.sqlQuery("SELECT * FROM customer_segmentation").execute().print();

        System.out.println("\n=== category_performance ===");
        tableEnv.sqlQuery("SELECT * FROM category_performance").execute().print();

        System.out.println("\n=== Lesson Complete ===");
        System.out.println("These views update continuously as new orders arrive — point the");
        System.out.println("'orders' table at a Kafka topic (see the DDL above) to run them live.");
    }

    /**
     * Prints the Kafka source-table DDL that backs these views on Confluent Cloud.
     * Note the computed {@code order_time} column + watermark: time-windowed views
     * (hourly-sales-metrics, top-performers-leaderboard) reference {@code order_time}
     * via {@code TUMBLE(..., DESCRIPTOR(order_time), ...)}.
     */
    private static void printKafkaTableDdl() {
        System.out.println("--- Kafka-backed 'orders' table (Confluent Cloud Flink SQL) ---");
        System.out.println("""
            CREATE TABLE orders (
                orderId     STRING,
                customerId  STRING,
                amount      DECIMAL(10,2),
                `timestamp` BIGINT,
                category    STRING,
                order_time  AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
                WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'orders',
                'properties.bootstrap.servers' = '<broker>',
                'format' = 'json',
                'scan.startup.mode' = 'latest-offset'
            );
            """);
    }
}
