package com.example.flink.lesson04;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import utils.FlinkEnvironmentConfig;

/**
 * Lesson 4: Materialized Views with Confluent Cloud (Conceptual)
 * <p>
 * This lesson explains the concepts of materialized views using Apache Flink's Table API
 * and SQL with Confluent Cloud integration. Since Flink 2.0 is not yet released, this
 * lesson focuses on teaching the concepts and provides code examples that will work
 * when the Table API dependencies become available.
 * <p>
 * What you'll learn:
 * - Understanding materialized views and their benefits
 * - How Table API bridges DataStream and SQL worlds
 * - Creating tables from Kafka topics (topic-to-table mapping)
 * - Building materialized views with SQL queries
 * - Managing view refresh strategies and persistence
 * - Monitoring materialized view performance
 * - Cross-environment query capabilities
 * - Integration patterns with Confluent Cloud Flink SQL
 * <p>
 * Key Concepts:
 * <p> 
 * 1. MATERIALIZED VIEWS
 *    - Pre-computed query results stored for fast access
 *    - Automatically updated as source data changes
 *    - Trade storage space for query performance
 *    - Essential for real-time analytics dashboards
 * <p>
 * 2. TABLE API INTEGRATION
 *    - StreamTableEnvironment bridges DataStream and Table APIs
 *    - SQL DDL creates tables from Kafka topics
 *    - Views defined using standard SQL syntax
 *    - Seamless integration with streaming data
 * <p>
 * 3. CONFLUENT CLOUD BENEFITS
 *    - Managed Kafka infrastructure
 *    - Built-in Flink SQL capabilities
 *    - Cross-environment data access
 *    - Automatic scaling and maintenance
 * <p>
 * Business Scenario:
 * We're creating materialized views for an e-commerce analytics platform:
 * - Real-time customer spending summaries
 * - Product category performance metrics
 * - Hourly sales dashboards
 * - Customer segmentation views
 * - Inventory level monitoring
 * <p>
 * Expected Output (when Table API is available):
 * Creating Kafka source table: orders
 * Creating materialized view: customer_spending_summary
 * Creating materialized view: hourly_sales_metrics
 * Creating materialized view: product_category_performance
 * View Results - Customer Spending:
 * +-------------+-------------+-------------+
 * | customer_id | total_spent | order_count |
 * +-------------+-------------+-------------+
 * | customer_001|      1247.89|           15|
 * | customer_002|       892.34|           12|
 * +-------------+-------------+-------------+
 * <p>
 * Try this (when dependencies are available):
 * 1. Modify the SQL queries to add new aggregation metrics
 * 2. Create additional materialized views for different business needs
 * 3. Experiment with different refresh strategies
 * 4. Add time-based windowing to the views
 * 5. Create views that join multiple Kafka topics
 */
public class MaterializedViewExample {

    public static void main(String[] args) throws Exception {

        System.out.println("=== Flink Lesson 4: Materialized Views (Conceptual) ===");
        System.out.println("This lesson explains materialized view concepts for Flink 2.0");
        System.out.println();

        // Step 1: Create streaming execution environment with Web UI enabled
        StreamExecutionEnvironment env = FlinkEnvironmentConfig.createEnvironmentWithUI();
        
        // Print Web UI access instructions
        FlinkEnvironmentConfig.printWebUIInstructions();
        
        // Configure for educational clarity
        env.setParallelism(1);
        env.enableCheckpointing(60000);

        System.out.println("✓ Streaming environment created");
        System.out.println("✓ Checkpointing enabled for fault tolerance");

        // Step 2: Explain Table API Environment Setup
        explainTableAPISetup();

        // Step 3: Demonstrate Kafka Source Table Creation
        demonstrateKafkaSourceTable();

        // Step 4: Show Materialized View Examples
        showMaterializedViewExamples();

        // Step 5: Explain View Querying
        explainViewQuerying();

        // Step 6: Discuss Continuous Maintenance
        discussContinuousMaintenance();

        // Step 7: Provide Implementation Roadmap
        provideImplementationRoadmap();

        System.out.println("\n=== Lesson Complete ===");
        System.out.println("When Flink 2.0 is released, you can:");
        System.out.println("1. Uncomment Table API dependencies in build.gradle.kts");
        System.out.println("2. Replace this conceptual code with actual Table API implementation");
        System.out.println("3. Run real materialized views with Confluent Cloud");
        
        // Note: This is a conceptual lesson - no actual streaming execution needed
        System.out.println("\nThis conceptual lesson is complete!");
        System.out.println("Ready to implement with real Confluent Cloud Flink SQL when available.");
    }

    /**
     * Explains how to set up Table API environment
     */
    private static void explainTableAPISetup() {
        System.out.println("\n=== Step 2: Table API Environment Setup ===");
        System.out.println("When Table API dependencies are available, you would:");
        System.out.println();
        
        System.out.println("// Import required classes:");
        System.out.println("import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;");
        System.out.println("import org.apache.flink.table.api.Table;");
        System.out.println();
        
        System.out.println("// Create Table Environment:");
        System.out.println("StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);");
        System.out.println();
        
        System.out.println("Key Benefits:");
        System.out.println("- Bridges DataStream API and SQL");
        System.out.println("- Enables SQL queries on streaming data");
        System.out.println("- Supports complex analytical operations");
        System.out.println("- Integrates with Confluent Cloud seamlessly");
    }

    /**
     * Demonstrates Kafka source table creation
     */
    private static void demonstrateKafkaSourceTable() {
        System.out.println("\n=== Step 3: Kafka Source Table Creation ===");
        System.out.println("Creating a table from Kafka topic with Confluent Cloud:");
        System.out.println();
        
        String exampleSQL = """
            CREATE TABLE orders (
                orderId STRING,
                customerId STRING,
                amount DECIMAL(10,2),
                `timestamp` BIGINT,
                category STRING,
                order_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
                WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'orders',
                'properties.bootstrap.servers' = 'your-confluent-cloud-broker',
                'properties.security.protocol' = 'SASL_SSL',
                'properties.sasl.mechanism' = 'PLAIN',
                'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="<api-key>" password="<api-secret>";',
                'format' = 'json',
                'scan.startup.mode' = 'latest-offset'
            )
            """;
        
        System.out.println("Example DDL:");
        System.out.println(exampleSQL);
        
        System.out.println("Key Features:");
        System.out.println("- Maps Kafka topic to SQL table");
        System.out.println("- Supports watermarks for event time");
        System.out.println("- Handles Confluent Cloud authentication");
        System.out.println("- Configurable startup modes");
    }

    /**
     * Shows examples of materialized views
     */
    private static void showMaterializedViewExamples() {
        System.out.println("\n=== Step 4: Materialized View Examples ===");
        System.out.println("Individual .flink.sql files have been created in src/main/resources/lesson04/ for each use case:");
        System.out.println();
        
        // Customer Spending Summary
        System.out.println("1. Customer Spending Summary View (customer-spending-summary.flink.sql):");
        System.out.println("   Use case: Customer analytics, loyalty programs, personalized marketing");
        String customerSpendingSQL = """
            CREATE VIEW customer_spending_summary AS
            SELECT 
                customerId,
                SUM(amount) as total_spent,
                COUNT(*) as order_count,
                AVG(amount) as avg_order_value,
                MAX(amount) as max_order_value,
                COUNT(DISTINCT category) as categories_purchased
            FROM orders
            GROUP BY customerId
            """;
        System.out.println(customerSpendingSQL);
        
        // Hourly Sales Metrics
        System.out.println("2. Hourly Sales Metrics View (hourly-sales-metrics.flink.sql):");
        System.out.println("   Use case: Real-time dashboards, performance monitoring, trend analysis");
        String hourlySalesSQL = """
            CREATE VIEW hourly_sales_metrics AS
            SELECT 
                window_start as hour_start,
                window_end as hour_end,
                COUNT(*) as total_orders,
                SUM(amount) as total_revenue,
                AVG(amount) as avg_order_value,
                COUNT(DISTINCT customerId) as unique_customers,
                COUNT(DISTINCT category) as categories_sold,
                MAX(amount) as max_order_amount,
                MIN(amount) as min_order_amount
            FROM TUMBLE(TABLE orders, DESCRIPTOR(order_time), INTERVAL '1' HOUR)
            GROUP BY window_start, window_end
            """;
        System.out.println(hourlySalesSQL);
        
        // Customer Segmentation
        System.out.println("3. Customer Segmentation View (customer-segmentation.flink.sql):");
        System.out.println("   Use case: Marketing campaigns, customer retention, personalized experiences");
        String segmentationSQL = """
            CREATE VIEW customer_segmentation AS
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
        System.out.println(segmentationSQL);
        
        // Reference to additional SQL files
        System.out.println("4. Category Performance Analysis (category-performance.flink.sql):");
        System.out.println("   Use case: Inventory management, product strategy, sales optimization");
        
        System.out.println("Benefits of Materialized Views:");
        System.out.println("- Pre-computed results for fast queries");
        System.out.println("- Automatically updated as data changes");
        System.out.println("- Support complex aggregations and joins");
        System.out.println("- Enable real-time analytics dashboards");
        
        System.out.println("\n=== Advanced: Materialized Tables with Freshness ===");
        System.out.println("For production use cases, consider MATERIALIZED TABLE with freshness guarantees:");
        String materializedTableSQL = """
            CREATE MATERIALIZED TABLE customer_spending_materialized
                FRESHNESS = INTERVAL '30' SECOND
                AS SELECT 
                    customerId,
                    SUM(amount) as total_spent,
                    COUNT(*) as order_count,
                    AVG(amount) as avg_order_value
                FROM orders
                GROUP BY customerId
            """;
        System.out.println(materializedTableSQL);
        System.out.println("Key Benefits of MATERIALIZED TABLE:");
        System.out.println("- Guaranteed freshness with FRESHNESS parameter");
        System.out.println("- Automatic refresh based on data changes");
        System.out.println("- Better performance for frequently accessed data");
        System.out.println("- Ideal for real-time dashboards and analytics");
    }

    /**
     * Explains how to query materialized views
     */
    private static void explainViewQuerying() {
        System.out.println("\n=== Step 5: Querying Materialized Views ===");
        System.out.println("Once views are created, you can query them like regular tables:");
        System.out.println();
        
        System.out.println("// Query customer spending with Confluent Cloud syntax");
        System.out.println("Table result = tableEnv.sqlQuery(\"SELECT * FROM customer_spending_summary ORDER BY total_spent DESC LIMIT 10\");");
        System.out.println("result.execute().print();");
        System.out.println();
        
        System.out.println("// Query recent hourly metrics");
        System.out.println("Table hourly = tableEnv.sqlQuery(\"SELECT * FROM hourly_sales_metrics ORDER BY hour_start DESC LIMIT 5\");");
        System.out.println("hourly.execute().print();");
        System.out.println();
        
        System.out.println("// Complex analytical queries with joins");
        System.out.println("Table analysis = tableEnv.sqlQuery(\"\"\"");
        System.out.println("    SELECT ");
        System.out.println("        cs.customer_segment,");
        System.out.println("        cs.purchase_behavior,");
        System.out.println("        COUNT(*) as customer_count,");
        System.out.println("        AVG(cs.total_spent) as avg_spending,");
        System.out.println("        AVG(cs.categories_purchased) as avg_categories");
        System.out.println("    FROM customer_segmentation cs");
        System.out.println("    GROUP BY cs.customer_segment, cs.purchase_behavior");
        System.out.println("    ORDER BY avg_spending DESC");
        System.out.println("\"\"\");");
        System.out.println();
        
        System.out.println("// Real-time category performance analysis");
        System.out.println("Table categoryPerf = tableEnv.sqlQuery(\"\"\"");
        System.out.println("    SELECT ");
        System.out.println("        category,");
        System.out.println("        COUNT(*) as order_count,");
        System.out.println("        SUM(amount) as total_revenue,");
        System.out.println("        AVG(amount) as avg_order_value");
        System.out.println("    FROM orders");
        System.out.println("    WHERE order_time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY");
        System.out.println("    GROUP BY category");
        System.out.println("    ORDER BY total_revenue DESC");
        System.out.println("\"\"\");");
        
        System.out.println("\nConfluent Cloud Query Capabilities:");
        System.out.println("- Standard SQL syntax with Flink extensions");
        System.out.println("- Joins between multiple views and tables");
        System.out.println("- Window functions and time-based analytics");
        System.out.println("- Real-time continuous queries");
        System.out.println("- Cross-environment data access");
        System.out.println("- Built-in functions for stream processing");
    }

    /**
     * Discusses continuous view maintenance
     */
    private static void discussContinuousMaintenance() {
        System.out.println("\n=== Step 6: Continuous View Maintenance ===");
        System.out.println("Confluent Cloud automatically maintains materialized views:");
        System.out.println();
        
        System.out.println("Automatic Updates:");
        System.out.println("- Views update as new data arrives in Kafka topics");
        System.out.println("- Incremental computation for efficiency");
        System.out.println("- Consistent results across all queries");
        System.out.println("- Built-in fault tolerance and recovery");
        System.out.println("- Automatic scaling based on workload");
        System.out.println();
        
        System.out.println("Confluent Cloud Benefits:");
        System.out.println("- Managed infrastructure - no cluster management");
        System.out.println("- Automatic resource scaling");
        System.out.println("- Built-in monitoring and alerting");
        System.out.println("- Cross-region data replication");
        System.out.println("- Enterprise security and compliance");
        System.out.println();
        
        System.out.println("Performance Optimization:");
        System.out.println("- Intelligent partitioning strategies");
        System.out.println("- Optimized state storage");
        System.out.println("- Query result caching");
        System.out.println("- Automatic resource allocation");
        System.out.println("- Real-time performance metrics");
        System.out.println();
        
        System.out.println("Production Best Practices:");
        System.out.println("- Use appropriate watermark strategies");
        System.out.println("- Monitor view refresh latency");
        System.out.println("- Set up proper alerting rules");
        System.out.println("- Regular performance reviews");
        System.out.println("- Implement proper access controls");
    }

    /**
     * Provides implementation roadmap for when dependencies are available
     */
    private static void provideImplementationRoadmap() {
        System.out.println("\n=== Step 7: Implementation Roadmap ===");
        System.out.println("Implementation approaches for Confluent Cloud Flink SQL:");
        System.out.println();
        
        System.out.println("Option 1: Confluent Cloud Console (Recommended for Learning)");
        System.out.println("   - Use browser-based Flink SQL workspace");
        System.out.println("   - Create tables directly with DDL statements");
        System.out.println("   - Build and test materialized views interactively");
        System.out.println("   - Monitor performance in real-time");
        System.out.println();
        
        System.out.println("Option 2: Flink SQL Shell (CLI-based)");
        System.out.println("   - Use confluent flink shell command");
        System.out.println("   - Execute SQL statements from command line");
        System.out.println("   - Suitable for scripted deployments");
        System.out.println();
        
        System.out.println("Option 3: Local Flink 2.0 (When Available)");
        System.out.println("   - Uncomment Table API dependencies in build.gradle.kts");
        System.out.println("   - Add flink-table-api-java-bridge:2.0.0");
        System.out.println("   - Add flink-table-planner:2.0.0");
        System.out.println("   - Use actual Order schema: orderId, customerId, amount, timestamp, category");
        System.out.println();
        
        System.out.println("Implementation Steps:");
        System.out.println("   1. Set up Confluent Cloud environment and API keys");
        System.out.println("   2. Create 'orders' topic with proper schema registry");
        System.out.println("   3. Use OrderDataGenerator to populate test data");
        System.out.println("   4. Create source table with correct field mapping:");
        System.out.println("      - orderId STRING, customerId STRING, amount DECIMAL(10,2)");
        System.out.println("      - timestamp BIGINT, category STRING");
        System.out.println("      - Computed column: order_time AS TO_TIMESTAMP_LTZ(timestamp, 3)");
        System.out.println("   5. Deploy materialized views using provided SQL examples");
        System.out.println("   6. Test queries and monitor performance");
        System.out.println();
        
        System.out.println("Production Considerations:");
        System.out.println("   - Use proper RBAC and API key management");
        System.out.println("   - Configure appropriate compute pools");
        System.out.println("   - Set up monitoring and alerting");
        System.out.println("   - Implement proper error handling");
        System.out.println("   - Document view dependencies and refresh patterns");
    }
}