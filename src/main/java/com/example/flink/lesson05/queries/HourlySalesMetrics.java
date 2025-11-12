package com.example.flink.lesson05.queries;

import com.example.flink.lesson05.utils.ChangelogFormatter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Hourly Sales Metrics Query with Tumbling Windows
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
public class HourlySalesMetrics {
    
    /**
     * Executes the hourly sales metrics query with tumbling windows
     * 
     * @param tableEnv The StreamTableEnvironment with 'orders' table registered
     */
    public static void runHourlySalesMetrics(StreamTableEnvironment tableEnv) {
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
                TO_TIMESTAMP_LTZ(`timestamp`, 3) as event_time,
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
        
        // Convert to DataStream with formatted output
        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
        resultStream
            .map(ChangelogFormatter.detailed("Hourly Sales"))
            .print();
        
        System.out.println("✓ Hourly sales metrics query registered\n");
    }
}
