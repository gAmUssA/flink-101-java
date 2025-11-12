package com.example.flink.lesson05.queries;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Category Performance Analysis Query
 * Implements: category-performance.flink.sql
 * <p> 
 * Provides real-time analytics on product category performance:
 * - Order volume and revenue by category
 * - Average order values per category
 * - Time-based filtering for recent performance (last 24 hours)
 * <p> 
 * Note: For educational purposes, we're showing all data instead of filtering by time
 */
public class CategoryPerformance {
    
    /**
     * Executes the category performance analysis query
     * 
     * @param tableEnv The StreamTableEnvironment with 'orders' table registered
     */
    public static void runCategoryPerformance(StreamTableEnvironment tableEnv) {
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
}
