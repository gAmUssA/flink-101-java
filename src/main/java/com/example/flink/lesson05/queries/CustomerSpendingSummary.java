package com.example.flink.lesson05.queries;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Customer Spending Summary Query
 * Implements: customer-spending-summary.flink.sql
 * <p> 
 * Aggregates order data to provide insights into:
 * - Total spending per customer
 * - Order frequency and patterns
 * - Average order values
 * - Purchase diversity across categories
 */
public class CustomerSpendingSummary {
    
    /**
     * Executes the customer spending summary query using Table API
     * 
     * @param tableEnv The StreamTableEnvironment with 'orders' table registered
     */
    public static void runCustomerSpendingSummary(StreamTableEnvironment tableEnv) {
        System.out.println("=== Step 4: Customer Spending Summary ===");
        
        // Using Table API instead of SQL for this example
        final Table orders = tableEnv.from("orders");
        final Table resultTable = orders
            .groupBy($("customerId"))
            .select(
                $("customerId"),
                $("amount").sum().as("total_spent"),
                $("customerId").count().as("order_count"),
                $("amount").avg().as("avg_order_value"),
                $("amount").max().as("max_order_value"),
                $("category").count().distinct().as("categories_purchased")
            );
        
        // Convert to DataStream and print results
        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
        resultStream.print("Customer Spending");
        
        System.out.println("✓ Customer spending summary query registered\n");
    }
    
    /**
     * Executes the customer spending summary query using SQL
     * 
     * @param tableEnv The StreamTableEnvironment with 'orders' table registered
     */
    public static void executeSQL(StreamTableEnvironment tableEnv) {
        System.out.println("=== Step 4: Customer Spending Summary (SQL) ===");
        
        String query = """
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
        
        System.out.println("SQL Query:");
        System.out.println(query);
        
        Table resultTable = tableEnv.sqlQuery(query);
        
        // Convert to DataStream and print results
        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
        resultStream.print("Customer Spending");
        
        System.out.println("✓ Customer spending summary query registered\n");
    }
}
