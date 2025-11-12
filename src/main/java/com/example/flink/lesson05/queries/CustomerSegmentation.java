package com.example.flink.lesson05.queries;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Customer Segmentation Query
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
public class CustomerSegmentation {
    
    /**
     * Executes the customer segmentation query
     * 
     * @param tableEnv The StreamTableEnvironment with 'orders' table registered
     */
    public static void runCustomerSegmentation(StreamTableEnvironment tableEnv) {
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
}
