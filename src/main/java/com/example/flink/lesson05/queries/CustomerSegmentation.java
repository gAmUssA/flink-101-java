package com.example.flink.lesson05.queries;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static com.example.flink.lesson05.utils.ChangelogFormatter.detailed;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.ifThenElse;
import static org.apache.flink.table.api.Expressions.lit;

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

    // Step 1: Access the 'orders' table registered in the environment
    Table orders = tableEnv.from("orders");

    // Step 2: Aggregate per customer (total spent, counts, averages, distinct categories)
    Table aggregated = orders
        .groupBy($("customerId"))
        .select(
            $("customerId"),
            $("amount").sum().as("total_spent"),
            $("customerId").count().as("order_count"),
            $("amount").avg().as("avg_order_value"),
            $("category").count().distinct().as("categories_purchased")
        );

    // Step 3: Derive customer_segment and purchase_behavior using conditional expressions
    Table resultTable = aggregated.select(
        $("customerId"),
        $("total_spent"),
        $("order_count"),
        $("avg_order_value"),
        $("categories_purchased"),
        // Customer segment (equivalent to CASE WHEN in SQL)
        ifThenElse(
            $("total_spent")
                .isGreaterOrEqual(lit(1000))
                .and($("order_count").isGreaterOrEqual(lit(10))),
            lit("VIP"),
            ifThenElse(
                $("total_spent").isGreaterOrEqual(lit(500))
                    .or($("order_count").isGreaterOrEqual(lit(5))),
                lit("Premium"),
                ifThenElse(
                    $("total_spent").isGreaterOrEqual(lit(100))
                        .or($("order_count").isGreaterOrEqual(lit(2))),
                    lit("Regular"),
                    lit("New")
                )
            )
        ).as("customer_segment"),
        // Purchase behavior classification
        ifThenElse(
            $("categories_purchased").isGreaterOrEqual(lit(4)),
            lit("Diverse"),
            ifThenElse(
                $("categories_purchased").isGreaterOrEqual(lit(2)),
                lit("Multi-Category"),
                lit("Single-Category")
            )
        ).as("purchase_behavior")
    );

    // Step 4: Convert to DataStream with formatted output
    DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
    resultStream
        .map(detailed("Customer Segmentation"))
        .print();

    System.out.println("✓ Customer segmentation query registered\n");
  }
}
