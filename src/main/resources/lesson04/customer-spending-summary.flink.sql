-- Customer Spending Summary Materialized View
-- Creates a comprehensive view of customer spending patterns and behavior
-- 
-- This view aggregates order data to provide insights into:
-- - Total spending per customer
-- - Order frequency and patterns
-- - Average order values
-- - Purchase diversity across categories
--
-- Use case: Customer analytics, loyalty programs, personalized marketing

CREATE VIEW customer_spending_summary AS
SELECT 
    customerId,
    SUM(amount) as total_spent,
    COUNT(*) as order_count,
    AVG(amount) as avg_order_value,
    MAX(amount) as max_order_value,
    COUNT(DISTINCT category) as categories_purchased
FROM orders
GROUP BY customerId;