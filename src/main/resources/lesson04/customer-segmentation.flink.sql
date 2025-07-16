-- Customer Segmentation Materialized View
-- Creates customer classifications based on spending patterns and behavior
-- 
-- This view segments customers into categories based on:
-- - Total spending amounts and order frequency
-- - Purchase diversity across product categories
-- - Customer lifecycle stage (New, Regular, Premium, VIP)
-- - Shopping behavior patterns (Single-Category, Multi-Category, Diverse)
--
-- Use case: Marketing campaigns, customer retention, personalized experiences

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
GROUP BY customerId;