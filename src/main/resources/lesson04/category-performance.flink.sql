-- Category Performance Analysis Query
-- Provides real-time analytics on product category performance
-- 
-- This query analyzes category performance including:
-- - Order volume and revenue by category
-- - Average order values per category
-- - Time-based filtering for recent performance
-- - Category ranking by revenue
--
-- Use case: Inventory management, product strategy, sales optimization

SELECT 
    category,
    COUNT(*) as order_count,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_order_value
FROM orders
WHERE TO_TIMESTAMP_LTZ(`timestamp`, 3) >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
GROUP BY category;