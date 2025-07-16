-- Hourly Sales Metrics Materialized View
-- Creates time-windowed analytics for sales performance tracking
-- 
-- This view provides hourly aggregations including:
-- - Total orders and revenue per hour
-- - Average order values
-- - Customer and category diversity metrics
-- - Min/max order amounts for outlier detection
--
-- Use case: Real-time dashboards, performance monitoring, trend analysis

CREATE VIEW hourly_sales_metrics AS
SELECT 
    TUMBLE_START(order_time, INTERVAL '1' HOUR) as hour_start,
    TUMBLE_END(order_time, INTERVAL '1' HOUR) as hour_end,
    COUNT(*) as total_orders,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_order_value,
    COUNT(DISTINCT customerId) as unique_customers,
    COUNT(DISTINCT category) as categories_sold,
    MAX(amount) as max_order_amount,
    MIN(amount) as min_order_amount
FROM orders
GROUP BY TUMBLE(order_time, INTERVAL '1' HOUR);