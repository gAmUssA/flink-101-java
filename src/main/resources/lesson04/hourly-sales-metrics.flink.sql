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
--
-- Assumes the `orders` table declares a time attribute column, e.g.:
--   order_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
--   WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
-- TUMBLE's DESCRIPTOR must reference that column name, not an inline expression.

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
GROUP BY window_start, window_end;