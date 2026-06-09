-- Dynamic Top-N Leaderboards: Best Performing Customers & Categories
-- Real-time ranking with windowed top-N analysis
--
-- Assumes the `orders` table declares a time attribute column, e.g.:
--   order_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
--   WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
-- TUMBLE's DESCRIPTOR must reference that column name, not an inline expression.
CREATE VIEW top_performers_leaderboard AS
SELECT *
FROM (
    SELECT 
        window_start,
        window_end,
        customerId,
        SUM(amount) as total_spent,
        COUNT(*) as order_count,
        AVG(amount) as avg_order_value,
        COUNT(DISTINCT category) as category_diversity,
        LISTAGG(DISTINCT category, ', ') as categories_purchased,
        ROW_NUMBER() OVER (
            PARTITION BY window_start, window_end 
            ORDER BY SUM(amount) DESC
        ) as spending_rank,
        ROW_NUMBER() OVER (
            PARTITION BY window_start, window_end 
            ORDER BY COUNT(*) DESC
        ) as frequency_rank,
        -- Performance score combining multiple factors
        (SUM(amount) * 0.6 + COUNT(*) * 50 * 0.3 + COUNT(DISTINCT category) * 20 * 0.1) as performance_score
    FROM TABLE(
        TUMBLE(
            TABLE orders,
            DESCRIPTOR(order_time),
            INTERVAL '1' HOUR
        )
    )
    GROUP BY window_start, window_end, customerId
) ranked_customers
WHERE spending_rank <= 10 OR frequency_rank <= 10;  -- Top 10 in either category