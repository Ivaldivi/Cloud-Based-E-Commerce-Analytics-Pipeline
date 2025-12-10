-- Query 1, conversion funnel
-- For each product, calculate view --> add_to_cart --> purchase conversion rates
SELECT DISTINCT m.product_id, ROUND(pc.purchase_count*100.0/vc.view_count,5) as conversion_rate
FROM capstone_analytics m
JOIN(
    SELECT product_id, count(*) as purchase_count
    FROM capstone_analytics
    WHERE event_type = 'purchase'
    group by product_id
    ) as pc
ON m.product_id = pc.product_id
JOIN (
    SELECT product_id, count(*) as view_count
    FROM capstone_analytics
    WHERE event_type = 'page_view'
    group by product_id
    ) as vc
ON m.product_id = vc.product_id
order by m.product_id


-- Query 2, hourly revenue 
-- Total revenue by hour (price x quantity for purchases)
SELECT hour, sum(quantity * price) as total_revenue FROM capstone_analytics 
WHERE event_type= 'purchase'
GROUP BY hour
ORDER BY hour asc;

-- Query 3, Top 10 Products
-- Most frequently viewed products with view counts
SELECT product_id, count(*) as view_count
FROM capstone_analytics 
WHERE event_type = 'page_view'
GROUP BY product_id
ORDER BY count(*) desc
LIMIT 10;


-- Query 4, Category Performance
-- Daily event counts (all types) grouped by category
SELECT year, month, day, category, count(*) as eventcount
FROM capstone_analytics
GROUP BY year, month, day, category
ORDER BY year, month, day asc;

-- Query 5, User Activity
-- Count of unique users and sessions per day 
SELECT year, month, day, count(DISTINCT user_id) as num_users, count(DISTINCT session_id) as num_sessions
FROM capstone_analytics
GROUP BY year, month, day
ORDER BY day asc;