-- =====================================================================
-- Business queries  |  database: thornquist_curated  |  workgroup: analyst-wg
-- Run each query separately in the Athena query editor (owner: Sukrutha)
-- =====================================================================

-- Q1. Channel comparison: orders, revenue and average order value
--     (opening finding of the demo: how much bigger are online orders?)
SELECT channel,
       COUNT(DISTINCT transaction_id)                                         AS orders,
       SUM(quantity)                                                          AS units,
       ROUND(SUM(line_amount), 2)                                             AS revenue,
       ROUND(SUM(line_amount) / COUNT(DISTINCT transaction_id), 2)            AS avg_order_value,
       ROUND(CAST(SUM(quantity) AS DOUBLE) / COUNT(DISTINCT transaction_id), 2) AS units_per_order
FROM thornquist_curated.sales_fact
GROUP BY channel
ORDER BY revenue DESC;

-- Q2. Revenue by region and category, with each category's share of the region
SELECT region,
       category,
       ROUND(SUM(line_amount), 2) AS revenue,
       ROUND(100 * SUM(line_amount) / SUM(SUM(line_amount)) OVER (PARTITION BY region), 1) AS pct_of_region
FROM thornquist_curated.sales_fact
GROUP BY region, category
ORDER BY region, revenue DESC;

-- Q3. Customer segment x channel (blends sales with the customer extract)
SELECT customer_segment,
       channel,
       COUNT(DISTINCT cust_num)                                    AS customers,
       COUNT(DISTINCT transaction_id)                              AS orders,
       ROUND(SUM(line_amount), 2)                                  AS revenue,
       ROUND(SUM(line_amount) / COUNT(DISTINCT transaction_id), 2) AS avg_order_value
FROM thornquist_curated.sales_fact
GROUP BY customer_segment, channel
ORDER BY customer_segment, channel;

-- Q4. Top 10 products by revenue, with the in-store / online split
SELECT product_name,
       category,
       SUM(quantity)                                                             AS units,
       ROUND(SUM(line_amount), 2)                                                AS revenue,
       ROUND(SUM(CASE WHEN channel = 'online' THEN line_amount ELSE 0 END), 2)  AS online_revenue,
       ROUND(SUM(CASE WHEN channel = 'in-store' THEN line_amount ELSE 0 END), 2) AS store_revenue
FROM thornquist_curated.sales_fact
GROUP BY product_name, category
ORDER BY revenue DESC
LIMIT 10;

-- Q5. Daily revenue trend by channel
SELECT sale_date,
       day_of_week,
       ROUND(SUM(CASE WHEN channel = 'in-store' THEN line_amount ELSE 0 END), 2) AS store_revenue,
       ROUND(SUM(CASE WHEN channel = 'online' THEN line_amount ELSE 0 END), 2)   AS online_revenue,
       ROUND(SUM(line_amount), 2)                                                AS total_revenue
FROM thornquist_curated.sales_fact
GROUP BY sale_date, day_of_week
ORDER BY sale_date;

-- Q6. Walk-in blind spot: share of in-store revenue with no customer attached, by region
SELECT region,
       ROUND(SUM(line_amount), 2)                                                       AS store_revenue,
       ROUND(SUM(CASE WHEN is_walk_in THEN line_amount ELSE 0 END), 2)                  AS walk_in_revenue,
       ROUND(100 * SUM(CASE WHEN is_walk_in THEN line_amount ELSE 0 END) / SUM(line_amount), 1) AS walk_in_pct
FROM thornquist_curated.sales_fact
WHERE channel = 'in-store'
GROUP BY region
ORDER BY walk_in_pct DESC;
