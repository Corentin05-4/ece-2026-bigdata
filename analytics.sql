
-- Lab 3: SQL analytics with DuckDB

-- Data quality: orders referencing unknown users
SELECT count(*) AS orphan_orders
FROM orders o
ANTI JOIN users u ON o.user_uuid = u.uuid;

-- Data quality: users without orders
SELECT count(*) AS inactive_users
FROM users u
ANTI JOIN orders o ON o.user_uuid = u.uuid;

-- Data quality: duplicate order identifiers
SELECT uuid, count(*) AS occurrences
FROM orders
GROUP BY uuid
HAVING count(*) > 1;

-- Quantity sold per product
SELECT
    product,
    count(*) AS orders,
    sum(quantity) AS quantity,
    round(
        100 * sum(quantity) / sum(sum(quantity)) OVER (),
        1
    ) AS share_pct
FROM orders
GROUP BY product
ORDER BY quantity DESC;

-- Orders per month
SELECT
    date_trunc('month', date) AS month,
    count(*) AS orders,
    sum(quantity) AS quantity
FROM orders
GROUP BY month
ORDER BY month;

-- Top 5 customers
SELECT
    u.username,
    u.name,
    count(*) AS orders,
    sum(o.quantity) AS quantity
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL
ORDER BY quantity DESC
LIMIT 5;

-- Orders per customer age group
SELECT
    (date_diff('year', u.birthdate, DATE '2020-01-01') // 20) * 20 AS age_group,
    count(DISTINCT u.uuid) AS users,
    count(*) AS orders,
    round(avg(o.quantity), 2) AS avg_quantity
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY age_group
ORDER BY age_group;

-- Daily sales, cumulative quantity, and 7-day moving average
WITH daily AS (
    SELECT
        date::DATE AS day,
        sum(quantity) AS quantity
    FROM orders
    GROUP BY day
)
SELECT
    day,
    quantity,
    sum(quantity) OVER (ORDER BY day) AS cumulative,
    round(
        avg(quantity) OVER (
            ORDER BY day
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 1
    ) AS avg_7d
FROM daily
ORDER BY day
LIMIT 10;

-- Best-selling product for each month
SELECT
    strftime(date, '%Y-%m') AS month,
    product,
    sum(quantity) AS quantity
FROM orders
GROUP BY month, product
QUALIFY rank() OVER (
    PARTITION BY month
    ORDER BY sum(quantity) DESC
) = 1
ORDER BY month;

-- Monthly sales pivot by product
PIVOT (
    SELECT
        strftime(date, '%Y-%m') AS month,
        product,
        quantity
    FROM orders
)
ON product
USING sum(quantity)
ORDER BY month;

-- Exercise 1: Average orders per user and average quantity per order
SELECT
    round(
        (SELECT count(*) FROM orders) * 1.0 /
        (SELECT count(*) FROM users), 2
    ) AS avg_orders_per_user,
    round(avg(quantity), 2) AS avg_quantity_per_order
FROM orders;

-- Exercise 2: First and last order for each user
SELECT
    u.username,
    min(o.date) AS first_order,
    max(o.date) AS last_order,
    date_diff('day', min(o.date), max(o.date)) AS days_between
FROM users u
JOIN orders o ON u.uuid = o.user_uuid
GROUP BY u.uuid, u.username
ORDER BY u.username;

-- Exercise 3: Hour with the highest quantity sold
SELECT
    hour(date) AS hour_of_day,
    sum(quantity) AS quantity_sold
FROM orders
GROUP BY hour_of_day
ORDER BY quantity_sold DESC
LIMIT 1;

-- Exercise 4: Month-over-month quantity variation per product
WITH monthly AS (
    SELECT
        product,
        strftime(date, '%Y-%m') AS month,
        sum(quantity) AS quantity
    FROM orders
    GROUP BY product, month
),
previous_month AS (
    SELECT
        product,
        month,
        quantity,
        lag(quantity) OVER (
            PARTITION BY product ORDER BY month
        ) AS previous_quantity
    FROM monthly
)
SELECT
    product,
    month,
    quantity,
    round(
        100.0 * (quantity - previous_quantity) /
        nullif(previous_quantity, 0), 2
    ) AS variation_pct
FROM previous_month
ORDER BY product, month;

-- Exercise 5: Users who ordered every available product
SELECT
    u.username,
    u.name,
    count(DISTINCT o.product) AS products_ordered
FROM users u
JOIN orders o ON u.uuid = o.user_uuid
GROUP BY u.uuid, u.username, u.name
HAVING count(DISTINCT o.product) = (
    SELECT count(DISTINCT product) FROM orders
)
ORDER BY u.username;
