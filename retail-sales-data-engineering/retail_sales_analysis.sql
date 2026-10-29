
-- 1. Total sales revenue
SELECT
    SUM(quantity * unit_price) AS total_sales
FROM retail_sales;

-- 2. Total number of orders
SELECT
    COUNT(DISTINCT order_id) AS total_orders
FROM retail_sales;

-- 3. Sales by product category
SELECT
    category,
    SUM(quantity * unit_price) AS total_sales
FROM retail_sales
GROUP BY category
ORDER BY total_sales DESC;

-- 4. Sales by region
SELECT
    region,
    SUM(quantity * unit_price) AS total_sales
FROM retail_sales
GROUP BY region
ORDER BY total_sales DESC;

-- 5. Top-selling products by revenue
SELECT
    product,
    SUM(quantity) AS total_quantity,
    SUM(quantity * unit_price) AS total_revenue
FROM retail_sales
GROUP BY product
ORDER BY total_revenue DESC;
