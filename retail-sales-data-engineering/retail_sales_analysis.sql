-- Retail Sales Data Engineering Project
-- SQL Analytics Queries

-- 1. Total Sales Revenue
SELECT
    SUM(total_sales) AS total_revenue
FROM retail_sales_gold;

-- 2. Sales by Product Category
SELECT
    category,
    SUM(total_sales) AS total_sales
FROM retail_sales_gold
GROUP BY category
ORDER BY total_sales DESC;

-- 3. Sales by Region
SELECT
    region,
    SUM(total_sales) AS total_sales
FROM retail_sales_gold
GROUP BY region
ORDER BY total_sales DESC;

-- 4. Total Quantity Sold
SELECT
    SUM(total_quantity) AS total_quantity_sold
FROM retail_sales_gold;

-- 5. Sales by Category and Region
SELECT
    category,
    region,
    SUM(total_sales) AS total_sales,
    SUM(total_quantity) AS total_quantity
FROM retail_sales_gold
GROUP BY category, region
ORDER BY total_sales DESC;
