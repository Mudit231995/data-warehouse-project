select * from gold.dim_customers;
select * from gold.dim_products;
select * from gold.fact_sales;

-- Whenever we want to perform our data analysis using any given dataset, 
-- We should always divide its columns into dimensions and measures to perform best analysis
-- Dimensions basically helps us to group the data by and measures helps us to aggregate the data to perform data analysis
-- We can divide entire EDA into 6 steps :
-- 1) Database Exploration
-- 2) Dimensions Exploration
-- 3) Date Exploration
-- 4) Measures Exploration
-- 5) Magnitude
-- 6) Ranking

--------------------------------------Database Exploration-----------------------------------------------------------------------------------------

-- Explore all objects in the database
select * from information_schema.tables;

-- Explore all information_schema.tablesl columns in the database
select * from information_schema.columns
where table_name = 'dim_customers';

---------------------------------------Dimensions Exploration---------------------------------------------------------------------------------------

-- Identifying the unique values 9or categories) in each dimension
-- Helps in recognizing how data might be grouped or segmented, useful for later analysis

select distinct country from gold.dim_customers;

select distinct category from gold.dim_products;

-- Now understanding the hierarchy of the category column

select distinct category, subcategory, product_name from gold.dim_products
order by 1,2,3;

----------------------------------------Date Exploration--------------------------------------------------------------------------------------------

-- Identify the earliest and latest dates
-- Understand the scope of data and the timespan

select * from gold.fact_sales; 

-- Finding the dates of the first and last order
select
min(order_date) as first_order_date,
max(order_date) as last_order_date
from gold.fact_sales;

-- How many years of sales are available
select
min(order_date) as first_order_date,
max(order_date) as last_order_date,
datediff(year, min(order_date), max(order_date)) as order_range_years
from gold.fact_sales;

select * from gold.dim_customers;

-- Find the youngest and oldest customer
select
min(birthdate) as oldest_customer_birthdate,
datediff(year, min(birthdate), getdate()) as oldest_customer_age,
max(birthdate) as youngest_customer_birthdate,
datediff(year, max(birthdate), getdate()) as youngest_customer_age
from gold.dim_customers;

-------------------------------------------Measures Exploration-------------------------------------------------------------------------------------

-- Find total sales
select sum(sales_amount) as total_sales_amount from gold.fact_sales;

-- Find the average selling price
select avg(price) as avg_selling_price from gold.fact_sales;

-- Find how many items are sold
select sum(quantity) as total_items_sold from gold.fact_sales;

-- Find the total number of orders
select count(distinct order_number) from gold.fact_sales;

-- Find the total number of products
select * from gold.dim_products;
select count(distinct product_key) as total_products from gold.dim_products;

-- Find the total number of customers'
select * from gold.dim_customers;
select count(customer_key) as total_customers from gold.dim_customers;

-- Find the total number of customers that have placed an order
select count(distinct customer_key) as total_customers from gold.fact_sales;

-- Generating the report that shows all key metrics of our business (big picture of the business)
select 'Total Sales' as measure_name, sum(sales_amount) as measure_value from gold.fact_sales
union all
select 'Total Quantity' as measure_name, sum(quantity) as measure_value from gold.fact_sales
union all
select 'Average Price' as measure_name, avg(price) as measure_value from gold.fact_sales
union all
select 'Total No. of Orders' as measure_name, count(distinct order_number) as measure_value from gold.fact_sales
union all
select 'Total No. of Products' as measure_name, count(product_name) as measure_value from gold.dim_products
union all
select 'Total No. of Customers' as measure_name, count(customer_key) as measure_value from gold.dim_customers;

------------------------------------------------------Magnitude Analysis-------------------------------------------------------------------------

-- Compare the measure values by category
-- It helps us understanding the importance of different categories

-- Find total customers by country
select
country,
count(customer_key) as total_customers
from gold.dim_customers
group by country
order by total_customers desc;

-- Find total customers by gender
select
gender,
count(customer_key) as total_customers
from gold.dim_customers
group by gender
order by total_customers desc;

-- Find total products by category
select
category,
count(distinct product_name) as total_products
from gold.dim_products
group by category
order by total_products desc;

-- What is the average costs in each category?
select
category,
avg(cost) as avg_cost
from gold.dim_products
group by category
order by avg_cost desc;

-- What is the total revenue generated for each category
select 
p.category,
sum(f.sales_amount) as total_revenue
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by p.category
order by total_revenue desc; 

-- Find total revenue generated by each customer
select 
c.customer_key,
c.first_name,
c.last_name,
sum(f.sales_amount) as total_revenue
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
group by c.customer_key, c.first_name, c.last_name
order by total_revenue desc; 

-- What is the distribution of sold items across countries?
-- Low cardinalty dimensions : Dimensions with very few number of unique values
-- High cardinalty dimensions : Dimensions with large number of unique values
select 
c.country,
sum(f.quantity) as total_sold_items
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
group by c.country
order by  total_sold_items desc;

---------------------------------------Ranking Analysis-----------------------------------------------------------------------------------------------

-- Ordering the values of dimensions by measure
-- Top N performers | Bottom N performers

-- Which 5 products generate the highest revenue
select top 5
p.product_name,
sum(f.sales_amount) as total_revenue
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
group by p.product_name
order by total_revenue desc;

-- What are the 5 worst performing products in terms of sales?
select top 5
p.product_name,
sum(f.sales_amount) as total_revenue
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
group by p.product_name
order by total_revenue;

-- Using Window function
select * 
from (
select
p.product_name,
sum(f.sales_amount) as total_revenue,
row_number() over (order by sum(f.sales_amount) desc) as rank_products
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
group by p.product_name) t
where rank_products <= 5;
