-- In advanced data analytics, we basically analyse the following analysis :
-- 1) Change-Over-Time (Trends)
-- 2) Cumulative Analysis
-- 3) Performance Analysis
-- 4) Part-to-Whole (Proportional)
-- 5) Data Segmentation
-- 6) Reporting

---------------------------------------Change-Over-Time (Trends)--------------------------------------------------------------------------------

-- Analyzes how measure evolves over time
-- Helps in tracking trends and identifying seasonality in your data
-- Basically whenever we are combining any aggregated measure together with a date column or a dimension then we are analysing changes over time
-- Usually we taget facct table for this type of analysis

-- Now analyzing sales performance over time
select * from gold.fact_sales;

select
order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by order_date
order by order_date;

-- Usually in real world, we do not do analysis day wise or date wise
-- So we will now analyse the trends year wise
select
YEAR(order_date) as order_year,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by YEAR(order_date)
order by YEAR(order_date);

-- We can add more measures to gain more detailed insights
select
YEAR(order_date) as order_year,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by YEAR(order_date)
order by YEAR(order_date);

-- We can drill it down to months
select
month(order_date) as order_month,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by month(order_date)
order by month(order_date);

-- Now agggregating data by month of specific years
select
year(order_date) as order_year,
month(order_date) as order_month,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by year(order_date), month(order_date)
order by year(order_date), month(order_date);

-- Now using DATETRUNC() function 
-- DATETRUNC() function rounds a date or timestam to a specified date part
select
datetrunc(month, order_date) as order_year,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by datetrunc(month, order_date)
order by datetrunc(month, order_date);

-- Using format function
select
format(order_date, 'yyyy-MMM') as order_date,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by format(order_date, 'yyyy-MMM')
order by format(order_date, 'yyyy-MMM');

-- How many new customers were added each year?
select * from gold.dim_customers;

select * from silver.crm_cust_info;

select cst_create_date from silver.crm_cust_info
where cst_create_date is null or cst_create_date = 'n/a';

EXEC sp_help 'gold.dim_customers';

EXEC sp_help 'silver.crm_cust_info';

ALTER table silver.crm_cust_info
ALTER COLUMN cst_create_date DATE

select
datetrunc(year, create_date) as create_year,
count(customer_key) as total_customer
from gold.dim_customers
group by datetrunc(year, create_date)
order by datetrunc(year, create_date);

-----------------------------------------Cumulative Analysis---------------------------------------------------------------------------------------

-- Aggregate the data progressively over time
-- Helps to understand whether our business is growing or declining
-- We use aggregate window functions to perform cumulative analysis

-- Calculating the total sales per month
-- and the running total of sales over time
select * from gold.fact_sales;

-- Default Window Frame : Between unbounded preceding and current row
select 
order_date,
total_sales,
sum(total_sales) over (order by order_date) as running_total_sales
from
(
select
datetrunc(month,order_date) as order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by datetrunc(month,order_date)
)t;

-- Now calculating the running total year wise
-- We can use partition by for it to get the result
select 
order_date,
total_sales,
sum(total_sales) over (partition by order_date order by order_date) as running_total_sales
from
(
select
datetrunc(month,order_date) as order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by datetrunc(month,order_date)
)t;

-- We can also change the granuality from month to year wise
select 
order_date,
total_sales,
sum(total_sales) over (order by order_date) as running_total_sales
from
(
select
datetrunc(year,order_date) as order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by datetrunc(year,order_date)
)t;

-- We can also add another measure moving average
select 
order_date,
total_sales,
sum(total_sales) over (order by order_date) as running_total_sales,
avg(avg_price) over (order by order_date) as moving_average_price
from
(
select
datetrunc(year,order_date) as order_date,
sum(sales_amount) as total_sales,
avg(price) as avg_price
from gold.fact_sales
where order_date is not null
group by datetrunc(year,order_date)
)t;

---------------------------Performance Analysis------------------------------------------------------------------------------------------------

-- In performance analysis, we compare the current value to a target value
-- It helps to measure success and compare performance
-- Example: current sale - avg sale, current year sales  - previous year sales (YOY analysis), current sales - lowest sales
-- For this type of analysis we use aggregate window function - sum, avg as well as value window fumction - lead, lag

-- Analyze the yearly performance of products 
-- by comparing each product's sales to both its average sales performance and the previous year sales
with yearly_product_sales as (
select
year(f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as current_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
where order_date is not null
group by year(f.order_date), p.product_name
)
select
order_year,
product_name,
current_sales,
avg(current_sales) over (partition by product_name) as avg_sales,
current_sales - avg(current_sales) over (partition by product_name) as diff_avg,
case when current_sales - avg(current_sales) over (partition by product_name) > 0 then 'Above Avg'
     when current_sales - avg(current_sales) over (partition by product_name) < 0 then 'Below Avg'
     else 'Avg'
end as 'Avg_Change'
from yearly_product_sales
order by product_name, order_year;

-- Now also adding and comparing with previous year sales by using lag window function

with yearly_product_sales as (
select
year(f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as current_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
where order_date is not null
group by year(f.order_date), p.product_name
)
select
order_year,
product_name,
current_sales,
avg(current_sales) over (partition by product_name) as avg_sales,
current_sales - avg(current_sales) over (partition by product_name) as diff_avg,
case when current_sales - avg(current_sales) over (partition by product_name) > 0 then 'Above Avg'
     when current_sales - avg(current_sales) over (partition by product_name) < 0 then 'Below Avg'
     else 'Avg'
end as 'Avg_Change',
-- Year-over-Year Analysis
lag(current_sales) over (partition by product_name order by order_year) as py_sales,
current_sales - lag(current_sales) over (partition by product_name order by order_year) as diff_py,
case when current_sales - lag(current_sales) over (partition by product_name order by order_year) > 0 then 'Increase'
     when current_sales - lag(current_sales) over (partition by product_name order by order_year) < 0 then 'Decrease'
     else 'No Change'
end as py_change
from yearly_product_sales
order by product_name, order_year;

-- Like wise we can also do Month-over-Month Analysis by replacing year with month

----------------------------------------Part-to-Whole(Proportional Analysis)-------------------------------------------------------------------------

-- Analysis of how an individual part is performing compared to the overall
-- Allows to to understand which category has the greatest impact on the business

-- Which category contribute most to overall sales
with category_sales as (
select
p.category,
sum(f.sales_amount) as total_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by p.category
)
select
category,
total_sales,
sum(total_sales) over () as overall_sales,
concat(round((cast(total_sales as float)/sum(total_sales) over ()) * 100, 2), '%') as percentage_of_total
from category_sales
order by total_sales desc;

------------------------------------------Data Segmentation------------------------------------------------------------------------------------------

-- Groups the data base on a specific range
-- Helps in understanding the correlation between two measures
-- Basically we take two measures, convert one measure into dimension by specifying the range and then aggregate the another measure 
-- based on the specified range
-- We mainly use case when function for this type of analysis

-- Segment products into cost ranges and 
-- count how many products fall into each segment
select * from gold.dim_products;

select 
product_key,
product_name,
cost,
case when cost < 100 then 'Below 100'
     when cost between 100 and 500 then '100-500'
     when cost between 500 and 1000 then '500-1000'
     else 'Above 1000'
end as cost_range
from gold.dim_products;

-- Using the abover query, we converted the cost measure into dimension 'cost_range' by converting it into range of values
-- Now aggregating another measure base on our new dimension 'cost_range'
with product_segments as (
select 
product_key,
product_name,
cost,
case when cost < 100 then 'Below 100'
     when cost between 100 and 500 then '100-500'
     when cost between 500 and 1000 then '500-1000'
     else 'Above 1000'
end as cost_range
from gold.dim_products
)
select
cost_range,
count(product_key) as total_product_count
from product_segments
group by cost_range;

-- Group customers into 3 segments based on their spending behaviour
-- 1) VIP: Customers having atleast 12 months of history and spending is more than 5000
-- 2) Regular: Customers having atleast 12 months of history and spending is less than 5000
-- 3) New: Customers having history less than 12 months
-- Also find the total number of customers by each group

select * from gold.dim_customers;
select * from gold.fact_sales;

with customer_spending as (
select 
c.customer_key,
sum(f.sales_amount) as total_sales,
min(order_date) as first_order,
max(order_date) as last_order,
datediff(month, min(order_date), max(order_date)) as customer_lifespan
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
group by c.customer_key
),
customer_segmention as (
select
customer_key,
total_sales,
customer_lifespan,
case when customer_lifespan >= 12 and total_sales > 5000 then 'VIP'
     when customer_lifespan >= 12 and total_sales < 5000 then 'Regular'
     else 'New'
end as customer_segment
from customer_spending
)
select
customer_segment,
count(customer_key) as total_customers
from customer_segmention
group by customer_segment
order by total_customers desc;

--------------------------------------Reporting (Customer Report)---------------------------------------------------------------------------------------------------

-- Purpose: This report consolidates customer key metrics and behaviours
-- Highlights:
-- 1) Gathers essential fields such as - name, age and transaction details
-- 2) Segments customers into categories (VIP, Regular, New) and age groups
-- 3) Aggregates customers level metrics:
-- total orders
-- total sales
-- total quantity purchased
-- total products
-- customer lifespan (in months)
-- 4) Calculates valuable KPIs
-- recency (months since last order)
-- average order value
-- average monthly spend

-- Extracting the core columns, defining the scope by filtering and doing transformations (1)
select
f.order_number,
f.product_key,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
concat(c.first_name,' ',c.last_name) as customer_name,
datediff(year, c.birthdate, getdate()) as customer_age
from 
gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
where order_date is not null;

-- Now aggregating the required columns (3)
with base_query as (
select
f.order_number,
f.order_date,
f.product_key,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
concat(c.first_name,' ',c.last_name) as customer_name,
datediff(year, c.birthdate, getdate()) as customer_age
from 
gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
where order_date is not null
),
customer_aggregation as (
select 
customer_key,
customer_number,
customer_name,
customer_age,
count(order_number) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity_purchased,
count(distinct product_key) as total_products,
datediff(month, min(order_date), max(order_date)) as customer_lifespan
from base_query
group by 
customer_key,
customer_number,
customer_name,
customer_age
);

-- 2) Segments customers into categories (VIP, Regular, New) and age groups
with base_query as (
select
f.order_number,
f.order_date,
f.product_key,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
concat(c.first_name,' ',c.last_name) as customer_name,
datediff(year, c.birthdate, getdate()) as customer_age
from 
gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
where order_date is not null
),
customer_aggregation as (
select 
customer_key,
customer_number,
customer_name,
customer_age,
count(order_number) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity_purchased,
count(distinct product_key) as total_products,
max(order_date) as last_order_date,
datediff(month, min(order_date), max(order_date)) as customer_lifespan
from base_query
group by 
customer_key,
customer_number,
customer_name,
customer_age
)
select
customer_key,
customer_number,
customer_name,
customer_age,
case when customer_age < 20 then 'Under 20'
     when customer_age between 20 and 29 then '20-29'
     when customer_age between 30 and 39 then '30-39'
     when customer_age between 40 and 49 then '40-49'
     else 'Above 50'
end as age_group,
case when customer_lifespan >= 12 and total_sales > 5000 then 'VIP'
     when customer_lifespan >= 12 and total_sales < 5000 then 'Regular'
     else 'New'
end as customer_segment,
total_orders,
total_sales,
total_quantity_purchased,
total_products,
last_order_date,
customer_lifespan
from customer_aggregation;

-- 4) Calculates valuable KPIs
-- recency (months since last order)
-- average order value
-- average monthly spend
with base_query as (
select
f.order_number,
f.order_date,
f.product_key,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
concat(c.first_name,' ',c.last_name) as customer_name,
datediff(year, c.birthdate, getdate()) as customer_age
from 
gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
where order_date is not null
),
customer_aggregation as (
select 
customer_key,
customer_number,
customer_name,
customer_age,
count(order_number) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity_purchased,
count(distinct product_key) as total_products,
max(order_date) as last_order_date,
datediff(month, min(order_date), max(order_date)) as customer_lifespan
from base_query
group by 
customer_key,
customer_number,
customer_name,
customer_age
)
select
customer_key,
customer_number,
customer_name,
customer_age,
case when customer_age < 20 then 'Under 20'
     when customer_age between 20 and 29 then '20-29'
     when customer_age between 30 and 39 then '30-39'
     when customer_age between 40 and 49 then '40-49'
     else 'Above 50'
end as age_group,
case when customer_lifespan >= 12 and total_sales > 5000 then 'VIP'
     when customer_lifespan >= 12 and total_sales < 5000 then 'Regular'
     else 'New'
end as customer_segment,
total_orders,
total_sales,
total_quantity_purchased,
total_products,
last_order_date,
datediff(month, last_order_date, getdate()) as recency,
customer_lifespan,
-- computing average order value (AVO)
case when total_sales = 0 then 0
     else total_sales/total_orders
end as average_order_value,
-- computing average monthly spend
case when customer_lifespan = 0 then 0
     else total_sales/customer_lifespan
end as avg_monthly_spend
from customer_aggregation;

-- Now we will convert this entire report in a view and put in the database to share with the team or can use it to create dashboards
create view gold.report_customers as 
with base_query as (
select
f.order_number,
f.order_date,
f.product_key,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
concat(c.first_name,' ',c.last_name) as customer_name,
datediff(year, c.birthdate, getdate()) as customer_age
from 
gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
where order_date is not null
),
customer_aggregation as (
select 
customer_key,
customer_number,
customer_name,
customer_age,
count(order_number) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity_purchased,
count(distinct product_key) as total_products,
max(order_date) as last_order_date,
datediff(month, min(order_date), max(order_date)) as customer_lifespan
from base_query
group by 
customer_key,
customer_number,
customer_name,
customer_age
)
select
customer_key,
customer_number,
customer_name,
customer_age,
case when customer_age < 20 then 'Under 20'
     when customer_age between 20 and 29 then '20-29'
     when customer_age between 30 and 39 then '30-39'
     when customer_age between 40 and 49 then '40-49'
     else 'Above 50'
end as age_group,
case when customer_lifespan >= 12 and total_sales > 5000 then 'VIP'
     when customer_lifespan >= 12 and total_sales < 5000 then 'Regular'
     else 'New'
end as customer_segment,
total_orders,
total_sales,
total_quantity_purchased,
total_products,
last_order_date,
datediff(month, last_order_date, getdate()) as recency,
customer_lifespan,
-- computing average order value (AVO)
case when total_sales = 0 then 0
     else total_sales/total_orders
end as average_order_value,
-- computing average monthly spend
case when customer_lifespan = 0 then 0
     else total_sales/customer_lifespan
end as avg_monthly_spend
from customer_aggregation;

select * from gold.report_customers;

----------------------------------------------Reporting (Product Report)-----------------------------------------------------------------------------------------

-- Purpose: This report consolidates key product metrics and behaviours
-- Highlights:
-- 1) Gathers essential fields such as product name, category, subcategory and cost
-- 2) Segment products by revenue to identify high performers, mid range or low performers
-- 3) Aggregates product level metrics:
-- Total Orders
-- total sales
-- total quantity sold
-- total customers (unique)
-- customer lifespan (in months)
-- 4) Calculates valuable KPIs:
-- Recency (months since last sale)
-- Average Order Revenue (AOR)
-- Average Monthly Revenue

select * from gold.dim_products;
select * from gold.fact_sales;

create view gold.report_products as 
with core_query as (
select 
p.product_key,
f.customer_key,
p.category,
p.subcategory,
p.product_name,
p.cost,
f.order_number,
f.order_date,
f.sales_amount,
f.quantity
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where order_date is not null
),
product_aggregation as (
select
product_key,
product_name,
category,
subcategory,
cost,
max(order_date) as last_order_date,
count(distinct order_number) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity_sold,
count(distinct customer_key) as total_customers,
datediff(month, min(order_date), max(order_date)) as customer_lifespan_with_website,
round(avg(cast(sales_amount as float)/nullif(quantity,0)),1) as average_selling_price
from core_query
group by
product_key,
product_name,
category,
subcategory,
cost
)
select 
product_key,
product_name,
category,
subcategory,
cost,
last_order_date,
case when total_sales > 50000 then 'High-Performer'
     when total_sales >= 10000 then 'Mid-Range'
     else 'Low-Performer'
end as product_segments,
total_quantity_sold,
total_customers,
average_selling_price,
customer_lifespan_with_website,
datediff(month, last_order_date, getdate()) as Recency,
case when total_orders = 0 then 0
     else total_sales/total_orders
end as average_order_revenue,
case when customer_lifespan_with_website = 0 then 0
     else total_sales/customer_lifespan_with_website
end as average_monthly_revenue
from product_aggregation;

select * from gold.report_products;
