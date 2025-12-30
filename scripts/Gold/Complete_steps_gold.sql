-- For creating data model, as per our data integration model, we have identified which table belongs to which category - customer, product and sales transaction
-- Therefore we will now join together all the customer category tables first and then other category tables
-- Use left join always and keep one table as a master table to join it with other tables using left join
-- Here, we have silver.crm_cust_info as the master table (from data integration model diagram)
select * from silver.crm_cust_info;
select * from silver.erp_cust_az12;
select * from silver.erp_loc_a101;

select
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.cntry
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid;

-- After joining the tables, check if any duplicates were introduced by the join logic
-- On executing the below query, we will see no data, means there were no duplicates introduced while joining the tables
select cst_id, count(*) from
(select
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.cntry
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid)t group by cst_id
having count(*) > 1;

-- If we see the result from the joined tables query, we can see that there are two columns: cst_gndr and gen giving gender information
-- We can deal with such type of scenarios using data integration
-- On executing the below query, we see that both the gender columns from the tables are not in sync and giving different gender informations
-- as well as null value also, this null gets introduced due to joining of the tables when sql finds no match
-- Since one gender column: cst_gndr is from crm source and another gender column: gen is from erp source
-- therefore we now need to go to source experts and understand from them that which source is the master source for customer data
-- Suppose source experts tells us that crm is the master source for customer data , it will mean that we will consider crm source information
-- as the most relevant information (cst_gndr) for customer data
select distinct
ci.cst_gndr,
ca.gen
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid
order by 1,2;

-- Now since crm source is the master or main source of information for customer details,
-- therefore we will give preference to the cst_gndr column as it is from crm source and if there is n/a value in it then we will give 
-- preference to the gen column from erp source
select distinct
ci.cst_gndr,
ca.gen,
case when ci.cst_gndr != 'n/a' then ci.cst_gndr -- CRM is the master for customer gender info
     else coalesce(ca.gen, 'n/a') -- Handling null value by usinf coalesce function, it will use erp gender data in case there is null value in ci.cst_gndr
end as new_gen
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid
order by 1,2;

-- Now applying the above gender column logic obtained by doing data integration into our main join query
select
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
case when ci.cst_gndr != 'n/a' then ci.cst_gndr -- CRM is the master for customer gender info
     else coalesce(ca.gen, 'n/a') -- Handling null value by usinf coalesce function, it will use erp gender data in case there is null value in ci.cst_gndr
end as new_gen,
ci.cst_create_date,
ca.bdate,
la.cntry
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid;

-- Now renaming the columns to more friendly and meaningful names
select
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
case when ci.cst_gndr != 'n/a' then ci.cst_gndr -- CRM is the master for customer gender info
     else coalesce(ca.gen, 'n/a') -- Handling null value by usinf coalesce function, it will use erp gender data in case there is null value in ci.cst_gndr
end as gender,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid;

-- Sorting the columns into logical groups to improve readability
select
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
case when ci.cst_gndr != 'n/a' then ci.cst_gndr -- CRM is the master for customer gender info
     else coalesce(ca.gen, 'n/a') -- Handling null value by usinf coalesce function, it will use erp gender data in case there is null value in ci.cst_gndr
end as gender,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid;

-- Now as we know that a table or object is called dimension table when it contains mostly descriptive information
-- Therefore, from our joined final customer table, we can say this table is a customer dimension table as it mostly contains customer details only
-- Whenever we create a new dimension table, then we also create its primary key, know as surrogate key 
-- Nobody from business know about this surrogate key, it is only for our purpose to create our data model and we do not have to depend on the source system primary key
-- Now creating a surrogate key : customer_key for our dimension table by using row_number window function
select
ROW_NUMBER() over (order by cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
case when ci.cst_gndr != 'n/a' then ci.cst_gndr -- CRM is the master for customer gender info
     else coalesce(ca.gen, 'n/a') -- Handling null value by usinf coalesce function, it will use erp gender data in case there is null value in ci.cst_gndr
end as gender,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid;

-- Since all the objects in the gold layer should be virtual ones, therefore we will create a view
create view gold.dim_customers as 
select
ROW_NUMBER() over (order by cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
case when ci.cst_gndr != 'n/a' then ci.cst_gndr -- CRM is the master for customer gender info
     else coalesce(ca.gen, 'n/a') -- Handling null value by usinf coalesce function, it will use erp gender data in case there is null value in ci.cst_gndr
end as gender,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid;

select * from gold.dim_customers;

-- Checking the Data Quality of the dim_customers
select distinct gender from gold.dim_customers;

-----------------------------------------dim_products------------------------------------------------------------------------------------------------

select * from silver.crm_prd_info;

-- Our crm_prd_info table contains both historical as well as latest information of the products
-- It depends on our reporting requirements whether we want to include historical data or not in a dimension table
-- Here, for dim_products table, we will only include the latest or current product data, means only products whose prd_end_dt is null
select
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt
from silver.crm_prd_info pn
where prd_end_dt is null;

-- Now as per our data integration model diagram, we need to join crm_prd_info table with erp_px_cat_g1v2 table using left join
-- CRM is again our master data source here
select
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pc.cat,
pc.subcat,
pc.maintenance
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null; -- Filter out all historical data

-- Now checking whether our prd_key is still unique or not
-- The output from the below query gives no data, means join didn't introduced any duplicates in the prd_key
select prd_key, count(*) from (
select
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pc.cat,
pc.subcat,
pc.maintenance
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null
)t group by prd_key 
having count(*) > 1;

-- We also do not have any columns in the dataset which shows us same type of informaation, therefore data integration is not required
-- Now sorting columns to improve readability
select
pn.prd_id,
pn.prd_key,
pn.prd_nm,
pn.cat_id,
pc.cat,
pc.subcat,
pc.maintenance,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null;

-- Now renaming columns to the friendly and meaningful names
select
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null;

-- Since our final obtained table is a products dimension object, therefore we will now create surrogate key to connect our data model
select
ROW_NUMBER() over (order by pn.prd_start_dt, pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null;

-- Now creating view : dim_products in the gold layer
create view gold.dim_products as 
select
ROW_NUMBER() over (order by pn.prd_start_dt, pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null;

select * from gold.dim_products;

---------------------------------------gold.fact_sales_details---------------------------------------------------------------------------------------

select * from silver.crm_sales_details;

-- Now we will use dimensions surrogate keys instead of sls_prd_key and sls_cust_id to easily connect facts with dimensions
-- We also follow a schema in the fact table to sort the columns : Dimension key - Dates - Measures or metrics
select
fs.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
fs.sls_order_dt as order_date,
fs.sls_ship_dt as shipping_date,
fs.sls_due_dt as due_date,
fs.sls_sales as sales_amount,
fs.sls_quantity as quantity,
fs.sls_price as price
from silver.crm_sales_details fs
left join gold.dim_products pr
on fs.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on fs.sls_cust_id = cu.customer_id;

-- Now creating the fact view in the gold layer

create view gold.fact_sales as 
select
fs.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
fs.sls_order_dt as order_date,
fs.sls_ship_dt as shipping_date,
fs.sls_due_dt as due_date,
fs.sls_sales as sales_amount,
fs.sls_quantity as quantity,
fs.sls_price as price
from silver.crm_sales_details fs
left join gold.dim_products pr
on fs.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on fs.sls_cust_id = cu.customer_id;

select * from gold.fact_sales;

-- Now check if all dimension tables can successfully join to the fact table (Foreign key integrity (dimensions))
-- On running the below query, we won't see any data, means dim_customers can be easily joined with fact_sales table using surrogate key
select * from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
where c.customer_key is null;

-- Similarly checking for dim_products table
-- On running the below query, we won't see any data, means dim_products can be easily joined with fact_sales table using surrogate key
select * from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
left join gold.dim_products p
on p.product_key = f.product_key
where p.product_key is null;

