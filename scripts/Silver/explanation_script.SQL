exec bronze.load_bronze;
-- Data Itegration Modelling
select * from bronze.crm_cust_info;
select count(*) from bronze.crm_cust_info;
select * from bronze.crm_prd_info;
select * from bronze.crm_sales_details;

select * from bronze.erp_cust_az12;
select * from bronze.crm_cust_info;
select * from bronze.erp_loc_a101;

select * from bronze.erp_px_cat_g1v2;
select * from bronze.crm_prd_info;

-----------------------------------------------bronze.crm_cust_info--------------------------------------------------------------------------

-- Check for Null or duplicates in the primary key

select * from bronze.crm_cust_info;

select cst_id, count(*) from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null;

-- Now checking for a single value and trying to identifying the latest value
select * from bronze.crm_cust_info
where cst_id = 29466;

-- Using a window function to rank the cst_id = 29466 on the basis of cst_create_date with the latest cst_create_date as rank 1 to pick the 
-- latest or newest cst_create_date for cst_id = 29466 as many other old duplicate values are present for cst_id = 29466 in the dataset.
select *,
ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info
where cst_id = 29466;

-- Now Getting the latest data by first ranking all the data as per cst_create_date(inner query) and then selecting only rows having 
-- flag_last = 1 to get the latest or most recent data from the dataset and removing all the duplicate primary keys from the dataset.
select
*
from (
select *,
ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info) t where flag_last = 1;

-- Now checking whether duplicate still exists or not for cst_id = 29466
-- On running the query we will see that only single and latest date row exists for cst_id = 29466
-- This makes us sure that we have successfully removed all the duplicates primary key and only latest data primary key is left
select
*
from (
select *,
ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info) t where flag_last = 1 and cst_id = 29466;

-- Data Quality Check: Checking for the unwanted spaces in the column: cst_firstname
-- Below query will give us cst_firstname having unwanted spaces
-- We can check this data quality issue for all the string columns in our dataset
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname);

-- Data Quality Check: Checking for the unwanted spaces in the column: cst_lastname
-- Below query will give us cst_lastname having unwanted spaces
select cst_lastname
from bronze.crm_cust_info
where cst_lastname != TRIM(cst_lastname);

-- Data Quality Check: Checking for the unwanted spaces in the column: cst_marital_status
-- Below query will give us cst_marital_status having unwanted spaces
select cst_marital_status
from bronze.crm_cust_info
where cst_marital_status != TRIM(cst_marital_status);

-- Data Quality Check: Checking for the unwanted spaces in the column: cst_gndr
-- Below query will give us cst_gndr having unwanted spaces
select cst_gndr
from bronze.crm_cust_info
where cst_gndr != TRIM(cst_gndr);

-- From the above queries, we found that cst_firstname and cst_lastname columns are having data quality issues (unwanted spaces)
-- So to remove the unwanted spaces from these 2 columns we will use TRIM fumction
select
  cst_id,
  cst_key,
  TRIM(cst_firstname) as cst_firstname,
  TRIM(cst_lastname) as cst_lastname,
  cst_marital_status,
  cst_gndr,
  cst_create_date
from (
select *,
ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info) t where flag_last = 1;

-- Data Standardization & Consistency
select distinct cst_gndr
from bronze.crm_cust_info;

-- Using case statement to make cst_gndr column more readable and standardized
select
  cst_id,
  cst_key,
  TRIM(cst_firstname) as cst_firstname,
  TRIM(cst_lastname) as cst_lastname,
  cst_marital_status,
  case when UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
       when UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
       else 'n/a'
  end as cst_gndr,
  cst_create_date
from (
select *,
ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info) t where flag_last = 1;

-- Data Standardization & Consistency
select distinct cst_marital_status
from bronze.crm_cust_info;

-- Doing also for cst_marital_status column
select
  cst_id,
  cst_key,
  TRIM(cst_firstname) as cst_firstname,
  TRIM(cst_lastname) as cst_lastname,
  case when UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
       when UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
       else 'n/a'
  end as cst_marital_status,
  case when UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
       when UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
       else 'n/a'
  end as cst_gndr,
  cst_create_date
from (
select *,
ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info) t where flag_last = 1;

-- Checking the datatype of all the columns of crm_cust_info table in bronze layer
EXEC sp_help 'bronze.crm_cust_info';

-- Changing the dataypt of column: cst_create_date from nvarchar to datetime
ALTER TABLE silver.crm_cust_info
ALTER COLUMN cst_create_date nvarchar(50);

-- First Truncating the silver.crm_cust_info before inserting data to prevent the duplication of the data
truncate table silver.crm_cust_info;

-- Now Inserting the clean and standardized data of bronze.crm_cust_info into silver layer
insert into silver.crm_cust_info( 
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date)

select
  cst_id,
  cst_key,
  TRIM(cst_firstname) as cst_firstname,
  TRIM(cst_lastname) as cst_lastname,
  case when UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
       when UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
       else 'n/a'
  end as cst_marital_status,
  case when UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
       when UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
       else 'n/a'
  end as cst_gndr,
  cst_create_date
from (
select *,
ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info) t where flag_last = 1;

select * from silver.crm_cust_info;
select * from bronze.crm_cust_info;

-- Data Qaulaity Checks for or silver.crm_cust_info table.
select cst_id, count(*) from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null;

select * from silver.crm_cust_info 
where cst_id is null;

DELETE FROM silver.crm_cust_info
WHERE cst_id is null;

select cst_firstname
from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname);

select cst_gndr
from silver.crm_cust_info
where cst_gndr != TRIM(cst_gndr);

select distinct cst_gndr
from silver.crm_cust_info;

select distinct cst_marital_status
from silver.crm_cust_info;

-----------------------------------------------bronze.crm_prd_info-------------------------------------------------------------------------

select * from bronze.crm_prd_info;

-- Check for nulls or deplicates in the primary key
select prd_id, count(*) from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;

-- We can see in the prd_key column that it contains lot of information
-- Therefore, we are applying substring function to extract first 5 characters of the prd_key and name it as cat_id in a separate columns
-- We have also seen during data integration diagram that we would need cat_id to join the table bronze.erp_px_cat_g1v2
select 
 prd_id,
 prd_key,
 SUBSTRING(prd_key, 1, 5) as cat_id, 
 prd_nm,
 prd_cost,
 prd_line,
 prd_start_dt,
 prd_end_dt
from bronze.crm_prd_info;

-- Now checking all the unique or distinct ids of the id column from bronze.erp_px_cat_g1v2 table
-- On seeing the result, we can see that ids have '_' in it whereas cat_id column from bronze.crm_prd_info table has '-' in it
select distinct id
from bronze.erp_px_cat_g1v2;

-- Therefor we will also change '-' to '_' in the cat_id column by using replace function
-- from our bronze.crm_prd_info table so that it coluld be joined with bronze.erp_px_cat_g1v2 table
select 
 prd_id,
 prd_key,
 replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id, 
 prd_nm,
 prd_cost,
 prd_line,
 prd_start_dt,
 prd_end_dt
from bronze.crm_prd_info;

-- Now querying the cat_id column of bronze.crm_prd_info which are not in the id column of bronze.erp_px_cat_g1v2
select 
 prd_id,
 prd_key,
 replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id, 
 prd_nm,
 prd_cost,
 prd_line,
 prd_start_dt,
 prd_end_dt
from bronze.crm_prd_info
where replace(SUBSTRING(prd_key, 1, 5), '-', '_') not in 
(select distinct id from bronze.erp_px_cat_g1v2);

-- Now we are extracting prd_key part to create prd_key 
-- which can help in joining bronze.crm_prd_info with the bronze.crm_sales_details table
-- Since there is no fixed length of the prd_key, therefore we have used len function to make the extraction dynamic
select 
 prd_id,
 prd_key,
 replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
 SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
 prd_nm,
 prd_cost,
 prd_line,
 prd_start_dt,
 prd_end_dt
from bronze.crm_prd_info;

-- Just seeing the sls_prd_key column from bronze.crm_sales_details table
-- which will help us join it with the prd_key column from bronze.crm_prd_info table
select distinct sls_prd_key from bronze.crm_sales_details;

-- Checking Data Quality: Unwanted Spaces check 
select prd_nm 
from bronze.crm_prd_info
where prd_nm != TRIM(prd_nm);

-- Now checking for any negative or null values in the prd_cost column
-- We will see in the output that prd_cost column have few null values
select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null;

-- Now replacing null values in the prd_cost column with zero values by using isnull function
-- isnull function basically first checks whether there is any null value present in the specified column and if null values is present,
-- it then replaces that null values with the specified value
select 
 prd_id,
 prd_key,
 replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
 SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
 prd_nm,
 ISNULL(prd_cost, 0) as prd_cost,
 prd_line,
 prd_start_dt,
 prd_end_dt
from bronze.crm_prd_info;

-- Now doing the data quality check for the next column: prd_line
select distinct prd_line
from bronze.crm_prd_info;

-- Since we have only abbreviations in the prd_line column, 
-- therefore we will map them with the proper names by discussing with the source system expert or other stakeholders
select 
 prd_id,
 prd_key,
 replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
 SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
 prd_nm,
 ISNULL(prd_cost, 0) as prd_cost,
 case when UPPER(TRIM(prd_line)) = 'M' then 'Mountain'
      when UPPER(TRIM(prd_line)) = 'R' then 'Road'
      when UPPER(TRIM(prd_line)) = 'S' then 'Other Sales'
      when UPPER(TRIM(prd_line)) = 'T' then 'Touring'
      else 'n/a'
 end as prd_line,
 prd_start_dt,
 prd_end_dt
from bronze.crm_prd_info;

-- Now checking the invalid dates
select * from bronze.crm_prd_info
where prd_end_dt < prd_start_dt;

-- From the result we can see that there are many dates which are having small end date than start date
-- Therefore to handle such cases, we can take data for a product and see in the excel how can we apporach such issues
-- There are basically 2 ways to handle such issue, first we can try to switch end date and start date, but on doing that we see that
-- there is overlap happening in the dates for the next record, so we cannot use this apporoach here
-- 2nd approach is that we can use the end date of a product as the start date of the next record and reduce the end date of the previous 
-- record by 1
-- We can use lead and lag functions to do this
-- we can also have null values in the end date in the last record of a product as it means that that product is currently being used
-- Now what we can do is that, we can use 2nd approach first on one product and test and if all good then apply it on all the products or records
-- On executing the below test query for two prd_key we can see that the end date of the current record becomes the start date of the next record for
-- a particular prd_key, it means our this approach is working fine
select
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt) as prd_end_dt_test
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

-- Now to avoid any kind of overlapping between the start date of the current record and end date of the previous record, we can 
-- decrease the end date of the previous record by 1
select
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt) - 1 as prd_end_dt_test
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

-- Now applying this logic in our main query
select 
 prd_id,
 prd_key,
 replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
 SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
 prd_nm,
 ISNULL(prd_cost, 0) as prd_cost,
 case when UPPER(TRIM(prd_line)) = 'M' then 'Mountain'
      when UPPER(TRIM(prd_line)) = 'R' then 'Road'
      when UPPER(TRIM(prd_line)) = 'S' then 'Other Sales'
      when UPPER(TRIM(prd_line)) = 'T' then 'Touring'
      else 'n/a'
 end as prd_line,
 prd_start_dt,
 LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt) - 1 as prd_end_dt
from bronze.crm_prd_info;

-- Now in our result table, we can see that there is also time info which is zero and we do not have that info, so we can cast
-- prd_start_dt and prd_end_dt columns as only date to remove time part
select 
 prd_id,
 prd_key,
 replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
 SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
 prd_nm,
 ISNULL(prd_cost, 0) as prd_cost,
 case when UPPER(TRIM(prd_line)) = 'M' then 'Mountain'
      when UPPER(TRIM(prd_line)) = 'R' then 'Road'
      when UPPER(TRIM(prd_line)) = 'S' then 'Other Sales'
      when UPPER(TRIM(prd_line)) = 'T' then 'Touring'
      else 'n/a'
 end as prd_line,
 cast(prd_start_dt as date) as prd_start_dt,
 cast(LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt) - 1 as date) as prd_end_dt
from bronze.crm_prd_info;

-- Now in our silver.crm_prd_info table schema, we do not have cat_id column
-- and also we have made changes in the datatype of prd_start_date and prd_end_dt columns
-- therefore we need to make changes in our silver.crm_prd_info table schema so that we can 
-- load cleaned and transformed bronze.crm_prd_info data into our silver layer and also add dwh_create_date column in the silver layer

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id          INT,
    cat_id          NVARCHAR(50),
    prd_key         NVARCHAR(50),
    prd_nm          NVARCHAR(50),
    prd_cost        INT,
    prd_line        NVARCHAR(50),
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Now inserting the cleaned data from bronze.crm_prd_info into our silver.crm_prd_info
insert into silver.crm_prd_info (
 prd_id,
 cat_id,
 prd_key,
 prd_nm,
 prd_cost,
 prd_line,
 prd_start_dt,
 prd_end_dt
)
select 
 prd_id,
 replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
 SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
 prd_nm,
 ISNULL(prd_cost, 0) as prd_cost,
 case when UPPER(TRIM(prd_line)) = 'M' then 'Mountain'
      when UPPER(TRIM(prd_line)) = 'R' then 'Road'
      when UPPER(TRIM(prd_line)) = 'S' then 'Other Sales'
      when UPPER(TRIM(prd_line)) = 'T' then 'Touring'
      else 'n/a'
 end as prd_line,
 cast(prd_start_dt as date) as prd_start_dt,
 cast(LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt) - 1 as date) as prd_end_dt
from bronze.crm_prd_info;

select * from silver.crm_prd_info;

-- Now performing DQ checks for our silver.crm_prd_info table

-- Check for nulls or deplicates in the primary key
select prd_id, count(*) from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;

-- Checking Data Quality: Unwanted Spaces check 
select prd_nm 
from silver.crm_prd_info
where prd_nm != TRIM(prd_nm);

-- Now checking for any negative or null values in the prd_cost column
-- We will see in the output that prd_cost column have few null values
select prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null;

-- Now doing the data quality check for the next column: prd_line
select distinct prd_line
from silver.crm_prd_info;

------------------------------------------------------bronze.crm_sales_details---------------------------------------------------------------------

select * from bronze.crm_sales_details;

-- Checking DQ of sls_ord_num column by checking unwanted spaces first
select * from bronze.crm_sales_details
where sls_ord_num != TRIM(sls_ord_num);

-- Now as per our data integration model, we will be joining crm_sales_details and crm_prd_info on prd_key column
-- therefore checking if everything is alright to join
-- From the below query result, we won't see any issue, means that we can easily use sls_prd_key column from crm_sales_details table to 
-- join with prd_info column from crm_prd_info table
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info);

-- Now similarly checking sls_cust_id column from crm_sales_details table to be joined with cst_id column from crm_cust_info table
-- From the below query result, we won't see any issue, means that we can easily use sls_cust_id column from crm_sales_details table to 
-- join with cst_id column from crm_cust_info table
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info);

-- Now checking for invalid dates
select sls_order_dt from bronze.crm_sales_details
where sls_order_dt <= 0;

-- Since from the crm_sales_details table, we can see that date columns are not date type columns, therefore we need to cast them to date columns
-- But as per the above query, we got that sls_order_dt colums has 0 value which is invalid for date column
-- therefore before converting sls_order_dt column to date column, we will first convert all the 0 values in it to null values by using 
-- nullif function
select 
nullif(sls_order_dt,0) as sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0;

-- Applying more logic to check for invalid dates
-- Since column: sls_ord_dt is having 8 digit in our original dataset, therefor we are checking that length should be 8 only
-- Also we want to check if our dataset contains an outlier kind of date
select 
nullif(sls_order_dt,0) as sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 or len(sls_order_dt) != 8 or sls_order_dt > 20500101;

-- Now cleaning the sls_order_dt column by using above logic
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
     else cast(cast(sls_order_dt as varchar) as date)
end as sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details;

-- Now checking DQ for sls_ship_dt
select 
nullif(sls_ship_dt,0) as sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt <= 0 or len(sls_ship_dt) != 8 or sls_ship_dt > 20500101;

-- We didn't find any issue with sls_ship_dt column but as a preventive measure for future, we can apply same conditions for sls_ship_dt
-- as well like what we did for sls_order_dt column
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
     else cast(cast(sls_order_dt as varchar) as date)
end as sls_order_dt,
case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
     else cast(cast(sls_ship_dt as varchar) as date)
end as sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details;

-- Now applying similar DQ check for sls_due_dt
select 
nullif(sls_due_dt,0) as sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0 or len(sls_due_dt) != 8 or sls_due_dt > 20500101;

-- We didn't find any issue with sls_due_dt column as well but as a preventive measure for future, we can apply same conditions for sls_due_dt
-- as well like what we did for sls_ship_dt column
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
     else cast(cast(sls_order_dt as varchar) as date)
end as sls_order_dt,
case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
     else cast(cast(sls_ship_dt as varchar) as date)
end as sls_ship_dt,
sls_due_dt,
case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
     else cast(cast(sls_due_dt as varchar) as date)
end as sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details;

-- We know that sls_order_dt must alsways be earlier than sls_ship_dt or sls_due_dt
-- Now checking for this in the below query
-- No such issue is found
select 
sls_order_dt
from bronze.crm_sales_details
where sls_order_dt >= sls_ship_dt or sls_order_dt >= sls_due_dt;

-- We have a business rule: total sales = quantity * price and sls_sales, sls_quantity or sls_price cannot have negative, zero or null values
-- Check data consistency between sales, quantity and price
-- Sales = Quantity * Price
-- Values must not be null, zero or negative
-- From the output of the below query, we will see bad data in all the three columns
-- Now this type of issue can be either solved by going to the source or business expert and ask them to fix it as data is wrongly coming from the source only
-- Otherwise we can try by ourself to improve the data quality by getting the support from the source or business experts

select distinct
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price;

-- Rule: If sales is negative, zero or null, then derive it using quantity and price
-- Rule: If price is zero or null, calculate it using sales and quantity
-- Rule: If price is negative, convert it to a positive value
-- Now as per the output of the below query, we will get much better data in the sls_sales, sls_quantity, and sls_price columns
select distinct
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,
case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price) then sls_quantity * abs(sls_price)
     else sls_sales
end as sls_sales,
case when sls_price is null or sls_price <= 0 then sls_sales/nullif(sls_quantity,0)
     else sls_price
end as sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price;

-- Now appying the above business rules and logic to our main logic to clean our crm_sales_details table
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
     else cast(cast(sls_order_dt as varchar) as date)
end as sls_order_dt,
case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
     else cast(cast(sls_ship_dt as varchar) as date)
end as sls_ship_dt,
sls_due_dt,
case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
     else cast(cast(sls_due_dt as varchar) as date)
end as sls_due_dt,
case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price) then sls_quantity * abs(sls_price)
     else sls_sales
end as sls_sales,
sls_quantity,
case when sls_price is null or sls_price <= 0 then sls_sales/nullif(sls_quantity,0)
     else sls_price
end as sls_price
from bronze.crm_sales_details;

-- Now compare and update the datatype in our silver.crm_sales_details ddl script so that we can load our cleaned data smoothly
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num     NVARCHAR(50),
    sls_prd_key     NVARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Now inserting our cleaned data into silver.crm_sales_details table
insert into silver.crm_sales_details (
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
     else cast(cast(sls_order_dt as varchar) as date)
end as sls_order_dt,
case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
     else cast(cast(sls_ship_dt as varchar) as date)
end as sls_ship_dt,
case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
     else cast(cast(sls_due_dt as varchar) as date)
end as sls_due_dt,
case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price) then sls_quantity * abs(sls_price)
     else sls_sales
end as sls_sales,
sls_quantity,
case when sls_price is null or sls_price <= 0 then sls_sales/nullif(sls_quantity,0)
     else sls_price
end as sls_price
from bronze.crm_sales_details;

select * from silver.crm_sales_details;

-- Now performing DQ checks for silver.crm_sales_details table
select distinct
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price;

-------------------------------------------------------bronze.erp_cust_az12-----------------------------------------------------------------

select * from bronze.erp_cust_az12;

-- As per our data integration model, we can join erp_cust_az12 table using cid column with crm_cust_info table on cst_key column
-- Now checking both the tables together
-- From the below output tables, we can see that there is extra characters - 'NAS' before cid column from bronze.erp_cust_az12 table
select * from bronze.erp_cust_az12;
select * from silver.crm_cust_info;

-- Now applying logic to remove 'NAS' characters from cid column
select 
cid,
case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
     else cid
end as cid,
bdate,
gen
from bronze.erp_cust_az12;

-- Now checking whether we can use cid column from erp_cust_az12 table to join with crm_cust_info table using cst_key column
-- We won't see any result from the below query, means we can now easily join these two tables
select 
cid,
case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
     else cid
end as cid,
bdate,
gen
from bronze.erp_cust_az12
where case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
     else cid
end not in (select cst_key from silver.crm_cust_info);

-- Now identifying out-of-range dates
-- From the below query output, we can see there are many out of range dates in our dataset
select distinct
bdate
from bronze.erp_cust_az12
where bdate <= '1924-01-01' or bdate > getdate();



select 
case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
     else cid
end as cid,
case when bdate > GETDATE() then null
     else bdate
end as bdate,
gen
from bronze.erp_cust_az12;

-- Data Consistency and Standardisation
select distinct
gen
from bronze.erp_cust_az12;

-- Applying and testing our logic to replace null and blank values with n/a and M with Male and F with Female 
select distinct
gen,
case when upper(trim(gen)) in ('M','MALE') then 'Male'
     when upper(trim(gen)) in ('F','FEMALE') then 'Female'
     else 'n/a'
end as gen
from bronze.erp_cust_az12;

-- Now applying the tested logic to our entire bronze.erp_cust_az12 table for gen column to clean our table
select 
case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
     else cid
end as cid,
case when bdate > GETDATE() then null
     else bdate
end as bdate,
case when upper(trim(gen)) in ('M','MALE') then 'Male'
     when upper(trim(gen)) in ('F','FEMALE') then 'Female'
     else 'n/a'
end as gen
from bronze.erp_cust_az12;

-- Now inserting our cleaned and transformed erp_cust_az12 table into silver.erp_cust_az12 table
insert into silver.erp_cust_az12 (
cid,
bdate,
gen
)
select 
case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
     else cid
end as cid,
case when bdate > GETDATE() then null
     else bdate
end as bdate,
case when upper(trim(gen)) in ('M','MALE') then 'Male'
     when upper(trim(gen)) in ('F','FEMALE') then 'Female'
     else 'n/a'
end as gen
from bronze.erp_cust_az12;

-- Now performing DQ checks for our silver.erp_cust_az12 table
select distinct
bdate
from silver.erp_cust_az12
where bdate <= '1924-01-01' or bdate > getdate();

-- Checking gen column
select distinct
gen
from silver.erp_cust_az12;

------------------------------------------bronze.erp_loc_a101---------------------------------------------------------------------------------

select * from bronze.erp_loc_a101;

-- As per our data integration model, we will use cid column from erp_loc_a101 table to join with crm_cust_info table using cst_key column
-- Therefore cid and cst_key columns must be matching to join the tables
-- From the below query results, we can see that cid column contains '-' in it whereas cst_key column does not
select cst_key from silver.crm_cust_info;
select * from bronze.erp_loc_a101;

-- Therefore we will remove '-' from cid column so that it becomes matching with cst_column
select 
replace(cid, '-', '') as cid,
cntry
from bronze.erp_loc_a101;

-- Now checking whether there is any value from cid column of erp_loc_a101 table which is not present in cst_key column of crm_cust_info table
-- Which will cause hindrance in joining both the tables
-- On executing the below query, we will find no result, means we can now smoothly use both the columns to join both the tables
select 
replace(cid, '-', '') as cid,
cntry
from bronze.erp_loc_a101
where replace(cid, '-', '') not in (select cst_key from silver.crm_cust_info);

-- Checking Data Standardization and Data Consistency
-- From the below query, we can see that the DQ for cntry column is not good
select distinct
cntry
from bronze.erp_loc_a101
order by cntry;

-- Now applying data transformation logics for cntry column
select DISTINCT
cntry as old_cntry,
case when UPPER(TRIM(cntry)) in ('US', 'USA') then 'United States'
     when UPPER(TRIM(cntry)) = 'DE' then 'Germany'
     when UPPER(TRIM(cntry)) is null or cntry = '' then 'n/a'
     else TRIM(cntry)
end as cntry
from bronze.erp_loc_a101
order by old_cntry;

-- Now applying above cntry column transformation login in our entire dataset
select 
replace(cid, '-', '') as cid,
case when UPPER(TRIM(cntry)) in ('US', 'USA') then 'United States'
     when UPPER(TRIM(cntry)) = 'DE' then 'Germany'
     when UPPER(TRIM(cntry)) is null or cntry = '' then 'n/a'
     else TRIM(cntry)
end as cntry
from bronze.erp_loc_a101;

-- Now inserting our cleaned and transformed erp_loc_a101 table into silver.erp_loc_a101 table
insert into silver.erp_loc_a101 (
cid,
cntry
)
select 
replace(cid, '-', '') as cid,
case when UPPER(TRIM(cntry)) in ('US', 'USA') then 'United States'
     when UPPER(TRIM(cntry)) = 'DE' then 'Germany'
     when UPPER(TRIM(cntry)) is null or cntry = '' then 'n/a'
     else TRIM(cntry)
end as cntry
from bronze.erp_loc_a101;

select * from silver.erp_loc_a101;

-- Now performing DQ check for our silver.erp_loc_a101 table
select distinct
cntry
from silver.erp_loc_a101
order by cntry;

----------------------------------------------------bronze.erp_px_cat_g1v2--------------------------------------------------------------------------

select * from bronze.erp_px_cat_g1v2;

-- As per our data integration model, id column from erp_px_cat_g1v2 table will be used to join with crm_prd_info table using cat_id column
-- From the below query results, we can see that id and cat_id columns match, therefore we do not need to make any changes and we will be 
-- able to join both the tables smoothly
select * from bronze.erp_px_cat_g1v2;
select cat_id from silver.crm_prd_info;

-- Now checking for unwanted spaces in all the column
select
*
from bronze.erp_px_cat_g1v2
where cat != TRIM(cat) or subcat != TRIM(subcat) or maintenance != TRIM(maintenance);

-- Checking data standardization and consistency
select distinct
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2;

-- Now inserting erp_px_cat_g1v2 table into silver.erp_px_cat_g1v2 table
insert into silver.erp_px_cat_g1v2 (
id,
cat,
subcat,
maintenance
)
select * from bronze.erp_px_cat_g1v2;

select * from silver.erp_px_cat_g1v2;
