/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

alter table bronze.crm_cust_info
alter column cst_create_date nvarchar(50);

create or alter procedure bronze.load_bronze as 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
	    SET @batch_start_time = GETDATE();
		print('====================================================');
		print('Loading Bronze Layer');
		print('====================================================');

		print('----------------------------------------------------');
		print('Loading CRM Tables');
		print('----------------------------------------------------');

		SET @start_time = GETDATE();
		print('>>Truncating the table bronze.crm_cust_info');
		truncate table bronze.crm_cust_info;

		print('>>Inserting Data into table : bronze.crm_cust_info');
		bulk insert bronze.crm_cust_info
		from 'C:\Users\Mudit Agarwal\OneDrive\Desktop\Data Warhousing Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		  firstrow = 2,
		  fieldterminator = ',',
		  tablock
		);
		SET @end_time = GETDATE();
		print('Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds');
		print('---------------------------------------------------------------');

		SET @start_time = GETDATE();
		print('>>Truncating the table bronze.crm_prd_info');
		truncate table bronze.crm_prd_info;

		print('>>Inserting Data into table : bronze.crm_prd_info');
		bulk insert bronze.crm_prd_info
		from 'C:\Users\Mudit Agarwal\OneDrive\Desktop\Data Warhousing Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
		  firstrow = 2,
		  fieldterminator = ',',
		  tablock
		);
		SET @end_time = GETDATE();
		print('Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds');
		print('---------------------------------------------------------------');

		SET @start_time = GETDATE();
		print('>>Truncating the table bronze.crm_sales_details');
		truncate table bronze.crm_sales_details;

		print('>>Inserting Data into table : bronze.crm_sales_details');
		bulk insert bronze.crm_sales_details
		from 'C:\Users\Mudit Agarwal\OneDrive\Desktop\Data Warhousing Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		  firstrow = 2,
		  fieldterminator = ',',
		  tablock
		);
		SET @end_time = GETDATE();
		print('Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds');
		print('---------------------------------------------------------------');

		print('----------------------------------------------------');
		print('Loading ERP Tables');
		print('----------------------------------------------------');

		SET @start_time = GETDATE();
		print('>>Truncating the table bronze.erp_cust_az12');
		truncate table bronze.erp_cust_az12;

		print('>>Inserting Data into table : bronze.erp_cust_az12');
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\Mudit Agarwal\OneDrive\Desktop\Data Warhousing Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
		  firstrow = 2,
		  fieldterminator = ',',
		  tablock
		);
		SET @end_time = GETDATE();
		print('Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds');
		print('---------------------------------------------------------------');

		SET @start_time = GETDATE();
		print('>>Truncating the table bronze.erp_loc_a101');
		truncate table bronze.erp_loc_a101;

		print('>>Inserting Data into table : bronze.erp_loc_a101');
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\Mudit Agarwal\OneDrive\Desktop\Data Warhousing Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
		  firstrow = 2,
		  fieldterminator = ',',
		  tablock
		);
		SET @end_time = GETDATE();
		print('Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds');
		print('---------------------------------------------------------------');

		SET @start_time = GETDATE();
		print('>>Truncating the table bronze.erp_px_cat_g1v2');
		truncate table bronze.erp_px_cat_g1v2;

		print('>>Inserting Data into table : bronze.erp_px_cat_g1v2');
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\Mudit Agarwal\OneDrive\Desktop\Data Warhousing Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
		  firstrow = 2,
		  fieldterminator = ',',
		  tablock
		);
		SET @end_time = GETDATE();
		print('Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds');
		print('---------------------------------------------------------------');

		SET @batch_end_time = GETDATE();
		PRINT '==========================================';
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '==========================================';
	END TRY
	BEGIN CATCH
	    PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END

