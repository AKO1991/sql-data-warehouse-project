/*
===================================================
DDL Script: Create Bronze layer tables

Script Purpose: 
	This script creates tables in the Bronze schema after dropping existing table if they already axist.
	Run this script to re-define the DDL structure of Bronza layer tables.
===================================================
*/

-- Create Customer Info Table
IF OBJECT_ID('bronze_crm_cust_info','U') IS NOT NULL
DROP TABLE bronze_crm_cust_info; -- T-SQL Command

CREATE TABLE bronze_crm_cust_info
(
	cst_id INT,
	cst_key NVARCHAR(20),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(10),
	cst_gender NVARCHAR(10),
	cst_create_date DATE
);

IF OBJECT_ID('bronze_crm_prod_info','U') IS NOT NULL
DROP TABLE bronze_crm_prod_info; -- T-SQL Command

-- Create Product Info Table
CREATE TABLE bronze_crm_prod_info
(
	prd_id INT,
	prd_key NVARCHAR(30),
	prd_nm NVARCHAR(100),
	prd_cost INT,
	prd_line NVARCHAR(10),
	prd_star_dt DATE,
	prd_end_dt DATE
);


-- Create Sales Details Table
IF OBJECT_ID('bronze_crm_sales_details','U') IS NOT NULL
DROP TABLE bronze_crm_sales_details; -- T-SQL Command

CREATE TABLE bronze_crm_sales_details
(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

-- Create Customer Table
IF OBJECT_ID('bronze_erp_cust_az12','U') IS NOT NULL
DROP TABLE bronze_erp_cust_az12; -- T-SQL Command

CREATE TABLE bronze_erp_cust_az12
(
	cid NVARCHAR(50),
	cdate NVARCHAR(50),
	gen NVARCHAR(10)
);

-- Create Location Table
IF OBJECT_ID('bronze_erp_loc_a101','U') IS NOT NULL
DROP TABLE bronze_erp_loc_a101; -- T-SQL Command

CREATE TABLE bronze_erp_loc_a101
(
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);

-- Create Product Category Table
IF OBJECT_ID('bronze_erp_px_cat_g1v2','U') IS NOT NULL
DROP TABLE bronze_erp_px_cat_g1v2; -- T-SQL Command

CREATE TABLE bronze_erp_px_cat_g1v2
(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
);

-- Bulk Insert CSV file data into Tables
-- Create Stored Procedure for the bulk insert operation

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '===========================';
		PRINT 'Loading Bronze layer';
		PRINT '===========================';

		PRINT '---------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze_crm_cust_info';
		TRUNCATE TABLE bronze_crm_cust_info;
	
		PRINT '>> Inserting Data into Table: bronze_crm_cust_info';
		BULK INSERT bronze_crm_cust_info
		FROM 'D:\Power BI Developer\SQL Ultimate Course - Baraa\Downloaded\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table:bronze_crm_prod_info';
		TRUNCATE TABLE bronze_crm_prod_info;
	
		PRINT '>> Inserting Data into Table: bronze_crm_prod_info';
		BULK INSERT bronze_crm_prod_info
		FROM 'D:\Power BI Developer\SQL Ultimate Course - Baraa\Downloaded\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze_crm_sales_details';
		TRUNCATE TABLE bronze_crm_sales_details;
	
		PRINT '>> Inserting Data into Table: bronze_crm_sales_details';
		BULK INSERT bronze_crm_sales_details
		FROM 'D:\Power BI Developer\SQL Ultimate Course - Baraa\Downloaded\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------';

		PRINT '---------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '---------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze_erp_cust_az12';
		TRUNCATE TABLE bronze_erp_cust_az12;
	
		PRINT '>> Inserting Data into Table: bronze_erp_cust_az12';
		BULK INSERT bronze_erp_cust_az12
		FROM 'D:\Power BI Developer\SQL Ultimate Course - Baraa\Downloaded\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze_erp_loc_a101'
		TRUNCATE TABLE bronze_erp_loc_a101;
	
		PRINT '>> Inserting Data into Table: bronze_erp_loc_a101';
		BULK INSERT bronze_erp_loc_a101
		FROM 'D:\Power BI Developer\SQL Ultimate Course - Baraa\Downloaded\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze_erp_px_cat_g1v2'
		TRUNCATE TABLE bronze_erp_px_cat_g1v2;
	
		PRINT '>> Inserting Data into Table: bronze_erp_px_cat_g1v2';
		BULK INSERT bronze_erp_px_cat_g1v2
		FROM 'D:\Power BI Developer\SQL Ultimate Course - Baraa\Downloaded\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------';

		SET @batch_end_time = GETDATE();
		PRINT '>> Loading Bronze Layer completed' 
		PRINT 'Total Load Duration:' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------';

	END TRY
	BEGIN CATCH
		PRINT '=========================================';
		PRINT 'Error occured during loading Bronze layer';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Number'  + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State'   + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================';
	END CATCH
END

EXEC bronze.load_bronze;
