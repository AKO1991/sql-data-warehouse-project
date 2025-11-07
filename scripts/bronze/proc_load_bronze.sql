-- Creating Stored Procedure for Full Load operation in Bronze layer
----------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
DECLARE @start_time DATETIME, @end_time DATETIME;
BEGIN
	BEGIN TRY		-- (TRY & CATCH: Throw Error message if there is any)
		PRINT '---------------------------';
		PRINT 'Loading Bronze Layer';
		PRINT '---------------------------';

		PRINT '---------------------------';
		PRINT 'Loading CRM  Tables';
		PRINT '---------------------------';
		
		SET @start_time = GETDATE();
		-- Full Load : Delete existing data and load complete data to the table
		PRINT 'Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
	
		PRINT 'Inserting Data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\Power BI Developer\SQL Ultimate Course - Baraa\SQL Data Warehouse Project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,				-- Data starts from 2nd row as the first row conains the headers
			FIELDTERMINATOR = ',',		-- Separator is Comma
			TABLOCK						-- Lock the entire table during data insertion (Performance Optimizer)
		);
		SET @end_time = GETDATE();
		PRINT 'Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT 'Inserting Data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\Power BI Developer\SQL Ultimate Course - Baraa\SQL Data Warehouse Project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Loading Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' second';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
	
		PRINT 'Inserting Data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\Power BI Developer\SQL Ultimate Course - Baraa\SQL Data Warehouse Project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '';

		PRINT '---------------------------';
		PRINT 'Loading ERP  Tables';
		PRINT '---------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
	
		PRINT 'Inserting Data into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\Power BI Developer\SQL Ultimate Course - Baraa\SQL Data Warehouse Project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'second'; 

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
	
		PRINT 'Inserting Data into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\Power BI Developer\SQL Ultimate Course - Baraa\SQL Data Warehouse Project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		
		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		
		PRINT 'Inserting Data into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\Power BI Developer\SQL Ultimate Course - Baraa\SQL Data Warehouse Project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Loading Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' second';
	END TRY
	BEGIN CATCH
		PRINT '------------------------------------------------';
		PRINT 'Error Occured during Loading Bronze Layer';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR);
		PRINT '-------------------------------------------------';
	END CATCH
END

EXEC bronze.load_bronze;
