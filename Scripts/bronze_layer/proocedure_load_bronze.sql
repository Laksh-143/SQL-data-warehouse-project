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


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	 DECLARE @start_time DATETIME , @end_time DATETIME ;
	BEGIN TRY
		PRINT('======================================');
		PRINT('Loading bronze layer');
		PRINT('======================================');

		PRINT('--------------------------------------');
		PRINT('Loading crm tables');
		PRINT('--------------------------------------');

		SET @start_time=GETDATE();
		PRINT('>> Truncating The Table: bronze.crm_cust_info');
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT('>> Inserting Data Into: bronze.crm_cust_info');
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\KISAN\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time=GETDATE();
		PRINT(' >> Load duration: '+ CAST(DATEDIFF(second , @start_time , @end_time) AS NVARCHAR) + 'seconds');
		PRINT('--------------------------------------');

		SET @start_time=GETDATE();
		PRINT('>> Truncating the Table: bronze.crm_prd_info');
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT('>> Inserting Data Into: bronze.crm_prd_info');
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\KISAN\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time=GETDATE();
		PRINT(' >> Load duration: '+ CAST(DATEDIFF(second , @start_time , @end_time) AS NVARCHAR) + 'seconds');
		PRINT('--------------------------------------');
		
		
		SET @start_time=GETDATE();
		PRINT('>> Truncating the Table: bronze.crm_sales_details');
		TRUNCATE TABLE bronze.crm_sales_details

		PRINT('>> Inserting Data Into: bronze.crm_sales_details');
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\KISAN\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time=GETDATE();
		PRINT(' >> Load duration: '+ CAST(DATEDIFF(second , @start_time , @end_time) AS NVARCHAR) + 'seconds');
		PRINT('--------------------------------------');



		PRINT('--------------------------------------');
		PRINT('Loading erp  tables');
		PRINT('--------------------------------------');

		SET @start_time=GETDATE();
		PRINT('>> Truncating the Table: bronze.erp_CUST_AZ12');
		TRUNCATE TABLE bronze.erp_CUST_AZ12

		PRINT('>> Inserting Data Into: bronze.erp_CUST_AZ12');
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\KISAN\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
		SET @end_time=GETDATE();
		PRINT(' >> Load duration: '+ CAST(DATEDIFF(second , @start_time , @end_time) AS NVARCHAR) + 'seconds');
		PRINT('--------------------------------------');

		SET @start_time=GETDATE();
		PRINT('>> Truncating the Table: bronze.erp_LOC_A101');
		TRUNCATE TABLE bronze.erp_LOC_A101

		PRINT('>> Inserting Data Into: bronze.erp_LOC_A101');
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\KISAN\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
		SET @end_time=GETDATE();
		PRINT(' >> Load duration: '+ CAST(DATEDIFF(second , @start_time , @end_time) AS NVARCHAR) + 'seconds');
		PRINT('--------------------------------------');

		SET @start_time=GETDATE();
		PRINT('>> Truncating the Table: bronze.erp_PX_CAT_G1V2');
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2

		PRINT('>> Inserting Data Into: bronze.erp_PX_CAT_G1V2');
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\KISAN\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
		SET @end_time=GETDATE();
		PRINT(' >> Load duration: '+ CAST(DATEDIFF(second , @start_time , @end_time) AS NVARCHAR) + 'seconds') ;
		PRINT('--------------------------------------');


	END TRY
	BEGIN CATCH
		PRINT('Error occured during loading bronze layer')
		PRINT('Error message' + ERROR_MESSAGE());
	END CATCH
END
