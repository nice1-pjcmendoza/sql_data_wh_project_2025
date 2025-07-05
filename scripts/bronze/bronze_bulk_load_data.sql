

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

-- EXEC bronze.load_bronze;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE 
		@start_time DATETIME, 
		@end_time DATETIME, 
		@batch_start_time DATETIME , 
		@batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();

		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE [bronze].[crm_cust_info];

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT [bronze].[crm_cust_info]
		FROM 'D:\Coding\sql_data_wh_project_2025\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------';

		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE [bronze].[crm_prd_info];

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT [bronze].[crm_prd_info]
		FROM 'D:\Coding\sql_data_wh_project_2025\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------';

		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE [bronze].[crm_sales_details];

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT [bronze].[crm_sales_details]
		FROM 'D:\Coding\sql_data_wh_project_2025\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE [bronze].[erp_cust_az12];

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT [bronze].[erp_cust_az12]
		FROM 'D:\Coding\sql_data_wh_project_2025\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------';

		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE [bronze].[erp_loc_a101];

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT [bronze].[erp_loc_a101]
		FROM 'D:\Coding\sql_data_wh_project_2025\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------';

		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT [bronze].[erp_px_cat_g1v2]
		FROM 'D:\Coding\sql_data_wh_project_2025\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------';

		SET @batch_end_time = GETDATE();

		PRINT '================================================';
		PRINT 'Loading Bronze Layer has been completed';
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '================================================';
	END TRY
	
	BEGIN CATCH
		PRINT '------------------------------------------------';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '------------------------------------------------';
	END CATCH
END


-- 1. Define the Procedure & Setup Timers

-- ```sql
-- CREATE OR ALTER PROCEDURE bronze.load_bronze AS
-- BEGIN
--   DECLARE 
--     @start_time DATETIME, 
--     @end_time DATETIME, 
--     @batch_start_time DATETIME, 
--     @batch_end_time DATETIME;
-- ```

-- * **CREATE OR ALTER**: Creates the procedure if it doesn’t exist, or updates it if it does.
-- * **DECLARE**: Sets up datetime variables to measure how long each import and the whole job take.

-- ---

-- 2. Start Try Block & Log Batch Start

-- ```sql
-- BEGIN TRY
--   SET @batch_start_time = GETDATE();

--   PRINT '================================================';
--   PRINT 'Loading Bronze Layer';
--   PRINT '================================================';
-- ```

-- * **BEGIN TRY**: Starts a block to handle errors.
-- * **GETDATE()**: Captures the timestamp when the batch begins.
-- * **PRINT**: Writes status messages to the console.

-- ---

-- 3. Load CRM Data

-- a) Customer Info

-- ```sql
-- SET @start_time = GETDATE();
-- PRINT '>> Truncating Table: bronze.crm_cust_info';
-- TRUNCATE TABLE [bronze].[crm_cust_info];

-- PRINT '>> Inserting Data Into: bronze.crm_cust_info';
-- BULK INSERT [bronze].[crm_cust_info]
-- FROM 'D:\...cust_info.csv'
-- WITH (
--   FIRSTROW = 2,
--   FIELDTERMINATOR = ',',
--   TABLOCK
-- );
-- SET @end_time = GETDATE();
-- PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
-- ```

-- * **TRUNCATE TABLE**: Quickly clears old data.
-- * **BULK INSERT**: Loads CSV data efficiently.

--   * `FIRSTROW = 2` skips the header row (`Metadata` may vary) ([reddit.com][1], [reddit.com][2], [reddit.com][3], [reddit.com][4], [codingsight.com][5]).
--   * `FIELDTERMINATOR` tells SQL it’s comma-separated.
--   * `TABLOCK` speeds up load by locking table for minimal logging ([reddit.com][6], [red-gate.com][7]).
-- * Measures loading time and prints it.

-- b) Product Info & Sales Details

-- * The same pattern (`TRUNCATE → BULK INSERT → timing`) loads:

--   * `crm_prd_info`
--   * `crm_sales_details`

-- ---

-- 4. Load ERP Data

-- Follows the same steps for:

-- * `erp_cust_az12`
-- * `erp_loc_a101`
-- * `erp_px_cat_g1v2`

-- Each table is truncated, loaded, and timed exactly as CRM tables.

-- ---

-- 5. Completion Logging

-- ```sql
-- SET @batch_end_time = GETDATE();

-- PRINT '================================================';
-- PRINT 'Loading Bronze Layer has been completed';
-- PRINT 'Total Load Duration: ... seconds';
-- PRINT '================================================';
-- ```

-- * Computes total runtime of the full load process.
-- * Logs completion to the console.

-- ---

-- 6. Error Handling: TRY–CATCH

-- ```sql
-- END TRY

-- BEGIN CATCH
--   PRINT '------------------------------------------------';
--   PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
--   PRINT 'Error Message' + ERROR_MESSAGE();
--   PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
--   PRINT 'Error State' + CAST(ERROR_STATE() AS NVARCHAR);
--   PRINT '------------------------------------------------';
-- END CATCH
-- END
-- ```

-- * If any error occurs (e.g., missing file), execution jumps here .
-- * Logs detailed error info but does not retry or rollback—just exits.

-- ---

-- ⚠️ Notes & Best Practices

-- * 💥 **Full data reload**: Every run clears old data (`TRUNCATE`). Only use if a full refresh is intended.
-- * 🛑 **Table locking**: `TABLOCK` speeds loading but prevents other queries during loads ([mssqltips.com][8], [reddit.com][9], [reddit.com][10]).
-- * 🧑‍🔧 **Permissions required**: Needs rights for `BULK INSERT` and file access.
-- * 📝 **Logging only**: This procedure logs durations and errors using `PRINT`. For production, consider:

--   * Logging to a table or file
--   * Handling transactions or partial failures
--   * Using incremental loading strategies

-- ---

-- ✅ Final Summary

-- 1. **Setup**: Declare timers.
-- 2. **Start TRY block**: Begin full load.
-- 3. **Loop through tables**:

--    * TRUNCATE,
--    * BULK INSERT,
--    * Log duration.
-- 4. **Finish**: Print total duration.
-- 5. **Handle Errors**: Catch exceptions and print error details.