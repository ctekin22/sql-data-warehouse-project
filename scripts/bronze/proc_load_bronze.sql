/*
If you’re using Docker for SQL Server on Mac, you need to:

Mount the local folder as a volume when starting the container:

docker run -d \
  --name sql_server_warehouse \
  -e 'ACCEPT_EULA=Y' \
  -e 'SA_PASSWORD=Your_password123' \
  -v /Users/cansucan/Desktop/Data-Engineering/sql-data-warehouse-project/datasets:/var/opt/mssql/import \
  -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2022-latest

-- Reconnect using the 'DataWarehouse' database if needed.

*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    SET @batch_start_time = GETDATE();
    BEGIN TRY

        PRINT '======================================================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '======================================================================================';


        PRINT '--------------------------------------------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '--------------------------------------------------------------------------------------';
       
        -- Table bronze.crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/import/source_crm/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------------------------------------------';
        -- Quick Controls
        --SELECT * FROM bronze.crm_cust_info;
        --SELECT COUNT(*) FROM bronze.crm_cust_info;


        -- Table bronze.crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM '/var/opt/mssql/import/source_crm/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK 
        )
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------------------------------------------';
        -- Quick Controls
        --SELECT * FROM bronze.crm_prd_info;
        --SELECT COUNT(*) FROM bronze.crm_prd_info;

        -- Table bronze.crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details
        
        PRINT '>> Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/import/source_crm/sales_details.csv'
        WITH(
            FIRSTROW =  2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------------------------------------------';
        -- Quick Controls
        --SELECT TOP 10 * FROM bronze.crm_sales_details;
        --SELECT COUNT(*) FROM bronze.crm_sales_details;

        PRINT '--------------------------------------------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '--------------------------------------------------------------------------------------';
        
        -- Table bronze.erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM '/var/opt/mssql/import/source_erp/LOC_A101.csv'
        WITH(
            FIRSTROW =  2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------------------------------------------';

        -- Quick Controls
        --SELECT TOP 10 * FROM bronze.erp_loc_a101;
        --SELECT COUNT(*) FROM bronze.erp_loc_a101;

        -- Table bronze.erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2

        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/var/opt/mssql/import/source_erp/PX_CAT_G1V2.csv'
        WITH(
            FIRSTROW =  2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------------------------------------------';

        -- Quick Controls
        --SELECT TOP 10 * FROM bronze.erp_px_cat_g1v2;
        --SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;

        -- Table bronze.erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/import/source_erp/CUST_AZ12.csv'
        WITH(
            FIRSTROW =  2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------------------------------------------';
       
        -- Quick Controls
        --SELECT TOP 10 * FROM bronze.erp_cust_az12;
        --SELECT COUNT(*) FROM bronze.erp_cust_az12;

        SET @batch_end_time = GETDATE();
        PRINT '--------------------------------------------------------------------------------------';
        PRINT 'Loading Bronze Layer is completed';
        PRINT 'Total load Duration: ' +  CAST(DATEDIFF(second,@batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------------------------------------------';

    END TRY
    BEGIN CATCH
        PRINT '======================================================================================';
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message' + ERROR_MESSAGE();
        PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR );
        PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR );
        PRINT '======================================================================================';
    END CATCH

END 

EXEC bronze.load_bronze;