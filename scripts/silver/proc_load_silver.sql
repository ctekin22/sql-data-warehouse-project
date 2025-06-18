/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN 
    BEGIN TRY
    PRINT '----------------------------------------------------------------------------------------------------';
    PRINT 'LOADING SILVER LAYER';
    PRINT '----------------------------------------------------------------------------------------------------';
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

    SET @start_time = GETDATE();
    SET @batch_end_time = GETDATE();
    PRINT '>> Truncate Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    PRINT '>> Inserting Data Into: silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date)

        SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END cst_marital_status,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END cst_gndr,
        cst_create_date
        FROM(
            SELECT *,
            ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rank
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
            )t WHERE rank = 1;
    SET @end_time = GETDATE();
    PRINT '>> silver.crm_cust_info LODING TIME IS ' + CAST(DATEDIFF(second, @end_time, @start_time) AS NVARCHAR) + ' seconds';

    SET @start_time = GETDATE();
    PRINT '>> Truncate Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    PRINT '>> Inserting Data Into: silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info(
        prd_id,
        cat_id,
        prd_key,
        prd_nm,	
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )

    SELECT prd_id,
    REPLACE (SUBSTRING(prd_key,1,5),'-', '_') AS cat_id, -- uses - as seperator, bronze.erp_px_cat_g1v2 uses -, we need to replase for further joins
    SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, -- matching prd_key of bronze.crm_sales_details
    prd_nm,
    COALESCE(prd_cost, 0) AS prd_cost,
    CASE 
        WHEN UPPER(TRIM(prd_line)) ='R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) ='S' THEN 'other Sales'
        WHEN UPPER(TRIM(prd_line)) ='M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) ='T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    CAST (prd_start_dt AS DATE) AS prd_start_dt,
    CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt -- Data Enrichment, adding or enhancing data 
    FROM bronze.crm_prd_info;
    SET @end_time = GETDATE();
    PRINT '>> silver.crm_prd_info LODING TIME IS ' + CAST(DATEDIFF(second, @end_time, @start_time) AS NVARCHAR) + ' seconds';

    SET @start_time = GETDATE();
    PRINT '>> Truncate Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    PRINT '>> Inserting Data Into: silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details
    (
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
    SELECT 
        sls_ord_num, 
        sls_prd_key,
        sls_cust_id,
    CASE 
        WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) !=8 THEN NULL
        ELSE CAST (CAST(sls_order_dt AS NVARCHAR) AS DATE)--We cannot cast directly from INT to DATE, We cast to string first.
    END AS sls_order_dt,
    CASE 
        WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) !=8 THEN NULL
        ELSE CAST (CAST(sls_ship_dt AS NVARCHAR) AS DATE)--We cannot cast directly from INT to DATE, We cast to string first.
    END AS sls_ship_dt,
    CASE 
        WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) !=8 THEN NULL
        ELSE CAST (CAST(sls_due_dt AS NVARCHAR) AS DATE)--We cannot cast directly from INT to DATE, We cast to string first.
    END AS sls_due_dt,
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity* ABS(sls_price) THEN sls_quantity*ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE 
        WHEN sls_price IS NULL OR sls_price <=0 THEN  sls_sales/NULLIF(sls_quantity,0)
        ELSE sls_price  
    END sls_price
    FROM bronze.crm_sales_details;
    SET @end_time = GETDATE();
    PRINT '>> silver.crm_sales_details LODING TIME IS ' + CAST(DATEDIFF(second, @end_time, @start_time) AS NVARCHAR) + ' seconds';
    
    SET @start_time = GETDATE();
    PRINT '>> Truncate Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    PRINT '>> Inserting Data Into: silver.erp_cust_az12';
    INSERT INTO silver.erp_cust_az12(
        cid,
        bdate,
        gen
    )
    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
            ELSE cid
        END AS cid,
        CASE WHEN bdate > GETDATE() THEN NULL
        ELSE bdate 
        END AS bdate,
            CASE 
            WHEN UPPER(TRIM(REPLACE(gen, CHAR(13),''))) IN ('M', 'MALE') THEN 'Male'
            WHEN UPPER(TRIM(REPLACE(gen, CHAR(13),''))) IN ('F', 'FEMALE') THEN 'Female'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;
    SET @end_time = GETDATE();
    PRINT '>> silver.erp_cust_az12 LODING TIME IS ' + CAST(DATEDIFF(second, @end_time, @start_time) AS NVARCHAR) + ' seconds';

    SET @start_time = GETDATE();
    PRINT '>> Truncate Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    PRINT '>> Inserting Data Into: silver.erp_loc_a101';
    INSERT INTO silver.erp_loc_a101(
        cid,
        cntry
    )
    SELECT 
        REPLACE(cid,'-', '') AS cid,
        CASE 
            WHEN UPPER(TRIM(REPLACE(cntry, CHAR(13),''))) IN ('USA', 'US') THEN 'United States'
            WHEN UPPER(TRIM(REPLACE(cntry, CHAR(13),''))) ='DE' THEN 'Germany'
            WHEN UPPER(TRIM(REPLACE(cntry, CHAR(13),''))) IS NOT NULL THEN TRIM(REPLACE(cntry, CHAR(13),''))
            ELSE 'n/a' 
        END AS cntry
    FROM bronze.erp_loc_a101;
    SET @end_time = GETDATE();
    PRINT '>> silver.erp_loc_a101 LODING TIME IS ' + CAST(DATEDIFF(second, @end_time, @start_time) AS NVARCHAR) + ' seconds';

    SET @start_time = GETDATE();
    PRINT '>> Truncate Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    PRINT '>> INserting Data Into: silver.erp_px_cat_g1v2'
    INSERT INTO silver.erp_px_cat_g1v2(
        id,
        cat,
        subcat,
        maintance
    )
    SELECT 
    id,
    cat,
    subcat,
    TRIM(REPLACE(maintance, CHAR(13),'')) AS maintance
    FROM bronze.erp_px_cat_g1v2;
    SET @end_time = GETDATE();
    SET @batch_end_time = GETDATE();
    PRINT '>> silver.erp_px_cat_g1v2 LODING TIME IS ' + CAST(DATEDIFF(second, @end_time, @start_time) AS NVARCHAR) + ' seconds';

    PRINT '>> BATCH LODING TIME IS ' + CAST(DATEDIFF(second, @batch_end_time, @batch_start_time) AS NVARCHAR) + ' seconds';
    
    END TRY 
    BEGIN CATCH 
        PRINT '----------------------------------------------------------------------------------------------------';
        PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
        PRINT 'Error message ' + ERROR_MESSAGE();
        PRINT 'Error message ' + CAST (ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error message ' + CAST (ERROR_STATE() AS NVARCHAR);
        PRINT '----------------------------------------------------------------------------------------------------';
    END CATCH

END


--EXEC silver.load_silver
