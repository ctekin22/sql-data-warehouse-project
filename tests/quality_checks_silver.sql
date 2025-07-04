/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

/*
===============================================================================
Quality Check for bronze.crm_cust_info
===============================================================================
*/

-- Quality Check-1. Check Duplicates and NULL values
-- Check for NULL values or Duplicates in Primary Key
-- Expectation: No Result

SELECT * FROM bronze.crm_cust_info;

-- Finding duplicates
SELECT cst_id,
    COUNT(*) AS count
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL ;

-- Ranking duplicates based on date
-- data with oldest date among the duplicates 
SELECT *
FROM(
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rank
    FROM bronze.crm_cust_info
    )t WHERE rank != 1;

-- Data without duplicates, Pick tha data with latest date among the duplicates 
SELECT *
FROM(
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rank
    FROM bronze.crm_cust_info
    )t WHERE rank = 1;

--    Quality Check-2. Check unwanted spaces in string value
--    Expectation: No Results
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);


-- FINAL version of the bronze.crm_cust_info table without duplicates and empty spaces
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
FROM(
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rank
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
    )t WHERE rank = 1 OR cst_id IS NOT NULL;

--    Quality Check-3. Check the consistency of values in low cardinality columns
--    Data Standardization and Consistency
--    In warehouse, we are aiming to store clear and meaningful values rather than abbreviation. Data Normalization and standardization
--    We use 'n/a' as default for missing values
--    ATransfrom abbreviations if any. 

SELECT DISTINCT cst_marital_status FROM bronze.crm_cust_info;
SELECT DISTINCT cst_gndr FROM bronze.crm_cust_info;

--   FINAL version of the bronze.crm_cust_info table without duplicates, empty spaces and abbreviation
--   After that, go and repeat the steps above changing scheme name from bronze to silver 
--   to check quality of the silver.crm_cust_info table one more time.
--   After all, insert data to silver layet's crm_cust_info table
TRUNCATE TABLE silver.crm_cust_info;
PRINT '>> INserting Data Into: silver.crm_cust_info'
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
        )t WHERE rank = 1 OR cst_id IS NOT NULL

SELECT * FROM silver.crm_cust_info;

/*
===============================================================================
Quality Check for bronze.crm_prd_info
===============================================================================
*/
SELECT * FROM bronze.crm_prd_info;

-- Check 1 - Duplicate check for primary key
SELECT prd_id,
COUNT(*) AS count 
FROM bronze.crm_prd_info
GROUP BY (prd_id)
HAVING COUNT(*) >1 OR prd_id IS NULL; -- no duplicates, no NULLs

-- first 4 letter of the prd_key is catagory id, comes from erp_px_cat_g1v2 table
-- We will split this information to 2 colums.
SELECT prd_id,
prd_key,
REPLACE (SUBSTRING(prd_key,1,5),'-', '_') AS cat_id, -- uses - as seperator, bronze.erp_px_cat_g1v2 uses -, we need to replase for further joins
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key -- matching prd_key of bronze.crm_sales_details
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key,1,5),'-', '_') NOT IN (SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2) -- Filter out unmatchind data as well
AND SUBSTRING(prd_key,7,LEN(prd_key)) NOT IN (SELECT sls_prd_key FROM bronze.crm_sales_details);

SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2; -- catogory id uses _ as seperator
SELECT sls_prd_key FROM bronze.crm_sales_details; -- matching prd_key of bronze.crm_prd_info

-- Check 2 - Check empty spaces for string values of prd_nm column 
SELECT prd_nm 
FROM bronze.crm_prd_info
WHERE TRIM(prd_nm ) != prd_nm; -- No need to trim

-- Check 3 - Check for NULLS or negative numbers for prd_cost column
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost< 0; --we have only 2 NULLs, will replace them with 0.

SELECT prd_cost,
-- OR ISNULL(prd_cost, 0) AS prd_cost,
COALESCE(prd_cost, 0) AS prd_cost
FROM bronze.crm_prd_info;

-- Check 4 - Data Standardization and Consistency, Check for abbriviatios and make them decriptive for prd_line column
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

SELECT prd_line, 
CASE 
    WHEN UPPER(TRIM(prd_line)) ='R' THEN 'Road'
    WHEN UPPER(TRIM(prd_line)) ='S' THEN 'Other Sales'
    WHEN UPPER(TRIM(prd_line)) ='M' THEN 'Mountain'
    WHEN UPPER(TRIM(prd_line)) ='T' THEN 'Touring'
    ELSE 'n/a'
END AS prd_line
FROM bronze.crm_prd_info;

-- Check 5 - Check the quality of the dates column prd_start_dt and prd_end_dt for invalid dates
-- End date must not be earlier than start date.
SELECT prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE DATEDIFF(day,prd_end_dt, prd_start_dt) <0; -- no invalid date
--WHERE prd_end_dt > prd_start_dt;

-- It should not be overlaping dates for the order, same day cannot have two different prices. 
-- First record should be smaller than the start of next record in record history.
-- All orders should have start date.
-- If an order does not have end date use next record as a end date
-- Finally remove time if there is no time record available
SELECT prd_id,prd_key, prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

-- FINAL QUERY 
-- Go and modify silver.crm_prd_info table in silver DDL if needed; 
-- Add cat_id and chance DATETIME data type to DATE based on the modification we did
-- INSERT CLEANDED DATA TO SILVER LAYER with FINAL QUERY 
TRUNCATE TABLE silver.crm_prd_info;
PRINT '>> INserting Data Into: silver.crm_prd_info'
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

-- After all go and check all the check condition above for silver.crm_prd_info table to make sure!
-- You can do that just changing schema name from bronze to silver
SELECT * FROM silver.crm_prd_info;


/*
===============================================================================
Quality Check for bronze.crm_sales_details
===============================================================================
*/
SELECT * FROM bronze.crm_sales_details;

-- Check 1 -  Check if sls_ord_num column has empty spaces
SELECT sls_ord_num 
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num); -- No transformation is needed

-- Check 2 -  Check if all the products in sls_prd_key and sls_cust_id column are available in their related table
SELECT sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info); -- All sold products are in product table

SELECT sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info); -- All costumer who bought products are in customer table table

-- Check 3 -  Check invalid dates
-- Check date colums sls_order_dt, sls_ship_dt, sls_due_dt
-- Those are recorded as INT, need to be transformed to DATE, convert them to NULL 
-- Check if any date information is 0 or less, as they are INT
-- Date Format YYYYMMDD, must be 8 character
SELECT 
NULLIF(sls_order_dt, 0) AS sls_order_dt,
sls_ship_dt,
sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt<= 0 OR LEN(sls_order_dt) !=8; -- sls_ship_dt<= 0; sls_due_dt <=0 ; 
                        -- Only sls_order_dt has 0 

-- You can also check date boundaries based on business running time
-- Check for outliers by validating the boundaries of the date range
SELECT 
NULLIF(sls_order_dt, 0) AS sls_order_dt,
sls_ship_dt,
sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt > 20500101 OR sls_order_dt < 19000101; 

-- The order date should be always smaller than shipping date and due date
SELECT 
sls_order_dt,
sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;  -- No qality issues here

-- Check 4 -  Check Data Consistency and Business Rules using sls_sales, sls_quantity, sls_price
-- sales = quantity * price
-- Negative, zero, NULL values are not allowed!
-- Rules:
    -- If Sales is negative, zero, or null, derive them using Quantity and Price
    -- If Price is zero or null, calculate it using Sales and Quantity
    -- If Price is negative, convert it to a positive value
SELECT DISTINCT 
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity* ABS(sls_price) THEN sls_quantity*ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE 
        WHEN sls_price IS NULL OR sls_price <=0 THEN  sls_sales/NULLIF(sls_quantity,0)
        ELSE sls_price  
    END sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity*sls_price 
OR sls_quantity <= 0 OR sls_quantity IS NULL
OR sls_sales <= 0 OR sls_sales IS NULL
OR sls_price <= 0 OR sls_price IS NULL
ORDER BY sls_quantity, sls_sales, sls_price;

-- FINAL QUERY
-- Go and modify silver.crm_sls_details table in silver DDL if needed; 
-- Chance INT data type to DATE based on the modification we did
-- INSERT CLEANDED DATA TO SILVER LAYER with FINAL QUERY
TRUNCATE TABLE silver.crm_sales_details;
PRINT '>> INserting Data Into: silver.crm_sales_details'
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
FROM bronze.crm_sales_details

-- After all go and check all the check condition above for silver.crm_prd_info table to make sure!
-- You can do that just changing schema name from bronze to silver
SELECT * FROM silver.crm_sales_details;


/*
===============================================================================
Quality Check for ERP Tables
===============================================================================
*/
/*
===============================================================================
Quality Check for bronze.erp_cust_az12
===============================================================================
*/
SELECT * FROM bronze.erp_cust_az12;

-- Check 1 - Check duplictes and null values of column cid
-- This column is connected to crm_cust_info table' s cst_key column
SELECT cid,
    COUNT(*) AS count
FROM bronze.erp_cust_az12
GROUP BY cid
HAVING COUNT(*)>2 OR cid IS NULL; -- No issues

SELECT cid
FROM bronze.erp_cust_az12
WHERE cid != TRIM(cid); -- No issues

-- This column is connected to crm_cust_info table' s cst_key column
-- We should be able to connect these two tables
SELECT cst_key FROM silver.crm_cust_info;

SELECT cid,
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
         ELSE cid
    END AS cid
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
         ELSE cid
    END NOT IN (SELECT cst_key FROM silver.crm_cust_info) -- Check for unmatching cid in cst_key of  silver.crm_cust_info

-- Check 2 - Check bdate column, check if date are in certain range
SELECT bdate 
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE(); -- check very old custumers, We have dates with future dates

-- Convert future dates to NULL
SELECT
CASE WHEN bdate > GETDATE() THEN NULL
      ELSE bdate 
END AS bdate
FROM bronze.erp_cust_az12
--ORDER BY bdate ASC;

-- Check 2 - Check gen column, we have abbreviation and desciriptive form with and without empty spacces. 
-- Make them consistent and descriptive, remove spaces 

SELECT DISTINCT gen
FROM bronze.erp_cust_az12;

SELECT DISTINCT TRIM(REPLACE(gen, CHAR(13),'')),LEN(gen)
FROM bronze.erp_cust_az12;

SELECT DISTINCT gen,
    CASE 
        WHEN UPPER(TRIM(REPLACE(gen, CHAR(13),''))) IN ('M', 'MALE') THEN 'Male'
        WHEN UPPER(TRIM(REPLACE(gen, CHAR(13),''))) IN ('F', 'FEMALE') THEN 'Female'
        ELSE 'n/a'
    END AS gen
FROM bronze.erp_cust_az12

-- FINAL QUERY
-- Go and modify silver.erp_cust_az12 table in silver DDL if needed; 
-- INSERT CLEANDED DATA TO SILVER LAYER with FINAL QUERY
TRUNCATE TABLE silver.erp_cust_az12;
PRINT '>> INserting Data Into: silver.erp_cust_az12'
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
FROM bronze.erp_cust_az12

-- After all go and check all the check condition above for silver.erp_cust_az12 table to make sure!
-- You can do that just changing schema name from bronze to silver
SELECT * FROM silver.erp_cust_az12;

/*
===============================================================================
Quality Check for bronze.erp_loc_a101
===============================================================================
*/

SELECT * FROM bronze.erp_loc_a101;

-- Check 1 - Check duplictes and null values of column cid
-- This column is connected to crm_cust_info table' s cst_key column
-- This information must be matching in order to join tables, remove - 
SELECT 
    REPLACE(cid,'-', '') AS cid
FROM bronze.erp_loc_a101
WHERE  REPLACE(cid,'-', '') NOT IN (SELECT cst_key FROM silver.crm_cust_info); -- Check if colums matching now

SELECT cst_key FROM silver.crm_cust_info;

-- Check 2 - Check cntry column
-- Data Standartizatin and Consistency
SELECT DISTINCT cntry,
    CASE 
        WHEN UPPER(TRIM(REPLACE(cntry, CHAR(13),''))) IN ('USA', 'US') THEN 'United States'
        WHEN UPPER(TRIM(REPLACE(cntry, CHAR(13),''))) ='DE' THEN 'Germany'
        WHEN UPPER(TRIM(REPLACE(cntry, CHAR(13),''))) IS NOT NULL THEN TRIM(REPLACE(cntry, CHAR(13),''))
        ELSE 'n/a' 
    END AS cntry2
FROM bronze.erp_loc_a101
ORDER BY cntry;

-- FINAL QUERY
-- Go and modify silver.erp_loc_a101 table in silver DDL if needed; 
-- INSERT CLEANDED DATA TO SILVER LAYER with FINAL QUERY
TRUNCATE TABLE silver.erp_loc_a101;
PRINT '>> INserting Data Into: silver.erp_loc_a101'
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
FROM bronze.erp_loc_a101

-- After all go and check all the check condition above for silver.erp_loc_a101 table to make sure!
-- You can do that just changing schema name from bronze to silver
SELECT * FROM silver.erp_loc_a101;

/*
===============================================================================
Quality Check for bronze.erp_px_cat_g1v2
===============================================================================
*/

SELECT * FROM bronze.erp_px_cat_g1v2;

-- Check 1 - Check the quality of the id column
SELECT id 
FROM bronze.erp_px_cat_g1v2


-- This column is connected to crm_prd_info table' s prd_key column
-- This information must be matching in order to join tables
-- We alredy did this part seperating prd_key to cat_id
SELECT * FROM silver.crm_prd_info;

-- Check 2 - Check the quality of the cat and subcat columns, unwanted spaces
SELECT DISTINCT cat 
FROM bronze.erp_px_cat_g1v2 --OK 

SELECT DISTINCT subcat 
FROM bronze.erp_px_cat_g1v2 --OK 

SELECT cat 
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintance != TRIM(maintance); -- No issues

-- Check 3 - Check for maintance column
-- Data Standartization and Normalization
SELECT DISTINCT maintance,
    TRIM(REPLACE(maintance, CHAR(13),'')) 
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT 
    TRIM(REPLACE(maintance, CHAR(13),'')) as maintance
FROM bronze.erp_px_cat_g1v2;

-- FINAL QUERY
-- Go and modify silver.erp_px_cat_g1v2 table in silver DDL if needed; 
-- INSERT CLEANDED DATA TO SILVER LAYER with FINAL QUERY
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

-- After all go and check all the check condition above for silver.erp_px_cat_g1v2 table to make sure!
-- You can do that just changing schema name from bronze to silver
SELECT * FROM silver.erp_px_cat_g1v2;