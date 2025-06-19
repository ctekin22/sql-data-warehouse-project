PRINT '=========================================================================================================='
PRINT 'JOINING TABLES TO CREATE DIMENSION AND FACT TABLES/VIEW'
PRINT '=========================================================================================================='

PRINT '----------------------------------------------------------------------------------------------------------'
PRINT 'Creating Customer Dimension Table, using master table silver.crm_cust_info'
PRINT 'Joining with related tables; silver.erp_cust_az12 and silver.erp_loc_a101'
PRINT '----------------------------------------------------------------------------------------------------------'
-- Join the silver.crm_cust_info table with its related tables to get other relevant information for the custumer object
-- After joining, check if any duplicate data has been introduced to the main table
SELECT 
    cst_id,
    COUNT(*)
 FROM
 (
    SELECT
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
    FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid 
    LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid
 )t GROUP BY cst_id HAVING COUNT(*)> 1;

-- We have two columns with the  same information; gen and cst_gndr
-- The master table, which is the crm table, should have more accurate data
-- Use data from the  master table in the case of inconsistency
-- Data Integration
SELECT DISTINCT
    ci.cst_gndr,
    ca.gen,
    CASE 
        WHEN ci.cst_gndr='n/a' AND ca.gen IS NOT NULL THEN ca.gen
        ELSE ci.cst_gndr
    END new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid 
LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid
ORDER BY 1,2;


-- FINAL QUERY
-- Rename columns to friendly, meaningful names
-- Sort the columns into logical groups to improve readability
-- Dimensions vs Fact? All columns are information about the customer -- dimension
-- There is no measurable data to be used as Fact
-- Surrogate Key: System-generated unique identifier assigned to each record in a table
    -- In the case that the primary key is not provided, we need to generate one to connect the dimension table and the fact table.
    -- It does not have any business meaning; it was just created for the data model.
-- Last step, we will create an object. We will create them as a View.
CREATE VIEW gold.dim_customer AS 
    SELECT
            ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key, -- Surrogate Key
            ci.cst_id AS customer_id,
            ci.cst_key AS customer_number,
            ci.cst_firstname AS first_name,
            ci.cst_lastname AS last_name,
            la.cntry AS country,
            ci.cst_marital_status AS marital_status,
            CASE 
                WHEN ci.cst_gndr='n/a' AND ca.gen IS NOT NULL THEN ca.gen
                ELSE ci.cst_gndr
            END gender,
            ca.bdate AS birthdate,
            ci.cst_create_date AS create_date
        FROM silver.crm_cust_info ci
        LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid 
        LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid

PRINT '----------------------------------------------------------------------------------------------------------'
PRINT 'Creating Product Dimension Table, using master table silver.crm_prd_info'
PRINT 'Joining with related tables; silver.erp_px_cat_g1v2'
PRINT '----------------------------------------------------------------------------------------------------------'
-- Join the silver.crm_prd_info table with its related tables to get other relevant information for the custumer object
-- After joining, check if any duplicate data has been introduced to the main table
-- If end date is NULL, then it is the  current info of the product!
-- After all, check if prd_key i unique after join
SELECT 
    prd_key,
    COUNT(*)
FROM 
 (         SELECT 
            pi.prd_id,
            pi.prd_key,
            pi.cat_id,
            pi.prd_nm,
            pi.prd_cost,
            pi.prd_line,
            pi.prd_start_dt,
            pi.prd_end_dt,
            cat.cat,
            cat.subcat,
            cat.maintance
        FROM silver.crm_prd_info pi
        LEFT JOIN silver.erp_px_cat_g1v2 cat ON pi.cat_id = cat.id
        WHERE pi.prd_end_dt IS NULL -- Selection of current products only. Historical data filtered out.
 )t GROUP BY prd_key 
 HAVING COUNT(*) >1; -- No duplicates

 -- FINAL QUERY
-- Rename columns to friendly, meaningful names
-- Sort the columns into logical groups to improve readability
-- Dimensions vs Fact? All columns contain information about the product -- dimension
-- There is no measurable data to be used as Fact
-- Surrogate Key: System-generated unique identifier assigned to each record in a table
    -- In the case that the primary key is not provided, we need to generate one to connect the dimension table and the fact table.
    -- It does not have any business meaning; it was just created for the data model.
-- In the last step, we will create an object. We will create them as a View.
-- Apply a quality check after it is done
CREATE VIEW gold.dim_product AS
    SELECT 
        ROW_NUMBER() OVER(ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key,
        pi.prd_id AS product_id,
        pi.prd_key AS product_number,
        pi.prd_nm AS product_name,
        pi.cat_id AS category_id,
        cat.cat AS category,
        cat.subcat AS subcategory,
        cat.maintance,
        pi.prd_cost AS cost,
        pi.prd_line AS product_line,
        pi.prd_start_dt AS start_date
    FROM silver.crm_prd_info pi
    LEFT JOIN silver.erp_px_cat_g1v2 cat ON pi.cat_id = cat.id
    WHERE pi.prd_end_dt IS NULL
