PRINT '=========================================================================================================='
PRINT 'JOINING TABLES TO CREATE DIMENTION AND FACT TABLES/VIEW'
PRINT '=========================================================================================================='

PRINT '----------------------------------------------------------------------------------------------------------'
PRINT 'Creating Costumer Dimention Table, using master table silver.crm_cust_info'
PRINT 'Joining with related tables; silver.erp_cust_az12 and silver.erp_loc_a101'
PRINT '----------------------------------------------------------------------------------------------------------'
-- Join silver.crm_cust_info table with its related tables to get other relevant information for custumer object
-- After joining check if any duplicate data introduced to the main table
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

-- We have two coulmn with same information; gen and cst_gndr
-- Mater table which is crm table should have more accurate data
-- Use data from master table in the case of inconsistancy
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
-- Dimentions vs Fact? All columns are actually information about the costumer -- dimention
-- There is no measurable data to be used as Fact
-- Surrogate Key : System generated unique identifier assigned to each record in a table
    -- In the case primary key is not provided, we need to generate one to connect dimention table and fact table.
    -- It does not have any business meaning, just created for data model.
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
