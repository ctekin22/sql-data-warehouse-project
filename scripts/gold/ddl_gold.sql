/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customer', 'V') IS NOT NULL
    DROP VIEW gold.dim_customer;
GO

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
        LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid;


-- =============================================================================
-- Create Dimension: gold.dim_product
-- =============================================================================
IF OBJECT_ID('gold.dim_product', 'V') IS NOT NULL
    DROP VIEW gold.dim_product;
GO

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
    WHERE pi.prd_end_dt IS NULL;

-- =============================================================================
-- Create Dimension: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS 
    SELECT
        sa.sls_ord_num AS order_number,
        pc.product_key,
        dc.customer_key,
        sa.sls_order_dt AS order_date,
        sa.sls_ship_dt AS shipping_date,
        sa.sls_due_dt AS due_date,
        sa.sls_sales AS sales,
        sa.sls_quantity AS quantity,
        sa.sls_price AS price
    FROM silver.crm_sales_details sa 
    LEFT JOIN gold.dim_customer dc ON sa.sls_cust_id = dc.customer_id 
    LEFT JOIN gold.dim_product pc ON sa.sls_prd_key = pc.product_number;
