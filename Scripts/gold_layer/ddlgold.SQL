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


-- =======================================
-- CREATE DIMENSION: gold.dim_customers
-- =======================================

IF OBJECT_ID ('gold.dim_customers' , 'V') IS NOT NULL
	DROP VIEW gold.dim_customers
GO

CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
	CI.cst_id AS customer_id,
	CI.cst_key AS customer_number,
	CI.cst_firstname AS first_name,
	CI.cst_lastname AS last_name,
	LA.cntry AS country,
	CI.cst_marital_status AS marital_status,
	CASE WHEN CI.cst_gndr !='n/a' THEN CI.cst_gndr
		ELSE COALESCE(CA.gen,'n/a')
	END AS gender,
	CA.bdate AS birth_date,
	CI.cst_create_date AS create_date
FROM silver.crm_cust_info  CI

LEFT JOIN silver.erp_cust_az12 CA
ON   CI.cst_key = CA.cid
LEFT JOIN silver.erp_loc_a101 LA
ON   CI.cst_key = LA.cid

-- =======================================
-- CREATE DIMENSION: gold.dim_products
-- =======================================

IF OBJECT_ID ('gold.dim_products' , 'V') IS NOT NULL
	DROP VIEW gold.dim_products
GO

CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY PN.prd_start_dt , PN.prd_key) AS product_key,
	PN.prd_id  AS product_id,
	PN.prd_key AS product_number,
	PN.prd_nm  AS product_name,
	PN.cat_id  AS category_id,
	PC.cat     AS category,
	PC.subcat AS sub_category,
	PC.maintenance,
	PN.prd_cost AS cost,
	PN.prd_line AS product_line,
	PN.prd_start_dt AS start_date
FROM silver.crm_prd_info PN
LEFT JOIN silver.erp_px_cat_g1v2 AS PC
ON PN.cat_id = PC.id
WHERE prd_end_dt IS NULL -- filter out all the historical data

-- =======================================
-- CREATE DIMENSION: gold.fact_sales
-- =======================================

IF OBJECT_ID ('gold.fact_sales' , 'V') IS NOT NULL
	DROP VIEW gold.fact_sales
GO


CREATE VIEW gold.fact_sales AS
SELECT
	SD.sls_ord_num AS order_number,
	PR.product_key,
	CU.customer_key,
	SD.sls_order_dt AS order_date,
	SD.sls_ship_dt AS shipping_date,
	SD.sls_due_dt AS due_date,
	SD.sls_sales AS sales_amount,
	SD.sls_quantity AS quantity,
	SD.sls_price AS price
FROM silver.crm_sales_details SD
LEFT JOIN gold.dim_products PR
ON    SD.sls_prd_key = PR.product_number

LEFT JOIN gold.dim_customers CU
ON     SD.sls_cust_id = CU.customer_id
