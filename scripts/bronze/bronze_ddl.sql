

/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

-- Step 1: Drop `bronze.crm_cust_info` if it exists

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

-- * **`OBJECT_ID(..., 'U')`** checks if the object exists and is a user table (`'U'`) ([reddit.com][1], [geeksforgeeks.org][2]).
-- * If found, the table is dropped—removing its schema and all data.

-- Step 2: Create `bronze.crm_cust_info`

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);
GO

-- * Defines a **raw CRM customer master table** with relevant fields to capture initial data ingestion.

-- Step 3: Drop `bronze.crm_prd_info` if it exists & recreate

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);
GO

-- * Cleans up old CRM product master table.
-- * Creates new staging table with product information and validity dates from CRM system.

-- Step 4: Drop & create `bronze.crm_sales_details`

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
GO

-- * Recreates CRM sales transaction data table.
-- * Note: order/ship/due dates stored as `INT` (potentially `YYYYMMDD`). These will need conversion during transformation.

-- Step 5: Drop & create `bronze.erp_loc_a101`

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO

-- * Defines raw location table from ERP system, capturing Customer/Company (`cid`) and Country information.

-- Step 6: Drop & create `bronze.erp_cust_az12`

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
GO

-- * Sets up ERP customer demographics staging table with birthdate and gender fields.

-- Step 7: Drop & create `bronze.erp_px_cat_g1v2`

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO

-- * Defines ERP product category mapping table with hierarchy and maintenance flags.

-- ## Why each step matters

-- * **Conditional drops** (`IF OBJECT_ID... DROP`) make the script *idempotent*, avoiding errors if rerun ([geeksforgeeks.org][2], [reddit.com][1]).
-- * **Sequential DDL** ensures a clean slate for each staging table—preventing schema drift and mismatches.
-- * **Bronze schema tables** mirror source systems *as-is*, enabling preservation of raw data for lineage, error recovery, and full traceability (Medallion architecture).

-- ## ⚠️ Warnings & Recommendations

-- * **Data loss risk**: Dropping tables erases all data—ensure you have backups or are working in dev/test environments.
-- * **`INT` date fields** need conversion during ETL (e.g., `CONVERT(date, CAST(sls_order_dt AS CHAR(8)), 112)`).
-- * Consider adding **primary keys or indexes** and normalizing naming conventions (e.g., `id` vs. `key`, consistent casing).
-- * To evolve schemas, consider using migration-based tools (Flyway, Liquibase) instead of destructive DDL.

-- ## 🧭 Summary View

-- | Step | Action                            | Purpose                       |
-- | ---- | --------------------------------- | ----------------------------- |
-- | 1–2  | Drop & create `crm_cust_info`     | Raw CRM customer data staging |
-- | 3    | Drop & create `crm_prd_info`      | Raw CRM product data staging  |
-- | 4    | Drop & create `crm_sales_details` | Raw CRM sales transactions    |
-- | 5    | Drop & create `erp_loc_a101`      | Raw ERP location data         |
-- | 6    | Drop & create `erp_cust_az12`     | Raw ERP customer demographics |
-- | 7    | Drop & create `erp_px_cat_g1v2`   | Raw ERP product categories    |