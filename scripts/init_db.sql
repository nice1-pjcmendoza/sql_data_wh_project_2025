

/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- 1. **Select or Create the Database**

USE master;
GO

-- * Switches context to the **master** system database to manage/create/drop other databases.

-- 2. **Drop & Recreate `DataWarehouse2025`**

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse2025')
BEGIN
    ALTER DATABASE DataWarehouse2025 SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse2025;
END;
GO

-- * Checks if a database named **DataWarehouse2025** already exists.
-- * If it does:

--   * Sets it to **SINGLE\_USER** mode to terminate any active connections immediately (`WITH ROLLBACK IMMEDIATE`).
--   * Drops the existing database to start fresh.

CREATE DATABASE DataWarehouse2025;
GO

-- * Recreates a clean **DataWarehouse2025** database.

-- 3. **Switch to `DataWarehouse2025` Context**

USE DataWarehouse2025;
GO

-- * Changes the session context to the newly created database, so subsequent commands apply here.

-- 4. **Set Up Schemas**

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

-- * Creates three distinct **schemas** within `DataWarehouse2025`:

--   * **bronze**: typically used for raw, ingested data with minimal processing.
--   * **silver**: for cleaned and standardized data.
--   * **gold**: for final, aggregated, analytics-ready data.

-- ### 🔄 Summary of Workflow

-- | Step | Action                                    | Purpose                                                                      |
-- | ---- | ----------------------------------------- | ---------------------------------------------------------------------------- |
-- | 1    | Drop existing `DataWarehouse2025`         | Ensure a fresh start                                                         |
-- | 2    | Create `DataWarehouse2025`                | Establish base database                                                      |
-- | 3    | Define `bronze`, `silver`, `gold` schemas | Establish a layered data architecture (also known as Medallion architecture) |

-- By implementing the **Bronze → Silver → Gold** schema separation, you establish a clean and organized data pipeline:

-- 1. **Bronze**—raw ingestion stage
-- 2. **Silver**—data preparation (cleaning, deduplication)
-- 3. **Gold**—finalized tables/views ready for reporting and BI

-- 💡 This structure promotes data quality, traceability, governance, and scalability in your ETL/data engineering workflows.