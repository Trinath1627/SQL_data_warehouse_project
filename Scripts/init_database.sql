/*
=========================================
Create Database and Schemas
=========================================

Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists.
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
    within the database: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists.
    All data in the database will be permanently deleted. Proceed with caution
    and ensure you have proper backups before running this script.

*/

--Step 1: Terminate all connections
DO $$
BEGIN
    PERFORM pg_terminate_backend(pid)
    FROM pg_stat_activity
    WHERE datname = 'DataWarehouse'
      AND pid <> pg_backend_pid();
END $$;

-- Step 2: Drop database
DROP DATABASE IF EXISTS DataWarehouse;

-- Step 3: Recreate database
CREATE DATABASE DataWarehouse
    WITH OWNER = postgres
         ENCODING = 'UTF8'
         TEMPLATE = template1;


create schema bronze
create schema silver
create schema gold
