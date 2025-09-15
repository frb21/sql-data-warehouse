/*
  Create Database and Schemas

  This script creates a new database named 'DataWarehouse' after checking if it already exists.
  Additionally, this script sets up three schemas within the database: 'bronze', 'silver', 'gold'.

WARNING:
  This script will drop the database 'DataWarehouse' if it already exists.
  Proceed with caution and ensure having proper backups before running this script.
*/

USE master;
GO
-- Drop and recreate the Database
IF EXISTS(SELECT FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK_IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO

-- Create Database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
