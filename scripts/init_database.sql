/*
====================================================
Create Database & Schemas
====================================================
Scripts Purpose:
  This script creates a new database named 'Datawarehouse' after checking if it already exist.
  If the database exist, it is dropped and recreated. Additionally, the script set up there schemas 
  within the database: 'bronze', 'silver', and 'gold'.
*/

\c DataWarehouse;

-- Create Database 'DataWarehouse'
create schema bronze;
create schema silver;
create schema gold;
