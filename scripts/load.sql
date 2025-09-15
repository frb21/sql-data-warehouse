/*
  Load Data into csv files to Database Tables
  Truncate and Insert
*/
TRUNCATE TABLE bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info
FROM '/data/source_crm/cust_info.csv'
WITH (FIRSTROW = 2,FIELDTERMINATOR = ',',ROWTERMINATOR = '\n',TABLOCK);

TRUNCATE TABLE bronze.crm_prd_info;
BULK INSERT bronze.crm_prd_info
FROM '/data/source_crm/prd_info.csv'
WITH (FIRSTROW = 2,FIELDTERMINATOR = ',',ROWTERMINATOR = '\n',TABLOCK);

TRUNCATE TABLE bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
FROM '/data/source_crm/sales_details.csv'
WITH (FIRSTROW = 2,FIELDTERMINATOR = ',',ROWTERMINATOR = '\n',TABLOCK);

TRUNCATE TABLE bronze.erp_cust_az12;
BULK INSERT bronze.erp_cust_az12
FROM '/data/source_erp/CUST_AZ12.csv'
WITH (FIRSTROW = 2,FIELDTERMINATOR = ',',ROWTERMINATOR = '\n',TABLOCK);

TRUNCATE TABLE bronze.erp_loc_a101;
BULK INSERT bronze.erp_loc_a101
FROM '/data/source_erp/LOC_A101.csv'
WITH (FIRSTROW = 2,FIELDTERMINATOR = ',',ROWTERMINATOR = '\n',TABLOCK);

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
BULK INSERT bronze.erp_px_cat_g1v2
FROM '/data/source_erp/PX_CAT_G1V2.csv'
WITH (FIRSTROW = 2,FIELDTERMINATOR = ',',ROWTERMINATOR = '\n',TABLOCK);
