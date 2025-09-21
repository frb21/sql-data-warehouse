CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '=======================================';
        PRINT 'Load Silver Layer';
        PRINT '=======================================';
        
        PRINT '---------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '---------------------------------------';
    
        -- Transform Customer Data
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>> Inserting Data Into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
                cst_id, 
                cst_key, 
                cst_firstname, 
                cst_lastname, 
                cst_marital_status, 
                cst_gndr,
                cst_create_date
            )
            SELECT
                cst_id,
                cst_key,
                TRIM(cst_firstname) AS cst_firstname,
                TRIM(cst_lastname) AS cst_lastname,
                CASE 
                    WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                    ELSE 'n/a'
                END AS cst_marital_status, -- Normalize marital status values to readable format
                CASE 
                    WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                    ELSE 'n/a'
                END AS cst_gndr, -- Normalize gender values to readable format
                cst_create_date
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
                FROM bronze.crm_cust_info
                WHERE cst_id IS NOT NULL
            ) t
            WHERE flag_last = 1; -- Select the most recent record per customer
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '---------------------------------------';


    -- Transform Product Data
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info
    PRINT '>> Inserting Data Into: silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info(
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_line,
        prd_cost,
        prd_start_dt,
        prd_end_dt
    )
    SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Extract product key
    prd_nm,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN  'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line, -- Map product line codes to descriptive values
    ISNULL(prd_cost, 0) AS prd_cost,
    prd_start_dt,
    DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt -- Calculate end date as one day before the next start date
    FROM bronze.crm_prd_info
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '---------------------------------------';

    -- Transform Sales Data
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    PRINT '>> Inserting Data Into: silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details(
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_price,
        sls_quantity
    )
    SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST(sls_due_dt AS DATE)
    END AS sls_due_dt,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_price) * sls_quantity THEN sls_quantity * ABS(sls_price) 
        ELSE sls_sales 
    END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
    CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_price / NULLIF(sls_quantity, 0) 
        ELSE sls_price
    END AS sls_price, -- Derive price if original value is invalid
    sls_quantity
    FROM bronze.crm_sales_details
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '---------------------------------------';

    PRINT '---------------------------------------'
    PRINT 'Loading ERP Tables'
    PRINT '---------------------------------------'

    -- Transform Customer Data
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    PRINT '>> Inserting Data Into: silver.erp_cust_az12';
    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' keyword if present
        ELSE cid
    END AS cid,
    CASE WHEN bdate > GETDATE() THEN NULL  -- Set future bdates to NULL 
        ELSE bdate
    END AS bdate, -- Check if bdate is valid
    CASE 
        WHEN UPPER(REPLACE(gen, CHAR(13), '')) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(REPLACE(gen, CHAR(13), '')) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen -- Normalize gender values ad handle unknown cases
    FROM bronze.erp_cust_az12
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '---------------------------------------';


    -- Transform Location Data
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    PRINT '>> Inserting Data Into: silver.erp_loc_a101';
    INSERT INTO silver.erp_loc_a101 (cid, cntry)
    SELECT 
    REPLACE(cid, '-', '') AS cid, -- Replace em dash with empty character
    CASE WHEN TRIM(REPLACE(cntry, CHAR(13), '')) = 'DE' THEN 'Germany'
        WHEN TRIM(REPLACE(cntry, CHAR(13), '')) IN ('US', 'USA', 'United States') THEN 'United States'
        WHEN TRIM(REPLACE(cntry, CHAR(13), '')) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(REPLACE(cntry, CHAR(13), ''))
    END AS cntry -- Data Standardization and Normalization
    FROM bronze.erp_loc_a101
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '---------------------------------------';

    -- Transform Product Category Data
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT 
    SUBSTRING(REPLACE(id, '_', '-'), 1, 5) AS id,
    cat,
    subcat,
    CASE WHEN TRIM(REPLACE(maintenance, CHAR(13), '')) = 'Yes' THEN 'Yes'
        WHEN TRIM(REPLACE(maintenance, CHAR(13), '')) = 'No' THEN 'No'
    END AS maintenance 
    FROM bronze.erp_px_cat_g1v2
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '---------------------------------------';
    SET @batch_end_time = GETDATE();
    PRINT '>> Silver Layer Batch Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
    END TRY
    BEGIN CATCH
        PRINT '=======================================';
        PRINT 'AN ERROR OCCURRED';
        PRINT 'Error Message:' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Number: ' + CAST (ERROR_STATE() AS NVARCHAR);
        PRINT '=======================================';
    END CATCH    
END


