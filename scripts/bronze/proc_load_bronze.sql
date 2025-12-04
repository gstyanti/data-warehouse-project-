/*
========================================================================================
Stored Procedure
========================================================================================
Script Purpose:
This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs the following actions:
- Truncate the bronze tables before loading data.
- Uses the 'COPY' command to load data from csv files to bronze tables.

Parameters:
  None.
    This stored procedure does not accept any parameters of return any values.
========================================================================================
*/

call bronze.load_bronze();

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time timestamp;
    end_time   timestamp;
    batch_start_time timestamp;
   	batch_end_time timestamp;
begin
	batch_start_time := NOW();
    --------------------------------------------------------
    -- HEADER
    --------------------------------------------------------
    RAISE NOTICE '======================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '======================================';

    --------------------------------------------------------
    -- CRM TABLES
    --------------------------------------------------------
    RAISE NOTICE '--------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '--------------------------------------';

    -- CRM CUST INFO
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
    COPY bronze.crm_cust_info
    FROM 'D:/GROWTH!/Portofolio/Data Engineer/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));


    -- CRM PRODUCT
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;

    RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
    COPY bronze.crm_prd_info
    FROM 'D:/GROWTH!/Portofolio/Data Engineer/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));


    -- CRM SALES DETAILS
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;

    RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
    COPY bronze.crm_sales_details
    FROM 'D:/GROWTH!/Portofolio/Data Engineer/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));


    --------------------------------------------------------
    -- ERP TABLES
    --------------------------------------------------------
    RAISE NOTICE '--------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '--------------------------------------';

    -- ERP CUST
   	start_time := NOW();
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;

    RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12
    FROM 'D:/GROWTH!/Portofolio/Data Engineer/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
	
    end_time := NOW();
    RAISE NOTICE'>> Load Duration: % seconds', EXTRACT(EPOCH from (end_time - start_time));
   
   
    -- ERP LOC
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;

    RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101
    FROM 'D:/GROWTH!/Portofolio/Data Engineer/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
	
   
   	end_time := NOW();
   	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH from (end_time - start_time));
   
    -- ERP PX_CAT
   	start_time := NOW();
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2
    FROM 'D:/GROWTH!/Portofolio/Data Engineer/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

   	end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH from (end_time - start_time));
   
   batch_end_time := NOW();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', extract(epoch from (batch_end_time - batch_start_time));
    RAISE NOTICE '==========================================';
  
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        RAISE NOTICE 'Message: %', SQLERRM;
        RAISE NOTICE 'State  : %', SQLSTATE;
        RAISE NOTICE '==========================================';
END;
$$;

