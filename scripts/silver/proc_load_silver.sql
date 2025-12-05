/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL Silver.load_silver;
===============================================================================
*/

call silver.load_silver();

CREATE OR REPLACE PROCEDURE silver.load_silver()
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
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '======================================';

    --------------------------------------------------------
    -- CRM TABLES
    --------------------------------------------------------
    RAISE NOTICE '--------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '--------------------------------------';

    -- CRM CUST INFO
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;

    RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
    insert into silver.crm_cust_info  (
    	cst_id,
    	cst_key,
    	cst_firstname,
    	cst_lastname, 
    	cst_marital_status,
    	cst_gender,
    	cst_create_date
    )
    
    select 
    	cst_id,
    	cst_key,
    	trim(cst_firstname) as cst_firstname,
    	trim(cst_lastname) as cst_lastname,
    	case 
	    	when upper(trim(cst_gender)) = 'F' then 'Female'
	    	when upper (trim(cst_gender)) = 'M' then 'Male'
	    	else 'n/a'
	    end as cst_gender,
	    cst_create_date
	from (
		select 
			*,
			row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
			from bronze.crm_cust_info cci 
			where cst_id is not null
		) t 
		where flag_last = 1;
	
    end_time := NOW();
   
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	raise notice '>>----------------------------';

    -- CRM PRODUCT
    start_time := NOW();

    RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;

    RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
    insert into silver.crm_prd_info (
    	prd_id,
    	cat_id,
    	prd_key,
    	prd_nm,
    	prd_cost,
    	prd_line,
    	prd_start_dt,
    	prd_end_dt 	
    )
    
    select 
    	prd_id,
    	replace(substring(prd_key, 1, 5), '-', '_') as cat_id, 	-- extract category ID
    	substring(prd_key, 7, length(prd_key)) as prd_key, 		-- extract product key
    	prd_nm,
    	coalesce (prd_cost::integer, 0) as prd_cost,
    	case 
	    	when upper(trim(prd_line)) = 'M' then 'Mountain'
	    	when upper(trim(prd_line)) = 'R' then 'Road'
	    	when upper(trim(prd_line)) = 'S' then 'Outher Sales'
	    	when upper(trim(prd_line)) = 'T' then 'Touring'
	    end as prd_line, --map product line codes to descriptive values
	    cast(prd_start_dt as date) as prd_start_dt,
	    cast(
	    	lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) - 1 as date 
	    ) as prd_end_dt --Calculate end date as one day before the next start date 
	    from bronze.crm_prd_info cpi ;
	    

    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
   	raise notice '>>----------------------------';


    -- CRM SALES DETAILS
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;

    RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
   	insert into silver.crm_sales_details (
	   	sls_ord_num ,
	   	sls_prd_key ,
	   	sls_cust_id ,
	   	sls_order_dt ,
	   	sls_ship_dt ,
	   	sls_due_dt ,
	   	sls_sales ,
	   	sls_quantity ,
	   	sls_price 
   	)
   select 
   	sls_ord_num ,
   	sls_prd_key ,
   	sls_cust_id ,
   	case 
	   	when sls_order_dt = 0 or char_length(sls_order_dt::varchar) != 8 then null 
	   	else to_date(sls_order_dt::varchar, 'YYYYMMDD')
	end as sls_order_dt ,
		case 
	   	when sls_ship_dt  = 0 or char_length(sls_ship_dt ::varchar) != 8 then null 
	   	else to_date(sls_ship_dt ::varchar, 'YYYYMMDD')
	end as sls_ship_dt  ,
   	case 
	   	when sls_due_dt  = 0 or char_length(sls_due_dt ::varchar) != 8 then null 
	   	else to_date(sls_due_dt ::varchar, 'YYYYMMDD')
	end as sls_due_dt  ,
	case 
		when sls_sales is null 
			or sls_sales <= 0 
			or sls_sales != sls_quantity  * abs(sls_price)  
		then sls_quantity * abs(sls_price)  
		else sls_sales 
	end as sls_sales , --Recalculate sales if original value is missing or incorrect
	sls_quantity,
	case 
		when sls_price is null or sls_price <= 0 
			then sls_sales / nullif(sls_quantity, 0)
		else sls_price --Derive price if original value is invalid
	end as sls_price 
from bronze.crm_sales_details ;

    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	raise notice '>>--------------------------';

    --------------------------------------------------------
    -- ERP TABLES
    --------------------------------------------------------
    RAISE NOTICE '--------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '--------------------------------------';

    -- ERP CUST
   	start_time := NOW();
    RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;

    RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
   	insert into silver.erp_cust_az12 (
   	cid, 
   	bdate,
   	gen 
   	)
   	select
		case when cid like 'NAS%' then substring (cid,4,length(cid))
		else cid
	end as cid, 
        case when bdate > now() then null 
        else bdate
    end as bdate,
        gen
      from bronze.erp_cust_az12 eca ;
	
    end_time := NOW();
    RAISE NOTICE'>> Load Duration: % seconds', EXTRACT(EPOCH from (end_time - start_time));
    raise notice'>>----------------------------';
   
    -- ERP LOC
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;

    RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
   	insert into silver.erp_loc_a101 (
   	cid,
   	cntry 
   	)
   	select
   		replace (cid, '-', '') as cid,
   		case 
	   		when trim(cntry) = 'DE' then 'Germany'
	   		when trim(cntry) in ('US','USA')then 'United States'
	   		when trim(cntry) = '' or cntry is null then 'n/a'
	   		else trim(cntry)
	   	end as cntry --Normalise and Handle missing or blank country codes
	   	from bronze.erp_loc_a101 ;
	
   
   	end_time := NOW();
   	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH from (end_time - start_time));
   	raise notice '>>---------------------------';
   
    -- ERP PX_CAT
   	start_time := NOW();
    RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
   	insert into silver.erp_px_cat_g1v2 (
   	id, 
   	cat, 
   	subcat,
   	maintenance 
   	)
	select
	id,
	cat, 
	subcat, 
	maintenance 
	from bronze.erp_px_cat_g1v2 ;
	
   	end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH from (end_time - start_time));
   	raise notice '>>--------------------------';
   
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
end;
$$;


