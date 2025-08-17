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
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

    DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY

		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';


		Set @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;

		PRINT '>> Inserting Data into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id ,	
			cst_key	,
			cst_firstname ,
			cst_lastname ,
			cst_marital_status ,
			cst_gndr ,	
			cst_create_date )

		select 
			cst_id,
			cst_key,
			rtrim(ltrim(cst_firstname)) as cst_firstname,
			rtrim(ltrim(cst_lastname))  as cst_lastname,
			CASE 
				WHEN UPPER(RTRIM(LTRIM(cst_marital_status))) = 'M' THEN 'Married'
					 WHEN UPPER(RTRIM(LTRIM(cst_marital_status))) = 'S' THEN 'Single'
					 else 'n/a'
			END AS  cst_marital_status,  -- Normalise marital status values to readable format
			CASE 
				WHEN UPPER(RTRIM(LTRIM(cst_gndr))) = 'F' THEN 'Female'
					 WHEN UPPER(RTRIM(LTRIM(cst_gndr))) = 'M' THEN 'Male'
					 else 'n/a'
			END AS cst_gndr,     -- Normalise gender values to readable format
			cst_create_date
		from 
		(
			select 
				* ,
				ROW_NUMBER() over (PARTITION BY cst_id ORDER BY cst_create_date DESC) as row_num 
			from bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t 
		where row_num = 1 ; -- select the most recent score per customer
		Set @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------';
		----------------------------------
		
		Set @start_time = GETDATE();	
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;

		PRINT '>> Inserting Data into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id ,	
			cat_id ,
			prd_key,
			prd_nm ,
			prd_cost ,
			prd_line ,	
			prd_start_dt,
			prd_end_dt )

		select 
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, -- EXTRACT CATEGORY ID
			SUBSTRING(prd_key,7,len(prd_key)) as prd_key,      -- EXTRACT PRODUCT KEY
			prd_nm,
			coalesce(prd_cost,0) as prd_cost, -- HANDLING NULLS
			CASE
				WHEN UPPER(RTRIM(LTRIM(prd_line))) = 'M' THEN 'Mountain'
				WHEN UPPER(RTRIM(LTRIM(prd_line))) = 'R' THEN 'Road'
				WHEN UPPER(RTRIM(LTRIM(prd_line))) = 'S' THEN 'Other Sales'
				WHEN UPPER(RTRIM(LTRIM(prd_line))) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, -- Map Product Line Codes to Descriptive values
			CAST(prd_start_dt as date) as prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) as prd_end_dt
			-- Calculate end date as one day before the next start date
		from bronze.crm_prd_info;
		Set @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------';

		-----------------------------------

		Set @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;

		PRINT '>> Inserting Data into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num	,
			sls_prd_key	,
			sls_cust_id ,
			sls_order_dt,
			sls_ship_dt ,
			sls_due_dt ,	
			sls_sales ,
			sls_quantity ,
			sls_price )
		select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 or len(sls_order_dt) != 8 then NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE WHEN sls_ship_dt = 0 or len(sls_ship_dt) != 8 then NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE WHEN sls_due_dt = 0 or len(sls_due_dt) != 8 then NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,

			CASE WHEN sls_sales IS NULL OR sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
				 THEN sls_quantity * ABS(sls_price)
				 ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN  sls_sales/NULLIF(sls_quantity,0)
				 ELSE sls_price
			END AS sls_price    -- Derive price if old value is invalid
		from bronze.crm_sales_details;
		Set @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------';
	    ---------------------------------

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
	
		Set @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;

		PRINT '>> Inserting Data into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen)

		select 
			CASE WHEN  cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) -- Remove NAS prefix if present
				 ELSE cid
			END AS cid,
			CASE WHEN bdate > GETDATE() THEN NULL   -- Set Future birthdates to Null
				 ELSE bdate
			END AS bdate,
		CASE WHEN UPPER(RTRIM(LTRIM(gen))) IN ('F','FEMALE') THEN 'Female'
				WHEN UPPER(RTRIM(LTRIM(gen))) IN ('M','MALE') THEN 'Male'
				ELSE 'n/a'  -- Normalise gender values and handle unknown cases
		END AS gen
		from bronze.erp_cust_az12;
		Set @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------';
		-------------------------------------

		Set @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT '>> Inserting Data into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			CID,
			CNTRY)

		select 
			REPLACE(CID,'-','') AS CID,
			CASE WHEN RTRIM(LTRIM(CNTRY)) ='DE' THEN 'Germany'
				 WHEN RTRIM(LTRIM(CNTRY)) IN ('US','USA') THEN 'United States'
				 WHEN RTRIM(LTRIM(CNTRY)) = '' OR CNTRY IS NULL THEN 'n/a'
				 ELSE RTRIM(LTRIM(CNTRY))
			END AS CNTRY  -- Normalize and  handle missing or blank country codes
		from
		bronze.erp_loc_a101;
		Set @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------';
		-----------------------------------------------------------------------------------------------------


		Set @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		PRINT '>> Inserting Data into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE)

		select 
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		from
		bronze.erp_px_cat_g1v2;
		Set @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------';	

		SET @batch_end_time = GETDATE();
		PRINT '================================================';
		PRINT 'Bronze layer data loading is completed';
		PRINT 'TOTAL LOAD DURATION: ' + CAST(DATEDIFF(Second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '================================================';

	END TRY
	BEGIN CATCH
			PRINT '================================================';
			PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
			PRINT 'ERROR MESSAGE' + + ERROR_MESSAGE();
			PRINT 'ERROR MESSAGE' + + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'ERROR MESSAGE' + + CAST(ERROR_STATE() AS NVARCHAR);
			PRINT '================================================';
	END CATCH


END
