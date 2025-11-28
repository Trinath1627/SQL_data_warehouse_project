/*
====================================================================================
DDL Script: Create Bronze Tables
====================================================================================

Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables
    if they already exist.
    Run this script to re-define the DDL structure of 'bronze' Tables

====================================================================================
*/

/*
 ***if table exists droping the table (MYSQL)***

if object_id('bronze.crm_cust_info','U') is not
	drop table bronze.crm_cust_inf



 *** use from inserting the data from files like .csv (sql) ***

BULK insert bronze.crm_cust_info
from ('')
with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
)
*/


create or replace procedure bronze.load_bronze()
LANGUAGE plpgsql
AS $$

DECLARE
    start_time TIMESTAMP;
    end_time   TIMESTAMP;
	batch_start_time timestamp;
	batch_end_time timestamp;
BEGIN
	    batch_start_time := clock_timestamp();
		RAISE NOTICE '===============================================================';
		RAISE NOTICE 'loading bronze layer';
		RAISE NOTICE '===============================================================';
	
		
		RAISE NOTICE '---------------------------------------------------------------';
		RAISE NOTICE 'loading CRM Tables';
		RAISE NOTICE '---------------------------------------------------------------';
	
		
		start_time := clock_timestamp();
	    raise notice '>> truncating Table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;
	
		raise notice '>> Inserting data into: bronze.crm_cust_info';
		copy bronze.crm_cust_info
		FROM 'C:\Program Files\PostgreSQL\18\data\temp\source_crm\cust_info.csv'
		WITH (
		    FORMAT csv,
		    HEADER true,
		    DELIMITER ','
		);
		DROP TABLE IF EXISTS bronze.crm_cust_info;
		create table bronze.crm_cust_info(
			cst_id int,
			cst_key varchar(50),
			cst_firstname varchar(50),
			cst_lastname varchar(50),
			cst_marital_status varchar(15),
			cst_gndr varchar(15),
			cst_create_date date
		);
		end_time := clock_timestamp();
		RAISE NOTICE 'Start time: %', start_time;
	    RAISE NOTICE 'End time: %', end_time;
	    RAISE NOTICE 'Total seconds: %', EXTRACT(EPOCH FROM (end_time - start_time));
		raise notice '--------------';
		

		start_time := clock_timestamp();
		raise notice '>> truncating Table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;
	
		raise notice '>> Inserting data into: bronze.crm_prd_info';
		copy bronze.crm_prd_info
		FROM 'C:\Program Files\PostgreSQL\18\data\temp\source_crm\prd_info.csv'
		WITH (
		    FORMAT csv,
		    HEADER true,
		    DELIMITER ','
		);
		DROP TABLE IF EXISTS bronze.crm_prd_info;
		create table bronze.crm_prd_info(
			prd_id int,
			prd_key varchar(50),
			prd_nm varchar(50),
			prd_cost int,
			prd_line varchar(30),
			prd_start_dt date,
			prd_end_dt date
		);
		end_time := clock_timestamp();
		RAISE NOTICE 'Start time: %', start_time;
	    RAISE NOTICE 'End time: %', end_time;
	    RAISE NOTICE 'Total seconds: %', EXTRACT(EPOCH FROM (end_time - start_time));
		raise notice '--------------';


		start_time := clock_timestamp();
		raise notice '>> truncating Table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
		
		raise notice '>> Inserting data into: bronze.crm_sales_details';
		copy bronze.crm_sales_details
		FROM 'C:\Program Files\PostgreSQL\18\data\temp\source_crm\sales_details.csv'
		WITH (
		    FORMAT csv,
		    HEADER true,
		    DELIMITER ','
		);
		drop table if exists bronze.crm_sales_details;
		create table bronze.crm_sales_details(
			sls_ord_num varchar(30),
			sls_prd_key varchar(50),
			sls_cust_id int,
			sls_order_dt int,
			sls_ship_dt int,
			sls_due_dt int,
			sls_sales int,
			sls_quantity int,
			sls_price int
		);
		end_time := clock_timestamp();
		RAISE NOTICE 'Start time: %', start_time;
	    RAISE NOTICE 'End time: %', end_time;
	    RAISE NOTICE 'Total seconds: %', EXTRACT(EPOCH FROM (end_time - start_time));
		raise notice '--------------';
		
		raise notice E'\n';
		RAISE NOTICE '---------------------------------------------------------------';
		RAISE NOTICE 'loading ERP Tables';
		RAISE NOTICE '---------------------------------------------------------------';
		

		start_time := clock_timestamp();
		raise notice '>> truncating Table: bronze.erp_cust_az1';
		truncate table bronze.erp_cust_az12;
	
		raise notice '>> Inserting data into: bronze.erp_cust_az1';
		copy bronze.erp_cust_az12
		FROM 'C:\Program Files\PostgreSQL\18\data\temp\source_erp\CUST_AZ12.csv'
		WITH (
		    FORMAT csv,
		    HEADER true,
		    DELIMITER ','
		);
		drop table if exists bronze.erp_cust_az12;
		create table bronze.erp_cust_az12(
			cid varchar(50),
			bdate date,
			gen varchar(20)
		);
		end_time := clock_timestamp();
		RAISE NOTICE 'Start time: %', start_time;
	    RAISE NOTICE 'End time: %', end_time;
	    RAISE NOTICE 'Total seconds: %', EXTRACT(EPOCH FROM (end_time - start_time));
		raise notice '--------------';
		
		start_time := clock_timestamp();
		raise notice '>> truncating Table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;
	
		raise notice '>> Inserting data into: bronze.erp_loc_a101';
		copy bronze.erp_loc_a101
		FROM 'C:\Program Files\PostgreSQL\18\data\temp\source_erp\LOC_A101.csv'
		WITH (
		    FORMAT csv,
		    HEADER true,
		    DELIMITER ','
		);
		drop table if exists bronze.erp_loc_a101;
		create table bronze.erp_loc_a101(
			cid varchar(50),
			cntry varchar(50)
		);
		end_time := clock_timestamp();
		RAISE NOTICE 'Start time: %', start_time;
	    RAISE NOTICE 'End time: %', end_time;
	    RAISE NOTICE 'Total seconds: %', EXTRACT(EPOCH FROM (end_time - start_time));
		raise notice '--------------';

		start_time := clock_timestamp();
		raise notice '>> truncating Table: bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
	
		raise notice '>> Inserting data into: bronze.erp_px_cat_g1v2';
		copy bronze.erp_px_cat_g1v2
		FROM 'C:\Program Files\PostgreSQL\18\data\temp\source_erp\PX_CAT_G1V2.csv'
		WITH (
		    FORMAT csv,
		    HEADER true,
		    DELIMITER ','
		);
		drop table if exists bronze.erp_px_cat_g1v2;
		create table bronze.erp_px_cat_g1v2(
			id varchar(30),
			cat varchar(50),
			subcat varchar(50),
			maintenance varchar(15)
		);
		end_time := clock_timestamp();
		RAISE NOTICE 'Start time: %', start_time;
	    RAISE NOTICE 'End time: %', end_time;
	    RAISE NOTICE 'Total seconds: %', EXTRACT(EPOCH FROM (end_time - start_time));
		raise notice '--------------';
		raise notice e'\n';
	batch_end_time := clock_timestamp();
	raise notice 'batch start time :%', batch_start_time;
	raise notice 'batch end time :%', batch_end_time;
	raise notice 'Total seconds:%', extract(epoch from (batch_end_time-batch_start_time));
	EXCEPTION
	    WHEN OTHERS THEN
	        RAISE NOTICE '===================================================';
	        RAISE NOTICE 'Error occured During loading bronze layer';
	        RAISE NOTICE 'error message: %', SQLERRM;
	        RAISE NOTICE 'error code: %', SQLSTATE;
	
END;
$$;








