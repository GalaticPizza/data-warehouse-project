-- ================================================
/* Creacion de las tablas de la base de datos de la capa de bronce */
/* Tablas de CRM y ERP */
-- ================================================
IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
	cst_id			 INT,
	cst_key			 NVARCHAR(50),
	cst_firstname		 NVARCHAR(50),
	cst_lastname		 NVARCHAR(50),
	cst_marital_status	 NVARCHAR(50),
	cst_gndr		 NVARCHAR(50),
	cst_create_date		 DATETIME
);
-- ================================================
IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
	prd_id		 INTEGER,
	prd_key		 NVARCHAR(50),
	prd_nm		 NVARCHAR(50),
	prd_cost	 INTEGER,
	prd_line	 NVARCHAR(20),
	prd_start_dt 	 DATETIME,
	prd_end_dt	 DATETIME
);
-- ================================================
IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num		 NVARCHAR(50),
	sls_prd_key		 NVARCHAR(50),
	sls_cust_id		 INTEGER,
	sls_order_dt	 	 INTEGER,
	sls_ship_dt		 INTEGER,
	sls_due_dt		 INTEGER,
	sls_sales		 INTEGER,
	sls_quantity	 	 INTEGER,
	sls_price		 INTEGER
);
-- ================================================
IF OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
	cid		 NVARCHAR(50),
	bdate		 DATE,
	gen		 NVARCHAR(20)
);
-- ================================================
IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
	cid		NVARCHAR(50),
	cntry		NVARCHAR(50)
);
-- ================================================
IF OBJECT_ID('bronze.erp_px_cat_g1v','U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v;
CREATE TABLE bronze.erp_px_cat_g1v2(
	id			NVARCHAR(50),
	cat			NVARCHAR(50),
	subcat			NVARCHAR(50),
	maintenance		NVARCHAR(50)
);

