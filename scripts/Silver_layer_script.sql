/* Creacion de las tablas de la base de datos de la capa de plata */
/* Tablas de CRM y ERP */
-- ================================================
IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info(
	cst_id				 INT,
	cst_key				 NVARCHAR(50),
	cst_firstname		 	 NVARCHAR(50),
	cst_lastname			 NVARCHAR(50),
	cst_marital_status		 NVARCHAR(50),
	cst_gndr			 NVARCHAR(50),
	cst_create_date			 DATE,
	dwh_create_date			 DATETIME2 DEFAULT GETDATE()
);
-- ================================================
IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
	prd_id		 INTEGER,
	prd_key		 NVARCHAR(50),
	prd_nm		 NVARCHAR(50),
	prd_cost	 INTEGER,
	prd_line	 NVARCHAR(20),
	prd_start_dt	 DATETIME,
	prd_end_dt	 DATETIME,
	dwh_create_date		 DATETIME2 DEFAULT GETDATE()
);
-- ================================================
IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
	sls_ord_num		 NVARCHAR(50),
	sls_prd_key		 NVARCHAR(50),
	sls_cust_id		 INTEGER,
	sls_order_dt	 INTEGER,
	sls_ship_dt		 INTEGER,
	sls_due_dt		 INTEGER,
	sls_sales		 INTEGER,
	sls_quantity	 INTEGER,
	sls_price		 INTEGER,
	dwh_create_date		 DATETIME2 DEFAULT GETDATE()
);
-- ================================================
IF OBJECT_ID('silver.erp_cust_az12','U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
	cid		 NVARCHAR(50),
	bdate	 DATE,
	gen		 NVARCHAR(20),
	dwh_create_date		 DATETIME2 DEFAULT GETDATE()
);
-- ================================================
IF OBJECT_ID('silver.erp_loc_a101','U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
	cid		NVARCHAR(50),
	cntry	NVARCHAR(50),
	dwh_create_date		 DATETIME2 DEFAULT GETDATE()
);
-- ================================================
IF OBJECT_ID('silver.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
	id			NVARCHAR(50),
	cat			NVARCHAR(50),
	subcat		NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_create_date		 DATETIME2 DEFAULT GETDATE()
);

