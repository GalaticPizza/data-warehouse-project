CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	-- variables para el manejo de la medida del tiempo
	-- manejo de errores que puedan surgir en la carga de los datos
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=========================';
		PRINT 'Carga de la capa de Plata';
		PRINT '=========================';
	
		PRINT '------------------------------------------';
		PRINT 'Carga de las tablas CRM';
		PRINT '------------------------------------------';

		-- aqui comenzaria a medir el tiempo teorico de duracion de la carga
		SET @start_time = GETDATE();
		PRINT' >> Truncado de la tabla: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;

		PRINT '>> Insercion de datos en tabla: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) as cst_firstname,
			TRIM(cst_lastname) as cst_lastname,
			-- cst_marital_status, aplico la misma metodica en el estado civil
			-- Aqui aplico la normalizacion de datos, o mejor dicho estandarizar una forma de verlos
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				-- manejo de valores nulos
				ELSE 'n/a'
			END cst_marital_status,
			-- cst_gndr,
			-- Estudio de la consistencia a lo largo del tiempo, siempre considerar la posibilidad 
			-- de que con el tiempo algun genio ponga f en lugar de F o Fimale en vez de Female
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END cst_gndr,
			cst_create_date
		FROM(
			-- limpieza de duplicados
			SELECT 
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) ultima_actualizacion
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t WHERE ultima_actualizacion = 1;
		SET @end_time = GETDATE();
		PRINT '>> Tiempo de carga: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' segundos.';
		PRINT '============';

		SET @start_time = GETDATE();
		PRINT' >> Truncado de la tabla: silver.crm_prd_infoo';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Insercion de datos en tabla: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			-- extraigo parte de la clave para trabajar
			-- tendre la clave partcionada en dos partes
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') cat_id, --extraigo el id de la categoria
			-- lo que hago aqui es dar dinamismo para que me devuelva la cantidad de caracteres exacta ya que no conozco todos
			-- los que estan en prd_key entonces lo que hago es restar desde la posicion que parto, menos el tama�o total 
			-- de esta manera puedo hacer un join con la tabla de erp
			SUBSTRING(prd_key,7, LEN(prd_key)) prd_key, -- extraigo el product key
			prd_nm,
			-- reemplazo valores nulos en la columna de los productos
			ISNULL(prd_cost,0) prd_cost, 
			-- transformacion: informacion perdida en este caso los nulls
			-- Quick case when, ideal cuando son pocos los valores o son muy sencillos
			-- normalizacion de los datos
			CASE UPPER(TRIM(prd_line)) 
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, -- mapeo el codigo de la linea de productos a valores mas descriptivos
			-- casteo del tipo de datos, para enriquecer los datos finales de produccion
			CAST(prd_start_dt AS DATE),
			CAST(
				LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 
				AS DATE
				) prd_end_dt 
			-- calculo de la fecha de fin o end date como un dia antes de la fecha siguiente osea el start date
		FROM bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT '>> Tiempo de carga: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' segundos.';
		PRINT '============';

		SET @start_time = GETDATE();
		PRINT' >> Truncado de la tabla: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Insercion de datos en tabla:silver.crm_sales_details';
		insert into silver.crm_sales_details(
			sls_ord_num		,
			sls_prd_key		,
			sls_cust_id		,
			sls_order_dt	,
			sls_ship_dt		,
			sls_due_dt		,
			sls_sales		,
			sls_quantity    ,
			sls_price		
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL --manejo de datos no validos
				ELSE CAST(CAST(sls_order_dt as varchar) as date) -- conversion de tipo de datos
			END as sls_order_dt,
			CASE
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt as varchar) as date)
			END as sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt as varchar) as date)
			END as sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) -- manejo de datos invalidos o perdidos
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END as sls_sales,
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 -- manejo de datos muchas veces invalidos
				THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END as sls_price
		FROM bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT '>> Tiempo de carga: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' segundos.';
		PRINT '============';

		PRINT '------------------------------------------';
		PRINT 'Carga de las tablas ERP';
		PRINT '------------------------------------------';
		SET @start_time = GETDATE();
		PRINT' >> Truncado de la tabla: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Insercion de datos en tabla: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
		SELECT 
		CASE
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- corte dinamico de esa parte de los caracteres del customer_id, remover 'NAS' como prefijo
			ELSE cid
		END AS cid,
		CASE
			WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate, -- setear los cumplea�os futuros como NULL
		CASE
			WHEN TRIM(UPPER(gen)) IN ('F', 'FEMALE') THEN 'Female'
			WHEN TRIM(UPPER(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
		END AS gen -- Normalizar los valores de generos y manejar los nulos y desconocidos en caso de aparecer
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE();
		PRINT '>> Tiempo de carga: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' segundos.';
		PRINT '============';

		SET @start_time = GETDATE();
		PRINT' >> Truncado de la tabla: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Insercion de datos en tabla: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(cid, cntry)
		SELECT
		-- hay que quitar el signo '-' de entre la clave ya que no podriamos unirlo con customers info
		REPLACE(cid, '-',''),
		CASE
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry -- normalizar y manejar datos perdidos o blancos en el codigo de paises
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT '>> Tiempo de carga: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' segundos.';
		PRINT '============';

		SET @start_time = GETDATE();
		PRINT' >> Truncado de la tabla: ilver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Insercion de datos en tabla: ilver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
		SELECT
			id, 
			cat,
			subcat,
			maintenance 
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT '>> Tiempo de carga: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' segundos.';
		PRINT '============';
		
		SET @batch_end_time = GETDATE();
		PRINT'=======================================================================';
		PRINT' Carga de la capa de plata completada';
		PRINT' - Duracion total: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) as nvarchar) + ' segundos.';
		PRINT'=======================================================================';
	END TRY
	BEGIN CATCH
		PRINT'=======================================================================';
		PRINT'HA OCURRIDO UN ERROR DURANTE LA CARGA DE LOS DATOS EN LA CAPA DE PLATA ';
		PRINT'Error Message'+ error_message();
		PRINT'Error Message'+ CAST(error_number() as nvarchar);
		PRINT'=======================================================================';
	END CATCH
END
