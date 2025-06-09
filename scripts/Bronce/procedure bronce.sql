/* 
 =======================================================================================
  Cargas a las tablas por medio de los Stored Procedures
	- Por medio de las siguientes puedo cargar las tablas de mi BD por medio de inserciones
	  masivas de los datos, denominadas BULK INSERTIONS
** proposito de los scripts
  Estos procedimientos almacenas deben de cargar la data dentro de la esquema de la capa de bronce,
  Lo realiza desde los CSV y realizan las siguientes acciones:
      - Truncan las tablas antes de cargar los datos.
      - Usa la clausula 'BULK INSERT' para cargar la data directamente desde los csv a las
        tablas de la capa de bronce

-- BULK INSERTION
-- consejo de los ingenieros: si todo es nuevo antes de insertar y se tiene permiso
-- una buena practica es TRUNCAR las Tablas ante el error de haber duplicado de datos
=========================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	-- variables para el manejo de la medida del tiempo
	-- manejo de errores que puedan surgir en la carga de los datos
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '============================';
		PRINT 'Carga de la capa de bronce';
		PRINT '============================';
	
		PRINT '----------------------------';
		PRINT 'Carga de las tablas CRM';
		PRINT '----------------------------';

		-- aqui comenzaria a medir el tiempo teorico de duracion de la carga
		SET @start_time = GETDATE();
		PRINT '>> Truncando tabla: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Insercion de datos en tabla: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Clast\OneDrive\Desktop\Almacen de datos\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, -- le indico al motor que se saltee tantas filas como encabezado de archivo tenga	
			FIELDTERMINATOR = ',', -- es decir el separador entre valores en este caso una coma
								   -- pero puede ser un --> ;|#,"
		TABLOCK -- bloquea la tabla durante el proceso de carga de los datos
		);
		SET @end_time = GETDATE();
		PRINT '>> Tiempo de carga: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' segundos.';
		PRINT '============';
		-- la mejor practica en estos casos es siempre
		-- controlar la calidad de los datos que en corto, es revisar que hayan sido cargados
		-- en el lugar correcto, y que los datos que se insertan coincidan con el valor de las columnas
	
		SET @start_time = GETDATE();
		PRINT '>> Truncando tabla: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Insercion de datos en tabla: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Clast\OneDrive\Desktop\Almacen de datos\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
						   
		TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Tiempo de carga: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' segundos.';
		PRINT '============';

		SET @start_time = GETDATE();
		PRINT '>> Truncando tabla: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Insercion de datos en tabla: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Clast\OneDrive\Desktop\Almacen de datos\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
						   
		TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Tiempo de carga: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' segundos.';
		PRINT '============';

		PRINT '----------------------------';
		PRINT 'Carga de las tablas ERP';
		PRINT '----------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncando tabla: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Insercion de datos en tabla: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Clast\OneDrive\Desktop\Almacen de datos\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
						   
		TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Tiempo de carga: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' segundos.';
		PRINT '============';

		SET @start_time = GETDATE();
		PRINT '>> Truncando tabla: erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Insercion de datos en tabla: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Clast\OneDrive\Desktop\Almacen de datos\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
						   
		TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Tiempo de carga: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' segundos.';
		PRINT '============';

		SET @start_time = GETDATE();
		PRINT '>> Truncando tabla: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Insercion de datos en tabla: erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Clast\OneDrive\Desktop\Almacen de datos\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
						   
		TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Tiempo de carga: ' + CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) + ' segundos.';
		PRINT '============';
		
		SET @batch_end_time = GETDATE();	
		PRINT'=======================================================================';
		PRINT '>> Completado el proceso de carga de los datos: ';
		PRINT '			>> DURACION TOTAL DEL PROCESO: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) as nvarchar) + ' segundos.';
		PRINT'=======================================================================';

	END TRY
	-- Siempre que hay un try habra un catch al final
	-- Algo importante que se debe realizar en todo proceso ETL
	-- es verificar la duracion del mismo proceso, ayuda a identificar cuellos de botella, optimizar funciones para rendimiento
	-- monitoreo constante y tendencias en cambios en el mismo, ademas claro de la deteccion de problemas en codigo o el DWH
	BEGIN CATCH
		PRINT'=======================================================================';
		PRINT'HA OCURRIDO UN ERROR DURANTE LA CARGA DE LOS DATOS EN LA CAPA DE BRONCE';
		PRINT'Error Message'+ error_message();
		PRINT'Error Message'+ CAST(error_number() as nvarchar);
		PRINT'=======================================================================';
	END CATCH
END
