/*
======================================================
    Verificacion de calidad
======================================================
    Proposito de los scripts:
        Todos los comandos y clausulas SQL realizan varios controles de calidad para la consistencias de los datos, exactitud de la informacion dentro de las tablas, y la estandarizacion a lo largo del esquema de la capa de 'plata'. Los cuales incluyen entre otras cosas verificaciones para:
            - Null's o valores duplicados en las claves primarias
            - Espacios indeseados en cadenas de caracteres
            - Rangos de fechas y compras, ordenes invalidos
            - Consistencia los datos entre columnas relacionadas

    Notas de uso:
        - Correr los siguientes scripts luego de cargar los datos dentro de la capa de plata
        - Porfavor no tomar todo lo expuesto a continuacion como un estandar, siempre investigar ante problemas de inconsistencias y buscar mejores maneras de resolver todo
*/

-- ===============================================================
-- tabla crm_cust_info

-- verificar por valores nulos o duplicados en la clave primaria
-- esperado: Sin resultados
SELECT 
 cst_id,
 count(*)
FROM silver.crm_cust_info
GROUP BY cst_id
 -- por supuesto que no necesito todos los conteos de claves que son de tipo unitaria
 -- me sirve con que UNA tenga mas de una para detectar valores extranios
 -- ademas no necesito que se cuenten nulos
 HAVING count(*) > 1 OR cst_id IS NULL;
-- verificar por espacios innecesarios entre cadenas de caracteres
--resultado esperado que no haya ninguno
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
--======================================
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- Estandarizacion de los datos y consistencia de los datos
SELECT DISTINCT(cst_marital_status)
FROM silver.crm_cust_info

SELECT DISTINCT(cst_gndr)
FROM silver.crm_cust_info
-- ========================================================
SELECT * FROM silver.crm_cust_info


-- ===============================================================
-- tabla crm_prd_info

-- verificar por valores nulos o duplicados en la clave primaria
-- esperado: Sin resultados
-- resultado no hay nulos ni duplicados
SELECT 
 prd_id,
 count(*)
FROM silver.crm_prd_info
GROUP BY prd_id 
HAVING count(*) > 1 OR prd_id IS NULL;

 -- verificar por espacios en blanco innecesarios
 -- resultado esperado: no hay espacios blancos

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

 -- verificar por Nulls o numeros negativos
 -- resultado esperado: no hay resultados

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR  prd_cost IS NULL

-- Estandarizacion y consistencia de los datos
SELECT distinct(prd_line) from silver.crm_prd_info

-- verificar por fechas de ordenes invalidas
-- EVITAR LA SUPERPOSICION DE FECHAS EN REGISTROS!!!
-- IMPORTANTE: No siempre vale con decir que el inicio debe de ser menor que el final en rango de fecha concreto
-- SINO QUE ADEMAS!, el final del primer historial debe de ser MAS RECIENTE que el inicio del siguiente
-- problema, los registros deben de tener fecha de inicio y fin no puede haber NULLS
-- PERO!!!!, si una fecha de inicio no tiene final, es decir la fecha de fin es un NULL es correcto ya que es una
-- fecha que puede ser actual

-- esto es para corroborar el proceso
SELECT 
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt,
	-- Tenemos dos funciones para esta situacion de fechas: LEAD() y LAG()
	-- en donde por medio de LEAD puedo ver la ventana de fin por medio de usar el inicio y asi acomodar la fecha de final
	-- de una fecha usando una de inicio del siguiente registro
	LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 end_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509')
-- No use silver por una cuestion de practica y corroborar que funcione
-- la pregunta por NOT IN me asegura que deben de coincidir todos los valores dentro de la tabla

-- ===============================================================
-- tabla crm_sales_details

-- verifico fechas invalidas
SELECT 
NULLIF(sls_order_dt,0) sls_order_dt
FROM silver.crm_sales_details
where sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101

-- ==========================
-- verifico fechas invalidas
SELECT 
NULLIF(sls_ship_dt,0) sls_ship_dt
FROM silver.crm_sales_details
where sls_ship_dt <= 0 
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101

-- =======================
-- verifico fechas invalidas
SELECT 
NULLIF(sls_due_dt,0) sls_due_dt
FROM silver.crm_sales_details
where sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101
-- =======================
-- verifico fechas invalidas
SELECT 
*
FROM silver.crm_sales_details
-- aqui compruebo que la fecha de la orden no sea mayor a la fecha de compra
-- basicamente porque un pedido siempre sera menor a una de envio o compra
-- y que la fecha de orden no sea mayor a la de vencimiento lo cual careceria de sentido directamente
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt


-- ============================================
-- Verificar consistencia de los datos: Entre las ventasm cantidad y el precio
-- >> Ventas = cantidad * precio
-- >> los valores nunca deben de ser CERO, NULL o Negativo

SELECT distinct
	sls_sales as old_sales,
	sls_price as old_price,

	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		 THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END as sls_sales,
	sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <= 0
		 THEN sls_sales / NULLIF(sls_quantity, 0)
		 ELSE sls_price
	END as sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- identificar fechas fuera de rango 
select distinct
bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > GETDATE() -- mayores a la fecha actual

-- estandarizacion de datos 
SELECT DISTINCT gen,
CASE
	WHEN TRIM(UPPER(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN TRIM(UPPER(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12

-- Estandarizacion de los datos
select  cid from silver.erp_loc_a101 


SELECT DISTINCT
 -- hay que quitar el signo '-' de entre la clave ya que no podriamos unirlo con customers info
 cntry as old_cntry,
 CASE
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101

-- verifico espacios innecesarios
SELECT * from bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR maintenance != TRIM(maintenance) OR subcat != TRIM(subcat)

-- estandarizacion de los datos
SELECT DISTINCT cat FROM bronze.erp_px_cat_g1v2

select * FROM silver.erp_px_cat_g1v2
