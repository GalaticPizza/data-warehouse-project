/*
==========================================================
Crear Base de Datos y Esquemas
==========================================================

Propósito del Script:
Este script crea una nueva base de datos llamada 'DataWarehouse' después de verificar si ya existe.
Si la base de datos existe, se elimina y se vuelve a crear. 
Además, el script configura tres esquemas dentro de la base de datos: 'bronze', 'silver' y 'gold'.

ADVERTENCIA:
Ejecutar este script eliminará por completo la base de datos 'DataWarehouse' si existe.
Todos los datos en la base de datos serán eliminados permanentemente. 
Recomiendo el que proceda ante la ejecucion lo haga con precaución y asegúrese de tener copias de seguridad adecuadas antes de ejecutar este script.
*/
-- Create Database 'DataWarehouse'

USE master;
-- creando la base de datos
-- el siguiente script verificara que exista una base de datos con el nombre Datawarehouse
-- de no existir la creara o si no sera borrada!
-- seleccionar la base de datos
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWareHouse')
	BEGIN 
		-- Establecer la base de datos en modo usuario único para evitar bloqueos
		ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
		-- Eliminar la base de datos
		DROP DATABASE NombreBaseDatos;
	END;
GO

-- Creacion de la base de Datos
CREATE DATABASE DataWareHouse;
GO
USE DataWareHouse;
GO

-- creacion de los esquemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
