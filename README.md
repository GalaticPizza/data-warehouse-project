# Proyecto Almacen de Datos

Construccion de un Data Warehouse con SQL Server, incluyendo procesos ETL, modelado de datos y analitica.

# Convenciones para el proyecto de Almacen de datos

- Usar el snake_case, con letras en lowercase y guiones bajos (_) para separar las palabras
- Lenguaje: Usar ingles para todos los nombres, o el español en caso de haber un consenso entre los miembros del equipo de proyecto
- Evitar palabras reservadas: Independientemente del SGBD debe evitarse a toda costa usar palabras reservadas de SQL para nombrar nombres de objetos de la base de datos

### Capa de Bronze:

- Todas los nombres deben comenzar con el nombre del sistema fuente, y los nombres de las tablas deben coincidir con sus nombres originales sin renombrar.

`<sourcesystem_entity>`:

- `<sourcesystem>`: Nombre del sistema fuente (por ejemplo, crm, erp).
- `<entity>`: Nombre exacto de la tabla del sistema fuente.
- Ejemplo: crm_customer_info ->Información del cliente del sistema CRM.

### Capa de Plata:

- Todas los nombres deben comenzar con el nombre del sistema fuente, y los nombres de las tablas deben coincidir con sus nombres originales sin renombrar.

`<sourcesystem_entity>`:

- `<sourcesystem>`: Nombre del sistema fuente (por ejemplo, crm, erp).
- `<entity>`: Nombre exacto de la tabla del sistema fuente.
- Ejemplo: crm_customer_info ->Información del cliente del sistema CRM.

### Capa de Oro

- Todos los nombres deben usar nombres significativos y alineados con el negocio para las tablas, comenzando con el prefijo de categoría.
    
    `<category>_<entity>`
    
- `<category>`: Describe el rol de la tabla, como `dim` (dimensión) o `fact` (tabla de hechos).
- `<entity>`: Nombre descriptivo de la tabla, alineado con el dominio del negocio (por ejemplo, `customers`, `products`, `sales`).

### Ejemplos:

- `dim_customers` → Tabla de dimensión para datos de clientes.
- `fact_sales` → Tabla de hechos que contiene transacciones de ventas.

### Glosario de Patrones de Categoría

| Patrón | Significado | Ejemplo(s) |
| --- | --- | --- |
| `dim_` | Tabla de dimensión | `dim_customer`, `dim_product` |
| `fact_` | Tabla de facturas | `fact_sales` |
| `agg_` | Tabla agregada | `agg_customers`, `agg_sales_monthly` |

### Convenciones de Nombres de Columnas

### Claves Sustitutas

- Todas las claves primarias en las tablas de dimensión deben usar el sufijo `_key`.
    - `<table_name>_key`
        - `<table_name>`: Se refiere al nombre de la tabla o entidad a la que pertenece la clave.
        - `_key`: Sufijo que indica que esta columna es una clave sustituta.
    - **Ejemplo:** `customer_key` → Clave sustituta en la tabla `dim_customers`.

### Columnas Técnicas

- Todas las columnas técnicas deben comenzar con el prefijo `dwh_`, seguido de un nombre descriptivo que indique el propósito de la columna.
    - `dwh_<column_name>`
        - `dwh_`: Prefijo exclusivo para metadatos generados por el sistema.
        - `<column_name>`: Nombre descriptivo que indica el propósito de la columna.
    - **Ejemplo:** `dwh_load_date` → Columna generada por el sistema utilizada para almacenar la fecha en que se cargó el registro.

### Procedimientos Almacenados

- Todos los procedimientos almacenados utilizados para cargar datos deben seguir el patrón de nombres: `load_<layer>`.
    - `<layer>`: Representa la capa que se está cargando, como `bronze`, `silver` o `gold`.
    - **Ejemplo:**
        - `load_bronze` → Procedimiento almacenado para cargar datos en la capa Bronze.
        - `load_silver` → Procedimiento almacenado para cargar datos en la capa Silver.

### Diseño de la arquitectura de los Datos

![Data warehouse - arquitectura](https://github.com/user-attachments/assets/e26fe7ae-8d8a-4e9f-a946-705bf130a809)

### Flujo de datos de la capa de Bronce (Bronze Layer)
![image](https://github.com/user-attachments/assets/cceab668-4859-481d-a0ed-2e31f375a80e)
### Base teorica del Data Flow:
En primera Instancia tendremos la capa de bronce con sus respectivo flujo de datos o data flow, donde tendremos los siguientes archivos:

- Bronze_Layer scripts.sql: el cual es el encargado de crear las tablas dentro del DWH y de crear la estructura interna           correspondiente para luego realizar la inserción dentro de cada tabla de la BD.
- procedimiento_carga_bronce_sql: este mismo es quien se encarga de la fase de EXTRACCION del proceso ETL para la insercion de los datos en cada esquema de tabla, se lo realiza por medio de un procedimiento almacenado el cual permite automatizar la carga de los datos, dando informacion tanto del tiempo de ejecucion de cada tabla, como del procedimiento en general. A su vez permite realizar un proceso de cheking para corroborar si hubo algun error durante la carga y da avisos por pantalla.

