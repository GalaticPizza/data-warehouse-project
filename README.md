# Proyecto Almacen de Datos

Construccion de un Data Warehouse con SQL Server, incluyendo procesos ETL, modelado de datos y analitica.

### Marco Teórico

Arquitectura de Datos Medallion (Medallón).

La Arquitectura de Datos Medallion es un patrón de diseño de datos utilizado para organizar lógicamente los datos en un Data WareHouse. Su objetivo es mejorar progresivamente la estructura y calidad de los datos a medida que fluyen a través de cada capa de la arquitectura, que generalmente se conocen como capas Bronce, Plata y Oro.

- Capa Bronce
    La capa Bronce es la etapa inicial donde se ingieren datos sin procesar de diversas fuentes, como almacenamiento en la nube, buses de mensajes y sistemas federados. Esta capa conserva el estado bruto de los datos en sus formatos originales y está destinada al consumo por cargas de trabajo que enriquecen los datos para las tablas Plata. Se realiza una validación mínima de datos en esta capa para evitar pérdidas de información.
    
- Capa Plata
    La capa Plata es donde ocurre la limpieza y validación de datos. Los datos de la capa Bronce se leen, limpian y validan para crear un conjunto de datos más refinado. Esta capa incluye operaciones como la aplicación de esquemas, el manejo de valores nulos y faltantes, la eliminación de duplicados y la normalización. La capa Plata mejora la calidad de los datos al corregir errores e inconsistencias y estructura la información en un formato más accesible para su procesamiento posterior.
    
- Capa Oro
    La capa Oro representa vistas altamente refinadas de los datos que impulsan análisis avanzados, paneles de control, aprendizaje automático (ML) y aplicaciones. Esta capa contiene datos agregados diseñados para análisis y generación de informes, alineándose con la lógica y necesidades del negocio. Está optimizada para el rendimiento en consultas y paneles, y proporciona conjuntos de datos con significado semántico que reflejan funciones empresariales.
- Beneficios de la Arquitectura Medallion
    - Mejora incremental: La calidad y estructura de los datos se mejoran progresivamente a través de cada capa.
    - Calidad de datos: Garantiza una alta calidad mediante procesos de validación y limpieza.
    - Optimización del rendimiento: Optimiza los datos para el rendimiento en análisis y generación de informes.
    - Flexibilidad: Permite reprocesamiento y auditoría al conservar todos los datos históricos.

Bibliografia (usada a modo de referencia adicional):

 - https://www.databricks.com/glossary/medallion-architecture
 - https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion

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

# Construccion de las 3 capas de la arquitectura

## Capa de Bronce

### Flujo de datos de la capa de Bronce (Bronze Layer)
![image](https://github.com/user-attachments/assets/cceab668-4859-481d-a0ed-2e31f375a80e)

### Marco Práctico:
En primera Instancia tendremos la capa de bronce con sus respectivo flujo de datos o data flow, donde tendremos los siguientes archivos:

- Bronze_Layer scripts.sql: el cual es el encargado de crear las tablas dentro del DWH y de crear la estructura interna           correspondiente para luego realizar la inserción dentro de cada tabla de la BD.
- procedimiento_carga_bronce_sql: este mismo es quien se encarga de la fase de EXTRACCION del proceso ETL para la insercion de los datos en cada esquema de tabla, se lo realiza por medio de un procedimiento almacenado el cual permite automatizar la carga de los datos, dando informacion tanto del tiempo de ejecucion de cada tabla, como del procedimiento en general. A su vez permite realizar un proceso de cheking para corroborar si hubo algun error durante la carga y da avisos por pantalla.

### Pasos que deben llevarse a cabo en la construccion de la capa de bronce:

## Capa de Plata
### Flujo de datos de la capa de Plata (Silver Layer)
![image](https://github.com/user-attachments/assets/351f435e-d1f5-4f14-bc71-98e1ae8858cd)

### Marco Práctico:

1. Analisis: es decir explorar y entender que datos se encuentran en nuestras fuentes de datos ya cargadas internamente dentro del data warehouse
2. limpieza de los datos (data cleansing), 3 pasos
    a. Check the quality of bronce o mejor dicho; verificar la calidad de datos de la capa de bronce, es decir primero intentar comprender que problemas tenemos con los datos antes de proceder con cualquier operacion de transformacion.
    b. Write data transformations, es decir escribir procesos de limpieza de los datos, para solucionar todos los problemas de calidad de datos que hayan surgido del analisis anterior.
    c. Insercion dentro de la capa de plata una vez que hayan sido limpiados.
3. Validar la correctitud y completitud de los datos, hay que asegurarnos que una vez que hayan sido transformados y limpiados no surgan problemas de calidad, de existir alguno debemos volver al paso 2.
4. Por ultimo siempre que sea posible y necesario el documentar todo cambio que se lleva a cabo en los procedimientos y pasos en cuanto a modelado de esquemas y codificacion de procedimientos.

![image](https://github.com/user-attachments/assets/b59881b2-eed7-4502-94b0-44cc766f54ad)
