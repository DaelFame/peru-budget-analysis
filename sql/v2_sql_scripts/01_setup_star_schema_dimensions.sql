-- Limpieza inicial
DROP TABLE IF EXISTS dim_institucion CASCADE;
DROP TABLE IF EXISTS dim_geografia CASCADE;
DROP TABLE IF EXISTS dim_clasificador CASCADE;
DROP TABLE IF EXISTS dim_programa CASCADE;

-- 1. Dimensión Institución
-- Usamos DISTINCT ON para que si el MEF repite el código con nombres distintos, no explote
CREATE TABLE dim_institucion AS
SELECT DISTINCT ON (sec_ejec, ejecutora)
    sec_ejec, 
    ejecutora, 
    ejecutora_nombre, 
    nivel_gobierno_nombre, 
    sector_nombre, 
    pliego_nombre
FROM gastos_raw_bronze
ORDER BY sec_ejec, ejecutora;

ALTER TABLE dim_institucion ADD COLUMN id_institucion SERIAL PRIMARY KEY;

-- 2. Dimensión Geografía
CREATE TABLE dim_geografia AS
SELECT DISTINCT ON (departamento_ejecutora, provincia_ejecutora, distrito_ejecutora)
    departamento_ejecutora AS dpto_cod, 
    departamento_ejecutora_nombre AS dpto_nom,
    provincia_ejecutora AS prov_cod, 
    provincia_ejecutora_nombre AS prov_nom,
    distrito_ejecutora AS dist_cod, 
    distrito_ejecutora_nombre AS dist_nom
FROM gastos_raw_bronze
ORDER BY departamento_ejecutora, provincia_ejecutora, distrito_ejecutora;

ALTER TABLE dim_geografia ADD COLUMN id_geografia SERIAL PRIMARY KEY;

-- 3. Dimensión Clasificador
CREATE TABLE dim_clasificador AS
SELECT DISTINCT ON (generica, especifica_det)
    generica, 
    generica_nombre, 
    especifica_det, 
    especifica_det_nombre, 
    categoria_gasto_nombre
FROM gastos_raw_bronze
ORDER BY generica, especifica_det;

ALTER TABLE dim_clasificador ADD COLUMN id_clasificador SERIAL PRIMARY KEY;

-- 4. Dimensión Programa
CREATE TABLE dim_programa AS
SELECT DISTINCT ON (actividad_accion_obra)
    actividad_accion_obra AS actividad_cod,
    actividad_accion_obra_nombre AS actividad_nom,
    producto_proyecto_nombre,
    programa_ppto_nombre
FROM gastos_raw_bronze
ORDER BY actividad_accion_obra;

ALTER TABLE dim_programa ADD COLUMN id_programa SERIAL PRIMARY KEY;