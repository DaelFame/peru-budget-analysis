-- SCRIPT: 08_create_gold_fact_table.sql
-- Objetivo: Generar la tabla final con Super Key para BI y Análisis.

DROP TABLE IF EXISTS public.gastos_gold;

CREATE TABLE public.gastos_gold AS
SELECT 
    -- Generamos el ID ÚNICO (Super Key)
    -- Combinamos: ID del MEF + Ejecutora + Meta + Fuente + Clasificador + AÑO
    MD5(CONCAT(
        "KEY_VALUE", 
        "SEC_EJEC", 
        meta, 
        fuente_financ, 
        especifica_det, 
        anio
    )) AS super_id,
    *
FROM public.gastos_long_format;

-- ---------------------------------------------------------
-- PRUEBA DE FUEGO (DATA QUALITY CHECK)
-- ---------------------------------------------------------
-- Este query DEBE devolver 0 registros. 
-- Si devuelve algo, significa que aún hay duplicados.

SELECT super_id, COUNT(*) 
FROM public.gastos_gold 
GROUP BY super_id 
HAVING COUNT(*) > 1;