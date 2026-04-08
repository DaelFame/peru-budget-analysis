-- SCRIPT: 06_detect_unique_attributes.sql
-- Objetivo: Identificar qué columnas cambian cuando el KEY_VALUE se repite.

SELECT 
    COUNT(DISTINCT "SEC_EJEC") as ejecutoras_distintas,
    COUNT(DISTINCT meta) as metas_distintas,
    COUNT(DISTINCT fuente_financ_nombre) as fuentes_distintas,
    COUNT(DISTINCT especifica_det_nombre) as especificas_distintas,
    COUNT(DISTINCT "KEY_VALUE") as total_ids_unicos
FROM public.gastos_filtered
WHERE "KEY_VALUE" IN (
    SELECT "KEY_VALUE" 
    FROM public.gastos_filtered 
    GROUP BY "KEY_VALUE" 
    HAVING COUNT(*) > 1
);