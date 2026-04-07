-- SCRIPT: 01_data_profiling_raw.sql
-- Objetivo: Detectar registros que NO sean numéricos en las columnas de montos

SELECT 
    COUNT(*) AS total_registros,
    -- Contamos cuántos NO son números en cada categoría
    COUNT(*) FILTER (WHERE "PIA_2022" !~ '^-?[0-9]+(\.[0-9]+)?$') AS basura_pia_2022,
    COUNT(*) FILTER (WHERE "PIM_2022" !~ '^-?[0-9]+(\.[0-9]+)?$') AS basura_pim_2022,
    COUNT(*) FILTER (WHERE "DEVENGADO_2024" !~ '^-?[0-9]+(\.[0-9]+)?$') AS basura_dev_2024,
    COUNT(*) FILTER (WHERE "GIRADO_2025" !~ '^-?[0-9]+(\.[0-9]+)?$') AS basura_gir_2025
FROM gastos_raw;