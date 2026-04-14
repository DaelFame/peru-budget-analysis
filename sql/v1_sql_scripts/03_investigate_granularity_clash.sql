-- SCRIPT: 03_investigate_granularity_clash.sql
-- Objetivo: Encontrar automáticamente los registros que causan duplicidad 
-- y comparar sus columnas para definir la Super Key.

WITH TopDuplicados AS (
    -- Buscamos los 5 KEY_VALUE que más se repiten actualmente
    SELECT "KEY_VALUE"
    FROM gastos_raw
    GROUP BY "KEY_VALUE"
    HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC
    LIMIT 5
)
SELECT 
    "KEY_VALUE",
    "EJECUTORA_NOMBRE",
    "PROGRAMA_PPTO_NOMBRE",
    "META",
    "ESPECIFICA_DET_NOMBRE",
    "FUENTE_FINANC_NOMBRE", 
    COUNT(*) OVER(PARTITION BY "KEY_VALUE") as repeticiones_id
FROM gastos_raw
WHERE "KEY_VALUE" IN (SELECT "KEY_VALUE" FROM TopDuplicados)
ORDER BY "KEY_VALUE", "DEVENGADO_2024" DESC;