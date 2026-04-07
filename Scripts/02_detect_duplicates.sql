-- SCRIPT: 02_check_key_value_duplicates.sql
-- Objetivo: Validar si la columna KEY_VALUE es realmente una Primary Key (PK)

SELECT 
    "KEY_VALUE", 
    COUNT(*) as repeticiones
FROM gastos_raw
GROUP BY "KEY_VALUE"
HAVING COUNT(*) > 1
ORDER BY repeticiones DESC;