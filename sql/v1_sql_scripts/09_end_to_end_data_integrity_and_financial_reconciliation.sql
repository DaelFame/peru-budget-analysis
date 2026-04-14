/* =============================================================================
SCRIPT_09: END_TO_END_DATA_INTEGRITY_AND_FINANCIAL_RECONCILIATION
=============================================================================
ENGLIGH OBJECTIVE:
Comprehensive audit of the data pipeline to ensure integrity, financial 
balancing (materiality), and record uniqueness (Super ID validation).

OBJETIVO EN ESPAÑOL:
Auditoría integral del pipeline de datos para asegurar la integridad, 
el cuadre financiero (materialidad) y la unicidad de los registros (Super ID).
=============================================================================
*/

-- 1. CAPA DE CÁLCULOS (DATA_AUDIT_LAYER)
WITH 
check_1 AS (
    SELECT '1. Raw-Homogenized Integrity' AS validation,
           (SELECT COUNT(*) FROM public.gastos_raw) AS valor_a,
           (SELECT COUNT(*) FROM public.gastos_homogenized) AS valor_b
),
check_2 AS (
    SELECT '2. Zero-Row Filtering (Clean)' AS validation,
           (SELECT COUNT(*) FROM public.gastos_homogenized) AS valor_a,
           (SELECT COUNT(*) FROM public.gastos_filtered) AS valor_b
),
check_3 AS (
    SELECT '3. Total PIM Reconciliation (2022-2025)' AS validation,
           -- Usamos COALESCE para evitar que un NULL mate la suma de la fila
           (SELECT SUM(COALESCE(pia_2022,0) + COALESCE(pia_2023,0) + COALESCE(pia_2024,0) + COALESCE(pia_2025,0)) FROM public.gastos_filtered) AS valor_a,
           (SELECT SUM(pia) FROM public.gastos_gold) AS valor_b
),
check_4 AS (
    SELECT '4. Super ID Uniqueness (Primary Key)' AS validation,
           (SELECT COUNT(*) FROM public.gastos_gold) AS valor_a,
           (SELECT COUNT(DISTINCT super_id) FROM public.gastos_gold) AS valor_b
),
check_nulos AS (
    SELECT '5. Null Values in Filtered Table' AS validation,
           (SELECT COUNT(*) FROM public.gastos_filtered) AS valor_a,
           (SELECT COUNT(*) FROM public.gastos_filtered 
            WHERE pia_2022 IS NULL OR pia_2023 IS NULL OR pia_2024 IS NULL OR pia_2025 IS NULL) AS valor_b
),
-- Comparativa por años para demostrar inmaterialidad (Financial Materiality)
check_anios AS (
    SELECT '6. Annual Variance 2022' AS v, (SELECT SUM(pia_2022) FROM public.gastos_filtered) AS va, (SELECT SUM(pia) FROM public.gastos_gold WHERE anio = 2022) AS vb UNION ALL
    SELECT '6. Annual Variance 2023', (SELECT SUM(pia_2023) FROM public.gastos_filtered), (SELECT SUM(pia) FROM public.gastos_gold WHERE anio = 2023) UNION ALL
    SELECT '6. Annual Variance 2024', (SELECT SUM(pia_2024) FROM public.gastos_filtered), (SELECT SUM(pia) FROM public.gastos_gold WHERE anio = 2024) UNION ALL
    SELECT '6. Annual Variance 2025', (SELECT SUM(pia_2025) FROM public.gastos_filtered), (SELECT SUM(pia) FROM public.gastos_gold WHERE anio = 2025)
),
pre_reporte AS (
    SELECT * FROM check_1 UNION ALL 
    SELECT * FROM check_2 UNION ALL 
    SELECT * FROM check_3 UNION ALL 
    SELECT * FROM check_4 UNION ALL 
    SELECT * FROM check_nulos UNION ALL
    SELECT * FROM check_anios
)

-- 2. REPORTE FINAL CON ANÁLISIS DE MATERIALIDAD (FINAL_AUDIT_REPORT)
SELECT 
    validation, 
    valor_a, 
    valor_b,
    ABS(valor_a - valor_b) AS abs_diff,
    CASE 
        WHEN valor_a = 0 THEN 0 
        ELSE ROUND((ABS(valor_a - valor_b) / valor_a) * 100, 6) 
    END AS error_percentage,
    CASE 
        WHEN validation LIKE '1.%' AND (valor_a = valor_b) THEN '✅ OK'
        WHEN validation LIKE '2.%' THEN 'ℹ️ Cleaned: ' || (valor_a - valor_b) || ' empty rows'
        WHEN validation LIKE '3.%' AND (ABS(valor_a - valor_b) / NULLIF(valor_a,0)) < 0.00001 THEN '✅ OK (Inmaterial)'
        WHEN validation LIKE '4.%' AND (valor_a = valor_b) THEN '✅ OK (Unique ID)'
        WHEN validation LIKE '5.%' AND valor_b = 0 THEN '✅ Clean'
        WHEN validation LIKE '5.%' AND valor_b > 0 THEN '⚠️ ' || valor_b || ' rows have nulls'
        WHEN validation LIKE '6.%' AND (ABS(valor_a - valor_b) / NULLIF(valor_a,0)) < 0.00001 THEN '✅ OK (Inmaterial)'
        ELSE '❌ REVIEW REQUIRED'
    END AS audit_status
FROM pre_reporte;