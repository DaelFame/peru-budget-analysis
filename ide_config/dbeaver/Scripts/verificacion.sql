/* =============================================================================
SCRIPT_09_ADAPTED: RECONCILIACIÓN BRONZE VS FCT_GOLD
=============================================================================
*/

WITH 
-- 1. Totales desde la tabla Bronze (Raw) - Sumamos las columnas horizontales
bronze_totals AS (
    SELECT 
        -- Totales 2024
        SUM((NULLIF(pia_2024, ''))::NUMERIC) as bronze_pia_2024,
        SUM((NULLIF(pim_2024, ''))::NUMERIC) as bronze_pim_2024,
        SUM((NULLIF(devengado_2024, ''))::NUMERIC) as bronze_dev_2024,
        -- Totales 2025
        SUM((NULLIF(pia_2025, ''))::NUMERIC) as bronze_pia_2025,
        SUM((NULLIF(pim_2025, ''))::NUMERIC) as bronze_pim_2025,
        SUM((NULLIF(devengado_2025, ''))::NUMERIC) as bronze_dev_2025
    FROM public.gastos_raw_bronze
),
-- 2. Totales desde la FCT (Gold) - Filtramos por las filas verticales
gold_totals AS (
    SELECT 
        SUM(monto) FILTER (WHERE anio = 2024 AND tipo_monto = 'PIA') as gold_pia_2024,
        SUM(monto) FILTER (WHERE anio = 2024 AND tipo_monto = 'PIM') as gold_pim_2024,
        SUM(monto) FILTER (WHERE anio = 2024 AND tipo_monto = 'DEVENGADO') as gold_dev_2024,
        SUM(monto) FILTER (WHERE anio = 2025 AND tipo_monto = 'PIA') as gold_pia_2025,
        SUM(monto) FILTER (WHERE anio = 2025 AND tipo_monto = 'PIM') as gold_pim_2025,
        SUM(monto) FILTER (WHERE anio = 2025 AND tipo_monto = 'DEVENGADO') as gold_dev_2025
    FROM public.fct_ejecucion_gasto
),
-- 3. Unión para el reporte
pre_reporte AS (
    SELECT 'Reconciliación PIA 2024' as validation, b.bronze_pia_2024 as valor_a, g.gold_pia_2024 as valor_b FROM bronze_totals b, gold_totals g UNION ALL
    SELECT 'Reconciliación PIM 2024', b.bronze_pim_2024, g.gold_pim_2024 FROM bronze_totals b, gold_totals g UNION ALL
    SELECT 'Reconciliación DEV 2024', b.bronze_dev_2024, g.gold_dev_2024 FROM bronze_totals b, gold_totals g UNION ALL
    SELECT 'Reconciliación PIA 2025', b.bronze_pia_2025, g.gold_pia_2025 FROM bronze_totals b, gold_totals g UNION ALL
    SELECT 'Reconciliación PIM 2025', b.bronze_pim_2025, g.gold_pim_2025 FROM bronze_totals b, gold_totals g UNION ALL
    SELECT 'Reconciliación DEV 2025', b.bronze_dev_2025, g.gold_dev_2025 FROM bronze_totals b, gold_totals g
)
-- 4. Reporte Final de Materialidad
SELECT 
    validation, 
    valor_a as total_bronze, 
    valor_b as total_gold,
    ABS(valor_a - valor_b) AS abs_diff,
    CASE 
        WHEN valor_a = 0 OR valor_a IS NULL THEN 0 
        ELSE ROUND((ABS(valor_a - valor_b) / valor_a) * 100, 10) 
    END AS error_percentage,
    CASE 
        WHEN (ABS(valor_a - valor_b) < 0.01) THEN '✅ OK (Exacto)'
        WHEN (ABS(valor_a - valor_b) / NULLIF(valor_a,0)) < 0.00001 THEN '✅ OK (Inmaterial)'
        ELSE '❌ REVISAR DIFERENCIA'
    END AS audit_status
FROM pre_reporte;