-- SCRIPT: 05_filter_active_records.sql
-- Objetivo: Eliminar registros que tienen 0 en todas sus métricas de 2022 a 2025.

DROP TABLE IF EXISTS gastos_filtered;

CREATE TABLE gastos_filtered AS
SELECT * FROM gastos_homogenized
WHERE (
    -- Verificamos que al menos uno de estos campos tenga un valor distinto de cero
    -- Sumamos todos los años y todas las métricas clave
    (pia_2022 + pim_2022 + certificado_2022 + comprometido_2022 + devengado_2022 + girado_2022) +
    (pia_2023 + pim_2023 + certificado_2023 + comprometido_2023 + devengado_2023 + girado_2023) +
    (pia_2024 + pim_2024 + certificado_2024 + comprometido_2024 + devengado_2024 + girado_2024) +
    (pia_2025 + pim_2025 + certificado_2025 + comprometido_2025 + devengado_2025 + girado_2025)
) > 0;

-- ANALÍTICA DE CONTROL: ¿Cuánto nos ahorramos?
SELECT 
    (SELECT COUNT(*) FROM gastos_homogenized) AS total_original,
    (SELECT COUNT(*) FROM gastos_filtered) AS total_filtrado,
    (SELECT COUNT(*) FROM gastos_homogenized) - (SELECT COUNT(*) FROM gastos_filtered) AS filas_eliminadas;