-- SCRIPT: 07_full_unpivot_all_columns.sql
-- Objetivo: Crear la tabla larga con TODAS las dimensiones para el análisis Gold.

DROP TABLE IF EXISTS public.gastos_long_format;

CREATE TABLE public.gastos_long_format AS
WITH unpivot_all AS (
    -- BLOQUE 2022
    SELECT 
        "KEY_VALUE", nivel_gobierno_nombre, nivel_gobierno, sector_nombre, sector,
        pliego_nombre, pliego, departamento_ejecutora, departamento_ejecutora_nombre,
        provincia_ejecutora, provincia_ejecutora_nombre, distrito_ejecutora, distrito_ejecutora_nombre,
        "SEC_EJEC", ejecutora, ejecutora_nombre, programa_ppto, programa_ppto_nombre,
        tipo_act_proy, tipo_act_proy_nombre, act_proy, act_proy_nombre,
        componente, componente_nombre, funcion, funcion_nombre,
        division_funcional, division_funcional_nombre, grupo_funcional, grupo_funcional_nombre,
        finalidad, meta, meta_nombre, departamento_meta, departamento_meta_nombre,
        fuente_financ, fuente_financ_nombre, fuente_financ_agregada, fuente_financ_agregada_nombre,
        tipo_recurso, tipo_recurso_nombre, categ_gasto, categ_gasto_nombre,
        tipo_transaccion, generica, generica_nombre, subgenerica, subgenerica_nombre,
        subgenerica_det, subgenerica_det_nombre, especifica, especifica_nombre,
        especifica_det, especifica_det_nombre,
        2022 AS anio, pia_2022 AS pia, pim_2022 AS pim, certificado_2022 AS certificado,
        comprometido_anual_2022 AS comp_anual, comprometido_2022 AS comp,
        devengado_2022 AS devengado, girado_2022 AS girado
    FROM public.gastos_filtered 
    WHERE (pia_2022 + pim_2022 + devengado_2022 + girado_2022) > 0

    UNION ALL

    -- BLOQUE 2023
    SELECT 
        "KEY_VALUE", nivel_gobierno_nombre, nivel_gobierno, sector_nombre, sector,
        pliego_nombre, pliego, departamento_ejecutora, departamento_ejecutora_nombre,
        provincia_ejecutora, provincia_ejecutora_nombre, distrito_ejecutora, distrito_ejecutora_nombre,
        "SEC_EJEC", ejecutora, ejecutora_nombre, programa_ppto, programa_ppto_nombre,
        tipo_act_proy, tipo_act_proy_nombre, act_proy, act_proy_nombre,
        componente, componente_nombre, funcion, funcion_nombre,
        division_funcional, division_funcional_nombre, grupo_funcional, grupo_funcional_nombre,
        finalidad, meta, meta_nombre, departamento_meta, departamento_meta_nombre,
        fuente_financ, fuente_financ_nombre, fuente_financ_agregada, fuente_financ_agregada_nombre,
        tipo_recurso, tipo_recurso_nombre, categ_gasto, categ_gasto_nombre,
        tipo_transaccion, generica, generica_nombre, subgenerica, subgenerica_nombre,
        subgenerica_det, subgenerica_det_nombre, especifica, especifica_nombre,
        especifica_det, especifica_det_nombre,
        2023 AS anio, pia_2023 AS pia, pim_2023 AS pim, certificado_2023 AS certificado,
        comprometido_anual_2023 AS comp_anual, comprometido_2023 AS comp,
        devengado_2023 AS devengado, girado_2023 AS girado
    FROM public.gastos_filtered 
    WHERE (pia_2023 + pim_2023 + devengado_2023 + girado_2023) > 0

    UNION ALL

    -- BLOQUE 2024
    SELECT 
        "KEY_VALUE", nivel_gobierno_nombre, nivel_gobierno, sector_nombre, sector,
        pliego_nombre, pliego, departamento_ejecutora, departamento_ejecutora_nombre,
        provincia_ejecutora, provincia_ejecutora_nombre, distrito_ejecutora, distrito_ejecutora_nombre,
        "SEC_EJEC", ejecutora, ejecutora_nombre, programa_ppto, programa_ppto_nombre,
        tipo_act_proy, tipo_act_proy_nombre, act_proy, act_proy_nombre,
        componente, componente_nombre, funcion, funcion_nombre,
        division_funcional, division_funcional_nombre, grupo_funcional, grupo_funcional_nombre,
        finalidad, meta, meta_nombre, departamento_meta, departamento_meta_nombre,
        fuente_financ, fuente_financ_nombre, fuente_financ_agregada, fuente_financ_agregada_nombre,
        tipo_recurso, tipo_recurso_nombre, categ_gasto, categ_gasto_nombre,
        tipo_transaccion, generica, generica_nombre, subgenerica, subgenerica_nombre,
        subgenerica_det, subgenerica_det_nombre, especifica, especifica_nombre,
        especifica_det, especifica_det_nombre,
        2024 AS anio, pia_2024 AS pia, pim_2024 AS pim, certificado_2024 AS certificado,
        comprometido_anual_2024 AS comp_anual, comprometido_2024 AS comp,
        devengado_2024 AS devengado, girado_2024 AS girado
    FROM public.gastos_filtered 
    WHERE (pia_2024 + pim_2024 + devengado_2024 + girado_2024) > 0

    UNION ALL

    -- BLOQUE 2025
    SELECT 
        "KEY_VALUE", nivel_gobierno_nombre, nivel_gobierno, sector_nombre, sector,
        pliego_nombre, pliego, departamento_ejecutora, departamento_ejecutora_nombre,
        provincia_ejecutora, provincia_ejecutora_nombre, distrito_ejecutora, distrito_ejecutora_nombre,
        "SEC_EJEC", ejecutora, ejecutora_nombre, programa_ppto, programa_ppto_nombre,
        tipo_act_proy, tipo_act_proy_nombre, act_proy, act_proy_nombre,
        componente, componente_nombre, funcion, funcion_nombre,
        division_funcional, division_funcional_nombre, grupo_funcional, grupo_funcional_nombre,
        finalidad, meta, meta_nombre, departamento_meta, departamento_meta_nombre,
        fuente_financ, fuente_financ_nombre, fuente_financ_agregada, fuente_financ_agregada_nombre,
        tipo_recurso, tipo_recurso_nombre, categ_gasto, categ_gasto_nombre,
        tipo_transaccion, generica, generica_nombre, subgenerica, subgenerica_nombre,
        subgenerica_det, subgenerica_det_nombre, especifica, especifica_nombre,
        especifica_det, especifica_det_nombre,
        2025 AS anio, pia_2025 AS pia, pim_2025 AS pim, certificado_2025 AS certificado,
        comprometido_anual_2025 AS comp_anual, comprometido_2025 AS comp,
        devengado_2025 AS devengado, girado_2025 AS girado
    FROM public.gastos_filtered 
    WHERE (pia_2025 + pim_2025 + devengado_2025 + girado_2025) > 0
)
SELECT * FROM unpivot_all;