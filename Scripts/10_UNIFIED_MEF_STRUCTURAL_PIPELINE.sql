/* =============================================================================
SCRIPT_10: UNIFIED_PIPELINE_FULL_REPLICA
OBJETIVO: Recrear la tabla Gold con TODAS las dimensiones y métricas del 
proceso manual (Pasos 1-9) en una sola ejecución.
=============================================================================
*/

DROP TABLE IF EXISTS public.gastos_raw_to_gold;

CREATE TABLE public.gastos_raw_to_gold AS
WITH cte_homogenized AS (
    SELECT 
        -- 1. IDENTIFICADORES Y CODIGOS
        "KEY_VALUE", "SEC_EJEC", "META", "FUENTE_FINANC", "ESPECIFICA_DET",
        "NIVEL_GOBIERNO", "SECTOR", "PLIEGO", "EJECUTORA", "DEPARTAMENTO_EJECUTORA",
        "PROVINCIA_EJECUTORA", "DISTRITO_EJECUTORA", "PROGRAMA_PPTO", "TIPO_ACT_PROY",
        "ACT_PROY", "COMPONENTE", "FUNCION", "DIVISION_FUNCIONAL", "GRUPO_FUNCIONAL",
        "FINALIDAD", "DEPARTAMENTO_META", "FUENTE_FINANC_AGREGADA", "TIPO_RECURSO",
        "CATEG_GASTO", "TIPO_TRANSACCION", "GENERICA", "SUBGENERICA", "SUBGENERICA_DET", "ESPECIFICA",

        -- 2. DIMENSIONES (NOMBRES LARGOS CON LIMPIEZA DE TILDES)
        UPPER(TRIM(TRANSLATE("NIVEL_GOBIERNO_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS nivel_gobierno_nombre,
        UPPER(TRIM(TRANSLATE("SECTOR_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS sector_nombre,
        UPPER(TRIM(TRANSLATE("PLIEGO_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS pliego_nombre,
        UPPER(TRIM(TRANSLATE("DEPARTAMENTO_EJECUTORA_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS departamento_ejecutora_nombre,
        UPPER(TRIM(TRANSLATE("PROVINCIA_EJECUTORA_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS provincia_ejecutora_nombre,
        UPPER(TRIM(TRANSLATE("DISTRITO_EJECUTORA_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS distrito_ejecutora_nombre,
        UPPER(TRIM(TRANSLATE("EJECUTORA_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS ejecutora_nombre,
        UPPER(TRIM(TRANSLATE("PROGRAMA_PPTO_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS programa_ppto_nombre,
        UPPER(TRIM(TRANSLATE("TIPO_ACT_PROY_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS tipo_act_proy_nombre,
        UPPER(TRIM(TRANSLATE("ACT_PROY_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS act_proy_nombre,
        UPPER(TRIM(TRANSLATE("COMPONENTE_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS componente_nombre,
        UPPER(TRIM(TRANSLATE("FUNCION_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS funcion_nombre,
        UPPER(TRIM(TRANSLATE("DIVISION_FUNCIONAL_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS division_funcional_nombre,
        UPPER(TRIM(TRANSLATE("GRUPO_FUNCIONAL_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS grupo_funcional_nombre,
        UPPER(TRIM(TRANSLATE("META_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS meta_nombre,
        UPPER(TRIM(TRANSLATE("DEPARTAMENTO_META_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS departamento_meta_nombre,
        UPPER(TRIM(TRANSLATE("FUENTE_FINANC_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS fuente_financ_nombre,
        UPPER(TRIM(TRANSLATE("FUENTE_FINANC_AGREGADA_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS fuente_financ_agregada_nombre,
        UPPER(TRIM(TRANSLATE("TIPO_RECURSO_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS tipo_recurso_nombre,
        UPPER(TRIM(TRANSLATE("CATEG_GASTO_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS categ_gasto_nombre,
        UPPER(TRIM(TRANSLATE("GENERICA_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS generica_nombre,
        UPPER(TRIM(TRANSLATE("SUBGENERICA_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS subgenerica_nombre,
        UPPER(TRIM(TRANSLATE("SUBGENERICA_DET_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS subgenerica_det_nombre,
        UPPER(TRIM(TRANSLATE("ESPECIFICA_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS especifica_nombre,
        UPPER(TRIM(TRANSLATE("ESPECIFICA_DET_NOMBRE", 'ÁÉÍÓÚáéíóú', 'AEIOUAEIOU'))) AS especifica_det_nombre,

        -- 3. MÉTRICAS (CASTS TEMPORALES)
        CAST(COALESCE(NULLIF("PIA_2022", ''), '0') AS NUMERIC) AS p22, CAST(COALESCE(NULLIF("PIM_2022", ''), '0') AS NUMERIC) AS m22, 
        CAST(COALESCE(NULLIF("CERTIFICADO_2022", ''), '0') AS NUMERIC) AS c22, CAST(COALESCE(NULLIF("COMPROMETIDO_ANUAL_2022", ''), '0') AS NUMERIC) AS ca22,
        CAST(COALESCE(NULLIF("COMPROMETIDO_2022", ''), '0') AS NUMERIC) AS co22, CAST(COALESCE(NULLIF("DEVENGADO_2022", ''), '0') AS NUMERIC) AS d22, CAST(COALESCE(NULLIF("GIRADO_2022", ''), '0') AS NUMERIC) AS g22,
        
        CAST(COALESCE(NULLIF("PIA_2023", ''), '0') AS NUMERIC) AS p23, CAST(COALESCE(NULLIF("PIM_2023", ''), '0') AS NUMERIC) AS m23, 
        CAST(COALESCE(NULLIF("CERTIFICADO_2023", ''), '0') AS NUMERIC) AS c23, CAST(COALESCE(NULLIF("COMPROMETIDO_ANUAL_2023", ''), '0') AS NUMERIC) AS ca23,
        CAST(COALESCE(NULLIF("COMPROMETIDO_2023", ''), '0') AS NUMERIC) AS co23, CAST(COALESCE(NULLIF("DEVENGADO_2023", ''), '0') AS NUMERIC) AS d23, CAST(COALESCE(NULLIF("GIRADO_2023", ''), '0') AS NUMERIC) AS g23,

        CAST(COALESCE(NULLIF("PIA_2024", ''), '0') AS NUMERIC) AS p24, CAST(COALESCE(NULLIF("PIM_2024", ''), '0') AS NUMERIC) AS m24, 
        CAST(COALESCE(NULLIF("CERTIFICADO_2024", ''), '0') AS NUMERIC) AS c24, CAST(COALESCE(NULLIF("COMPROMETIDO_ANUAL_2024", ''), '0') AS NUMERIC) AS ca24,
        CAST(COALESCE(NULLIF("COMPROMETIDO_2024", ''), '0') AS NUMERIC) AS co24, CAST(COALESCE(NULLIF("DEVENGADO_2024", ''), '0') AS NUMERIC) AS d24, CAST(COALESCE(NULLIF("GIRADO_2024", ''), '0') AS NUMERIC) AS g24,

        CAST(COALESCE(NULLIF("PIA_2025", ''), '0') AS NUMERIC) AS p25, CAST(COALESCE(NULLIF("PIM_2025", ''), '0') AS NUMERIC) AS m25, 
        CAST(COALESCE(NULLIF("CERTIFICADO_2025", ''), '0') AS NUMERIC) AS c25, CAST(COALESCE(NULLIF("COMPROMETIDO_ANUAL_2025", ''), '0') AS NUMERIC) AS ca25,
        CAST(COALESCE(NULLIF("COMPROMETIDO_2025", ''), '0') AS NUMERIC) AS co25, CAST(COALESCE(NULLIF("DEVENGADO_2025", ''), '0') AS NUMERIC) AS d25, CAST(COALESCE(NULLIF("GIRADO_2025", ''), '0') AS NUMERIC) AS g25
    FROM public.gastos_raw
),

cte_unpivot AS (
    -- Unpivot 2022
    SELECT *, 2022 AS anio, p22 AS pia, m22 AS pim, c22 AS certificado, ca22 AS comp_anual, co22 AS comp, d22 AS devengado, g22 AS girado FROM cte_homogenized WHERE (p22+m22+c22+ca22+co22+d22+g22) > 0
    UNION ALL
    -- Unpivot 2023
    SELECT *, 2023, p23, m23, c23, ca23, co23, d23, g23 FROM cte_homogenized WHERE (p23+m23+c23+ca23+co23+d23+g23) > 0
    UNION ALL
    -- Unpivot 2024
    SELECT *, 2024, p24, m24, c24, ca24, co24, d24, g24 FROM cte_homogenized WHERE (p24+m24+c24+ca24+co24+d24+g24) > 0
    UNION ALL
    -- Unpivot 2025
    SELECT *, 2025, p25, m25, c25, ca25, co25, d25, g25 FROM cte_homogenized WHERE (p25+m25+c25+ca25+co25+d25+g25) > 0
)

SELECT 
    MD5(CONCAT("KEY_VALUE", "SEC_EJEC", "META", "FUENTE_FINANC", "ESPECIFICA_DET", anio)) AS super_id,
    -- Seleccionamos exactamente las columnas de tu Gold original
    "KEY_VALUE", nivel_gobierno_nombre, "NIVEL_GOBIERNO" AS nivel_gobierno, sector_nombre, "SECTOR" AS sector,
    pliego_nombre, "PLIEGO" AS pliego, "DEPARTAMENTO_EJECUTORA" AS departamento_ejecutora, departamento_ejecutora_nombre,
    "PROVINCIA_EJECUTORA" AS provincia_ejecutora, provincia_ejecutora_nombre, "DISTRITO_EJECUTORA" AS distrito_ejecutora,
    distrito_ejecutora_nombre, "SEC_EJEC", "EJECUTORA" AS ejecutora, ejecutora_nombre, "PROGRAMA_PPTO" AS programa_ppto,
    programa_ppto_nombre, "TIPO_ACT_PROY" AS tipo_act_proy, tipo_act_proy_nombre, "ACT_PROY" AS act_proy,
    act_proy_nombre, "COMPONENTE" AS componente, componente_nombre, "FUNCION" AS funcion, funcion_nombre,
    "DIVISION_FUNCIONAL" AS division_funcional, division_funcional_nombre, "GRUPO_FUNCIONAL" AS grupo_funcional,
    grupo_funcional_nombre, "FINALIDAD" AS finalidad, "META" AS meta, meta_nombre, "DEPARTAMENTO_META" AS departamento_meta,
    departamento_meta_nombre, "FUENTE_FINANC" AS fuente_financ, fuente_financ_nombre, "FUENTE_FINANC_AGREGADA" AS fuente_financ_agregada,
    fuente_financ_agregada_nombre, "TIPO_RECURSO" AS tipo_recurso, tipo_recurso_nombre, "CATEG_GASTO" AS categ_gasto,
    categ_gasto_nombre, "TIPO_TRANSACCION" AS tipo_transaccion, "GENERICA" AS generica, generica_nombre,
    "SUBGENERICA" AS subgenerica, subgenerica_nombre, "SUBGENERICA_DET" AS subgenerica_det, subgenerica_det_nombre,
    "ESPECIFICA" AS especifica, especifica_nombre, "ESPECIFICA_DET" AS especifica_det, especifica_det_nombre,
    anio, pia, pim, certificado, comp_anual, comp, devengado, girado
FROM cte_unpivot;