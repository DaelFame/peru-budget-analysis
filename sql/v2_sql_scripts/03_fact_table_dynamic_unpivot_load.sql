-- 1. Creamos la estructura de la tabla de hechos con sus restricciones
DROP TABLE IF EXISTS fct_ejecucion_gasto CASCADE;

CREATE TABLE fct_ejecucion_gasto (
    fact_key TEXT PRIMARY KEY, -- Nuestra Super Key (Hash)
    key_value TEXT,
    id_institucion INT REFERENCES dim_institucion(id_institucion),
    id_geografia INT REFERENCES dim_geografia(id_geografia),
    id_clasificador INT REFERENCES dim_clasificador(id_clasificador),
    id_programa INT REFERENCES dim_programa(id_programa),
    anio INT,
    tipo_monto TEXT,
    monto NUMERIC(18,2)
);

-- 2. Bloque de procesamiento dinámico
DO $$ 
DECLARE 
    col RECORD;
    v_anio INT;
    v_tipo TEXT;
    t_inicio TIMESTAMP;
BEGIN
    t_inicio := clock_timestamp();

    -- Buscamos columnas con formato monto_año (ej: pia_2023, devengado_2022)
    FOR col IN 
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'gastos_raw_bronze' 
          AND column_name ~ '.*_[0-9]{4}$'
    LOOP
        -- Extraer metadatos de la columna
        v_anio := (regexp_matches(col.column_name, '[0-9]{4}'))[1]::INT;
        v_tipo := UPPER(split_part(col.column_name, '_', 1));

        RAISE NOTICE 'Cargando: % | Año: % | Tipo: %', col.column_name, v_anio, v_tipo;

        EXECUTE format('
            INSERT INTO fct_ejecucion_gasto (
                fact_key, key_value, id_institucion, id_geografia, 
                id_clasificador, id_programa, anio, tipo_monto, monto
            )
            SELECT 
                md5(b.key_value || %L || %L), -- Creación de Super Key Única
                b.key_value,
                i.id_institucion,
                g.id_geografia,
                c.id_clasificador,
                p.id_programa,
                %L, 
                %L,
                (NULLIF(b.%I, ''''))::NUMERIC
            FROM gastos_raw_bronze b
            JOIN dim_institucion i ON b.sec_ejec = i.sec_ejec AND b.ejecutora = i.ejecutora
            JOIN dim_geografia g ON b.departamento_ejecutora = g.dpto_cod AND b.provincia_ejecutora = g.prov_cod AND b.distrito_ejecutora = g.dist_cod
            JOIN dim_clasificador c ON b.generica = c.generica AND b.especifica_det = c.especifica_det
            JOIN dim_programa p ON b.actividad_accion_obra = p.actividad_cod
            WHERE (NULLIF(b.%I, ''''))::NUMERIC > 0
            ON CONFLICT (fact_key) DO NOTHING;', -- Evita duplicados si re-corres el script
            v_anio, v_tipo, v_anio, v_tipo, col.column_name, col.column_name);
            
    END LOOP;
    
    RAISE NOTICE 'Proceso finalizado en %', clock_timestamp() - t_inicio;
END $$;