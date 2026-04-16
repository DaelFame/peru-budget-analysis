import polars as pl
import os
import re
from sqlalchemy import create_engine, text
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Configuración
DB_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
PATH_FILE = Path(os.getenv("PATH_RAW_GASTOS")) / os.getenv("FILE_GASTOS_CURRENT")
engine = create_engine(DB_URL)

def ejecutar_transformacion_optimizada():
    if not PATH_FILE.exists(): 
        print(f"❌ Archivo no encontrado: {PATH_FILE}")
        return

    print(f"🚀 Procesando con modo Streaming (Eficiencia Máxima)...")

    # 1. Escaneo Lazy
    lf = pl.scan_csv(PATH_FILE, infer_schema_length=0, encoding="utf8-lossy")

    # 2. Normalizar nombres
    mapping = {c: c.lower().replace(" ", "_").strip() for c in lf.collect_schema().names()}
    lf = lf.rename(mapping)

    # 3. Detectar métricas y años
    todas = lf.collect_schema().names()
    cols_con_anio = [c for c in todas if re.search(r'_\d{4}$', c)]
    cols_fijas = [c for c in todas if c not in cols_con_anio]

    # 4. UNPIVOT
    lf_silver = lf.unpivot(
        index=cols_fijas,
        on=cols_con_anio,
        variable_name="temp_meta",
        value_name="monto"
    )

    # 5. Extraer Año y Métrica
    lf_silver = lf_silver.with_columns([
        pl.col("temp_meta").str.extract(r"^(.*)_\d{4}$", 1).alias("tipo_metrica"),
        pl.col("temp_meta").str.extract(r"(\d{4})$", 1).cast(pl.Int32).alias("anio"),
        pl.col("monto").cast(pl.Float64, strict=False).fill_null(0)
    ]).drop("temp_meta")

    # 6. CREAR ARCHIVO TEMPORAL
    temp_csv = PATH_FILE.parent / "temp_silver_stream.csv"
    
    print("🧹 Generando archivo temporal en disco...")
    # Aseguramos el orden final de columnas antes de guardar
    cols_finales_csv = lf_silver.collect_schema().names()
    lf_silver.sink_csv(temp_csv)

    # 7. CARGA Y GENERACIÓN DE MD5 DENTRO DE POSTGRES
    tabla_destino = "gastos_fact_silver"
    
    with engine.connect() as conn:
        print(f"🗑️ Limpiando tabla {tabla_destino}...")
        conn.execute(text(f"DROP TABLE IF EXISTS {tabla_destino} CASCADE"))
        
        sql_cols = [f"{c} TEXT" for c in cols_fijas]
        sql_cols += ["tipo_metrica TEXT", "anio INTEGER", "monto NUMERIC"]
        
        conn.execute(text(f"CREATE TABLE {tabla_destino} ({', '.join(sql_cols)})"))
        conn.commit()

        print("📥 Volcando datos a Postgres...")
        raw_conn = engine.raw_connection()
        try:
            cursor = raw_conn.cursor()
            with open(temp_csv, 'r', encoding='utf-8') as f:
                # LA CORRECCIÓN: Especificamos las columnas exactas en el COPY
                # para que coincidan con el CSV generado por Polars
                columnas_string = ", ".join(cols_finales_csv)
                copy_query = f"COPY {tabla_destino} ({columnas_string}) FROM STDIN WITH CSV HEADER"
                cursor.copy_expert(copy_query, f)
            raw_conn.commit()
        finally:
            raw_conn.close()

        # 8. Generar la Super Key MD5 en SQL
        print("🔐 Generando Super Key MD5 y Primary Key...")
        sql_pk = f"""
        ALTER TABLE {tabla_destino} ADD COLUMN pk_md5 TEXT;
        
        UPDATE {tabla_destino} 
        SET pk_md5 = md5(UPPER(
            COALESCE(key_value, '') || 
            COALESCE(sec_ejec, '') || 
            COALESCE(meta, '') || 
            COALESCE(fuente_financiamiento, '') || 
            COALESCE(especifica_det, '') || 
            anio::text
        ));
        
        -- Creamos un índice para que las consultas sean rápidas
        CREATE INDEX idx_silver_anio ON {tabla_destino}(anio);
        CREATE INDEX idx_silver_metrica ON {tabla_destino}(tipo_metrica);
        
        -- Primary Key final
        ALTER TABLE {tabla_destino} ADD PRIMARY KEY (pk_md5, tipo_metrica);
        """
        conn.execute(text(sql_pk))
        conn.commit()
        
    if temp_csv.exists(): 
        os.remove(temp_csv)
    print(f"✅ ¡Proceso terminado exitosamente!")

if __name__ == "__main__":
    ejecutar_transformacion_optimizada()