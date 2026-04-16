import polars as pl
import os
import re
from sqlalchemy import create_engine, text
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Configuración
DB_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
# Asegúrate de que las variables en tu .env coincidan con estas rutas
PATH_FILE = Path(os.getenv("PATH_RAW_GASTOS")) / os.getenv("FILE_GASTOS_CURRENT")
engine = create_engine(DB_URL)

def normalizar_nombre(nombre):
    """Estandariza a minúsculas, sin tildes, estilo snake_case."""
    n = str(nombre).lower().strip()
    n = re.sub(r'[áéíóú]', lambda m: {'á':'a','é':'e','í':'i','ó':'o','ú':'u'}[m.group()], n)
    n = re.sub(r'[^a-z0-9_]', '_', n)
    return re.sub(r'_+', '_', n).strip('_')

def ejecutar_carga_polars():
    if not PATH_FILE.exists():
        print(f"❌ Archivo no encontrado en: {PATH_FILE}")
        return

    print(f"🚀 Iniciando motor Polars para: {PATH_FILE.name}")

    # 1. Escaneo Lazy (No carga a RAM todavía)
    # Usamos low_memory=True y dejamos que Polars maneje el streaming
    lf = pl.scan_csv(
        PATH_FILE, 
        infer_schema_length=0, 
        encoding="utf8-lossy",
        rechunk=False
    )

    # 2. Renombrar y Ordenar (Operaciones lógicas, no físicas)
    raw_cols = lf.columns
    new_cols = {old: normalizar_nombre(old) for old in raw_cols}
    lf = lf.rename(new_cols)

    todas = lf.columns
    fijas = [c for c in todas if not re.search(r'\d{4}', c)]
    anios = sorted([c for c in todas if re.search(r'\d{4}', c)])
    lf = lf.select(fijas + anios)

    # 3. Preparar PostgreSQL (Esquema Bronze - Todo TEXT)
    tabla_destino = "gastos_raw_bronze"
    columnas_finales = lf.columns
    
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {tabla_destino}"))
        cols_sql = ", ".join([f"{c} TEXT" for c in columnas_finales])
        conn.execute(text(f"CREATE TABLE {tabla_destino} ({cols_sql})"))
        conn.commit()

    # 4. Carga de alta velocidad con STREAMING
    print("📥 Volcando datos a PostgreSQL (Modo Streaming)...")
    
    temp_csv = PATH_FILE.with_name("temp_load.csv")
    
    # MODIFICACIÓN CLAVE: sink_csv procesa el archivo por partes (chunks internos)
    # sin cargar todo el CSV original en la RAM.
    lf.sink_csv(temp_csv, include_header=True)

    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        with open(temp_csv, 'r', encoding='utf-8') as f:
            # COPY de Postgres es mucho más rápido que INSERT
            cursor.copy_expert(f"COPY {tabla_destino} FROM STDIN WITH CSV HEADER", f)
        raw_conn.commit()
        print(f"✅ ¡Carga exitosa! Tabla '{tabla_destino}' creada.")
    finally:
        raw_conn.close()
        if temp_csv.exists(): 
            os.remove(temp_csv)

if __name__ == "__main__":
    ejecutar_carga_polars()