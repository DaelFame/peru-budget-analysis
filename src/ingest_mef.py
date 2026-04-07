import os
import time
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 1. Cargar configuración desde el archivo .env
load_dotenv()

# 2. Variables de Conexión (Extraídas del entorno)
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")

# 3. Configuración del Archivo
# Si no encuentra CSV_NAME en el .env, usará el nombre por defecto
CSV_NAME = os.getenv("CSV_NAME", "gastos_2022_2025.csv")
CSV_PATH = os.path.join("data", "raw", CSV_NAME)

# Construcción de la URL de SQLAlchemy
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def main():
    """
    Función principal para cargar datos masivos del MEF a PostgreSQL
    siguiendo la arquitectura de Capa Bronze (Raw).
    """
    # Verificación de existencia del archivo
    if not os.path.exists(CSV_PATH):
        print(f"❌ Error: No se encontró el archivo en: {CSV_PATH}")
        print("💡 Asegúrate de que el nombre en el .env coincida con el archivo en data/raw/")
        return

    try:
        print(f"🚀 Conectando a la base de datos '{DB_NAME}' en {DB_HOST}...")
        engine = create_engine(DB_URL)
        
        start_time = time.time()
        print(f"📈 Iniciando ingesta de: {CSV_NAME}")
        print("📦 Procesando por lotes (chunks) de 200,000 filas...")

        # Lectura por bloques para optimizar memoria RAM
        # Cargamos todo como string (dtype=str) para asegurar la ingesta en Bronze
        reader = pd.read_csv(CSV_PATH, sep=',', dtype=str, chunksize=200000)
        
        for i, chunk in enumerate(reader):
            # 'replace' para el primer lote (crea la tabla), 'append' para los demás
            mode = 'replace' if i == 0 else 'append'
            
            chunk.to_sql('gastos_raw', engine, if_exists=mode, index=False)
            
            # Log de progreso
            print(f"✅ Lote {i+1} procesado con éxito.")

        duration = round(time.time() - start_time, 2)
        print(f"\n🏆 ¡CARGA COMPLETADA EXITOSAMENTE!")
        print(f"⏱️ Tiempo total de ejecución: {duration} segundos.")
        print(f"📂 Tabla 'gastos_raw' lista para análisis en DBeaver.")

    except Exception as e:
        print(f"❌ Error crítico durante la ingesta: {e}")

if __name__ == "__main__":
    main()