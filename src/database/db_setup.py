import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Cargamos tus credenciales del .env
load_dotenv()

def bootstrap_database():
    """
    Asegura que la base de datos exista antes de cualquier proceso.
    """
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_host = os.getenv("DB_HOST", "localhost")

    print(f"🔍 Verificando existencia de la base de datos: {db_name}...")

    try:
        # Conexión a la base 'postgres' (por defecto) para poder crear la nueva
        conn = psycopg2.connect(
            dbname='postgres',
            user=db_user,
            password=db_pass,
            host=db_host
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Intentamos crearla. Si ya existe, saltará al bloque except.
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        print(f"🌟 ¡Base de datos '{db_name}' creada desde cero!")

    except psycopg2.errors.DuplicateDatabase:
        print(f"✅ El terreno está listo: La base de datos '{db_name}' ya existe.")
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
    finally:
        if 'conn' in locals():
            cur.close()
            conn.close()

if __name__ == "__main__":
    bootstrap_database()