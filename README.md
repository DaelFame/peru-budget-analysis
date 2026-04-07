# 🇵🇪 Peru Budget Analysis (MEF Data Pipeline)

Este proyecto automatiza la ingesta, limpieza y análisis de los datos de ejecución presupuestal del Ministerio de Economía y Finanzas (MEF) de Perú, procesando un dataset de +6.8 millones de registros.

## 🚀 Objetivos del Proyecto
* **Escalabilidad:** Procesamiento eficiente de grandes volúmenes de datos usando Python (Pandas) y PostgreSQL.
* **Integridad:** Implementación de arquitectura de datos (Capa Bronze y Silver) para asegurar la calidad de la información.
* **Financial BI:** Transformar datos crudos en insights sobre el gasto público y presupuestos (PIA/PIM).

## 🛠️ Stack Tecnológico
* **Lenguaje:** Python 3.14 (Gestionado con `uv`)
* **Base de Datos:** PostgreSQL 16
* **Infraestructura:** Fedora Linux / Windows (Sincronizado vía Git/SSH)
* **Herramientas:** SQL, SQLAlchemy, DBeaver, Pandas.

## 📊 Estructura y Diccionario de Datos
Para garantizar la transparencia del análisis, este pipeline utiliza una estructura basada en el protocolo del MEF. Puedes consultar la definición de cada variable (llaves primarias, tipos de gasto y montos) en el siguiente enlace:

👉 [**Ver Diccionario de Datos Detallado (Markdown)**](./docs/DICTIONARY.md)

## ⚖️ Enfoque de Auditoría de Datos (Data Profiling)
Como estudiante de Contabilidad, el pipeline integra validaciones de integridad financiera:
1. **Validación de Tipos:** Verificación masiva de columnas de montos (PIA, PIM, Devengado) para asegurar pureza numérica.
2. **Análisis de Duplicados:** Validación de la columna `KEY_VALUE` para evitar la sobreestimación de presupuestos.
3. **Normalización:** Transformación de datasets "anchos" a modelos relacionales eficientes.

## 📁 Estructura del Repositorio
* `src/`: Scripts de Python para limpieza, ingesta y automatización.
* `sql/`: Scripts de perfilamiento y transformaciones en base de datos.
* `data/raw/`: Archivos CSV originales (Ignorados en Git por tamaño).
* `docs/`: Documentación técnica y diccionarios generados automáticamente.

## ⚙️ Cómo empezar
1. Clonar el repositorio.
2. Configurar el entorno con `uv sync`.
3. Ejecutar los scripts de utilidad:
   ```bash
   uv run src/utils/clean_dictionary.py