import pandas as pd
import os

def generate_dictionary_markdown():
    input_file = 'data/raw/diccionario.csv' 
    output_csv = 'docs/diccionario_mef_clean.csv'
    output_md = 'docs/DICTIONARY.md'
    
    if not os.path.exists(input_file):
        print(f"❌ No se encontró {input_file}")
        return

    try:
        # 1. Leer y limpiar (usando el encoding que nos funcionó)
        df = pd.read_csv(input_file, encoding='utf-8-sig', sep=None, engine='python')
        df.columns = df.columns.str.replace('"', '').str.strip()
        
        cols_finales = ['VARIABLE', 'TIPO_DATO', 'DESCRIPCION']
        df_clean = df[cols_finales].drop_duplicates()

        # 2. Guardar CSV (para procesos internos)
        os.makedirs('docs', exist_ok=True)
        df_clean.to_csv(output_csv, index=False)

        # 3. Generar el archivo Markdown profesional
        with open(output_md, 'w', encoding='utf-8') as f:
            f.write("# 📖 Diccionario de Datos - MEF\n\n")
            f.write("Este documento detalla las variables utilizadas en el pipeline de análisis presupuestal.\n\n")
            f.write(df_clean.to_markdown(index=False))
        
        print(f"✅ Diccionario Markdown generado en: {output_md}")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    generate_dictionary_markdown()