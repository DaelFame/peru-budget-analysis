import os
import json
from curl_cffi import requests # Importación especial
from dotenv import load_dotenv

load_dotenv()

def get_mef_data_stealth(resource_id):
    # Usamos la URL del portal que mencionaste
    url = "https://api.datosabiertos.mef.gob.pe/DatosAbiertos/v1/datastore_search_sql"
    
    query = f'SELECT * FROM "{resource_id}" LIMIT 5'
    
    print(f"🕵️ Simulando navegación humana para ID: {resource_id}")
    
    try:
        # impersonate="chrome110" hace que la petición sea IDÉNTICA a Chrome
        # esto salta la mayoría de los firewalls que bloquean a Python
        response = requests.get(
            url, 
            params={'sql': query},
            impersonate="chrome110", 
            timeout=30,
            verify=False
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ Respuesta del servidor: {response.status_code}")
            # Si sigue fallando, veremos el HTML que nos bloquea
            print(response.text[:300]) 
            return None
            
    except Exception as e:
        print(f"❌ Error de red: {e}")
        return None

def run_audit():
    # Asegúrate de que este ID sea el que aparece en la URL del portal
    resource_id = "0e2469d8-5872-4bc2-a5bc-91ee01c99df8"
    
    data = get_mef_data_stealth(resource_id)
    
    if data and data.get("success"):
        records = data.get("result", {}).get("records", [])
        if records:
            print("\n✅ ¡CONEXIÓN EXITOSA!")
            print(f"📊 Campos detectados: {list(records[0].keys())}")
        else:
            print("\n⚠️ Conectó pero no hay registros.")
    else:
        print("\n❌ Bloqueo persistente.")

if __name__ == "__main__":
    run_audit()