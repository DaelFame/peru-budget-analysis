"""
MEF Peru - Pipeline de ingesta de datos presupuestales (V2)
=======================================================
Optimizado para: Bloques multianuales de 6.3 GB
Estrategia: curl_cffi + Playwright Fallback + Descubrimiento por Slug
"""

from __future__ import annotations
import json
import os
import re
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

# ---------------------------------------------------------------------------
# Configuración Global
# ---------------------------------------------------------------------------
BASE_API   = "https://api.datosabiertos.mef.gob.pe/DatosAbiertos/v1"
BASE_CKAN  = "https://datosabiertos.mef.gob.pe/api/3/action"
PORTAL_URL = "https://datosabiertos.mef.gob.pe"

# Slugs reales identificados en el portal
PACKAGE_IDS = {
    "gastos":  "comparacion-de-presupuesto-ejecucion-gasto",
    "ingresos": "comparacion-de-presupuesto-ejecucion-ingreso",
}

# Directorio local (se lee de .env o usa default)
PATH_RAW = Path(os.getenv("PATH_RAW_GASTOS", "./data/raw/presupuesto/ejecucion_gasto"))
PATH_RAW.mkdir(parents=True, exist_ok=True)

RESOURCE_MAP_FILE = Path("resource_map.json")
MAX_RETRIES = 3
DELAY_SEC   = 2

# ===========================================================================
# CAPA 1 — Bypass WAF
# ===========================================================================

def _make_session_curl():
    try:
        from curl_cffi.requests import Session
        return Session(impersonate="chrome124")
    except ImportError:
        print("[!] curl_cffi no instalado. Ejecuta: uv add curl-cffi")
        return None

def _fetch_with_playwright(url: str) -> Optional[str]:
    """Fallback con navegador real y bypass de detección de bots."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return None

    print(f"[Playwright] 🛡️ Resolviendo WAF para: {url}")
    with sync_playwright() as p:
        # Lanzamos con un User Agent de una persona real en Windows/Mac para despistar
        browser = p.chromium.launch(headless=True) 
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        
        try:
            # 1. Visitamos la home para que nos den la cookie de sesión
            page.goto(PORTAL_URL, wait_until="networkidle", timeout=60000)
            time.sleep(5) # Esperamos a que carguen los scripts de seguridad
            
            # 2. Intentamos ir al JSON del paquete
            page.goto(url, wait_until="networkidle", timeout=60000)
            time.sleep(2)
            
            # Extraemos el texto crudo (el JSON)
            content = page.locator("pre").inner_text() if page.locator("pre").count() > 0 else page.content()
            browser.close()
            return content
        except Exception as e:
            print(f"[Playwright] ❌ Error de navegación: {e}")
            browser.close()
            return None

    print(f"[Playwright] 🛡️ Resolviendo WAF para: {url}")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 (X11; Linux x86_64) Chrome/124.0.0.0")
        page = context.new_page()
        
        # Primero validamos sesión en el portal
        page.goto(PORTAL_URL, wait_until="networkidle")
        time.sleep(2)
        
        # Ir a la API
        response = page.goto(url, wait_until="networkidle")
        text = page.content() if response is None else response.text()
        browser.close()
        return text

def safe_get(session, url: str, params: dict = None) -> Optional[dict]:
    full_url = f"{url}?{urlencode(params)}" if params else url
    
    # Intento con curl_cffi
    if session:
        try:
            r = session.get(full_url, timeout=30, verify=False)
            if "json" in r.headers.get("content-type", "").lower():
                return r.json()
        except Exception as e:
            print(f"[curl_cffi] Error: {e}")

    # Fallback Playwright
    text = _fetch_with_playwright(full_url)
    if text:
        try:
            match = re.search(r"\{.*\}", text, re.DOTALL)
            if match: return json.loads(match.group())
        except: pass
    return None

# ===========================================================================
# CAPA 2 — Descubrimiento y Auditoría
# ===========================================================================

def discover_resource_ids(session, dataset_key: str = "gastos") -> dict:
    package_id = PACKAGE_IDS.get(dataset_key, dataset_key)
    url = f"{BASE_CKAN}/package_show"

    print(f"\n[Discovery] 🔍 Buscando recursos para: {package_id}")
    data = safe_get(session, url, params={"id": package_id})

    if not data or not data.get("success"):
        print("[Discovery] ❌ Error al obtener paquete.")
        return {}

    resources = data["result"].get("resources", [])
    resource_map = {}
    
    for res in resources:
        name = res.get("name", "")
        # Detectar periodos tipo 2022-2026, 2022_2026 o años simples
        period_m = re.search(r"20\d{2}(?:_20\d{2}|-20\d{2})?", name)
        period = period_m.group() if period_m else name.replace(" ", "_")
        
        resource_map[period] = {
            "id": res.get("id"),
            "url": res.get("url"),
            "name": name,
            "format": res.get("format", "").upper()
        }
        print(f"  ✅ Encontrado: [{period}] -> {name}")

    RESOURCE_MAP_FILE.write_text(json.dumps(resource_map, indent=2))
    return resource_map

def audit(resource_map: dict) -> list[str]:
    print(f"\n[Audit] 📂 Escaneando: {PATH_RAW}")
    missing = []
    for period, info in sorted(resource_map.items(), reverse=True):
        local_files = list(PATH_RAW.glob(f"*{period}*.csv"))
        if not local_files:
            missing.append(period)
            print(f"  [{period}] ❌ Pendiente")
        else:
            size = sum(f.stat().st_size for f in local_files) / 1e6
            print(f"  [{period}] ✅ Local ({size:.1f} MB)")
    return missing

# ===========================================================================
# CAPA 3 — Descarga Directa
# ===========================================================================

def download_file(session, period: str, info: dict):
    url = info["url"]
    dest = PATH_RAW / f"mef_gasto_{period}.csv"
    
    print(f"\n🚀 Iniciando descarga de bloque: {period}")
    try:
        # Usamos stream para no saturar la RAM del Ryzen con archivos de 6GB
        r = session.get(url, stream=True, timeout=600, verify=False)
        total = int(r.headers.get("content-length", 0))
        
        with open(dest, "wb") as f, tqdm(
            total=total, unit="B", unit_scale=True, desc=f"📦 {period}"
        ) as bar:
            for chunk in r.iter_content(chunk_size=1024*1024): # 1MB chunks
                f.write(chunk)
                bar.update(len(chunk))
        print(f"✅ Descarga completada: {dest}")
    except Exception as e:
        print(f"❌ Error descargando {period}: {e}")

# ===========================================================================
# Main
# ===========================================================================

def run(force_rediscover=False):
    session = _make_session_curl()
    
    if force_rediscover or not RESOURCE_MAP_FILE.exists():
        resource_map = discover_resource_ids(session)
    else:
        resource_map = json.loads(RESOURCE_MAP_FILE.read_text())

    missing = audit(resource_map)
    
    if not missing:
        print("\n✨ Todo al día.")
        return

    for period in missing:
        download_file(session, period, resource_map[period])

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--rediscover", action="store_true")
    args = parser.parse_args()
    
    run(force_rediscover=args.rediscover)