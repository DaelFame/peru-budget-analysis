"""
MEF Peru - Pipeline de ingesta de datos presupuestales
=======================================================
Estrategia:
  1. curl_cffi (impersona Chrome) → intento primario, rápido, sin browser
  2. Playwright                   → fallback si WAF rechaza curl_cffi
  3. CKAN package_show            → descubrimiento dinámico de resource_id reales
  4. Descarga CSV paginada        → datastore_search o link directo

Instalación:
  uv add curl-cffi playwright python-dotenv tqdm
  uv run playwright install chromium
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
BASE_API   = "https://api.datosabiertos.mef.gob.pe/DatosAbiertos/v1"
BASE_CKAN  = "https://datosabiertos.mef.gob.pe/api/3/action"
PORTAL_URL = "https://datosabiertos.mef.gob.pe"

# IDs de los paquetes CKAN (obtenidos desde la URL del portal)
# Ajusta si el slug del dataset es diferente
PACKAGE_IDS = {
    "gastos":  "gasto-del-gobierno-general",
    "ingresos": "ingreso-del-gobierno-general",
}

# Directorio local donde se guardan los CSV descargados
PATH_RAW = Path(os.getenv("PATH_RAW_GASTOS", "./data/raw"))
PATH_RAW.mkdir(parents=True, exist_ok=True)

# Archivo donde se cachean los resource_id descubiertos
RESOURCE_MAP_FILE = Path("resource_map.json")

# Parámetros de descarga
PAGE_SIZE   = 50_000   # filas por request a datastore_search
MAX_RETRIES = 3
DELAY_SEC   = 2        # pausa entre páginas


# ===========================================================================
# CAPA 1 — Bypass WAF: curl_cffi + Playwright fallback
# ===========================================================================

def _make_session_curl():
    """
    Crea sesión HTTP que impersona Chrome usando curl_cffi.
    Evita la detección por firma TLS/JA3 que bloquea requests/urllib.
    """
    try:
        from curl_cffi.requests import Session
        session = Session(impersonate="chrome")
        print("[curl_cffi] Sesión creada — impersonando Chrome")
        return session
    except ImportError:
        print("[curl_cffi] No instalado. Ejecuta: uv add curl-cffi")
        return None


def _fetch_with_playwright(url: str, extra_headers: dict = None) -> Optional[str]:
    """
    Usa Playwright como fallback para:
      - Resolver el challenge JS/WAF de Azure F5
      - Capturar cookies de sesión
    Devuelve el texto de la respuesta o None si falla.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("[Playwright] No instalado. Ejecuta: uv add playwright && uv run playwright install chromium")
        return None

    print(f"[Playwright] Abriendo browser para: {url}")
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            extra_http_headers=extra_headers or {},
        )
        page = context.new_page()

        # Primero visitamos el portal para que el WAF valide la sesión
        page.goto(PORTAL_URL, wait_until="networkidle", timeout=30_000)
        time.sleep(2)

        # Ahora hacemos el fetch a la URL deseada
        response = page.goto(url, wait_until="networkidle", timeout=30_000)
        text = page.content() if response is None else response.text()

        browser.close()
        return text


def safe_get(session, url: str, params: dict = None, use_playwright=False) -> Optional[dict]:
    """
    Intenta GET con curl_cffi; si falla o devuelve HTML (WAF), cae a Playwright.
    Retorna el JSON parseado o None.
    """
    full_url = url
    if params:
        from urllib.parse import urlencode
        full_url = f"{url}?{urlencode(params)}"

    # -- Intento 1: curl_cffi --
    if session is not None and not use_playwright:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = session.get(full_url, timeout=60)
                content_type = r.headers.get("content-type", "")
                if "json" in content_type:
                    return r.json()
                elif "<html" in r.text[:200].lower():
                    print(f"[curl_cffi] WAF devolvió HTML en intento {attempt}. Reintentando…")
                    time.sleep(DELAY_SEC * attempt)
                else:
                    return r.json()
            except Exception as e:
                print(f"[curl_cffi] Error intento {attempt}: {e}")
                time.sleep(DELAY_SEC)

    # -- Fallback: Playwright --
    print("[!] Cayendo a Playwright…")
    text = _fetch_with_playwright(full_url)
    if text:
        try:
            # El contenido puede estar envuelto en etiquetas <pre> o similar
            import re
            match = re.search(r"\{.*\}", text, re.DOTALL)
            if match:
                return json.loads(match.group())
        except json.JSONDecodeError:
            print("[Playwright] No se pudo parsear JSON de la respuesta")
    return None


# ===========================================================================
# CAPA 2 — Descubrimiento dinámico de resource_id
# ===========================================================================

def discover_resource_ids(session, dataset_key: str = "gastos") -> dict[str, str]:
    """
    Llama a CKAN package_show para obtener la lista de recursos reales.
    Retorna un dict: {"2024": "uuid-real", "2023": "uuid-real", ...}

    El portal del MEF usa CKAN estándar, por lo que el endpoint es:
      /api/3/action/package_show?id=<slug-del-dataset>
    """
    package_id = PACKAGE_IDS.get(dataset_key, dataset_key)
    url = f"{BASE_CKAN}/package_show"

    print(f"\n[Discovery] Obteniendo recursos del paquete: {package_id}")
    data = safe_get(session, url, params={"id": package_id})

    if not data:
        print("[Discovery] ❌ No se pudo obtener el paquete. Verifica el slug del dataset.")
        print(f"[Discovery] Intenta buscar el slug real en: {PORTAL_URL}/dataset")
        return {}

    if not data.get("success"):
        print(f"[Discovery] ❌ CKAN devolvió error: {data.get('error')}")
        return {}

    resources = data["result"].get("resources", [])
    print(f"[Discovery] ✅ Encontrados {len(resources)} recursos en el paquete")

    resource_map = {}
    for res in resources:
        name    = res.get("name", "")
        res_id  = res.get("id", "")
        fmt     = res.get("format", "").upper()
        # Intenta extraer el año del nombre del recurso
        import re
        year_match = re.search(r"20\d{2}", name)
        if year_match:
            year = year_match.group()
            resource_map[year] = {
                "id":     res_id,
                "name":   name,
                "format": fmt,
                "url":    res.get("url", ""),
            }
            print(f"  [{year}] {res_id[:8]}… | {fmt} | {name}")

    # Guardar mapa para no tener que redescubrir en cada ejecución
    RESOURCE_MAP_FILE.write_text(json.dumps(resource_map, indent=2, ensure_ascii=False))
    print(f"[Discovery] Mapa guardado en: {RESOURCE_MAP_FILE}")
    return resource_map


def load_or_discover(session, dataset_key: str = "gastos") -> dict[str, str]:
    """Carga el mapa cacheado o lo descubre si no existe."""
    if RESOURCE_MAP_FILE.exists():
        cached = json.loads(RESOURCE_MAP_FILE.read_text())
        if cached:
            print(f"[Discovery] Usando mapa cacheado ({len(cached)} años): {list(cached.keys())}")
            return cached
    return discover_resource_ids(session, dataset_key)


# ===========================================================================
# CAPA 3 — Auditoría y descarga CSV
# ===========================================================================

def audit(resource_map: dict) -> list[str]:
    """
    Compara recursos disponibles en la nube vs archivos locales.
    Retorna lista de años que faltan o están desactualizados.
    """
    print("\n[Audit] Escaneando archivos locales...")
    missing = []
    for year, info in sorted(resource_map.items(), reverse=True):
        local_files = list(PATH_RAW.glob(f"*{year}*.csv"))
        if not local_files:
            print(f"  [{year}] ❌ FALTA  → {info['name']}")
            missing.append(year)
        else:
            size_mb = sum(f.stat().st_size for f in local_files) / 1e6
            print(f"  [{year}] ✅ LOCAL  → {local_files[0].name} ({size_mb:.1f} MB)")
    print(f"\n[Audit] {len(missing)} años por descargar: {missing}")
    return missing


def download_via_direct_url(session, year: str, info: dict) -> bool:
    """
    Descarga el CSV directamente desde la URL del recurso si está disponible.
    Más rápido que paginar datastore_search para archivos completos.
    """
    url = info.get("url", "")
    if not url or info.get("format", "") not in ("CSV", "XLSX"):
        return False

    dest = PATH_RAW / f"gastos_{year}.csv"
    print(f"\n[Download] Descargando {year} desde URL directa…")

    try:
        if session:
            r = session.get(url, stream=True, timeout=300)
            total = int(r.headers.get("content-length", 0))
            with open(dest, "wb") as f, tqdm(
                total=total, unit="B", unit_scale=True,
                desc=f"gastos_{year}.csv"
            ) as bar:
                for chunk in r.iter_content(chunk_size=1024 * 256):
                    f.write(chunk)
                    bar.update(len(chunk))
            print(f"[Download] ✅ Guardado: {dest} ({dest.stat().st_size/1e6:.1f} MB)")
            return True
    except Exception as e:
        print(f"[Download] Error en descarga directa: {e}")
    return False


def download_via_datastore(session, year: str, resource_id: str) -> bool:
    """
    Descarga paginada usando datastore_search.
    Útil cuando no hay URL de descarga directa o el archivo está en el datastore.
    """
    dest = PATH_RAW / f"gastos_{year}.csv"
    print(f"\n[Datastore] Descargando {year} | resource_id: {resource_id[:12]}…")

    url    = f"{BASE_API}/datastore_search"
    offset = 0
    rows   = []
    headers_written = False

    while True:
        params = {
            "resource_id": resource_id,
            "limit":       PAGE_SIZE,
            "offset":      offset,
        }
        data = safe_get(session, url, params=params)

        if not data or not data.get("success"):
            print(f"[Datastore] ❌ Error en offset {offset}")
            break

        result  = data["result"]
        records = result.get("records", [])
        total   = result.get("total", 0)

        if not records:
            break

        import csv
        mode = "w" if not headers_written else "a"
        with open(dest, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=records[0].keys())
            if not headers_written:
                writer.writeheader()
                headers_written = True
            writer.writerows(records)

        offset += len(records)
        pct = offset / total * 100 if total else 0
        print(f"  [Datastore] {offset:,} / {total:,} filas ({pct:.1f}%)")

        if len(records) < PAGE_SIZE:
            break

        time.sleep(DELAY_SEC)

    if dest.exists() and dest.stat().st_size > 0:
        print(f"[Datastore] ✅ {dest} ({dest.stat().st_size/1e6:.1f} MB)")
        return True
    return False


def download_year(session, year: str, info: dict) -> bool:
    """Intenta descarga directa primero; si falla, usa datastore."""
    success = download_via_direct_url(session, year, info)
    if not success:
        success = download_via_datastore(session, year, info["id"])
    return success


# ===========================================================================
# PUNTO DE ENTRADA
# ===========================================================================

def run(dataset_key: str = "gastos", force_rediscover: bool = False):
    """
    Ejecuta el pipeline completo:
      1. Crear sesión bypass WAF
      2. Descubrir resource_ids reales
      3. Auditar vs local
      4. Descargar los que faltan
    """
    print("=" * 60)
    print("  MEF Peru — Pipeline de ingesta presupuestal")
    print("=" * 60)

    # 1. Sesión
    session = _make_session_curl()

    # 2. Descubrimiento
    if force_rediscover and RESOURCE_MAP_FILE.exists():
        RESOURCE_MAP_FILE.unlink()

    resource_map = load_or_discover(session, dataset_key)

    if not resource_map:
        print("\n[!] Sin recursos. Posibles causas:")
        print("    - El slug del dataset es incorrecto. Busca en:")
        print(f"      {PORTAL_URL}/dataset")
        print("    - El WAF bloqueó el acceso (intenta con Playwright headless=False para debug)")
        return

    # 3. Auditoría
    missing_years = audit(resource_map)

    if not missing_years:
        print("\n✅ Todos los archivos están al día. Nada que descargar.")
        return

    # 4. Descarga
    print(f"\n[Pipeline] Descargando {len(missing_years)} año(s): {missing_years}")
    for year in missing_years:
        info = resource_map[year]
        ok = download_year(session, year, info)
        if not ok:
            print(f"[Pipeline] ❌ Falló descarga de {year}")

    print("\n[Pipeline] Proceso terminado.")
    audit(resource_map)   # resumen final


# ---------------------------------------------------------------------------
# CLI rápido
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="MEF Pipeline — Descarga datos presupuestales")
    parser.add_argument(
        "--dataset", default="gastos",
        help="Clave del dataset: 'gastos' o 'ingresos' (default: gastos)"
    )
    parser.add_argument(
        "--rediscover", action="store_true",
        help="Fuerza redescubrimiento de resource_ids (ignora caché)"
    )
    parser.add_argument(
        "--only-audit", action="store_true",
        help="Solo muestra el estado local vs nube sin descargar"
    )
    args = parser.parse_args()

    if args.only_audit:
        if RESOURCE_MAP_FILE.exists():
            rmap = json.loads(RESOURCE_MAP_FILE.read_text())
            audit(rmap)
        else:
            print("No hay resource_map.json. Ejecuta primero sin --only-audit.")
    else:
        run(dataset_key=args.dataset, force_rediscover=args.rediscover)
