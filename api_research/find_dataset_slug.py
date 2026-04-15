import json
import sys
import re
from urllib.parse import urlencode

BASE_CKAN = "https://datosabiertos.mef.gob.pe/api/3/action"

def get_session():
    """Configura la sesión con curl_cffi para intentar el bypass inicial."""
    try:
        from curl_cffi.requests import Session
        # Impersonamos Chrome 124 que es más reciente y estable
        return Session(impersonate="chrome124")
    except ImportError:
        print("[!] curl_cffi no instalado: uv add curl-cffi")
        return None

def list_packages(session, query: str = "gasto"):
    """Lista paquetes CKAN que coincidan con la búsqueda."""
    url = f"{BASE_CKAN}/package_search"
    params = {"q": query, "rows": 20}
    
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Fedora; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://datosabiertos.mef.gob.pe/dataset"
    }

    print(f"\n🚀 Buscando paquetes con q='{query}'…")
    
    try:
        r = session.get(url, params=params, headers=headers, timeout=30, verify=False)
        
        # Validación de WAF
        if "json" not in r.headers.get("content-type", "").lower():
            print("[!] El WAF bloqueó curl_cffi (devolvió HTML).")
            print("\n💡 Slugs conocidos para tu proyecto (puedes usarlos con --inspect):")
            print("-" * 65)
            print(f"{'comparacion-de-presupuesto-ejecucion-gasto':<45} | Gasto")
            print(f"{'comparacion-de-presupuesto-ejecucion-ingreso':<45} | Ingreso")
            return

        data = r.json()
        if not data.get("success"):
            print(f"❌ Error CKAN: {data.get('error')}")
            return

        results = data["result"]["results"]
        total   = data["result"]["count"]
        
        print(f"\n✅ {total} paquete(s) encontrados:\n")
        print(f"{'SLUG (id)':<45} {'NOMBRE':<50} {'RECURSOS'}")
        print("-" * 110)
        
        for pkg in results:
            slug  = pkg.get("name", "")
            title = pkg.get("title", "")[:48]
            n_res = len(pkg.get("resources", []))
            print(f"{slug:<45} {title:<50} {n_res}")

    except Exception as e:
        print(f"❌ Error durante la búsqueda: {e}")

def inspect_package(session, slug: str):
    """Muestra los resources de un paquete específico para obtener los IDs."""
    url = f"{BASE_CKAN}/package_show"
    full_url = f"{url}?{urlencode({'id': slug})}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Fedora; Linux x86_64) AppleWebKit/537.36",
        "Referer": "https://datosabiertos.mef.gob.pe/"
    }

    print(f"\n🔍 Inspeccionando paquete: {slug}")
    try:
        r = session.get(full_url, headers=headers, timeout=30, verify=False)
        
        if "json" not in r.headers.get("content-type", "").lower():
            print("❌ El WAF bloqueó la inspección. Intenta usar el script principal mef_pipeline.py")
            return

        data = r.json()
        if not data.get("success"):
            print(f"❌ Error: {data.get('error')}")
            return

        resources = data["result"].get("resources", [])
        print(f"\n📂 {len(resources)} recursos encontrados:\n")
        print(f"{'AÑO/BLOQUE':<15} {'RESOURCE_ID':<40} {'FMT':<8} {'NOMBRE'}")
        print("-" * 100)
        
        for res in sorted(resources, key=lambda x: x.get("name", ""), reverse=True):
            # Buscar años en el nombre (ej: 2022_2026)
            year_m = re.search(r"20\d{2}(?:_20\d{2})?", res.get("name", ""))
            year   = year_m.group() if year_m else "N/A"
            rid    = res.get("id", "")
            fmt    = res.get("format", "")
            name   = res.get("name", "")[:45]
            print(f"{year:<15} {rid:<40} {fmt:<8} {name}")

    except Exception as e:
        print(f"❌ Error al inspeccionar: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Buscador de Slugs e IDs del MEF")
    parser.add_argument("--q",       default="gasto",  help="Término de búsqueda (ej: gasto, ingreso)")
    parser.add_argument("--inspect", default=None,     help="Slug del paquete a inspeccionar")
    args = parser.parse_args()

    session = get_session()
    if session is None:
        sys.exit(1)

    if args.inspect:
        inspect_package(session, args.inspect)
    else:
        list_packages(session, args.q)
        print("\n💡 Sugerencia: Copia un SLUG de la lista y ejecútalo con --inspect [SLUG]")