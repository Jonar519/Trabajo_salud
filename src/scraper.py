import pandas as pd
import requests
import os

BASE_DIR      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw")

os.makedirs(RAW_DATA_PATH, exist_ok=True)

DATASETS = [
    
    {
        "nombre_csv": "chikungunya.csv",
        "url": "http://medata.gov.co/sites/default/files/distribution/1-026-22-000130/sivigila_chikungunya.csv",
        "descripcion": "Chikungunya Colombia",
    },
    {
        "nombre_csv": "dengue.csv",
        "url": "http://medata.gov.co/sites/default/files/distribution/1-026-22-000135/sivigila_dengue.csv",
        "descripcion": "Dengue Colombia",
    },
    {
        "nombre_csv": "zika.csv",
        "url": "http://medata.gov.co/sites/default/files/distribution/1-026-22-000171/sivigila_zika.csv",
        "descripcion": "Zika Colombia",
    },
   
    {
        "nombre_csv": "hospitales_antioquia.csv",
        "url": "https://www.datos.gov.co/resource/pi36-fdpk.json",
        "descripcion": "Hospitales ESE Antioquia",
        "socrata": True,
    },
    {
        "nombre_csv": "normales_climatologicas.csv",
        "url": "https://www.datos.gov.co/resource/nsz2-kzcq.json",
        "descripcion": "Normales Climatológicas Colombia",
        "socrata": True,
    },
]

PAGE_SIZE = 50_000
APP_TOKEN = ""   # opcional

def descargar_csv_directo(url: str, descripcion: str) -> pd.DataFrame:
    
    print(f"   → GET {url}")
    resp = requests.get(url, timeout=120)

    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code} para '{descripcion}'")

    from io import StringIO
    try:
        df = pd.read_csv(StringIO(resp.text), encoding="utf-8", low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(StringIO(resp.content.decode("latin1")), low_memory=False)

    return df


def descargar_socrata(url: str, descripcion: str) -> pd.DataFrame:
   
    import time

    headers = {"Accept": "application/json"}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN

    frames = []
    offset = 0

    while True:
        paged_url = f"{url}?$limit={PAGE_SIZE}&$offset={offset}&$order=:id"
        print(f"   → GET {paged_url[:80]}...")
        resp = requests.get(paged_url, headers=headers, timeout=120)

        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code} para '{descripcion}': {resp.text[:200]}")

        lote = resp.json()
        if not lote:
            break

        frames.append(pd.DataFrame(lote))
        offset += len(lote)
        print(f"   … {offset:,} registros obtenidos", end="\r")

        if len(lote) < PAGE_SIZE:
            break

        time.sleep(0.3)

    print()
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def ejecutar_scraping():
    print("═" * 55)
    print("  SCRAPING — medata.gov.co + datos.gov.co")
    print(f"  Destino: {RAW_DATA_PATH}")
    print("═" * 55)

    for ds in DATASETS:
        nombre_csv  = ds["nombre_csv"]
        url         = ds["url"]
        descripcion = ds["descripcion"]
        es_socrata  = ds.get("socrata", False)
        ruta_csv    = os.path.join(RAW_DATA_PATH, nombre_csv)

        print(f"\n▶  {descripcion}")

        try:
            if es_socrata:
                df = descargar_socrata(url, descripcion)
            else:
                df = descargar_csv_directo(url, descripcion)

            if df.empty:
                print(f"   ⚠️  Sin registros — se omite.")
                continue

            df.to_csv(ruta_csv, index=False, encoding="utf-8")
            print(f"   ✅ Guardado: {nombre_csv}  ({len(df):,} filas × {len(df.columns)} cols)")

        except Exception as e:
            print(f"   ❌ Error: {e}")

    print("\n" + "═" * 55)
    print("  SCRAPING FINALIZADO")
    print(f"  Archivos en: {RAW_DATA_PATH}")
    print("═" * 55)


if __name__ == "__main__":
    ejecutar_scraping()