import pandas as pd
import os
from sqlalchemy import create_engine

BASE_DIR      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw")
DB_PATH       = os.path.join(BASE_DIR, "data", "data_lake_sucia.db")


def limpiar_nombre_tabla(nombre_archivo: str) -> str:

    nombre = nombre_archivo.lower().replace(".csv", "")
    for ch in (" ", "(", ")", "-"):
        nombre = nombre.replace(ch, "_")
    return nombre

def ejecutar_ingesta():
    engine = create_engine(f"sqlite:///{DB_PATH}")

    print("═" * 55)
    print("  INGESTA — data/raw  →  data_lake_sucia.db")
    print("═" * 55)

    archivos = [f for f in os.listdir(RAW_DATA_PATH) if f.endswith(".csv")]

    if not archivos:
        print("❌ No hay archivos CSV en data/raw.")
        print("   Ejecuta primero:  python src/scraper.py")
        return

    for archivo in sorted(archivos):
        ruta_csv     = os.path.join(RAW_DATA_PATH, archivo)
        nombre_tabla = limpiar_nombre_tabla(archivo)

        print(f"\n▶  {archivo}")

        try:
            try:
                df = pd.read_csv(ruta_csv, encoding="utf-8", low_memory=False)
            except UnicodeDecodeError:
                df = pd.read_csv(ruta_csv, encoding="latin1", low_memory=False)

            df.to_sql(nombre_tabla, engine, if_exists="replace", index=False)
            print(f"   ✅ Tabla '{nombre_tabla}' — {len(df):,} filas × {len(df.columns)} cols")

        except Exception as e:
            print(f"   ❌ Error: {e}")

    print("\n" + "═" * 55)
    print("  INGESTA FINALIZADA")
    print(f"  Data Lake: {DB_PATH}")
    print("═" * 55)


if __name__ == "__main__":
    ejecutar_ingesta()