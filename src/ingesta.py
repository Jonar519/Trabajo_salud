import pandas as pd
import os
import sqlite3
from sqlalchemy import create_engine

# 1. Configurar rutas de carpetas
# Usamos rutas relativas para que funcione en cualquier PC
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw")
DB_PATH = os.path.join(BASE_DIR, "data", "data_lake_sucia.db")

def limpiar_nombre_tabla(nombre_archivo):
    """Limpia el nombre del archivo para usarlo como nombre de tabla SQL"""
    nombre = nombre_archivo.lower().replace(".csv", "")
    nombre = nombre.replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_")
    return nombre

def ejecutar_ingesta():
    # Crear conexión a la base de datos local (se creará el archivo .db si no existe)
    engine = create_engine(f"sqlite:///{DB_PATH}")
    
    print("--- Iniciando Ingesta de Datos ---")
    
    # Listar archivos en data/raw
    archivos = [f for f in os.listdir(RAW_DATA_PATH) if f.endswith('.csv')]
    
    if not archivos:
        print("❌ No se encontraron archivos CSV en data/raw")
        return

    for archivo in archivos:
        path_completo = os.path.join(RAW_DATA_PATH, archivo)
        nombre_tabla = limpiar_nombre_tabla(archivo)
        
        try:
            print(f"⏳ Procesando: {archivo} ...")
            
            # Leer el CSV con pandas
            # Usamos low_memory=False por si los archivos son grandes
            # Usamos encoding='latin1' o 'utf-8' según necesite el archivo de datos.gov.co
            try:
                df = pd.read_csv(path_completo, encoding='utf-8', low_memory=False)
            except UnicodeDecodeError:
                df = pd.read_csv(path_completo, encoding='latin1', low_memory=False)

            # Cargar a la Base de Datos Local
            df.to_sql(nombre_tabla, engine, if_exists='replace', index=False)
            
            print(f"✅ Cargado en tabla: '{nombre_tabla}' ({len(df)} registros)")
            
        except Exception as e:
            print(f"⚠️ Error cargando {archivo}: {e}")

    print("\n--- Ingesta Finalizada con Éxito ---")
    print(f"Tu Data Lake local está en: {DB_PATH}")

if __name__ == "__main__":
    ejecutar_ingesta()