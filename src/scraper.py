import pandas as pd
import requests
import os
import time
from io import StringIO

# Configuración
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw")

# Crear carpeta si no existe
if not os.path.exists(RAW_DATA_PATH):
    os.makedirs(RAW_DATA_PATH)

# DATASETS CON URLs CORRECTOS - SOLO 5 DATASETS SOLICITADOS
DATASETS = [
    {
        "nombre_csv": "normales_climatologicas.csv",
        "url": "https://www.datos.gov.co/resource/nsz2-kzcq.json",
        "descripcion": "Normales Climatologicas de Colombia",
        "tipo": "socrata"
    },
    {
        "nombre_csv": "vigilancia_salud_publica.csv",
        "url": "https://www.datos.gov.co/resource/4hyg-wa9d.json",
        "descripcion": "Datos de Vigilancia en Salud Publica de Colombia",
        "tipo": "socrata"
    },
    {
        "nombre_csv": "calidad_aire_promedio_anual.csv",
        "url": "https://www.datos.gov.co/resource/kekd-7v7h.json",
        "descripcion": "Calidad del Aire en Colombia (Promedio Anual - IDEAM)",
        "tipo": "socrata"
    },
    {
        "nombre_csv": "vacunacion_departamento.csv",
        "url": "https://www.datos.gov.co/resource/6i25-2hdt.json",
        "descripcion": "Coberturas administrativas de vacunacion por departamento",
        "tipo": "socrata"
    },
    {
        "nombre_csv": "prestadores_sedes.csv",
        "url": "https://www.datos.gov.co/resource/c36g-9fc2.json",
        "descripcion": "Registro Especial de Prestadores y Sedes de Servicios de Salud",
        "tipo": "socrata"
    }
]

def descargar_csv_directo(url: str, descripcion: str) -> pd.DataFrame:
    """Descarga CSV directamente desde URL"""
    print(f"   → Descargando CSV directo...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=120)
        response.raise_for_status()
        
        # Intentar diferentes codificaciones
        for encoding in ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']:
            try:
                df = pd.read_csv(StringIO(response.content.decode(encoding)), low_memory=False)
                if len(df) > 0:
                    print(f"   → Descargados: {len(df):,} registros")
                    return df
            except:
                continue
        
        raise ValueError("No se pudo leer el CSV")
        
    except Exception as e:
        print(f"   ✗ Error: {e}")
        raise

def descargar_socrata_completo(url: str, descripcion: str) -> pd.DataFrame:
    """Descarga datos COMPLETOS de Socrata API sin límites"""
    headers = {"Accept": "application/json"}
    
    todos_datos = []
    offset = 0
    page_size = 50000  # Máximo permitido por la API
    
    print(f"   → Descargando dataset completo (sin límites)...")
    
    while True:
        paged_url = f"{url}?$limit={page_size}&$offset={offset}"
        
        try:
            response = requests.get(paged_url, headers=headers, timeout=120)
            response.raise_for_status()
            
            data = response.json()
            if not data:
                break
                
            todos_datos.extend(data)
            offset += len(data)
            print(f"   . Progreso: {len(todos_datos):,} registros descargados", end="\r")
            
            # Si obtuvimos menos de page_size, es la última página
            if len(data) < page_size:
                break
                
            # Pequeña pausa para no sobrecargar el servidor
            time.sleep(0.5)
            
        except Exception as e:
            print(f"\n   ✗ Error en la descarga: {e}")
            break
    
    print(f"\n   → Total descargado: {len(todos_datos):,} registros")
    
    if todos_datos:
        return pd.DataFrame(todos_datos)
    return pd.DataFrame()

def ejecutar_descarga():
    print("=" * 70)
    print("  DESCARGA COMPLETA - 10 DATASETS SIN LÍMITES")
    print("  (Descargando TODOS los registros disponibles)")
    print(f"  Destino: {RAW_DATA_PATH}")
    print("=" * 70)
    
    # Deshabilitar verificaciones SSL para medata.gov.co
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    exitosos = 0
    fallidos = 0
    
    for i, ds in enumerate(DATASETS, 1):
        nombre_csv = ds["nombre_csv"]
        url = ds["url"]
        descripcion = ds["descripcion"]
        tipo = ds.get("tipo", "socrata")
        ruta_csv = os.path.join(RAW_DATA_PATH, nombre_csv)
        
        print(f"\n[{i}/10] {descripcion}")
        
        try:
            start_time = time.time()
            
            if tipo == "directo":
                df = descargar_csv_directo(url, descripcion)
            else:
                # Descargar COMPLETO sin límites
                df = descargar_socrata_completo(url, descripcion)
            
            # Verificar si df es válido y tiene datos
            if df is None or df.empty:
                print(f"   ✗ No se obtuvieron datos")
                fallidos += 1
                continue
            
            print(f"   → Procesando {len(df):,} filas...")
            
            # Limpiar nombres de columnas
            df.columns = [str(col).strip().replace(' ', '_').replace(':', '_').replace('-', '_') 
                         for col in df.columns]
            
            # Guardar CSV
            df.to_csv(ruta_csv, index=False, encoding='utf-8-sig')
            
            elapsed_time = time.time() - start_time
            tamaño_mb = os.path.getsize(ruta_csv) / (1024 * 1024)
            print(f"   ✓ Guardado: {nombre_csv}")
            print(f"     → {len(df):,} filas x {len(df.columns)} columnas")
            print(f"     → Tamaño: {tamaño_mb:.2f} MB")
            print(f"     → Tiempo: {elapsed_time:.2f} segundos")
            exitosos += 1
            
        except Exception as e:
            print(f"   ✗ Error: {e}")
            fallidos += 1
            import traceback
            traceback.print_exc()
    
    print("\n" + "=" * 70)
    print("  RESUMEN FINAL")
    print(f"  ✓ Descargados exitosamente: {exitosos}/10")
    print(f"  ✗ Fallidos: {fallidos}/10")
    print(f"  📁 Ubicación: {RAW_DATA_PATH}")
    
    # Listar todos los archivos descargados
    if os.path.exists(RAW_DATA_PATH):
        archivos = sorted([f for f in os.listdir(RAW_DATA_PATH) if f.endswith('.csv')])
        if archivos:
            print(f"\n  Archivos descargados:")
            for archivo in archivos:
                tamaño = os.path.getsize(os.path.join(RAW_DATA_PATH, archivo))
                print(f"    ✓ {archivo} ({tamaño:,} bytes)")
    
    print("=" * 70)

if __name__ == "__main__":
    ejecutar_descarga()