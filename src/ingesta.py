import pandas as pd
import os
from sqlalchemy import create_engine

RAW_DATA_PATH = "data/raw/"

DB_PATH = "sqlite:///data/data_lake_sucia.db" 

def cargar_datos_a_db():
    engine = create_engine(DB_PATH)
    
    for archivo in os.listdir(RAW_DATA_PATH):
        if archivo.endswith(".csv"):
            print(f"Cargando {archivo} a la base de datos local...")

            df = pd.read_csv(os.path.join(RAW_DATA_PATH, archivo), low_memory=False)
            
            nombre_tabla = archivo.replace(".csv", "").lower()
            
            # Cargar a DB Local
            df.to_sql(nombre_tabla, engine, if_exists='replace', index=False)
            print(f"Tabla '{nombre_tabla}' creada con éxito.")

if __name__ == "__main__":
    cargar_datos_a_db()