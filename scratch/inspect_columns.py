import csv
import os

files = [
    'calidad_aire_promedio_anual.csv',
    'normales_climatologicas.csv',
    'prestadores_sedes.csv',
    'vacunacion_departamento.csv',
    'vigilancia_salud_publica.csv'
]

base_path = r'c:\Users\dirki\Desktop\SextoSemestre\Base_de_Datos\Trabajo_salud\data\raw'

for file_name in files:
    full_path = os.path.join(base_path, file_name)
    try:
        with open(full_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            print(f"File: {file_name}")
            print(f"Column Count: {len(headers)}")
            print(f"Columns: {', '.join(headers)}")
            print("-" * 30)
    except Exception as e:
        # Try with different encoding if utf-8 fails
        try:
            with open(full_path, mode='r', encoding='latin-1') as f:
                reader = csv.reader(f)
                headers = next(reader)
                print(f"File: {file_name}")
                print(f"Column Count: {len(headers)}")
                print(f"Columns: {', '.join(headers)}")
                print("-" * 30)
        except Exception as e2:
            print(f"Error reading {file_name}: {e2}")
