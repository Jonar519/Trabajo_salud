import csv

file_path = r'c:\Users\dirki\Desktop\SextoSemestre\Base_de_Datos\Trabajo_salud\data\raw\vigilancia_salud_publica.csv'

counts = {
    'CHIKUNGUNYA': 0,
    'DENGUE': 0,
    'ZIKA': 0
}

unique_names = set()

with open(file_path, mode='r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        event = row['nombre_evento'].upper()
        if 'CHIKUNGUNYA' in event:
            counts['CHIKUNGUNYA'] += int(row['conteo'])
            unique_names.add(row['nombre_evento'])
        elif 'DENGUE' in event:
            counts['DENGUE'] += int(row['conteo'])
            unique_names.add(row['nombre_evento'])
        elif 'ZIKA' in event:
            counts['ZIKA'] += int(row['conteo'])
            unique_names.add(row['nombre_evento'])

print(counts)
print("Eventos encontrados:", unique_names)
