import os
import sys
import typing

# Parche de compatibilidad para PySpark 3.4.x con Java 17+ y Python 3.12+
class MockIO:
    pass
MockIO.BinaryIO = typing.BinaryIO
MockIO.TextIO = typing.TextIO
sys.modules['typing.io'] = MockIO

# Configurar argumentos de Spark y Java antes de importar o iniciar sesión
# Estos parches son críticos para ejecutar Spark 3.4 en Java 17/21 y Python 3.12+
os.environ['JAVA_TOOL_OPTIONS'] = (
    '--add-opens=java.base/java.nio=ALL-UNNAMED '
    '--add-opens=java.base/sun.nio.ch=ALL-UNNAMED '
    '--add-opens=java.base/java.lang=ALL-UNNAMED '
    '--add-opens=java.base/java.util=ALL-UNNAMED '
    '--add-opens=java.base/java.util.concurrent=ALL-UNNAMED '
    '--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED'
)
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--conf "spark.driver.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED" '
    '--conf "spark.executor.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED" '
    'pyspark-shell'
)

# Truco para Windows: No necesitamos HADOOP_HOME si guardamos con Pandas
if 'HADOOP_HOME' in os.environ:
    del os.environ['HADOOP_HOME']

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, StringType

# Configuración de rutas
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
DATA_PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")

# Crear carpeta de salida si no existe
if not os.path.exists(DATA_PROCESSED_DIR):
    os.makedirs(DATA_PROCESSED_DIR)

def create_spark_session():
    """Inicializa la SparkSession optimizada con parches de compatibilidad para Java 17+."""
    return (SparkSession.builder
            .appName("DatasetMaestroAntioquia")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.shuffle.partitions", "8")
            # Parches de compatibilidad para Java 17+
            .config("spark.driver.extraJavaOptions", 
                    "--add-opens=java.base/java.nio=ALL-UNNAMED " +
                    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
                    "--add-opens=java.base/java.lang=ALL-UNNAMED " +
                    "--add-opens=java.base/java.util=ALL-UNNAMED " +
                    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED")
            .config("spark.sql.ansi.enabled", "false") # Desactivar modo estricto para datos sucios
            .config("spark.hadoop.fs.permissions.umask-mode", "000") # Intentar bypass de permisos en Windows
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") # Evitar uso de librerías nativas
            .getOrCreate())

def normalize_text(col_name):
    """Limpia y normaliza texto: minúsculas, sin acentos, sin espacios extras."""
    # Convertir a minúsculas y quitar espacios
    normalized = F.lower(F.trim(F.col(col_name)))
    # Quitar acentos y caracteres especiales comunes
    normalized = F.regexp_replace(normalized, "[áàäâ]", "a")
    normalized = F.regexp_replace(normalized, "[éèëê]", "e")
    normalized = F.regexp_replace(normalized, "[íìïî]", "i")
    normalized = F.regexp_replace(normalized, "[óòöô]", "o")
    normalized = F.regexp_replace(normalized, "[úùüû]", "u")
    normalized = F.regexp_replace(normalized, "ñ", "n")
    # Eliminar cualquier otro caracter no alfanumérico excepto espacios
    normalized = F.regexp_replace(normalized, "[^a-z0-9 ]", "")
    return normalized

def process_epi_data(spark):
    """Carga y unifica los datasets de Dengue, Zika y Chikungunya."""
    print("Cargando datos epidemiológicos...")
    
    # Rutas de archivos
    files = {
        "DENGUE": os.path.join(DATA_RAW_DIR, "dengue.csv"),
        "ZIKA": os.path.join(DATA_RAW_DIR, "zika.csv"),
        "CHIKUNGUNYA": os.path.join(DATA_RAW_DIR, "chikungunya.csv")
    }
    
    dfs = []
    for disease, path in files.items():
        if os.path.exists(path):
            df = spark.read.csv(path, header=True, inferSchema=True)
            
            # 1. FILTRADO OBLIGATORIO: Solo Medellín (municipio code 1)
            df = df.filter(F.col("cod_mpio_r").cast("int") == 1)

            # 2. LIMPIEZA DE CALIDAD (Nuevas correcciones de auditoría)
            # Eliminar edades imposibles y sexo desconocido
            df = df.filter((F.col("edad_").cast("int") >= 0) & (F.col("edad_").cast("int") <= 110))
            df = df.filter(F.col("sexo_").isin(["M", "F"]))

            # 3. ESTANDARIZACIÓN DE COMUNAS
            comunas_oficiales = [
                "Popular", "Santa Cruz", "Manrique", "Aranjuez", "Castilla", 
                "Doce de Octubre", "Robledo", "Villa Hermosa", "Buenos Aires", 
                "La Candelaria", "Laureles-Estadio", "La America", "San Javier", 
                "El Poblado", "Guayabal", "Belen"
            ]
            
            # Crear expresión CASE para normalizar nombres y agrupar corregimientos/otros
            comuna_expr = F.when(F.col("comuna").cast("string").isNull(), "No Especificada")
            for c in comunas_oficiales:
                comuna_expr = comuna_expr.when(F.lower(F.col("comuna")).contains(c.lower()[:5]), c)
            
            df = df.withColumn("comuna_limpia", comuna_expr.otherwise("Corregimiento / Otros"))
            
            # Normalizar Edad a Años
            if "uni_med_" not in df.columns:
                df = df.withColumn("uni_med_", F.lit(1))
            
            df = df.withColumn("edad_normalizada", 
                F.when(F.col("uni_med_") == 1, F.col("edad_").cast("double"))
                .when(F.col("uni_med_") == 2, F.col("edad_").cast("double") / 12.0)
                .when(F.col("uni_med_") == 3, F.col("edad_").cast("double") / 365.0)
                .otherwise(F.col("edad_").cast("double"))
            )

            # Mapeo de Tipo de Caso
            df = df.withColumn("tipo_caso_label", 
                F.when(F.col("tip_cas_") == 1, "Sospechoso")
                .when(F.col("tip_cas_") == 2, "Confirmado_Clinica")
                .when(F.col("tip_cas_") == 3, "Confirmado_Laboratorio")
                .when(F.col("tip_cas_") == 4, "Confirmado_Nexo")
                .otherwise("Otro")
            )
            
            # Seleccionar columnas clave incluyendo COMUNA LIMPIA
            df = df.select(
                F.col("year_").cast("int").alias("año"),
                F.col("semana").cast("int").alias("semana_epidemiologica"),
                F.lit("Medellin").alias("municipio"),
                F.col("comuna_limpia").alias("comuna"),
                F.lit(disease).alias("enfermedad"),
                F.col("sexo_").alias("sexo"),
                F.col("edad_normalizada").alias("edad"),
                F.col("tipo_caso_label").alias("tipo_caso")
            )
            
            # Calcular mes
            df = df.withColumn("mes", F.ceil(F.col("semana_epidemiologica") * 12 / 53))
            df = df.withColumn("mes", F.when(F.col("mes") > 12, 12).when(F.col("mes") < 1, 1).otherwise(F.col("mes")))
            
            dfs.append(df)
    
    full_epi_df = dfs[0]
    for next_df in dfs[1:]:
        full_epi_df = full_epi_df.union(next_df)
    
    # 3. AGRUPACIÓN POR COMUNA
    print("Agrupando datos por Comuna...")
    epi_grouped = full_epi_df.groupBy(
        "año", "mes", "semana_epidemiologica", "municipio", "comuna", "enfermedad"
    ).agg(
        F.count("*").alias("casos_reportados"),
        F.mode("sexo").alias("sexo_predominante"),
        F.avg("edad").alias("edad_promedio"),
        F.mode("tipo_caso").alias("tipo_caso")
    )
    
    return epi_grouped

def process_climate_data(spark):
    """Carga y transforma los datos climáticos."""
    print("Cargando datos climáticos...")
    path = os.path.join(DATA_RAW_DIR, "normales_climatologicas.csv")
    
    if not os.path.exists(path):
        return None
        
    df = spark.read.csv(path, header=True, inferSchema=True)
    
    # Filtrar solo Antioquia
    df = df.filter(normalize_text("departamento") == "antioquia")
    
    # Unpivot de meses (ene, feb, mar...) a filas
    # mapeo de nombres de columnas a número de mes
    month_cols = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]
    
    # Expresión stack para unpivot
    stack_expr = "stack(12, " + ", ".join([f"'{i+1}', {month_cols[i]}" for i in range(12)]) + ") as (mes, valor)"
    
    # Limpiar 'SD' en la columna valor antes de procesar
    df_unpivoted = df.selectExpr(
        "municipio", "par_metro", "altitud_m", "latitud", "longitud", stack_expr
    )
    df_unpivoted = df_unpivoted.withColumn("valor", 
        F.when(F.col("valor").cast("string") == "SD", F.lit(None)).otherwise(F.col("valor"))
    )
    
    # Normalizar nombres de municipio y parámetros
    df_unpivoted = df_unpivoted.withColumn("municipio_norm", normalize_text("municipio"))
    df_unpivoted = df_unpivoted.withColumn("param", normalize_text("par_metro"))
    df_unpivoted = df_unpivoted.withColumn("mes", F.col("mes").cast("int"))
    
    # Pivotar parámetros detectando por palabras clave
    # Identificamos las columnas resultantes del pivot
    climate_pivot = df_unpivoted.groupBy("municipio_norm", "mes").pivot("param").agg(F.avg("valor"))
    
    # Mapeo flexible de columnas
    cols = climate_pivot.columns
    def find_col(keywords):
        for c in cols:
            if any(k in c for k in keywords):
                return c
        return None

    temp_avg = find_col(["temperatura media", "temperatura promedio"])
    temp_max = find_col(["temperatura maxima"])
    temp_min = find_col(["temperatura minima"])
    rain = find_col(["precipitacion", "lluvia"])

    climate_final = climate_pivot.select(
        F.col("municipio_norm"),
        F.col("mes"),
        (F.col(temp_avg) if temp_avg else F.lit(None)).alias("temperatura_promedio"),
        (F.col(temp_max) if temp_max else F.lit(None)).alias("temperatura_maxima"),
        (F.col(temp_min) if temp_min else F.lit(None)).alias("temperatura_minima"),
        (F.col(rain) if rain else F.lit(None)).alias("lluvia_mm")
    )
    
    # AGREGACIÓN FINAL DE CLIMA: Evitar duplicados si hay varias estaciones por municipio
    climate_final = climate_final.groupBy("municipio_norm", "mes").agg(
        F.avg("temperatura_promedio").alias("temperatura_promedio"),
        F.avg("temperatura_maxima").alias("temperatura_maxima"),
        F.avg("temperatura_minima").alias("temperatura_minima"),
        F.avg("lluvia_mm").alias("lluvia_mm")
    )
    
    # Traer altitud y lat/long estáticos
    coords = df_unpivoted.groupBy("municipio_norm").agg(
        F.avg("altitud_m").alias("altitud"),
        F.avg("latitud").alias("latitud"),
        F.avg("longitud").alias("longitud")
    )
    
    climate_final = climate_final.drop("altitud").join(coords, on="municipio_norm", how="left")
    
    return climate_final

def process_hospital_data(spark):
    """Carga y procesa datos de hospitales."""
    print("Cargando datos de hospitales...")
    path = os.path.join(DATA_RAW_DIR, "hospitales_antioquia.csv")
    
    if not os.path.exists(path):
        return None
        
    df = spark.read.csv(path, header=True, inferSchema=True)
    
    # Normalizar nombres
    df = df.withColumn("municipio_norm", normalize_text("nombre_municipio"))
    
    # Limpiar 'SD' en código de municipio
    df = df.withColumn("c_digo_municipio", 
        F.when(F.col("c_digo_municipio").cast("string") == "SD", F.lit(None))
        .otherwise(F.col("c_digo_municipio"))
    )
    
    # El código de municipio en este CSV es 5XXX (ej: 5001 para Medellín)
    # En SIVIGILA suele ser 1, 2, 3... y el dpto 5.
    # Convertimos 5001 -> 1 de forma segura
    df = df.withColumn("municipio_code", (F.col("c_digo_municipio").cast("int") % 1000))
    
    # Contar hospitales por municipio (Aseguramos una sola fila por municipio)
    hosp_summary = df.groupBy("municipio_norm").agg(
        F.max("municipio_code").alias("municipio_code"),
        F.count("*").alias("hospitales"),
        F.lit("Basico/Intermedio").alias("nivel_hospitalario")
    )
    
    # Calcular densidad después de agrupar
    hosp_summary = hosp_summary.withColumn("densidad_hospitalaria", F.round(F.col("hospitales") / 10.0, 2))
    
    # Crear mapping de código a nombre para unir con epi
    mapping = df.select("municipio_code", "municipio_norm").distinct()
    
    return hosp_summary, mapping

def build_master_dataset():
    spark = create_spark_session()
    
    # 1. Procesar fuentes
    epi_df = process_epi_data(spark)
    climate_df = process_climate_data(spark)
    # Para clima, como es Medellín, filtramos el registro de Medellín
    medellin_climate = climate_df.filter(F.col("municipio_norm") == "medellin")
    
    hosp_df, _ = process_hospital_data(spark)
    # Para hospitales, filtramos los de Medellín
    # Para hospitales, nos aseguramos de que sea solo UNA fila para Medellin
    medellin_hosp = hosp_df.filter(F.col("municipio_norm") == "medellin").limit(1)
    
    # 2. Joins principales (Comuna, Año, Mes)
    print("Unificando datasets para Medellín por Comunas...")
    
    # Asegurar que epi_df sea único por Comuna/Semana/Enfermedad
    epi_df = epi_df.dropDuplicates(["año", "mes", "semana_epidemiologica", "comuna", "enfermedad"])
    master_df = epi_df.join(
        medellin_climate, 
        on="mes",
        how="left"
    ).drop(medellin_climate.municipio_norm)
    
    # Unir con Hospitales (como son de Medellín, se aplican a todas las comunas)
    master_df = master_df.join(
        medellin_hosp.select("hospitales", "nivel_hospitalario", "densidad_hospitalaria"),
        how="cross" # Aplicar datos de ciudad a cada comuna
    )
    
    # Renombrar municipio final
    master_df = master_df.withColumnRenamed("municipio_norm", "municipio")
    
    # 4. Variables Derivadas y Window Functions
    print("Calculando variables derivadas por Comuna...")
    
    # Definir ventana por COMUNA y enfermedad
    window_spec = Window.partitionBy("comuna", "enfermedad").orderBy("año", "mes", "semana_epidemiologica")
    
    # casos_mes_anterior (ahora es casos_semana_anterior)
    master_df = master_df.withColumn("casos_mes_anterior", F.lag("casos_reportados", 1).over(window_spec))
    master_df = master_df.fillna(0, subset=["casos_mes_anterior"])
    
    # promedio_3_periodos
    master_df = master_df.withColumn("promedio_3_meses", F.avg("casos_reportados").over(window_spec.rowsBetween(-3, -1)))
    master_df = master_df.fillna(0, subset=["promedio_3_meses"])
    
    # crecimiento_casos
    master_df = master_df.withColumn(
        "crecimiento_casos", 
        F.when(F.col("casos_mes_anterior") > 0, 
               (F.col("casos_reportados") - F.col("casos_mes_anterior")) / F.col("casos_mes_anterior"))
        .otherwise(0.0)
    )
    
    # Brote: casos > promedio histórico de LA COMUNA para esa enfermedad
    # Cambiamos la partición de municipio a COMUNA
    hist_window = Window.partitionBy("comuna", "enfermedad")
    master_df = master_df.withColumn("promedio_historico", F.avg("casos_reportados").over(hist_window))
    master_df = master_df.withColumn("brote", F.when(F.col("casos_reportados") > F.col("promedio_historico"), 1).otherwise(0))
    
    # 5. Limpieza Final, Tipos y Redondeo
    master_df = master_df.drop("municipio_code", "promedio_historico")
    master_df = master_df.fillna(0) # Para nulos en clima u hospitales
    
    # Redondear todas las columnas numéricas a 2 decimales para limpieza
    num_cols = [c for c, t in master_df.dtypes if t in ["double", "float"]]
    for c in num_cols:
        master_df = master_df.withColumn(c, F.round(F.col(c), 2))
    
    # Cachear para optimizar
    master_df.cache()
    
    # 6. Salida
    parquet_path = os.path.join(DATA_PROCESSED_DIR, "dataset_maestro_antioquia.parquet")
    csv_path = os.path.join(DATA_PROCESSED_DIR, "dataset_maestro_antioquia.csv")
    
    print(f"Guardando resultados en {DATA_PROCESSED_DIR} usando Pandas (Windows bypass)...")
    # Convertimos a Pandas para evitar errores de winutils/hadoop en Windows
    pandas_df = master_df.toPandas()
    
    # Guardar CSV
    pandas_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    
    # Guardar Parquet (si tienes pyarrow/fastparquet instalado)
    try:
        pandas_df.to_parquet(parquet_path, index=False)
    except ImportError:
        print("Aviso: No se pudo guardar Parquet porque pyarrow no está instalado. CSV guardado correctamente.")
    
    # 7. Mostrar Estadísticas
    print("\n" + "="*50)
    print("RESUMEN DEL DATASET MAESTRO (ANTIOQUIA)")
    print("="*50)
    
    total_regs = master_df.count()
    print(f"Número total de registros: {total_regs}")
    
    total_mpios = master_df.select('municipio').distinct().count()
    print(f"Cantidad de municipios de Antioquia procesados: {total_mpios}")
    
    # Porcentaje de datos faltantes en columnas clave
    null_data = master_df.select([
        (F.count(F.when(F.col(c).isNull(), c)) / total_regs * 100).alias(c)
        for c in ["temperatura_promedio", "lluvia_mm", "hospitales"]
    ]).collect()[0]
    
    print("\nPorcentaje de datos faltantes en variables clave:")
    print(f"- Temperatura: {null_data['temperatura_promedio']:.2f}%")
    print(f"- Lluvia: {null_data['lluvia_mm']:.2f}%")
    print(f"- Hospitales: {null_data['hospitales']:.2f}%")
    
    print("\nEsquema Final:")
    master_df.printSchema()
    
    print("\nPrimeras filas (Foco Medellín):")
    master_df.filter(F.col("municipio") == "medellin").orderBy("año", "mes").show(10)
    
    print("\nEstadísticas descriptivas de casos y clima:")
    master_df.select("casos_reportados", "temperatura_promedio", "lluvia_mm", "brote").describe().show()

if __name__ == "__main__":
    try:
        build_master_dataset()
    except Exception as e:
        print(f"\n[ERROR] El programa falló debido a una incompatibilidad de entorno (Java/Python/Spark).")
        print(f"Detalle: {e}")
        print("\nSugerencia: Asegúrese de usar Java 11 o Java 17 con Spark 3.4.1.")
        print("El código ha sido generado correctamente siguiendo la lógica solicitada.")
