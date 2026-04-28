import os
import sys
import typing

# Ignorar SPARK_HOME del sistema
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

os.environ["PYSPARK_PYTHON"] = sys.executable

# Parche de compatibilidad para Python 3.13 y PySpark
if sys.version_info >= (3, 13):
    sys.modules['typing.io'] = typing
    sys.modules['typing.re'] = typing

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType, StringType

# ==========================================
# CONFIGURACIÓN DE RUTAS
# ==========================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "raw")
PROCESSED_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "processed")

if not os.path.exists(PROCESSED_DATA_PATH):
    os.makedirs(PROCESSED_DATA_PATH)

print(f"📁 Proyecto: {PROJECT_ROOT}")
print(f"📂 Raw data: {RAW_DATA_PATH}")
print(f"📂 Processed: {PROCESSED_DATA_PATH}")

def get_spark_session():
    jvm_flags = (
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.text=ALL-UNNAMED "
        "--add-opens=java.sql/java.sql=ALL-UNNAMED "
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
        "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED "
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED"
    )
    os.environ['JDK_JAVA_OPTIONS'] = jvm_flags

    return SparkSession.builder \
        .appName("EpidemiologicalMasterDatasetETL") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.ansi.enabled", "false") \
        .getOrCreate()

def load_csv(spark, file_name, **read_options):
    path = os.path.join(RAW_DATA_PATH, file_name)
    if not os.path.exists(path):
        print(f"  [WARN] {file_name} no encontrado")
        return None
    try:
        df = spark.read.csv(path, header=True, inferSchema=True, **read_options)
        print(f"  [OK] {file_name}: {df.count()} filas, {len(df.columns)} columnas")
        return df
    except Exception as e:
        print(f"  [ERROR] {file_name}: {e}")
        return None

def clean_column_names(df):
    """Limpia nombres de columnas"""
    for col in df.columns:
        new_col = col.lower().strip()
        replacements = {
            'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
            'Á': 'a', 'É': 'e', 'Í': 'i', 'Ó': 'o', 'Ú': 'u',
            'ñ': 'n', 'Ñ': 'n', 'ü': 'u', 'Ü': 'u'
        }
        for old, new in replacements.items():
            new_col = new_col.replace(old, new)
        new_col = new_col.replace(" ", "_").replace("-", "_").replace(".", "_")
        new_col = new_col.replace("(", "").replace(")", "").replace("/", "_")
        if new_col != col:
            df = df.withColumnRenamed(col, new_col)
    return df

def normalize_text(col):
    c = F.upper(F.col(col))
    c = F.translate(c, "ÁÉÍÓÚÜÑ", "AEIOUUN")
    c = F.regexp_replace(c, "[^A-Z0-9 ]", " ")
    return F.trim(F.regexp_replace(c, " +", " "))

def normalize_text_str(text):
    if not text:
        return ""
    text = text.upper()
    replacements = [('Á','A'),('É','E'),('Í','I'),('Ó','O'),('Ú','U'),('Ü','U'),('Ñ','N')]
    for a, b in replacements:
        text = text.replace(a, b)
    import re
    text = re.sub(r'[^A-Z0-9 ]', ' ', text)
    text = re.sub(r' +', ' ', text).strip()
    return text

# ==========================================
# 1. CONSTRUCCIÓN DEL MAESTRO GEOGRÁFICO
# ==========================================
def build_geo_master(spark):
    print("\n[1/8] Construyendo maestro geográfico...")
    
    geo_set = set()
    
    # Extraer de vigilancia_salud_publica.csv
    df_vig = load_csv(spark, "vigilancia_salud_publica.csv")
    if df_vig is not None:
        df_vig = clean_column_names(df_vig)
        rows = df_vig.select("departamento_ocurrencia", "municipio_ocurrencia").distinct().collect()
        for row in rows:
            if row[0] and row[1]:
                geo_set.add((normalize_text_str(row[0]), normalize_text_str(row[1])))
    
    # Extraer de dengue_caqueta.csv
    df_caq = load_csv(spark, "dengue_caqueta.csv")
    if df_caq is not None:
        df_caq = clean_column_names(df_caq)
        rows = df_caq.select("departamento_reporte", "municipio_reporte").distinct().collect()
        for row in rows:
            if row[0] and row[1]:
                geo_set.add((normalize_text_str(row[0]), normalize_text_str(row[1])))
    
    # Extraer de dengue_pereira.csv
    df_per = load_csv(spark, "dengue_pereira.csv")
    if df_per is not None:
        df_per = clean_column_names(df_per)
        rows = df_per.select("departamento_de_ocurrencia", "municipio_de_ocurrencia").distinct().collect()
        for row in rows:
            if row[0] and row[1]:
                geo_set.add((normalize_text_str(row[0]), normalize_text_str(row[1])))
    
    if not geo_set:
        raise ValueError("No se pudo construir geo_master")
    
    from pyspark.sql import Row
    geo_data = [Row(departamento=d, municipio=m) for d, m in geo_set]
    geo_master = spark.createDataFrame(geo_data)
    
    print(f"  Geo master: {geo_master.count()} registros")
    geo_master.show(10, truncate=False)
    
    return geo_master

# ==========================================
# 2. PROCESAMIENTO DE ENFERMEDADES
# ==========================================
def process_disease_files(spark, geo_master):
    print("\n[2/8] Procesando archivos de enfermedades...")
    
    disease_dfs = []
    
    # 1. Procesar dengue_caqueta.csv
    print(f"\n  Procesando dengue_caqueta.csv -> DENGUE")
    df_caq = load_csv(spark, "dengue_caqueta.csv")
    if df_caq is not None:
        df_caq = clean_column_names(df_caq)
        df_caq = process_caqueta_format(df_caq, "DENGUE", geo_master)
        if df_caq is not None and df_caq.count() > 0:
            disease_dfs.append(df_caq)
    
    # 2. Procesar dengue_pereira.csv
    print(f"\n  Procesando dengue_pereira.csv -> DENGUE")
    df_per = load_csv(spark, "dengue_pereira.csv")
    if df_per is not None:
        df_per = clean_column_names(df_per)
        df_per = process_pereira_format(df_per, "DENGUE", geo_master)
        if df_per is not None and df_per.count() > 0:
            disease_dfs.append(df_per)
    
    # 3. Procesar dengue.csv
    print(f"\n  Procesando dengue.csv -> DENGUE")
    df_den = load_csv(spark, "dengue.csv")
    if df_den is not None:
        df_den = clean_column_names(df_den)
        df_den = process_dengue_format(spark, df_den, "DENGUE", geo_master)
        if df_den is not None and df_den.count() > 0:
            disease_dfs.append(df_den)
    
    # 4. Procesar chikungunya.csv
    print(f"\n  Procesando chikungunya.csv -> CHIKUNGUNYA")
    df_chik = load_csv(spark, "chikungunya.csv")
    if df_chik is not None:
        df_chik = clean_column_names(df_chik)
        df_chik = process_dengue_format(spark, df_chik, "CHIKUNGUNYA", geo_master)
        if df_chik is not None and df_chik.count() > 0:
            disease_dfs.append(df_chik)
    
    # 5. Procesar zika.csv
    print(f"\n  Procesando zika.csv -> ZIKA")
    df_zika = load_csv(spark, "zika.csv")
    if df_zika is not None:
        df_zika = clean_column_names(df_zika)
        df_zika = process_dengue_format(spark, df_zika, "ZIKA", geo_master)
        if df_zika is not None and df_zika.count() > 0:
            disease_dfs.append(df_zika)
    
    # 6. Procesar vigilancia_salud_publica.csv
    print(f"\n  Procesando vigilancia_salud_publica.csv")
    df_vig = load_csv(spark, "vigilancia_salud_publica.csv")
    if df_vig is not None:
        df_vig = clean_column_names(df_vig)
        df_vig = process_vigilance_format(df_vig, geo_master)
        if df_vig is not None:
            for disease in ["DENGUE", "CHIKUNGUNYA", "ZIKA"]:
                df_filtered = df_vig.filter(F.col("enfermedad") == disease)
                if df_filtered.count() > 0:
                    disease_dfs.append(df_filtered)
    
    return disease_dfs

def process_caqueta_format(df, disease_name, geo_master):
    """Procesa dengue_caqueta.csv"""
    
    # Seleccionar y renombrar columnas
    df = df.select(
        F.col("a_o_reporte").alias("year"),
        F.col("semana_epidemiol_gica").alias("semana"),
        F.col("departamento_reporte").alias("dept_raw"),
        F.col("municipio_reporte").alias("mpio_raw"),
        F.col("edad").cast(DoubleType()).alias("edad"),
        F.col("sexo").alias("sexo")
    )
    
    # Limpiar datos
    df = df.filter(
        (F.col("year").isNotNull()) & 
        (F.col("semana").isNotNull()) &
        (F.col("dept_raw").isNotNull()) &
        (F.col("mpio_raw").isNotNull())
    )
    
    # Normalizar geografía
    df = df.withColumn("dept_raw", normalize_text("dept_raw")) \
           .withColumn("mpio_raw", normalize_text("mpio_raw"))
    
    # Unir con geo_master
    df = df.join(geo_master, 
                (df["dept_raw"] == geo_master["departamento"]) &
                (df["mpio_raw"] == geo_master["municipio"]), 
                "inner") \
           .drop("dept_raw", "mpio_raw")
    
    if df.count() == 0:
        print(f"      ADVERTENCIA: 0 registros con geografía válida")
        return None
    
    return aggregate_by_week(df, disease_name)

def process_pereira_format(df, disease_name, geo_master):
    """Procesa dengue_pereira.csv"""
    
    # Seleccionar y renombrar columnas
    df = df.select(
        F.col("ano").alias("year"),
        F.col("semana").alias("semana"),
        F.col("departamento_de_ocurrencia").alias("dept_raw"),
        F.col("municipio_de_ocurrencia").alias("mpio_raw"),
        F.col("edad").cast(DoubleType()).alias("edad"),
        F.col("sexo").alias("sexo")
    )
    
    # Limpiar datos
    df = df.filter(
        (F.col("year").isNotNull()) & 
        (F.col("semana").isNotNull()) &
        (F.col("dept_raw").isNotNull()) &
        (F.col("mpio_raw").isNotNull())
    )
    
    # Normalizar geografía
    df = df.withColumn("dept_raw", normalize_text("dept_raw")) \
           .withColumn("mpio_raw", normalize_text("mpio_raw"))
    
    # Unir con geo_master
    df = df.join(geo_master, 
                (df["dept_raw"] == geo_master["departamento"]) &
                (df["mpio_raw"] == geo_master["municipio"]), 
                "inner") \
           .drop("dept_raw", "mpio_raw")
    
    if df.count() == 0:
        print(f"      ADVERTENCIA: 0 registros con geografía válida")
        return None
    
    return aggregate_by_week(df, disease_name)

def process_dengue_format(spark, df, disease_name, geo_master):
    """Procesa dengue.csv, chikungunya.csv, zika.csv"""
    
    # Verificar qué columnas existen
    has_hosp = "pac_hos_" in df.columns
    has_grave = "clas_dengue" in df.columns
    
    # Seleccionar columnas
    select_cols = [
        F.col("year_").alias("year"),
        F.col("semana").alias("semana"),
        F.col("cod_dpto_o").cast(StringType()).alias("cod_departamento"),
        F.col("cod_mpio_o").cast(StringType()).alias("cod_municipio"),
        F.col("edad_").cast(DoubleType()).alias("edad"),
        F.col("sexo_").alias("sexo")
    ]
    
    if has_hosp:
        select_cols.append(F.col("pac_hos_").alias("hospitalizado"))
    if has_grave:
        select_cols.append(F.col("clas_dengue").alias("grave"))
    
    df = df.select(*select_cols)
    
    # Limpiar datos
    df = df.filter(
        (F.col("year").isNotNull()) & 
        (F.col("semana").isNotNull()) &
        (F.col("cod_departamento").isNotNull()) &
        (F.col("cod_municipio").isNotNull())
    )
    
    # Crear un mapeo de códigos a nombres
    code_mapping = create_code_mapping(spark, geo_master)
    
    # Unir con el mapeo de códigos
    if code_mapping is not None and code_mapping.count() > 0:
        df = df.join(code_mapping, ["cod_departamento", "cod_municipio"], "inner")
    else:
        print(f"      ERROR: No se pudo crear mapeo de códigos")
        return None
    
    if df.count() == 0:
        print(f"      ADVERTENCIA: 0 registros con geografía válida")
        return None
    
    return aggregate_by_week(df, disease_name)

def process_vigilance_format(df, geo_master):
    """Procesa vigilancia_salud_publica.csv"""
    
    # Seleccionar columnas
    df = df.select(
        F.col("ano").alias("year"),
        F.col("semana").alias("semana"),
        F.col("nombre_evento").alias("enfermedad_raw"),
        F.col("departamento_ocurrencia").alias("dept_raw"),
        F.col("municipio_ocurrencia").alias("mpio_raw")
    )
    
    # Limpiar datos
    df = df.filter(
        (F.col("year").isNotNull()) & 
        (F.col("semana").isNotNull()) &
        (F.col("dept_raw").isNotNull()) &
        (F.col("mpio_raw").isNotNull())
    )
    
    # Normalizar geografía
    df = df.withColumn("dept_raw", normalize_text("dept_raw")) \
           .withColumn("mpio_raw", normalize_text("mpio_raw"))
    
    # Unir con geo_master
    df = df.join(geo_master, 
                (df["dept_raw"] == geo_master["departamento"]) &
                (df["mpio_raw"] == geo_master["municipio"]), 
                "inner") \
           .drop("dept_raw", "mpio_raw")
    
    # Mapear enfermedades
    df = df.withColumn("enfermedad",
        F.when(F.upper(F.col("enfermedad_raw")).contains("DENGUE"), "DENGUE")
         .when(F.upper(F.col("enfermedad_raw")).contains("CHIKUNGUNYA"), "CHIKUNGUNYA")
         .when(F.upper(F.col("enfermedad_raw")).contains("ZIKA"), "ZIKA")
         .otherwise(None)
    ).filter(F.col("enfermedad").isNotNull())
    
    df = df.drop("enfermedad_raw")
    
    # Agregar por semana (sin edad/sexo porque no están disponibles)
    df_agg = df.groupBy("year", "semana", "departamento", "municipio", "enfermedad").agg(
        F.count("*").alias("total_casos")
    ).withColumn("edad_promedio", F.lit(None).cast(DoubleType())) \
     .withColumn("porcentaje_hombres", F.lit(None).cast(DoubleType())) \
     .withColumn("porcentaje_mujeres", F.lit(None).cast(DoubleType())) \
     .withColumn("hospitalizados", F.lit(None).cast(IntegerType())) \
     .withColumn("casos_graves", F.lit(None).cast(IntegerType()))
    
    print(f"      Agregado: {df_agg.count()} registros")
    return df_agg

def create_code_mapping(spark, geo_master):
    """Crea un mapeo de códigos DANE a departamento/municipio"""
    
    code_mapping = {}
    
    # Extraer de vigilancia (tiene códigos y nombres)
    df_vig = load_csv(spark, "vigilancia_salud_publica.csv")
    if df_vig is not None:
        df_vig = clean_column_names(df_vig)
        rows = df_vig.select("cod_dpto_o", "cod_mun_o", "departamento_ocurrencia", "municipio_ocurrencia").distinct().collect()
        for row in rows:
            if row[0] and row[1] and row[2] and row[3]:
                cod_dept = str(row[0]).strip()
                cod_mpio = str(row[1]).strip()
                dept = normalize_text_str(row[2])
                mpio = normalize_text_str(row[3])
                if dept and mpio:
                    code_mapping[(cod_dept, cod_mpio)] = (dept, mpio)
    
    if not code_mapping:
        print(f"      ADVERTENCIA: No se encontró mapeo de códigos")
        return None
    
    from pyspark.sql import Row
    mapping_data = [Row(cod_departamento=k[0], cod_municipio=k[1], 
                        departamento=v[0], municipio=v[1]) 
                    for k, v in code_mapping.items()]
    mapping_df = spark.createDataFrame(mapping_data)
    
    return mapping_df

def aggregate_by_week(df, disease_name):
    """Agrega datos por semana epidemiológica"""
    
    # Convertir tipos
    df = df.withColumn("year", F.col("year").cast(IntegerType())) \
           .withColumn("semana", F.col("semana").cast(IntegerType()))
    
    # Verificar disponibilidad de datos
    has_age = "edad" in df.columns and df.filter(F.col("edad").isNotNull()).count() > 0
    has_sex = "sexo" in df.columns and df.filter(F.col("sexo").isNotNull()).count() > 0
    has_hosp = "hospitalizado" in df.columns and df.filter(F.col("hospitalizado").isNotNull()).count() > 0
    has_grave = "grave" in df.columns and df.filter(F.col("grave").isNotNull()).count() > 0
    
    # Verificar columnas geográficas
    has_dept = "departamento" in df.columns
    has_mpio = "municipio" in df.columns
    
    if not has_dept or not has_mpio:
        print(f"      ERROR: No hay columnas geográficas")
        return None
    
    # Preparar columnas
    if has_age:
        df = df.withColumn("edad_valida", 
                          F.when((F.col("edad") >= 0) & (F.col("edad") <= 120), F.col("edad"))
                          .otherwise(None))
    else:
        df = df.withColumn("edad_valida", F.lit(None).cast(DoubleType()))
    
    if has_sex:
        df = df.withColumn("h_count", F.when(F.upper(F.col("sexo")) == "M", 1).otherwise(0)) \
               .withColumn("m_count", F.when(F.upper(F.col("sexo")) == "F", 1).otherwise(0))
    else:
        df = df.withColumn("h_count", F.lit(None).cast(IntegerType())) \
               .withColumn("m_count", F.lit(None).cast(IntegerType()))
    
    if has_hosp:
        df = df.withColumn("is_hosp", F.when(F.col("hospitalizado") == 1, 1).otherwise(0))
    else:
        df = df.withColumn("is_hosp", F.lit(None).cast(IntegerType()))
    
    if has_grave:
        df = df.withColumn("is_grave", F.when(F.col("grave") == 1, 1).otherwise(0))
    else:
        df = df.withColumn("is_grave", F.lit(None).cast(IntegerType()))
    
    # Agregación
    df_agg = df.groupBy("year", "semana", "departamento", "municipio").agg(
        F.count("*").alias("total_casos"),
        F.avg("edad_valida").alias("edad_promedio"),
        F.sum("is_hosp").alias("hospitalizados"),
        F.sum("is_grave").alias("casos_graves"),
        F.sum("h_count").alias("total_hombres"),
        F.sum("m_count").alias("total_mujeres")
    )
    
    # Calcular porcentajes
    if has_sex:
        df_agg = df_agg.withColumn("porcentaje_hombres", 
                                   F.when(F.col("total_casos") > 0, 
                                         F.col("total_hombres") / F.col("total_casos"))
                                   .otherwise(None)) \
                       .withColumn("porcentaje_mujeres", 
                                   F.when(F.col("total_casos") > 0, 
                                         F.col("total_mujeres") / F.col("total_casos"))
                                   .otherwise(None))
    else:
        df_agg = df_agg.withColumn("porcentaje_hombres", F.lit(None).cast(DoubleType())) \
                       .withColumn("porcentaje_mujeres", F.lit(None).cast(DoubleType()))
    
    df_agg = df_agg.drop("total_hombres", "total_mujeres") \
                   .withColumn("enfermedad", F.lit(disease_name))
    
    print(f"      Agregado: {df_agg.count()} registros")
    return df_agg

# ==========================================
# 3. DATOS CLIMÁTICOS
# ==========================================
def process_climate_data(spark, geo_master):
    print("\n[3/8] Procesando datos climáticos...")
    
    df = load_csv(spark, "normales_climatologicas.csv")
    if df is None:
        print("  No se encontró normales_climatologicas.csv")
        return None
    
    df = clean_column_names(df)
    
    # Seleccionar y limpiar datos
    df = df.select(
        F.col("municipio").alias("mpio_raw"),
        F.col("departamento").alias("dept_raw"),
        F.col("latitud").cast(DoubleType()).alias("latitud"),
        F.col("longitud").cast(DoubleType()).alias("longitud"),
        F.col("altitud_m").cast(DoubleType()).alias("altitud"),
        F.col("ene").cast(DoubleType()).alias("ene"),
        F.col("feb").cast(DoubleType()).alias("feb"),
        F.col("mar").cast(DoubleType()).alias("mar"),
        F.col("abr").cast(DoubleType()).alias("abr"),
        F.col("may").cast(DoubleType()).alias("may"),
        F.col("jun").cast(DoubleType()).alias("jun"),
        F.col("jul").cast(DoubleType()).alias("jul"),
        F.col("ago").cast(DoubleType()).alias("ago"),
        F.col("sep").cast(DoubleType()).alias("sep"),
        F.col("oct").cast(DoubleType()).alias("oct"),
        F.col("nov").cast(DoubleType()).alias("nov"),
        F.col("dic").cast(DoubleType()).alias("dic"),
        F.col("anual").cast(DoubleType()).alias("anual")
    )
    
    # Calcular promedios anuales
    df = df.withColumn("temperatura_promedio",
        (F.coalesce(F.col("ene"), F.lit(0)) + F.coalesce(F.col("feb"), F.lit(0)) + 
         F.coalesce(F.col("mar"), F.lit(0)) + F.coalesce(F.col("abr"), F.lit(0)) +
         F.coalesce(F.col("may"), F.lit(0)) + F.coalesce(F.col("jun"), F.lit(0)) + 
         F.coalesce(F.col("jul"), F.lit(0)) + F.coalesce(F.col("ago"), F.lit(0)) +
         F.coalesce(F.col("sep"), F.lit(0)) + F.coalesce(F.col("oct"), F.lit(0)) + 
         F.coalesce(F.col("nov"), F.lit(0)) + F.coalesce(F.col("dic"), F.lit(0))) / 12
    )
    
    # Usar anual para precipitación si está disponible
    df = df.withColumn("precipitacion_promedio", F.col("anual"))
    
    # Para humedad (no disponible)
    df = df.withColumn("humedad_promedio", F.lit(None).cast(DoubleType()))
    
    # Limpiar
    df = df.filter(
        (F.col("dept_raw").isNotNull()) & 
        (F.col("mpio_raw").isNotNull())
    )
    
    # Normalizar geografía
    df = df.withColumn("dept_raw", normalize_text("dept_raw")) \
           .withColumn("mpio_raw", normalize_text("mpio_raw"))
    
    # Unir con geo_master y agregar
    df = df.join(geo_master, 
                (df["dept_raw"] == geo_master["departamento"]) &
                (df["mpio_raw"] == geo_master["municipio"]), 
                "inner") \
           .drop("dept_raw", "mpio_raw")
    
    # Agregar por municipio
    climate_df = df.groupBy("departamento", "municipio").agg(
        F.avg("latitud").alias("latitud"),
        F.avg("longitud").alias("longitud"),
        F.avg("altitud").alias("altitud"),
        F.avg("temperatura_promedio").alias("temperatura_promedio"),
        F.avg("precipitacion_promedio").alias("precipitacion_promedio"),
        F.avg("humedad_promedio").alias("humedad_promedio")
    )
    
    print(f"  Datos climáticos: {climate_df.count()} municipios")
    climate_df.show(5, truncate=False)
    
    return climate_df

# ==========================================
# 4. CALIDAD DEL AIRE
# ==========================================
def process_air_quality(spark, geo_master):
    print("\n[4/8] Procesando calidad del aire...")
    
    df = load_csv(spark, "calidad_aire_promedio_anual.csv")
    if df is None:
        print("  No se encontró calidad_aire_promedio_anual.csv")
        return None
    
    df = clean_column_names(df)
    
    # Seleccionar columnas
    df = df.select(
        F.col("a_o").alias("year"),
        F.col("nombre_del_departamento").alias("dept_raw"),
        F.col("nombre_del_municipio").alias("mpio_raw"),
        F.col("promedio").cast(DoubleType()).alias("calidad_aire_promedio")
    )
    
    # Limpiar datos
    df = df.filter(
        (F.col("year").isNotNull()) & 
        (F.col("dept_raw").isNotNull()) &
        (F.col("mpio_raw").isNotNull())
    )
    
    # Normalizar geografía
    df = df.withColumn("dept_raw", normalize_text("dept_raw")) \
           .withColumn("mpio_raw", normalize_text("mpio_raw"))
    
    # Unir con geo_master
    df = df.join(geo_master, 
                (df["dept_raw"] == geo_master["departamento"]) &
                (df["mpio_raw"] == geo_master["municipio"]), 
                "inner") \
           .drop("dept_raw", "mpio_raw")
    
    if df.count() == 0:
        print("  ADVERTENCIA: No se pudieron mapear datos de calidad del aire")
        return None
    
    result = df.select("year", "departamento", "municipio", "calidad_aire_promedio")
    print(f"  Calidad del aire: {result.count()} registros")
    
    return result

# ==========================================
# 5. VACUNACIÓN
# ==========================================
def process_vaccination(spark, geo_master):
    print("\n[5/8] Procesando vacunación...")
    
    df = load_csv(spark, "vacunacion_departamento.csv")
    if df is None:
        print("  No se encontró vacunacion_departamento.csv")
        return None
    
    df = clean_column_names(df)
    
    # Seleccionar columnas
    df = df.select(
        F.col("a_o").alias("year"),
        F.col("departamento").alias("dept_raw"),
        F.col("cobertura_de_vacunaci_n").cast(DoubleType()).alias("cobertura_vacunacion")
    )
    
    # Limpiar datos
    df = df.filter(
        (F.col("year").isNotNull()) & 
        (F.col("dept_raw").isNotNull())
    )
    
    # Normalizar geografía
    df = df.withColumn("dept_raw", normalize_text("dept_raw"))
    
    # Agregar por departamento (unir con geo_master para validar)
    depts_geo = geo_master.select("departamento").distinct()
    df = df.join(depts_geo, df["dept_raw"] == depts_geo["departamento"], "inner") \
           .drop("dept_raw")
    
    print(f"  Vacunación: {df.count()} registros")
    
    return df

# ==========================================
# 6. HOSPITALES
# ==========================================
def process_hospitals(spark, geo_master):
    print("\n[6/8] Procesando hospitales...")
    
    df = load_csv(spark, "prestadores_sedes.csv")
    if df is None:
        print("  No se encontró prestadores_sedes.csv")
        return None
    
    df = clean_column_names(df)
    
    # Seleccionar columnas
    df = df.select(
        F.col("departamentoprestadordesc").alias("dept_raw"),
        F.col("municipioprestadordesc").alias("mpio_raw")
    )
    
    # Limpiar datos
    df = df.filter(
        (F.col("dept_raw").isNotNull()) & 
        (F.col("mpio_raw").isNotNull())
    )
    
    # Normalizar geografía
    df = df.withColumn("dept_raw", normalize_text("dept_raw")) \
           .withColumn("mpio_raw", normalize_text("mpio_raw"))
    
    # Unir con geo_master
    df = df.join(geo_master, 
                (df["dept_raw"] == geo_master["departamento"]) &
                (df["mpio_raw"] == geo_master["municipio"]), 
                "inner") \
           .drop("dept_raw", "mpio_raw")
    
    # Contar hospitales por municipio
    hosp_count = df.groupBy("departamento", "municipio").agg(
        F.count("*").alias("cantidad_hospitales")
    )
    
    print(f"  Hospitales: {hosp_count.count()} municipios")
    
    return hosp_count

# ==========================================
# 7. ENSAMBLAJE DEL DATASET MAESTRO
# ==========================================
def assemble_master_dataset(spark, disease_dfs, climate_df, air_df, vax_df, hosp_df):
    print("\n[7/8] Ensamblando dataset maestro...")
    
    # Unir todas las enfermedades
    master_df = None
    for df in disease_dfs:
        if df is not None:
            master_df = df if master_df is None else master_df.unionByName(df, allowMissingColumns=True)
    
    if master_df is None:
        raise ValueError("No se procesaron enfermedades")
    
    print(f"  Enfermedades base: {master_df.count()} registros")
    
    # Crear dimensión de tiempo
    years = master_df.select("year").distinct()
    weeks = spark.createDataFrame([(i,) for i in range(1, 53)], ["semana"])
    time_dim = years.crossJoin(weeks)
    
    # Unir clima
    if climate_df is not None and climate_df.count() > 0:
        climate_expanded = climate_df.crossJoin(time_dim)
        master_df = master_df.join(climate_expanded, 
                                  ["year", "semana", "departamento", "municipio"], 
                                  "left")
    else:
        for col in ["temperatura_promedio", "precipitacion_promedio", "humedad_promedio", 
                    "latitud", "longitud", "altitud"]:
            if col not in master_df.columns:
                master_df = master_df.withColumn(col, F.lit(None).cast(DoubleType()))
    
    # Unir calidad del aire
    if air_df is not None and air_df.count() > 0:
        air_expanded = air_df.crossJoin(weeks)
        master_df = master_df.join(air_expanded, 
                                  ["year", "semana", "departamento", "municipio"], 
                                  "left")
    else:
        if "calidad_aire_promedio" not in master_df.columns:
            master_df = master_df.withColumn("calidad_aire_promedio", F.lit(None).cast(DoubleType()))
    
    # Unir vacunación
    if vax_df is not None and vax_df.count() > 0:
        vax_expanded = vax_df.crossJoin(weeks)
        master_df = master_df.join(vax_expanded, 
                                  ["year", "semana", "departamento"], 
                                  "left")
    else:
        if "cobertura_vacunacion" not in master_df.columns:
            master_df = master_df.withColumn("cobertura_vacunacion", F.lit(None).cast(DoubleType()))
    
    # Unir hospitales
    if hosp_df is not None and hosp_df.count() > 0:
        master_df = master_df.join(hosp_df, ["departamento", "municipio"], "left")
    else:
        if "cantidad_hospitales" not in master_df.columns:
            master_df = master_df.withColumn("cantidad_hospitales", F.lit(None).cast(IntegerType()))
    
    # Solo total_casos debe tener 0 como valor por defecto
    master_df = master_df.fillna(0, subset=["total_casos"])
    
    # ==========================================
    # 8. VARIABLES HISTÓRICAS
    # ==========================================
    master_df = master_df.orderBy("departamento", "municipio", "enfermedad", "year", "semana")
    
    win_lag = Window.partitionBy("departamento", "municipio", "enfermedad").orderBy("year", "semana")
    win_stats = Window.partitionBy("departamento", "municipio", "enfermedad").orderBy("year", "semana").rowsBetween(-8, -1)
    
    master_df = master_df.withColumn("row_num", F.row_number().over(win_lag))
    
    # Históricos
    master_df = master_df \
        .withColumn("historico_casos_4_semanas", 
                   F.when(F.col("row_num") > 4, F.lag("total_casos", 4).over(win_lag)).otherwise(None)) \
        .withColumn("historico_casos_8_semanas",
                   F.when(F.col("row_num") > 8, F.lag("total_casos", 8).over(win_lag)).otherwise(None)) \
        .withColumn("historico_casos_12_semanas",
                   F.when(F.col("row_num") > 12, F.lag("total_casos", 12).over(win_lag)).otherwise(None)) \
        .withColumn("promedio_historico_8",
                   F.when(F.col("row_num") >= 4, F.avg("total_casos").over(win_stats)).otherwise(None)) \
        .withColumn("stddev_historico_8",
                   F.when(F.col("row_num") >= 4, F.stddev("total_casos").over(win_stats)).otherwise(None)) \
        .withColumn("brote",
                   F.when((F.col("row_num") >= 4) & 
                         (F.col("promedio_historico_8").isNotNull()) &
                         (F.col("total_casos") > F.col("promedio_historico_8") + F.coalesce(F.col("stddev_historico_8"), F.lit(0))), 
                         1).otherwise(0))
    
    master_df = master_df.drop("row_num", "stddev_historico_8")
    
    # Llave de integración
    master_df = master_df.withColumn("llave_integracion",
                                     F.concat_ws("_", F.col("year"), F.col("semana"), 
                                                F.col("departamento"), F.col("municipio")))
    
    # Seleccionar y ordenar columnas
    final_columns = [
        "year", "semana", "departamento", "municipio", "enfermedad",
        "total_casos", "edad_promedio", "porcentaje_hombres", "porcentaje_mujeres",
        "hospitalizados", "casos_graves", "calidad_aire_promedio",
        "temperatura_promedio", "precipitacion_promedio", "humedad_promedio",
        "cobertura_vacunacion", "cantidad_hospitales",
        "latitud", "longitud", "altitud",
        "historico_casos_4_semanas", "historico_casos_8_semanas", "historico_casos_12_semanas",
        "brote", "llave_integracion"
    ]
    
    # Asegurar que todas las columnas existan
    for col in final_columns:
        if col not in master_df.columns:
            master_df = master_df.withColumn(col, F.lit(None))
    
    master_df = master_df.select(*final_columns)
    
    print(f"  Dataset final: {master_df.count()} registros, {len(master_df.columns)} columnas")
    
    return master_df

# ==========================================
# 8. VALIDACIÓN
# ==========================================
def validate_master_dataset(df):
    print("\n[8/8] Validando dataset maestro...")
    
    total = df.count()
    print(f"  Total registros: {total}")
    
    print("\n  --- Columnas del dataset ---")
    for col in df.columns:
        non_null = df.where(F.col(col).isNotNull()).count()
        pct = (non_null / total) * 100 if total > 0 else 0
        print(f"    {col:30}: {pct:5.1f}% no nulos")
    
    print("\n  --- Distribución por enfermedad ---")
    df.groupBy("enfermedad").count().show()
    
    print("\n  --- Rango de años ---")
    df.select(F.min("year"), F.max("year")).show()
    
    print("\n  --- Muestra de datos ---")
    df.show(10, truncate=False)

# ==========================================
# PIPELINE PRINCIPAL
# ==========================================
def run_pipeline():
    spark = get_spark_session()
    print("="*60)
    print("PIPELINE EPIDEMIOLÓGICO - DATASET MAESTRO")
    print("="*60)
    
    try:
        # 1. Construir geo_master
        geo_master = build_geo_master(spark)
        
        # 2. Procesar enfermedades
        disease_dfs = process_disease_files(spark, geo_master)
        
        # 3. Procesar clima
        climate_df = process_climate_data(spark, geo_master)
        
        # 4. Procesar calidad del aire
        air_df = process_air_quality(spark, geo_master)
        
        # 5. Procesar vacunación
        vax_df = process_vaccination(spark, geo_master)
        
        # 6. Procesar hospitales
        hosp_df = process_hospitals(spark, geo_master)
        
        # 7. Ensamblar dataset maestro
        master_df = assemble_master_dataset(spark, disease_dfs, climate_df, air_df, vax_df, hosp_df)
        
        # 8. Validar
        validate_master_dataset(master_df)
        
        # 9. Guardar
        output_path = os.path.join(PROCESSED_DATA_PATH, "master_dataset.csv")
        master_df.toPandas().to_csv(output_path, index=False, na_rep='')
        print(f"\n  ✅ Dataset guardado: {output_path}")
        
    except Exception as e:
        print(f"\n  ❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("\n  🏁 Pipeline completado")

if __name__ == "__main__":
    run_pipeline()