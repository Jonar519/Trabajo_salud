import os
import sys
import re
import json
import argparse
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from pathlib import Path

SparkSession = None
F = None
Window = None
_PYSPARK_IMPORT_ERROR = None

def _check_runtime():
    exe = Path(sys.executable)
    if not exe.exists():
        raise RuntimeError(
            "No se encontró el ejecutable de Python que está usando este proceso:\n"
            f"  sys.executable = {sys.executable}\n"
            "Esto suele pasar por un entorno virtual roto o activado a medias.\n"
            "Solución recomendada (en la raíz del proyecto):\n"
            "  1) borrar .venv\n"
            "  2) python -m venv .venv\n"
            "  3) activar .venv y ejecutar: pip install -r requirements.txt"
        )

    try:
        p = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as e:
        raise RuntimeError(
            "No se pudo ejecutar 'java -version'. Instala Java y configura JAVA_HOME.\n"
            f"Detalle: {e}"
        )

    version_output = (p.stderr or "") + "\n" + (p.stdout or "")
    m = re.search(r'version\s+"(\d+)(?:\.(\d+))?', version_output)
    major = int(m.group(1)) if m else None

    if major is None:
        raise RuntimeError(
            "No se pudo detectar la versión de Java desde 'java -version'.\n"
            f"Salida:\n{version_output.strip()}"
        )

    if major >= 18:
        raise RuntimeError(
            "Tu Java es demasiado nuevo para PySpark/Spark 3.4.1 en Windows.\n"
            f"Detectado: Java {major}\n"
            "Instala Java 17 (recomendado) o Java 11 y asegúrate de que sea el que se usa en consola.\n"
            "Luego vuelve a ejecutar el pipeline."
        )

def _load_pyspark():
    global SparkSession, F, Window, _PYSPARK_IMPORT_ERROR
    if SparkSession is not None and F is not None and Window is not None:
        return

    _PYSPARK_IMPORT_ERROR = None

    spark_home = os.environ.get("SPARK_HOME")
    if spark_home:
        os.environ.pop("SPARK_HOME", None)

    try:
        from pyspark.sql import SparkSession as _SparkSession
        from pyspark.sql import functions as _F
        from pyspark.sql.window import Window as _Window
        SparkSession = _SparkSession
        F = _F
        Window = _Window
    except Exception as e:
        SparkSession = None
        F = None
        Window = None
        _PYSPARK_IMPORT_ERROR = e


# ─────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────

def create_spark_session():
    _require_pyspark()
    _check_runtime()
    """
    Initializes and returns a SparkSession tuned for local-mode execution
    on a single Windows machine.

    Key settings
    ──────────────────────────────────────────────────────────────────────
    driver.memory / memoryFraction
        Without explicit limits the JVM defaults to 1 g, which is too small
        for multi-join pipelines with ~230 k base rows + expansions.

    shuffle.partitions = 4
        Lower than the default (200) so each shuffle write fits in memory
        and doesn't overflow to disk excessively.

    maxResultSize = 0
        Removes the 1 GB cap on results collected to the driver (needed for
        the coalesce(1) CSV write that materialises everything on the driver).

    adaptiveQueryExecution = false
        AQE's dynamic partition coalescing triggers many extra broadcast joins
        in local mode, which amplifies the "Cannot find endpoint" RPC crashes
        seen when the driver's BlockManager port is unexpectedly reassigned
        mid-job on Windows.

    localDir
        Windows TEMP paths can have spaces, which confuse Hadoop's file APIs.
        Pointing to a short path under the project avoids those failures.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Short temp dir: avoid Windows paths with spaces breaking Hadoop I/O
    tmp_dir = str(Path(__file__).resolve().parent.parent / "spark_tmp")
    Path(tmp_dir).mkdir(parents=True, exist_ok=True)

    return (
        SparkSession.builder
        .appName("Epidemiological Master Dataset Pipeline")
        .master("local[2]")                              # 2 threads; more threads → more RPC endpoints that can die
        .config("spark.driver.memory",                  "4g")
        .config("spark.executor.memory",                "4g")
        .config("spark.memory.fraction",                "0.8")
        .config("spark.memory.storageFraction",         "0.3")
        .config("spark.driver.maxResultSize",           "2g")
        .config("spark.sql.shuffle.partitions",         "4")
        .config("spark.sql.adaptive.enabled",           "false")   # AQE causes extra RPC in local mode
        .config("spark.local.dir",                      tmp_dir)
        .config("spark.network.timeout",                "600s")
        .config("spark.executor.heartbeatInterval",     "60s")
        .config("spark.sql.broadcastTimeout",           "300")
        .getOrCreate()
    )

def _require_pyspark():
    _load_pyspark()
    if SparkSession is None or F is None or Window is None:
        raise RuntimeError(
            "PySpark no está disponible en este entorno. "
            "Instala dependencias (requirements.txt) y asegúrate de tener Java configurado. "
            f"Detalle: {_PYSPARK_IMPORT_ERROR}"
        )


# ─────────────────────────────────────────────
# SHARED UTILITIES
# ─────────────────────────────────────────────

def normalize_column_names(df):
    """
    Normalizes all column headers:
      - strip whitespace
      - lowercase
      - replace accented chars (á→a, é→e, í→i, ó→o, ú→u, ñ→n)
      - replace spaces with underscores
      - remove any remaining non-alphanumeric/underscore characters
    """
    accents = "áéíóúñ"
    replacements = "aeioun"

    new_cols = []
    for col_name in df.columns:
        name = col_name.strip().lower()
        for src, dst in zip(accents, replacements):
            name = name.replace(src, dst)
        name = name.replace(" ", "_")
        name = re.sub(r"[^a-z0-9_]", "", name)
        new_cols.append(name)

    return df.toDF(*new_cols)


def normalize_text(df, columns):
    """
    Standardizes free-text values in the given columns:
      - trim leading/trailing whitespace
      - uppercase
      - replace accented vowels and common mangled characters (┴, ═, Ë, etc.)
      - collapse multiple spaces into one
    """
    # Mapping of mangled/accented characters to their clean counterparts
    # Based on observation of 'calidad_aire_promedio_anual.csv' and 'normales_climatologicas.csv'
    char_map = {
        "Á": "A", "É": "E", "Í": "I", "Ó": "O", "Ú": "U", "Ñ": "N",
        "┴": "A", "═": "I", "Ë": "O", "Ð": "N", "Ý": "I", "¾": "O",
        "ß": "A", "Ú": "U", "×": "O", "¡": "A", "¢": "E", "£": "I", "¤": "O"
    }
    
    src_chars = "".join(char_map.keys())
    dst_chars = "".join(char_map.values())

    for col in columns:
        # Initial clean: upper, trim, and basic translation
        df = df.withColumn(
            col,
            F.upper(F.trim(F.translate(F.col(col), src_chars, dst_chars)))
        )
        # Clean common noise in municipality/dept names from surveillance sources
        df = df.withColumn(col, F.regexp_replace(F.col(col), r"^\*\s*", ""))
        df = df.withColumn(col, F.regexp_replace(F.col(col), r"\.\s*MUNICIPIO\s*DESCONOCIDO", ""))
        
        # Handle common name variations/misspellings
        df = df.withColumn(col, F.regexp_replace(F.col(col), r"BOGOTA\s*,?\s*D\.?\s*C\.?", "BOGOTA"))
        df = df.withColumn(col, F.regexp_replace(F.col(col), r"SANTA MARTHA", "SANTA MARTA"))
        df = df.withColumn(col, F.regexp_replace(F.col(col), r"CARTAGENA DE INDIAS", "CARTAGENA"))
        
        # Collapse spaces and final trim
        df = df.withColumn(col, F.regexp_replace(F.col(col), r"\s+", " "))
        df = df.withColumn(col, F.trim(F.col(col)))

    return df


def get_geographic_mapping(spark, df_target, df_reference, max_dist_km=50):
    """
    Creates a mapping from target municipalities to their nearest reference municipality
    within a maximum distance.
    """
    # Get unique locations from reference
    ref_locs = df_reference.select(
        F.col("departamento").alias("ref_dept"),
        F.col("municipio").alias("ref_muni"),
        F.col("latitud").alias("ref_lat"),
        F.col("longitud").alias("ref_lon")
    ).distinct().filter(F.col("ref_lat").isNotNull() & F.col("ref_lon").isNotNull())

    # Get unique locations from target that need mapping
    target_locs = df_target.select(
        F.col("departamento").alias("target_dept"),
        F.col("municipio").alias("target_muni"),
        F.col("latitud").alias("target_lat"),
        F.col("longitud").alias("target_lon")
    ).distinct().filter(F.col("target_lat").isNotNull() & F.col("target_lon").isNotNull())

    # Cross join and calculate distance
    # R = 6371 km
    distance_expr = """
        2 * 6371 * asin(sqrt(
            pow(sin(radians(ref_lat - target_lat) / 2), 2) +
            cos(radians(target_lat)) * cos(radians(ref_lat)) * pow(sin(radians(ref_lon - target_lon) / 2), 2)
        ))
    """
    
    mapping = target_locs.crossJoin(ref_locs) \
        .withColumn("dist_km", F.expr(distance_expr)) \
        .filter(F.col("dist_km") <= max_dist_km)
    
    # Keep only the nearest neighbor for each target
    window_spec = Window.partitionBy("target_dept", "target_muni").orderBy("dist_km")
    
    mapping = mapping.withColumn("row_num", F.row_number().over(window_spec)) \
        .filter(F.col("row_num") == 1) \
        .select(
            "target_dept", "target_muni",
            "ref_dept", "ref_muni", "dist_km"
        )
        
    return mapping


# Vigilancia uses abbreviated department names; clima and other sources use
# the official full names. This map harmonizes them before any join.
_DEPT_HARMONIZE = {
    "VALLE":           "VALLE DEL CAUCA",
    "NORTE SANTANDER": "NORTE DE SANTANDER",
    "BOGOTA":          "BOGOTA D.C.",
    "GUAJIRA":         "LA GUAJIRA",
    "SAN ANDRES":      "SAN ANDRES, PROVIDENCIA Y SANTA CATALINA",
}

def harmonize_departamentos(df, col="departamento"):
    """
    Replaces abbreviated departamento names used in vigilancia_salud_publica
    with the official full names used in the climate and other datasets,
    so that join keys match across all sources.

    Mapping applied (post-normalization, so all values are already uppercase):
      VALLE           → VALLE DEL CAUCA
      NORTE SANTANDER → NORTE DE SANTANDER
      BOGOTA          → BOGOTA D.C.
      GUAJIRA         → LA GUAJIRA
      SAN ANDRES      → SAN ANDRES, PROVIDENCIA Y SANTA CATALINA
    """
    expr = F.col(col)
    for short, full in _DEPT_HARMONIZE.items():
        expr = F.when(F.col(col) == short, full).otherwise(expr)
    return df.withColumn(col, expr)


def expand_to_weeks(df, year_col="ano"):
    """Replicates each row 52 times, adding a 'semana' column (1–52)."""
    weeks = F.array([F.lit(w) for w in range(1, 53)])
    return df.withColumn("semana", F.explode(weeks))


def validate_dataframe(df, name, key_cols):
    """
    Prints a quick quality report for a dataframe:
      - row count
      - null count per key column
      - duplicate count on key columns
    """
    print(f"\n{'='*60}")
    print(f"  VALIDATION: {name}")
    print(f"{'='*60}")
    total = df.count()
    print(f"  Rows total : {total:,}")

    for c in key_cols:
        if c in df.columns:
            nulls = df.filter(F.col(c).isNull()).count()
            print(f"  Nulls [{c}] : {nulls:,}")

    dupes = total - df.dropDuplicates(key_cols).count()
    print(f"  Duplicates on {key_cols}: {dupes:,}")
    print(f"{'='*60}\n")


# ─────────────────────────────────────────────
# DATASET PROCESSORS
# ─────────────────────────────────────────────

def process_vigilancia(spark, path):
    _require_pyspark()
    """
    Source  : vigilancia_salud_publica.csv
    Granularity: year + week + municipality + disease
    Output columns:
        ano, semana, departamento, municipio, enfermedad, casos_totales
    """
    TARGET_DISEASES = ["DENGUE", "ZIKA", "CHIKUNGUNYA"]

    df = spark.read.csv(path, header=True, inferSchema=True)
    df = normalize_column_names(df)

    # Normalize geographic/disease text before filtering
    df = normalize_text(df, ["departamento_ocurrencia", "municipio_ocurrencia", "nombre_evento"])

    # Keep only the three target diseases
    df = df.filter(F.col("nombre_evento").isin(TARGET_DISEASES))

    df = df.select(
        F.col("ano").cast("int"),
        F.col("semana").cast("int"),
        F.col("departamento_ocurrencia").alias("departamento"),
        F.col("municipio_ocurrencia").alias("municipio"),
        F.col("nombre_evento").alias("enfermedad"),
        F.col("conteo").cast("int").alias("casos_totales"),
    )

    # Aggregate to remove any source duplicates
    df = (
        df.groupBy("ano", "semana", "departamento", "municipio", "enfermedad")
        .agg(F.sum("casos_totales").alias("casos_totales"))
    )

    # Harmonize abbreviated dept names → official names used by other datasets
    df = harmonize_departamentos(df, "departamento")

    validate_dataframe(df, "vigilancia_salud_publica", ["ano", "semana", "departamento", "municipio", "enfermedad"])
    return df


def process_clima(spark, path):
    _require_pyspark()
    """
    Source  : normales_climatologicas.csv
    Granularity: week + municipality (Averaged across all historical periods 'ao')
    Month columns (ene–dic) are expanded to weeks (1–52).
    par_metro values are pivoted into:
        temperatura_promedio, precipitacion_promedio, humedad_promedio
    Also returns latitud / longitud.
    """
    MONTH_COLS = ["ene", "feb", "mar", "abr", "may", "jun",
                  "jul", "ago", "sep", "oct", "nov", "dic"]

    # Month → week ranges: jan=1-4, feb=5-8, ..., dec=45-52
    week_map = []
    for idx, month in enumerate(MONTH_COLS):
        start = idx * 4 + 1
        end = (idx + 1) * 4 if month != "dic" else 52
        for w in range(start, end + 1):
            week_map.append((month, w))

    df = spark.read.csv(path, header=True, inferSchema=True)
    df = normalize_column_names(df)

    # After normalize_column_names: 'ao' stays 'ao', 'par_metro' stays 'par_metro'
    df = normalize_text(df, ["departamento", "municipio", "par_metro"])

    # Melt month columns → long format
    stack_expr = (
        "stack(12, "
        + ", ".join([f"'{m}', {m}" for m in MONTH_COLS])
        + ") as (month_name, valor_clima)"
    )
    df_long = df.select(
        "departamento", "municipio", "par_metro", "latitud", "longitud",
        F.expr(stack_expr),
    )

    # Join with week mapping
    mapping_df = spark.createDataFrame(week_map, ["month_name", "semana"])
    df_weekly = df_long.join(mapping_df, on="month_name").drop("month_name")

    # Standardize par_metro values to target column names
    df_weekly = df_weekly.withColumn(
        "par_metro",
        F.when(F.col("par_metro").contains("TEMPERATURA MEDIA"), "temperatura_promedio")
         .when(F.col("par_metro").contains("PRECIPITACION"), "precipitacion_promedio")
         .otherwise(None),  # discard irrelevant parameters
    ).filter(F.col("par_metro").isNotNull())

    # Aggregate before pivoting to avoid issues with repeated measurements per station
    # We ignore 'ao' here to get a general climatological normal for each location
    df_agg = (
        df_weekly.groupBy("semana", "departamento", "municipio", "par_metro")
        .agg(
            F.avg("valor_clima").alias("valor_clima"),
            F.avg("latitud").alias("latitud"),
            F.avg("longitud").alias("longitud"),
        )
    )

    # Pivot: one row per (week, dept, municipality)
    df_pivoted = (
        df_agg.groupBy("semana", "departamento", "municipio")
        .pivot("par_metro", ["temperatura_promedio", "precipitacion_promedio"])
        .agg(F.avg("valor_clima"))
    )

    # Average coordinates per location-week
    df_coords = (
        df_agg.groupBy("semana", "departamento", "municipio")
        .agg(
            F.avg("latitud").alias("latitud"),
            F.avg("longitud").alias("longitud"),
        )
    )

    df_final = df_pivoted.join(df_coords, on=["semana", "departamento", "municipio"])

    validate_dataframe(df_final, "normales_climatologicas", ["semana", "departamento", "municipio"])
    return df_final


def process_calidad_aire(spark, path):
    _require_pyspark()
    """
    Source  : calidad_aire_promedio_anual.csv
    Granularity: annual → expanded to 52 weeks
    Filters for air quality pollutants (PM2.5, PM10, O3, NO2, SO2, CO)
    to calculate the 'calidad_aire_promedio'.
    Also extracts coordinates and temperature.
    Output columns:
        ano, semana, departamento, municipio, calidad_aire_promedio, latitud, longitud
    """
    # Standard pollutants that represent "air quality"
    POLLUTANTS  = ["PM2.5", "PM10", "O3", "NO2", "SO2", "CO", "PST"]
    TEMPERATURE = ["TAire", "TAire10", "TAire2"]

    df = spark.read.csv(path, header=True, inferSchema=True)
    df = normalize_column_names(df)

    # After normalize_column_names:
    #   'nombre_del_departamento' → 'nombre_del_departamento'
    #   'nombre_del_municipio'    → 'nombre_del_municipio'
    #   'a_o'                     → 'a_o'
    #   'variable'                → 'variable'
    df = (
        df.withColumnRenamed("nombre_del_departamento", "departamento")
          .withColumnRenamed("nombre_del_municipio", "municipio")
          .withColumnRenamed("a_o", "ano")
    )

    df = normalize_text(df, ["departamento", "municipio"])

    # Pivot variables to get air quality and temperature
    df_filtered = df.filter(F.col("variable").isin(POLLUTANTS + TEMPERATURE))
    
    # Map variables to target groups
    df_filtered = df_filtered.withColumn(
        "target_var",
        F.when(F.col("variable").isin(POLLUTANTS), "calidad_aire_promedio")
         .when(F.col("variable").isin(TEMPERATURE), "temperatura_promedio")
         .otherwise(None)
    )

    df_agg = (
        df_filtered.groupBy("ano", "departamento", "municipio")
        .agg(
            F.avg(F.when(F.col("target_var") == "calidad_aire_promedio", F.col("promedio"))).alias("calidad_aire_promedio"),
            F.avg(F.when(F.col("target_var") == "temperatura_promedio", F.col("promedio"))).alias("temperatura_promedio_aire"),
            F.avg("latitud").alias("latitud_aire"),
            F.avg("longitud").alias("longitud_aire")
        )
    )

    # Expand annual record to 52 weeks
    df_weekly = expand_to_weeks(df_agg, "ano")

    validate_dataframe(df_weekly, "calidad_aire_promedio_anual", ["ano", "semana", "departamento", "municipio"])
    return df_weekly


# ── vacunacion: intentionally not processed ───────────────────────────────────
#
# vacunacion_departamento.csv is NOT used as a predictive variable.
#
# Reason: Dengue, Zika, and Chikungunya have no consistent national vaccination
# programme in Colombia for the studied period. Ingesting partial or proxy
# coverage figures would introduce noise and bias into the model.
#
# Decision: the 'vacunacion' column is kept in the final schema with the
# constant value "NO_REPORTA" for all rows. This preserves schema stability
# and allows future versions to slot in validated data without restructuring
# the pipeline.
# ──────────────────────────────────────────────────────────────────────────────


def process_prestadores(spark, path):
    _require_pyspark()
    """
    Source  : prestadores_sedes.csv
    Granularity: static (no year/week) — joined only on (departamento, municipio)
    Output columns:
        departamento, municipio, cantidad_hospitales
    """
    df = spark.read.csv(path, header=True, inferSchema=True)
    df = normalize_column_names(df)

    df = (
        df.withColumnRenamed("departamentoprestadordesc", "departamento")
          .withColumnRenamed("municipioprestadordesc", "municipio")
    )

    df = normalize_text(df, ["departamento", "municipio"])

    df_agg = (
        df.groupBy("departamento", "municipio")
        .agg(F.count("nombreprestador").alias("cantidad_hospitales"))
    )

    validate_dataframe(df_agg, "prestadores_sedes", ["departamento", "municipio"])
    return df_agg


# ─────────────────────────────────────────────
# OUTBREAK LABEL
# ─────────────────────────────────────────────

def create_brote_column(master_df, df_vsp):
    _require_pyspark()
    """
    Creates the binary target column 'brote':
      brote = 1  if casos_totales > p75 (historic, per disease + municipality)
      brote = 0  otherwise

    The percentile is computed on the base surveillance dataset to avoid
    leakage from the joined dataframe.
    """
    percentiles_df = (
        df_vsp.groupBy("enfermedad", "municipio")
        .agg(F.percentile_approx("casos_totales", 0.75).alias("percentil_75"))
    )

    master_df = master_df.join(percentiles_df, on=["enfermedad", "municipio"], how="left")
    master_df = master_df.fillna({"casos_totales": 0, "percentil_75": 0})

    master_df = master_df.withColumn(
        "brote",
        F.when(F.col("casos_totales") > F.col("percentil_75"), "SI").otherwise("NO"),
    ).drop("percentil_75")

    return master_df


# ─────────────────────────────────────────────
# DATA QUALITY FILTERS
# ─────────────────────────────────────────────

def apply_sanity_filters(df):
    _require_pyspark()
    """
    Removes rows with values that are physically impossible or highly 
    unlikely for the Colombian context (outlier cleaning).
    """
    # 1. Temperature: Colombia's municipality averages stay between 0 and 45°C
    df = df.filter((F.col("temperatura_promedio") >= 0) & (F.col("temperatura_promedio") <= 45))
    
    # 2. Precipitation: Cannot be negative, and monthly averages > 1200mm are extreme errors
    df = df.filter((F.col("precipitacion_promedio") >= 0) & (F.col("precipitacion_promedio") <= 1200))
    
    # 3. Air Quality: Concentrations (PM10/PM2.5) or indices shouldn't be negative 
    # and annual averages > 500 are usually sensor malfunctions.
    df = df.filter((F.col("calidad_aire_promedio") >= 0) & (F.col("calidad_aire_promedio") <= 500))
    
    # 4. Coordinates: Must be within Colombia's bounding box
    # Lat: ~ -4.2 to 13.5 | Long: ~ -79.0 to -66.0
    df = df.filter((F.col("latitud") >= -5) & (F.col("latitud") <= 15))
    df = df.filter((F.col("longitud") >= -82) & (F.col("longitud") <= -65))
    
    # 5. Epidemiological: Cases cannot be negative
    df = df.filter(F.col("casos_totales") >= 0)
    
    return df


# ─────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────

def main():
    _require_pyspark()
    spark = create_spark_session()

    # Build absolute paths relative to this script's location so the pipeline
    # works correctly regardless of the working directory used to launch it.
    #
    # Layout expected:
    #   Trabajo_salud/
    #     src/
    #       etl_pipeline.py   ← __file__
    #     data/
    #       raw/              ← CSVs go here
    #       processed/        ← output goes here
    #
    SRC_DIR       = Path(__file__).resolve().parent          # …/src
    PROJECT_ROOT  = SRC_DIR.parent                           # …/Trabajo_salud
    base_path     = str(PROJECT_ROOT / "data" / "raw") + "/"
    output_dir    = PROJECT_ROOT / "data" / "processed"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"  Project root : {PROJECT_ROOT}")
    print(f"  Raw data     : {base_path}")
    print(f"  Output dir   : {output_dir}\n")

    print("\n" + "=" * 60)
    print("  EPIDEMIOLOGICAL MASTER DATASET PIPELINE")
    print("=" * 60 + "\n")

    # ── 1. Read and transform each source ──────────────────────────
    print("Step 1/5 — Processing individual datasets...\n")

    df_vsp   = process_vigilancia(spark, base_path + "vigilancia_salud_publica.csv")
    df_clima = process_clima(spark, base_path + "normales_climatologicas.csv")
    df_aire  = process_calidad_aire(spark, base_path + "calidad_aire_promedio_anual.csv")
    df_hosp  = process_prestadores(spark, base_path + "prestadores_sedes.csv")

    # ── 2. Build master dataset through sequential joins ───────────
    print("Step 2/5 — Joining datasets with geographic imputation...\n")

    # Get base coordinates for all municipalities to enable distance calculation
    # We use clima as primary coordinate source, then air quality
    df_geo_ref = df_clima.select("departamento", "municipio", "latitud", "longitud").distinct() \
        .union(df_aire.select("departamento", "municipio", 
                              F.col("latitud_aire").alias("latitud"), 
                              F.col("longitud_aire").alias("longitud")).distinct()) \
        .groupBy("departamento", "municipio") \
        .agg(F.avg("latitud").alias("latitud"), F.avg("longitud").alias("longitud"))

    # Map surveillance municipalities to their available coordinates
    df_vsp_munis = df_vsp.select("departamento", "municipio").distinct()
    df_vsp_geo = df_vsp_munis.join(df_geo_ref, on=["departamento", "municipio"], how="left")

    # Create mapping for Clima neighbors (max 50km)
    print("     Finding nearest neighbors for climate data...")
    mapping_clima = get_geographic_mapping(spark, df_vsp_geo, df_clima)
    
    # Create mapping for Air Quality neighbors (max 50km)
    print("     Finding nearest neighbors for air quality data...")
    df_aire_geo = df_aire.select(
        "departamento", "municipio", 
        F.col("latitud_aire").alias("latitud"), 
        F.col("longitud_aire").alias("longitud")
    ).distinct()
    mapping_aire = get_geographic_mapping(spark, df_vsp_geo, df_aire_geo)

    # Base: one row per (año, semana, departamento, municipio, enfermedad)
    master_df = df_vsp

    # Join with climate data using geographic mapping
    master_df = master_df.join(mapping_clima.select(
        F.col("target_dept").alias("departamento"),
        F.col("target_muni").alias("municipio"),
        F.col("ref_dept").alias("clima_dept"),
        F.col("ref_muni").alias("clima_muni")
    ), on=["departamento", "municipio"], how="left")
    
    # Fallback to original name if no neighbor found within 50km
    master_df = master_df.withColumn("clima_dept", F.coalesce(F.col("clima_dept"), F.col("departamento"))) \
                         .withColumn("clima_muni", F.coalesce(F.col("clima_muni"), F.col("municipio")))

    master_df = master_df.join(
        df_clima.withColumnRenamed("departamento", "clima_dept")
                .withColumnRenamed("municipio", "clima_muni")
                .withColumnRenamed("latitud", "latitud_clima")
                .withColumnRenamed("longitud", "longitud_clima"),
        on=["semana", "clima_dept", "clima_muni"], how="left"
    ).drop("clima_dept", "clima_muni")

    # Join with air quality data using geographic mapping
    master_df = master_df.join(mapping_aire.select(
        F.col("target_dept").alias("departamento"),
        F.col("target_muni").alias("municipio"),
        F.col("ref_dept").alias("aire_dept"),
        F.col("ref_muni").alias("aire_muni")
    ), on=["departamento", "municipio"], how="left")

    master_df = master_df.withColumn("aire_dept", F.coalesce(F.col("aire_dept"), F.col("departamento"))) \
                         .withColumn("aire_muni", F.coalesce(F.col("aire_muni"), F.col("municipio")))

    master_df = master_df.join(
        df_aire.withColumnRenamed("departamento", "aire_dept")
               .withColumnRenamed("municipio", "aire_muni"),
        on=["ano", "semana", "aire_dept", "aire_muni"], how="left"
    ).drop("aire_dept", "aire_muni")

    # Coalesce metrics and coordinates from both sources
    # Note: we also join with df_geo_ref to keep the ORIGINAL coordinates of the target municipality
    master_df = master_df.join(df_geo_ref.withColumnRenamed("latitud", "latitud_original")
                                        .withColumnRenamed("longitud", "longitud_original"), 
                               on=["departamento", "municipio"], how="left")
    
    master_df = master_df.withColumn(
        "temperatura_promedio", F.coalesce(F.col("temperatura_promedio"), F.col("temperatura_promedio_aire"))
    ).withColumn(
        "latitud", F.coalesce(F.col("latitud_original"), F.col("latitud_clima"), F.col("latitud_aire"))
    ).withColumn(
        "longitud", F.coalesce(F.col("longitud_original"), F.col("longitud_clima"), F.col("longitud_aire"))
    ).drop("temperatura_promedio_aire", "latitud_aire", "longitud_aire", 
           "latitud_clima", "longitud_clima", "latitud_original", "longitud_original")

    # vacunacion: no join — column added as constant (see note above process_vacunacion)
    master_df = master_df.withColumn("vacunacion", F.lit("NO_REPORTA"))

    # Healthcare providers: static, location-only join
    master_df = master_df.join(
        df_hosp, on=["departamento", "municipio"], how="left"
    )

    # ── Checkpoint: materialise joins before brote calculation ─────
    # The DAG up to this point has ~300 stages. Caching here prevents
    # the brote percentile join from re-executing all upstream shuffles.
    master_df.cache()
    master_df.count()   # trigger cache population

    # ── 3. Add outbreak label ──────────────────────────────────────
    print("Step 3/5 — Creating brote column...\n")
    master_df = create_brote_column(master_df, df_vsp)

    # ── 4. Build join_key and enforce schema ───────────────────────
    print("Step 4/5 — Finalizing schema and deduplication...\n")

    master_df = master_df.withColumn(
        "join_key",
        F.concat_ws("_", F.col("ano"), F.col("semana"), F.col("departamento"), F.col("municipio")),
    )

    # Fill missing integer counts with 0; leave float metrics as null
    master_df = master_df.fillna({"cantidad_hospitales": 0})

    FINAL_COLS = [
        "ano", "semana", "departamento", "municipio", "enfermedad",
        "casos_totales", "temperatura_promedio", "precipitacion_promedio",
        "calidad_aire_promedio", "vacunacion",
        "cantidad_hospitales", "latitud", "longitud", "brote",
    ]

    # Guarantee every expected column exists even if a source had no data
    INT_COLS   = {"ano", "semana", "casos_totales", "brote", "cantidad_hospitales"}
    FLOAT_COLS = {
        "temperatura_promedio", "precipitacion_promedio",
        "calidad_aire_promedio", "latitud", "longitud",
    }
    # 'vacunacion' is already a string literal — no need to add it here
    for col in FINAL_COLS:
        if col not in master_df.columns:
            if col in INT_COLS:
                master_df = master_df.withColumn(col, F.lit(None).cast("int"))
            elif col in FLOAT_COLS:
                master_df = master_df.withColumn(col, F.lit(None).cast("double"))
            else:
                master_df = master_df.withColumn(col, F.lit(None).cast("string"))

    # Round numeric metrics to 2 decimal places for readability
    master_df = master_df.withColumn("temperatura_promedio", F.round("temperatura_promedio", 2))
    master_df = master_df.withColumn("precipitacion_promedio", F.round("precipitacion_promedio", 2))
    master_df = master_df.withColumn("calidad_aire_promedio", F.round("calidad_aire_promedio", 2))
    
    # Coordinates look better with 5 decimal places
    master_df = master_df.withColumn("latitud", F.round("latitud", 5))
    master_df = master_df.withColumn("longitud", F.round("longitud", 5))

    master_df = master_df.select(*FINAL_COLS)

    # Remove any duplicates introduced by joins
    master_df = master_df.dropDuplicates()

    # ── CACHE ──────────────────────────────────────────────────────
    # Persist the final dataframe in memory so every subsequent action
    # (validation counts, brote groupBy, CSV write) reads from cache
    # instead of re-executing the full multi-join DAG from scratch.
    # Without this, each .count()/.show()/.write triggers 500+ stages.
    master_df.cache()
    total_rows = master_df.count()   # single action that populates the cache

    # ── 5. Filter incomplete records and export ───────────────────
    print("Step 5/5 — Filtering incomplete and unreal records...\n")

    # Remove rows with any null values as requested by user
    master_df = master_df.dropna()
    
    # Apply sanity filters to remove unreal data
    master_df = apply_sanity_filters(master_df)
    
    total_rows = master_df.count()

    print(f"{'='*60}")
    print(f"  VALIDATION: MASTER DATASET (CLEAN & COMPLETE RECORDS)")
    print(f"{'='*60}")
    print(f"  Rows total : {total_rows:,}")

    # Null audit — one aggregation over all columns in a single pass
    null_exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in FINAL_COLS]
    null_counts = master_df.agg(*null_exprs).collect()[0]

    print("\n  Null summary per column:")
    for col in FINAL_COLS:
        n = null_counts[col]
        pct = (n / total_rows * 100) if total_rows > 0 else 0
        print(f"    {col:<35} nulls={n:>7,}  ({pct:.1f}%)")

    # Brote distribution
    print(f"\n  Brote distribution:")
    master_df.groupBy("brote").count().orderBy("brote").show()

    # Disease summary
    print("  Cases per disease:")
    master_df.groupBy("enfermedad").agg(
        F.sum("casos_totales").alias("total_casos"),
        F.count("*").alias("filas"),
    ).orderBy("total_casos", ascending=False).show()

    print(f"{'='*60}\n")

    # Export
    # ── Why pandas instead of Spark's native CSV writer? ──────────────────────
    # Spark's FileOutputCommitter calls NativeIO$Windows.access0() to list the
    # output directory after writing. On Windows without winutils.exe installed
    # this raises UnsatisfiedLinkError and aborts the job even though all data
    # was already written successfully by the workers.
    #
    # Converting to pandas and writing with Python's built-in I/O bypasses
    # Hadoop's FileOutputCommitter entirely — no winutils required.
    # 229k rows × 15 columns fits comfortably in memory.
    # ─────────────────────────────────────────────────────────────────────────
    output_path = str(output_dir / "dataset_maestro_epidemiologico.csv")
    print(f"\n  Saving to: {output_path}")

    pandas_df = master_df.toPandas()
    pandas_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    clean_db_path = PROJECT_ROOT / "data" / "data_lake_limpia.db"
    clean_db_path.parent.mkdir(parents=True, exist_ok=True)
    from sqlalchemy import create_engine
    engine = create_engine(f"sqlite:///{clean_db_path}")
    pandas_df.to_sql("dataset_maestro_epidemiologico", engine, if_exists="replace", index=False)
    print(f"  Data Lake limpio (SQLite): {clean_db_path}")

    print("\n  Pipeline completed successfully.\n")
    spark.stop()

def run_frontend_server(host="127.0.0.1", port=8000, csv_path=None, pbix_path=None):
    import pandas as pd
    import unicodedata
    import urllib.request

    src_dir = Path(__file__).resolve().parent
    project_root = src_dir.parent
    default_csv = project_root / "data" / "processed" / "dataset_maestro_epidemiologico.csv"
    csv_file = Path(csv_path) if csv_path else default_csv
    default_pbix = project_root / "data" / "processed" / "reporte_powerbi.pbix"
    pbix_file = Path(pbix_path) if pbix_path else default_pbix
    if not pbix_file.is_absolute():
        pbix_file = (project_root / pbix_file).resolve()

    if not csv_file.exists():
        raise FileNotFoundError(
            f"No se encontró el CSV del dataset maestro en: {csv_file}\n"
            "Ejecuta primero el pipeline para generarlo."
        )

    df = pd.read_csv(csv_file)
    columns = list(df.columns)

    def norm_text(value):
        s = "" if value is None else str(value)
        s = unicodedata.normalize("NFKD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = s.upper().strip()
        s = re.sub(r"[^A-Z0-9]+", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def html_escape(value):
        s = "" if value is None else str(value)
        return (
            s.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )

    sources = [
        {
            "archivo": "vigilancia_salud_publica.csv",
            "tipo": "Vigilancia en salud pública",
            "nivel": "año + semana + municipio + enfermedad",
            "salidas": ["ano", "semana", "departamento", "municipio", "enfermedad", "casos_totales"],
        },
        {
            "archivo": "normales_climatologicas.csv",
            "tipo": "Clima (normales climatológicas)",
            "nivel": "semana + municipio (derivado de meses ene–dic)",
            "salidas": ["semana", "departamento", "municipio", "temperatura_promedio", "precipitacion_promedio", "latitud", "longitud"],
        },
        {
            "archivo": "calidad_aire_promedio_anual.csv",
            "tipo": "Calidad del aire (promedio anual)",
            "nivel": "anual → expandido a 52 semanas",
            "salidas": ["ano", "semana", "departamento", "municipio", "calidad_aire_promedio", "latitud", "longitud"],
        },
        {
            "archivo": "prestadores_sedes.csv",
            "tipo": "Prestadores/sedes de salud",
            "nivel": "estático por municipio",
            "salidas": ["departamento", "municipio", "cantidad_hospitales"],
        },
        {
            "archivo": "vacunacion_departamento.csv",
            "tipo": "Vacunación (no usada en el modelo actual)",
            "nivel": "departamento",
            "salidas": ["vacunacion (NO_REPORTA)"],
        },
    ]

    def to_json_bytes(payload, status=200):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        return status, data

    def parse_int(value, default):
        try:
            return int(value)
        except Exception:
            return default

    def parse_list(value):
        if value is None:
            return []
        s = str(value).strip()
        if not s:
            return []
        parts = [p.strip() for p in s.split(",")]
        return [p for p in parts if p]

    def apply_filters(df_in, params):
        df_out = df_in

        q = (params.get("q") or "").strip()
        if q:
            q_upper = q.upper()
            mask = pd.Series(False, index=df_out.index)
            for col in ["departamento", "municipio", "enfermedad"]:
                if col in df_out.columns:
                    mask = mask | df_out[col].astype(str).str.upper().str.contains(q_upper, na=False)
            df_out = df_out[mask]

        departamentos = parse_list(params.get("departamento"))
        if departamentos and "departamento" in df_out.columns:
            dept_norm = set(norm_text(x) for x in departamentos)
            df_out = df_out[df_out["departamento"].astype(str).map(norm_text).isin(dept_norm)]

        municipio = (params.get("municipio") or "").strip()
        if municipio and "municipio" in df_out.columns:
            muni_norm = norm_text(municipio)
            df_out = df_out[df_out["municipio"].astype(str).map(norm_text) == muni_norm]

        enfermedades = parse_list(params.get("enfermedad"))
        if enfermedades and "enfermedad" in df_out.columns:
            enf_norm = set(norm_text(x) for x in enfermedades)
            df_out = df_out[df_out["enfermedad"].astype(str).map(norm_text).isin(enf_norm)]

        brote = (params.get("brote") or "").strip()
        if brote and "brote" in df_out.columns:
            brote_norm = norm_text(brote)
            df_out = df_out[df_out["brote"].astype(str).map(norm_text) == brote_norm]

        ano = (params.get("ano") or "").strip()
        if ano and "ano" in df_out.columns:
            df_out = df_out[df_out["ano"] == parse_int(ano, -1)]

        ano_min = (params.get("ano_min") or "").strip()
        ano_max = (params.get("ano_max") or "").strip()
        if (ano_min or ano_max) and "ano" in df_out.columns:
            min_v = parse_int(ano_min, None) if ano_min else None
            max_v = parse_int(ano_max, None) if ano_max else None
            if min_v is not None:
                df_out = df_out[df_out["ano"] >= min_v]
            if max_v is not None:
                df_out = df_out[df_out["ano"] <= max_v]

        semana = (params.get("semana") or "").strip()
        if semana and "semana" in df_out.columns:
            df_out = df_out[df_out["semana"] == parse_int(semana, -1)]

        return df_out

    def get_values():
        payload = {}
        if "departamento" in df.columns:
            payload["departamentos"] = sorted([x for x in df["departamento"].dropna().astype(str).unique().tolist() if x.strip()])
        else:
            payload["departamentos"] = []

        if "enfermedad" in df.columns:
            payload["enfermedades"] = sorted([x for x in df["enfermedad"].dropna().astype(str).unique().tolist() if x.strip()])
        else:
            payload["enfermedades"] = []

        if "ano" in df.columns:
            anos = sorted([int(x) for x in df["ano"].dropna().unique().tolist() if str(x).strip().isdigit()])
            payload["anos"] = anos
        else:
            payload["anos"] = []

        if "brote" in df.columns:
            payload["brotes"] = sorted([x for x in df["brote"].dropna().astype(str).unique().tolist() if x.strip()])
        else:
            payload["brotes"] = ["SI", "NO"]

        return payload

    def build_summary(df_filtered):
        total_rows = int(len(df_filtered))
        total_cases = int(df_filtered["casos_totales"].fillna(0).sum()) if "casos_totales" in df_filtered.columns else 0
        municipios = int(df_filtered["municipio"].nunique()) if "municipio" in df_filtered.columns else 0

        brote_counts = {}
        if "brote" in df_filtered.columns and total_rows:
            brote_counts = df_filtered["brote"].astype(str).value_counts(dropna=False).to_dict()

        disease_stats = []
        if "enfermedad" in df_filtered.columns and total_rows:
            agg = (
                df_filtered.groupby("enfermedad", dropna=False)
                .agg(casos=("casos_totales", "sum"), filas=("enfermedad", "size"))
                .reset_index()
                .sort_values("casos", ascending=False)
            )
            for _, row in agg.iterrows():
                disease_stats.append({"enfermedad": str(row["enfermedad"]), "casos": int(row["casos"]), "filas": int(row["filas"])})

        weekly = []
        if {"ano", "semana", "casos_totales"}.issubset(df_filtered.columns) and total_rows:
            w = (
                df_filtered.groupby(["ano", "semana"], dropna=False)["casos_totales"]
                .sum()
                .reset_index()
                .sort_values(["ano", "semana"])
            )
            w = w.tail(52)
            for _, row in w.iterrows():
                label = f"{int(row['ano'])}-W{int(row['semana']):02d}"
                weekly.append({"label": label, "casos": int(row["casos_totales"])})

        top_munis = []
        if {"departamento", "municipio", "casos_totales"}.issubset(df_filtered.columns) and total_rows:
            m = (
                df_filtered.groupby(["departamento", "municipio"], dropna=False)["casos_totales"]
                .sum()
                .reset_index()
                .sort_values("casos_totales", ascending=False)
                .head(10)
            )
            for _, row in m.iterrows():
                top_munis.append(
                    {
                        "departamento": str(row["departamento"]),
                        "municipio": str(row["municipio"]),
                        "casos": int(row["casos_totales"]),
                    }
                )

        def mean_or_none(col):
            if col not in df_filtered.columns or not total_rows:
                return None
            s = pd.to_numeric(df_filtered[col], errors="coerce")
            v = float(s.mean()) if s.notna().any() else None
            return None if v is None else round(v, 2)

        metrics = {
            "temperatura_promedio": mean_or_none("temperatura_promedio"),
            "precipitacion_promedio": mean_or_none("precipitacion_promedio"),
            "calidad_aire_promedio": mean_or_none("calidad_aire_promedio"),
            "cantidad_hospitales": mean_or_none("cantidad_hospitales"),
        }

        top_enfermedad = None
        if disease_stats:
            top_enfermedad = disease_stats[0]["enfermedad"]

        top_departamento = None
        if {"departamento", "casos_totales"}.issubset(df_filtered.columns) and total_rows:
            d = (
                df_filtered.groupby("departamento", dropna=False)["casos_totales"]
                .sum()
                .reset_index()
                .sort_values("casos_totales", ascending=False)
                .head(1)
            )
            if not d.empty:
                top_departamento = str(d.iloc[0]["departamento"])

        variation = None
        if {"ano", "casos_totales"}.issubset(df_filtered.columns) and total_rows:
            y = (
                df_filtered.groupby("ano", dropna=False)["casos_totales"]
                .sum()
                .reset_index()
                .sort_values("ano")
            )
            if len(y) >= 2:
                first_year = int(y.iloc[0]["ano"])
                last_year = int(y.iloc[-1]["ano"])
                first_val = float(y.iloc[0]["casos_totales"])
                last_val = float(y.iloc[-1]["casos_totales"])
                pct = None if first_val <= 0 else round(((last_val - first_val) / first_val) * 100, 1)
                variation = {
                    "from_year": first_year,
                    "to_year": last_year,
                    "from_cases": int(first_val),
                    "to_cases": int(last_val),
                    "pct": pct,
                    "direction": "sube" if last_val > first_val else ("baja" if last_val < first_val else "igual"),
                }

        insights = []
        if variation and variation.get("pct") is not None:
            pct = variation["pct"]
            if variation["direction"] == "sube":
                insights.append(f"En el periodo {variation['from_year']}–{variation['to_year']}, los casos subieron {pct}%.")
            elif variation["direction"] == "baja":
                insights.append(f"En el periodo {variation['from_year']}–{variation['to_year']}, los casos bajaron {abs(pct)}%.")
            else:
                insights.append(f"En el periodo {variation['from_year']}–{variation['to_year']}, los casos se mantuvieron estables.")
        if top_enfermedad:
            insights.append(f"La enfermedad con más casos en la selección actual es {top_enfermedad}.")
        if top_departamento:
            insights.append(f"El departamento con más casos en la selección actual es {top_departamento}.")

        trend = {"labels": [], "series": []}
        if {"ano", "casos_totales"}.issubset(df_filtered.columns) and total_rows:
            if "enfermedad" in df_filtered.columns:
                t = (
                    df_filtered.groupby(["ano", "enfermedad"], dropna=False)["casos_totales"]
                    .sum()
                    .reset_index()
                    .sort_values(["ano"])
                )
                years = sorted([int(x) for x in t["ano"].dropna().unique().tolist()])
                labels = [str(y) for y in years]
                series = []
                for enf in sorted([str(x) for x in t["enfermedad"].dropna().unique().tolist()]):
                    sub = t[t["enfermedad"].astype(str) == enf]
                    by_year = {int(r["ano"]): int(r["casos_totales"]) for _, r in sub.iterrows()}
                    series.append({"name": enf, "data": [by_year.get(y, 0) for y in years]})
                series = sorted(series, key=lambda s: sum(s["data"]), reverse=True)[:5]
                trend = {"labels": labels, "series": series}
            else:
                t = (
                    df_filtered.groupby(["ano"], dropna=False)["casos_totales"]
                    .sum()
                    .reset_index()
                    .sort_values(["ano"])
                )
                labels = [str(int(x)) for x in t["ano"].tolist()]
                values = [int(x) for x in t["casos_totales"].tolist()]
                trend = {"labels": labels, "series": [{"name": "Casos", "data": values}]}

        return {
            "filtered_total_rows": total_rows,
            "total_cases": total_cases,
            "municipios": municipios,
            "brote_counts": brote_counts,
            "disease_stats": disease_stats,
            "weekly_cases": weekly,
            "top_municipios": top_munis,
            "metrics": metrics,
            "top_enfermedad": top_enfermedad,
            "top_departamento": top_departamento,
            "variation": variation,
            "insights": insights,
            "trend": trend,
        }

    def load_colombia_map():
        url = "https://unpkg.com/@svg-maps/colombia@1.0.1/index.js"
        try:
            raw = urllib.request.urlopen(url, timeout=25).read().decode("utf-8", errors="replace").strip()
            if raw.startswith("export default"):
                raw = raw[len("export default"):].strip()
            if raw.endswith(";"):
                raw = raw[:-1]
            obj = json.loads(raw)
            if isinstance(obj, dict) and "locations" in obj and "viewBox" in obj:
                return {"viewBox": obj["viewBox"], "locations": obj["locations"]}
        except Exception:
            return None
        return None

    colombia_map = load_colombia_map()

    def answer_question(question, df_filtered):
        qn = norm_text(question)
        if not qn:
            return "Escribe una pregunta, por ejemplo: ¿Qué enfermedad ha crecido más en los últimos años?"

        disease_info = {
            "DENGUE": {
                "title": "Dengue",
                "what": "Enfermedad viral transmitida por mosquitos (principalmente Aedes). Puede ser leve o complicarse en algunos casos.",
                "symptoms": ["fiebre alta", "dolor de cabeza", "dolor detrás de los ojos", "dolor muscular y articular", "náuseas o malestar general"],
                "prevention": ["eliminar agua estancada", "usar repelente", "usar mosquiteros o mallas", "ropa que cubra la piel en horas de mayor actividad del mosquito"],
                "when": ["sangrado", "dolor abdominal fuerte", "somnolencia o confusión", "dificultad para respirar", "signos de deshidratación"],
            },
            "ZIKA": {
                "title": "Zika",
                "what": "Virus transmitido por mosquitos. Suele causar síntomas leves, pero requiere especial atención durante el embarazo.",
                "symptoms": ["sarpullido", "fiebre baja", "dolor articular", "ojos rojos (conjuntivitis)", "cansancio"],
                "prevention": ["evitar picaduras (repelente, ropa larga)", "eliminar criaderos", "consultar ante síntomas si estás embarazada o planeas estarlo"],
                "when": ["síntomas durante el embarazo", "malestar que empeora", "fiebre con sarpullido intenso"],
            },
            "CHIKUNGUNYA": {
                "title": "Chikungunya",
                "what": "Virus transmitido por mosquitos. Puede causar dolor articular fuerte que limita actividades y puede durar semanas.",
                "symptoms": ["fiebre", "dolor articular intenso", "hinchazón articular", "dolor muscular", "dolor de cabeza"],
                "prevention": ["usar repelente", "eliminar agua estancada", "usar mosquiteros", "proteger el hogar con mallas"],
                "when": ["dolor articular severo o prolongado", "fiebre alta persistente", "síntomas que empeoran o deshidratación"],
            },
        }
        disease = None
        for k in disease_info:
            if k in qn:
                disease = k
                break
        if disease is None:
            if "CHIK" in qn:
                disease = "CHIKUNGUNYA"
            elif "DENG" in qn:
                disease = "DENGUE"
            elif "ZIK" in qn:
                disease = "ZIKA"

        if ("SINTOM" in qn or "SÍNTOM" in qn) and disease in disease_info:
            info = disease_info[disease]
            return f"Síntomas comunes de {info['title']}: " + ", ".join(info["symptoms"]) + "."

        if ("PREVEN" in qn or "EVIT" in qn) and disease in disease_info:
            info = disease_info[disease]
            return f"Para prevenir {info['title']}: " + "; ".join(info["prevention"]) + "."

        if ("CUANDO" in qn and ("CONSULT" in qn or "URGEN" in qn or "MEDIC" in qn)) and disease in disease_info:
            info = disease_info[disease]
            return f"Consulta a un profesional si hay señales de alarma en {info['title']}: " + "; ".join(info["when"]) + "."

        if ("QUE ES" in qn or "QUÉ ES" in qn or "EXPLIC" in qn) and disease in disease_info:
            info = disease_info[disease]
            return info["what"] + " (Este contenido es educativo y no reemplaza una consulta médica)."

        if ("TRANSM" in qn or "CONTAG" in qn or "COMO SE" in qn and "TRANSM" in qn) and disease in disease_info:
            info = disease_info[disease]
            return f"{info['title']} se transmite principalmente por picadura de mosquitos. La prevención más efectiva es reducir criaderos y evitar picaduras."

        if ("DIFEREN" in qn or "COMPAR" in qn) and ("DENG" in qn and "ZIK" in qn or "DENG" in qn and "CHIK" in qn or "ZIK" in qn and "CHIK" in qn):
            return "Dengue, Zika y Chikungunya son enfermedades transmitidas por mosquitos. De forma general: dengue suele dar fiebre alta y puede complicarse; zika suele ser más leve pero es importante en embarazo; chikungunya destaca por dolor articular intenso. Si tienes síntomas, consulta a un profesional."

        if "CREC" in qn and "ENFERMEDAD" in qn and {"ano", "enfermedad", "casos_totales"}.issubset(df_filtered.columns) and len(df_filtered):
            g = (
                df_filtered.groupby(["enfermedad", "ano"], dropna=False)["casos_totales"]
                .sum()
                .reset_index()
                .sort_values(["enfermedad", "ano"])
            )
            best = None
            for enf, sub in g.groupby("enfermedad"):
                if len(sub) < 2:
                    continue
                first = float(sub.iloc[0]["casos_totales"])
                last = float(sub.iloc[-1]["casos_totales"])
                if first <= 0:
                    continue
                pct = (last - first) / first * 100
                if best is None or pct > best["pct"]:
                    best = {"enfermedad": str(enf), "pct": pct, "from_year": int(sub.iloc[0]["ano"]), "to_year": int(sub.iloc[-1]["ano"])}
            if best:
                return f"{best['enfermedad']} es la que más creció entre {best['from_year']} y {best['to_year']} (≈ {best['pct']:.1f}%)."
            return "No hay suficiente información para calcular crecimiento por enfermedad en la selección actual."

        if ("MAS" in qn and "AFECT" in qn) and {"departamento", "casos_totales"}.issubset(df_filtered.columns) and len(df_filtered):
            d = (
                df_filtered.groupby("departamento", dropna=False)["casos_totales"]
                .sum()
                .reset_index()
                .sort_values("casos_totales", ascending=False)
                .head(1)
            )
            if not d.empty:
                return f"El departamento más afectado en la selección actual es {d.iloc[0]['departamento']}."
            return "No encontré un departamento más afectado en la selección actual."

        if "RESUMEN" in qn:
            s = build_summary(df_filtered)
            parts = [p for p in s.get("insights", []) if p]
            if not parts:
                return "No hay suficientes datos en la selección actual para generar un resumen."
            return " ".join(parts[:3])

        examples = [
            "¿Qué enfermedad ha crecido más en los últimos años?",
            "¿Cuál es la región más afectada?",
            "¿Cuál es la enfermedad más común?",
            "¿En qué año hubo más casos?",
            "Dame un resumen",
        ]
        if "AYUDA" in qn or "PREGUNT" in qn or "QUE PUEDO" in qn or "QUE PUEDO PREGUNTAR" in qn:
            return "Puedes preguntar, por ejemplo: " + " | ".join(examples)

        if ("ENFERMEDAD" in qn and "COMUN" in qn) and "enfermedad" in df_filtered.columns and "casos_totales" in df_filtered.columns and len(df_filtered):
            agg = (
                df_filtered.groupby("enfermedad", dropna=False)["casos_totales"]
                .sum()
                .reset_index()
                .sort_values("casos_totales", ascending=False)
                .head(1)
            )
            if not agg.empty:
                return f"La enfermedad más común en la selección actual es {agg.iloc[0]['enfermedad']}."

        if ("ANO" in qn or "AÑO" in qn) and ("MAS" in qn and "CASO" in qn) and {"ano", "casos_totales"}.issubset(df_filtered.columns) and len(df_filtered):
            y = (
                df_filtered.groupby("ano", dropna=False)["casos_totales"]
                .sum()
                .reset_index()
                .sort_values("casos_totales", ascending=False)
                .head(1)
            )
            if not y.empty:
                return f"El año con más casos en la selección actual es {int(y.iloc[0]['ano'])}."

        if ("TOTAL" in qn or "CUANT" in qn) and "CASO" in qn and "casos_totales" in df_filtered.columns:
            return f"Total de casos en la selección actual: {int(df_filtered['casos_totales'].fillna(0).sum()):,}."

        if ("MAS" in qn and "AFECT" in qn) and {"municipio", "casos_totales"}.issubset(df_filtered.columns) and len(df_filtered):
            m = (
                df_filtered.groupby("municipio", dropna=False)["casos_totales"]
                .sum()
                .reset_index()
                .sort_values("casos_totales", ascending=False)
                .head(1)
            )
            if not m.empty:
                return f"El municipio más afectado en la selección actual es {m.iloc[0]['municipio']}."

        if disease in disease_info:
            info = disease_info[disease]
            return f"{info['what']} Si quieres, pregunta por síntomas, prevención o señales de alarma."

        return "Puedo ayudarte a entender tendencias (casos por año, región más afectada, crecimiento) y también con información educativa básica de dengue, zika y chikungunya. Escribe “ayuda” para ver ejemplos."

    def answer_chat(messages, df_filtered, params):
        key = (os.getenv("OPENAI_API_KEY") or "").strip()
        base_url = (os.getenv("OPENAI_BASE_URL") or "https://api.openai.com").strip().rstrip("/")
        model = (os.getenv("OPENAI_MODEL") or "gpt-4o-mini").strip()

        last_user = ""
        try:
            for m in reversed(messages or []):
                if (m or {}).get("role") == "user" and (m or {}).get("content"):
                    last_user = str(m.get("content"))
                    break
        except Exception:
            last_user = ""

        summary = build_summary(df_filtered) if len(df_filtered) else {"total_cases": 0, "top_enfermedad": "", "top_departamento": ""}
        v = summary.get("variation") or {}
        var_txt = ""
        if v and v.get("pct") is not None and v.get("from_year") is not None and v.get("to_year") is not None:
            sign = "-" if v.get("direction") == "baja" else ""
            var_txt = f"Cambio en el tiempo (primer vs último año del filtro): {sign}{abs(float(v.get('pct'))):.1f}% ({v.get('from_year')}→{v.get('to_year')})."

        context = "\n".join(
            [
                "Contexto (datos del dashboard, según filtros actuales):",
                f"- Total de casos: {int(summary.get('total_cases') or 0)}",
                f"- Enfermedad más común: {summary.get('top_enfermedad') or '—'}",
                f"- Región más afectada: {summary.get('top_departamento') or '—'}",
                f"- {var_txt or 'Cambio en el tiempo: —'}",
                "",
                "Notas:",
                "- El contenido es educativo y no reemplaza una consulta médica.",
                "- Si el usuario pregunta por números, usa el contexto y responde en lenguaje simple.",
            ]
        )

        if not key:
            if last_user:
                return answer_question(last_user, df_filtered) + " (Para un modo tipo ChatGPT que responda preguntas abiertas, configura OPENAI_API_KEY.)"
            return "Escribe una pregunta. (Para un modo tipo ChatGPT, configura OPENAI_API_KEY.)"

        try:
            import requests  # type: ignore

            sys_prompt = (
                "Eres un asistente conversacional para público general sobre salud en Colombia. "
                "Responde en español, con tono claro, empático y sin tecnicismos. "
                "Cuando hables de salud, incluye recomendaciones generales y señales de alarma, "
                "y recuerda que no reemplazas a un profesional médico. "
                "Cuando uses datos, explícalos con claridad y evita conclusiones exageradas."
            )

            msgs = [{"role": "system", "content": sys_prompt + "\n\n" + context}]
            for m in (messages or [])[-16:]:
                role = (m or {}).get("role") or "user"
                content = (m or {}).get("content") or ""
                if role not in ("user", "assistant"):
                    role = "user"
                if content:
                    msgs.append({"role": role, "content": str(content)})

            resp = requests.post(
                f"{base_url}/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={"model": model, "messages": msgs, "temperature": 0.4},
                timeout=25,
            )
            if resp.status_code >= 400:
                detail = ""
                try:
                    err = resp.json()
                    msg = ((err or {}).get("error") or {}).get("message") or ""
                    if msg:
                        detail = str(msg)
                except Exception:
                    detail = (resp.text or "").strip()
                code = int(resp.status_code)
                hint = ""
                if code == 401:
                    hint = "Revisa que OPENAI_API_KEY sea válida."
                elif code == 403:
                    hint = "Puede que tu red bloquee el acceso al proveedor o que falte habilitar el servicio."
                elif code == 404:
                    hint = "Revisa OPENAI_BASE_URL y el nombre del modelo (OPENAI_MODEL)."
                elif code == 429:
                    hint = "Límite de uso alcanzado. Intenta más tarde o revisa tu plan."
                fallback = answer_question(last_user, df_filtered) if last_user else "No pude responder."
                extra = (" Detalle: " + detail) if detail else ""
                extra2 = (" " + hint) if hint else ""
                return "No pude responder en modo ChatGPT (" + str(code) + ")." + extra + extra2 + " Mientras tanto: " + fallback
            data = resp.json()
            ans = (((data or {}).get("choices") or [{}])[0].get("message") or {}).get("content") or ""
            ans = str(ans).strip()
            return ans or "No pude responder."
        except Exception:
            if last_user:
                return answer_question(last_user, df_filtered) + " (Para un modo tipo ChatGPT, configura OPENAI_API_KEY.)"
            return "No pude responder."

    html = """<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Enfermedades en Colombia</title>
  <style>
    :root {
      --bg: #f7fbff;
      --panel: #ffffff;
      --panel2: #f0f7ff;
      --text: #0f172a;
      --muted: #475569;
      --border: rgba(15,23,42,0.10);
      --accent: #0284c7;
      --accent2: #10b981;
      --accent3: #22c55e;
      --shadow: 0 12px 30px rgba(2, 8, 23, 0.08);
      --shadow2: 0 8px 18px rgba(2, 8, 23, 0.06);
    }
    [data-theme="dark"] {
      --bg: #07101f;
      --panel: rgba(255,255,255,0.06);
      --panel2: rgba(255,255,255,0.08);
      --text: rgba(255,255,255,0.92);
      --muted: rgba(255,255,255,0.68);
      --border: rgba(255,255,255,0.14);
      --accent: #60a5fa;
      --accent2: #34d399;
      --accent3: #22c55e;
      --shadow: 0 16px 40px rgba(0,0,0,0.28);
      --shadow2: 0 10px 22px rgba(0,0,0,0.22);
    }
    * { box-sizing: border-box; }
    html, body { height: 100%; }
    body {
      margin: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      color: var(--text);
      background:
        radial-gradient(900px 600px at 10% 0%, rgba(2,132,199,0.14), transparent 55%),
        radial-gradient(900px 600px at 90% 0%, rgba(16,185,129,0.12), transparent 55%),
        var(--bg);
      min-height: 100vh;
    }
    a { color: inherit; text-decoration: none; }
    .app {
      width: 100%;
      max-width: 100%;
      margin: 0;
      padding: 16px 14px 28px;
      min-height: calc(100vh - var(--headerH, 0px) - 24px);
      display: flex;
      flex-direction: column;
    }
    [data-route="inicio"] .app {
      min-height: auto;
      padding-bottom: 14px;
    }
    .header {
      position: sticky;
      top: 0;
      z-index: 20;
      padding: 12px 0 10px;
      background: color-mix(in srgb, var(--bg) 70%, transparent);
      backdrop-filter: blur(10px);
      border-bottom: 1px solid var(--border);
    }
    .header-inner {
      width: 100%;
      max-width: 100%;
      margin: 0;
      padding: 0 14px;
      display: grid;
      grid-template-columns: 1fr;
      gap: 12px;
    }
    .hero {
      display: flex;
      gap: 12px;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
    }
    .brand {
      display: flex;
      gap: 12px;
      align-items: center;
      min-width: 280px;
    }
    .logo {
      width: 44px;
      height: 44px;
      border-radius: 14px;
      background: linear-gradient(135deg, var(--accent), var(--accent2));
      box-shadow: var(--shadow2);
      flex: 0 0 auto;
    }
    .titles h1 {
      margin: 0;
      font-size: 18px;
      letter-spacing: 0.2px;
    }
    .titles p {
      margin: 2px 0 0;
      color: var(--muted);
      font-size: 12.5px;
      line-height: 1.35;
    }
    .controls {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .btn {
      border: 1px solid var(--border);
      background: color-mix(in srgb, var(--panel) 92%, transparent);
      color: var(--text);
      border-radius: 12px;
      padding: 9px 12px;
      cursor: pointer;
      box-shadow: var(--shadow2);
      transition: transform .12s ease, box-shadow .12s ease, background .12s ease;
      font-size: 13px;
      display: inline-flex;
      gap: 8px;
      align-items: center;
    }
    .btn:hover { transform: translateY(-1px); box-shadow: var(--shadow); }
    .btn.primary { border-color: color-mix(in srgb, var(--accent) 45%, var(--border)); }
    .btn.ghost { box-shadow: none; background: transparent; }
    .pill {
      display: inline-flex;
      gap: 6px;
      align-items: center;
      padding: 6px 10px;
      border-radius: 999px;
      border: 1px solid var(--border);
      background: color-mix(in srgb, var(--panel) 90%, transparent);
      color: var(--muted);
      font-size: 12px;
    }
    .grid {
      display: grid;
      grid-template-columns: 360px 1fr;
      gap: 14px;
      align-items: start;
    }
    @media (max-width: 1020px) { .grid { grid-template-columns: 1fr; } }
    .nav {
      display: flex;
      gap: 8px;
      align-items: center;
      flex-wrap: wrap;
      margin: 12px 0 0;
      min-height: var(--navH, 0px);
    }
    .nav a {
      border: 1px solid var(--border);
      background: color-mix(in srgb, var(--panel) 92%, transparent);
      border-radius: 999px;
      padding: 8px 12px;
      font-size: 13px;
      color: var(--text);
      box-shadow: var(--shadow2);
      transition: transform .12s ease, box-shadow .12s ease;
    }
    .nav a:hover { transform: translateY(-1px); box-shadow: var(--shadow); }
    .nav a.active {
      border-color: color-mix(in srgb, var(--accent) 55%, var(--border));
      box-shadow: 0 0 0 4px color-mix(in srgb, var(--accent) 16%, transparent);
    }
    .page { display: none; animation: fade .18s ease; flex: 1 1 auto; }
    .page.active { display: block; }
    .page > .card { height: auto; }
    #page-dashboard { min-height: calc(100vh - var(--headerH, 0px) - var(--navH, 0px) - 64px); }
    @keyframes fade { from { opacity: 0; transform: translateY(2px); } to { opacity: 1; transform: translateY(0); } }
    .home-grid {
      display: grid;
      grid-template-columns: minmax(0, 1.3fr) minmax(0, 1fr);
      gap: 14px;
      align-items: start;
      height: auto;
    }
    @media (max-width: 1020px) { .home-grid { grid-template-columns: 1fr; } }
    .disease-grid {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 12px;
    }
    @media (max-width: 1020px) { .disease-grid { grid-template-columns: 1fr 1fr; } }
    @media (max-width: 520px) { .disease-grid { grid-template-columns: 1fr; } }
    .disease-card h3 { margin: 0 0 6px; font-size: 15px; }
    .disease-card ul { margin: 8px 0 0; padding-left: 18px; color: var(--muted); font-size: 12.5px; line-height: 1.5; }
    .disease-card li { margin: 4px 0; }
    .cta {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 10px 14px;
      border-radius: 14px;
      border: 1px solid color-mix(in srgb, var(--accent) 45%, var(--border));
      background: color-mix(in srgb, var(--accent) 12%, var(--panel));
      box-shadow: var(--shadow2);
      cursor: pointer;
      font-size: 13px;
      transition: transform .12s ease, box-shadow .12s ease;
    }
    .cta:hover { transform: translateY(-1px); box-shadow: var(--shadow); }
    .stack { display: grid; gap: 12px; }
    .actions { display:flex; gap:10px; flex-wrap:wrap; align-items:center; }
    .feature-grid {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 12px;
      margin-top: 2px;
    }
    @media (max-width: 1020px) { .feature-grid { grid-template-columns: 1fr; } }
    .feature {
      border: 1px solid var(--border);
      border-radius: 16px;
      background: color-mix(in srgb, var(--panel2) 70%, transparent);
      padding: 12px;
      box-shadow: var(--shadow2);
      transition: transform .12s ease, box-shadow .12s ease;
      display: grid;
      grid-template-columns: 40px 1fr;
      gap: 10px;
      align-items: start;
      min-height: 92px;
      cursor: pointer;
    }
    .feature:hover { transform: translateY(-1px); box-shadow: var(--shadow); }
    .ico {
      width: 40px; height: 40px; border-radius: 14px;
      border: 1px solid var(--border);
      background: linear-gradient(180deg, color-mix(in srgb, var(--accent) 18%, transparent), color-mix(in srgb, var(--accent2) 10%, transparent));
      display:flex; align-items:center; justify-content:center;
      font-size: 16px;
      color: color-mix(in srgb, var(--accent) 85%, var(--text));
      box-shadow: var(--shadow2);
      user-select: none;
    }
    .feature h4 { margin: 0; font-size: 13px; }
    .feature p { margin: 4px 0 0; color: var(--muted); font-size: 12.5px; line-height: 1.45; }
    details.faq {
      border: 1px solid var(--border);
      border-radius: 16px;
      background: color-mix(in srgb, var(--panel2) 68%, transparent);
      padding: 10px 12px;
      box-shadow: var(--shadow2);
    }
    details.faq + details.faq { margin-top: 8px; }
    details.faq summary { cursor: pointer; font-size: 13px; }
    details.faq summary::marker { color: var(--muted); }
    details.faq .muted { margin-top: 8px; }
    .diseases-layout {
      display: grid;
      grid-template-columns: 360px 1fr;
      gap: 14px;
      align-items: start;
    }
    @media (max-width: 1020px) { .diseases-layout { grid-template-columns: 1fr; } }
    .disease-list { display: grid; gap: 10px; }
    .disease-item {
      border: 1px solid var(--border);
      border-radius: 16px;
      background: color-mix(in srgb, var(--panel2) 70%, transparent);
      padding: 12px;
      cursor: pointer;
      transition: transform .12s ease, box-shadow .12s ease, border-color .12s ease;
      box-shadow: var(--shadow2);
    }
    .disease-item:hover { transform: translateY(-1px); box-shadow: var(--shadow); }
    .disease-item.active {
      border-color: color-mix(in srgb, var(--accent) 55%, var(--border));
      box-shadow: 0 0 0 4px color-mix(in srgb, var(--accent) 16%, transparent);
    }
    .disease-item h3 { margin: 0; font-size: 14px; display:flex; gap:8px; align-items:center; }
    .disease-item .tag {
      display:inline-flex; align-items:center; justify-content:center;
      width: 28px; height: 28px; border-radius: 12px;
      border: 1px solid var(--border);
      background: color-mix(in srgb, var(--panel) 92%, transparent);
      color: var(--muted);
      font-size: 13px;
    }
    .chips { display:flex; gap: 8px; flex-wrap:wrap; margin-top: 8px; }
    .chip {
      display:inline-flex; gap: 6px; align-items:center;
      padding: 6px 10px;
      border-radius: 999px;
      border: 1px solid var(--border);
      background: color-mix(in srgb, var(--panel) 90%, transparent);
      color: var(--muted);
      font-size: 12px;
      box-shadow: var(--shadow2);
    }
    .chip strong { color: var(--text); font-weight: 600; }
    .detail-title { margin: 0; font-size: 18px; letter-spacing: 0.1px; }
    .detail-lead { margin: 6px 0 0; color: var(--muted); line-height: 1.55; }
    .detail-cols { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-top: 12px; }
    @media (max-width: 1020px) { .detail-cols { grid-template-columns: 1fr; } }
    .panel {
      border: 1px solid var(--border);
      border-radius: 16px;
      background: color-mix(in srgb, var(--panel2) 70%, transparent);
      padding: 12px;
      box-shadow: var(--shadow2);
    }
    .panel h4 { margin: 0 0 8px; font-size: 13px; }
    .panel ul { margin: 0; padding-left: 18px; color: var(--muted); font-size: 12.5px; line-height: 1.5; }
    .panel li { margin: 5px 0; }
    .compare-box { margin-top: 12px; }
    .compare-actions { display:flex; gap:10px; flex-wrap:wrap; align-items:center; margin-top: 10px; }
    .chat-float {
      position: fixed;
      right: 18px;
      bottom: 18px;
      width: 360px;
      max-width: calc(100vw - 28px);
      border-radius: 18px;
      border: 1px solid var(--border);
      background: color-mix(in srgb, var(--panel) 92%, transparent);
      box-shadow: var(--shadow);
      overflow: hidden;
      z-index: 80;
      display: none;
    }
    .chat-float.active { display: block; }
    .chat-float .hd {
      padding: 12px 12px 10px;
      border-bottom: 1px solid var(--border);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      background: linear-gradient(180deg, color-mix(in srgb, var(--accent) 10%, transparent), transparent);
    }
    .chat-float .hd .title {
      display:flex;
      align-items:center;
      gap: 8px;
      min-width: 0;
    }
    .chat-float .hd .title .dot {
      width: 10px; height: 10px; border-radius: 999px;
      background: color-mix(in srgb, var(--accent2) 80%, var(--accent));
      box-shadow: 0 0 0 4px color-mix(in srgb, var(--accent2) 16%, transparent);
      flex: 0 0 auto;
    }
    .chat-float .hd h3 { margin: 0; font-size: 13px; }
    .chat-float .hd .muted { font-size: 12px; }
    .chat-body {
      padding: 12px;
      display: grid;
      gap: 8px;
      max-height: 48vh;
      overflow: auto;
      background: color-mix(in srgb, var(--panel2) 65%, transparent);
    }
    .msg {
      padding: 10px 12px;
      border-radius: 16px;
      border: 1px solid var(--border);
      box-shadow: var(--shadow2);
      font-size: 13px;
      line-height: 1.45;
      max-width: 92%;
      white-space: pre-wrap;
    }
    .msg.user { justify-self: end; background: color-mix(in srgb, var(--accent) 12%, var(--panel)); }
    .msg.bot { justify-self: start; background: color-mix(in srgb, var(--panel) 92%, transparent); }
    .chat-tools { padding: 10px 12px; border-top: 1px solid var(--border); background: color-mix(in srgb, var(--panel) 92%, transparent); }
    .chat-row { display: grid; grid-template-columns: 1fr auto; gap: 10px; align-items: center; }
    .chat-quick { display:flex; gap: 8px; flex-wrap: wrap; margin-top: 10px; }
    .qbtn {
      border: 1px solid var(--border);
      background: color-mix(in srgb, var(--panel) 92%, transparent);
      color: var(--muted);
      border-radius: 999px;
      padding: 6px 10px;
      font-size: 12px;
      cursor: pointer;
      box-shadow: var(--shadow2);
      transition: transform .12s ease, box-shadow .12s ease;
    }
    .qbtn:hover { transform: translateY(-1px); box-shadow: var(--shadow); }
    .chat-mini {
      position: fixed;
      right: 18px;
      bottom: 18px;
      z-index: 79;
      display: none;
    }
    .chat-mini.active { display: block; }
    .chat-mini .bubble {
      width: 52px; height: 52px;
      border-radius: 18px;
      border: 1px solid var(--border);
      background: linear-gradient(180deg, color-mix(in srgb, var(--accent) 14%, var(--panel)), color-mix(in srgb, var(--accent2) 10%, var(--panel)));
      box-shadow: var(--shadow);
      display:flex; align-items:center; justify-content:center;
      cursor: pointer;
      user-select: none;
      font-size: 18px;
    }
    @media (max-width: 1020px) {
      .chat-float { left: 14px; right: 14px; width: auto; }
      .chat-body { max-height: 40vh; }
    }
    .card {
      border: 1px solid var(--border);
      border-radius: 18px;
      background: var(--panel);
      box-shadow: var(--shadow);
      overflow: hidden;
    }
    .card .hd {
      padding: 14px 14px 10px;
      border-bottom: 1px solid var(--border);
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 10px;
    }
    .card .hd h2 {
      margin: 0;
      font-size: 12px;
      letter-spacing: 0.4px;
      text-transform: uppercase;
    }
    .card .hd .sub {
      color: var(--muted);
      font-size: 12px;
    }
    .card .bd { padding: 14px; }
    .field { margin-bottom: 10px; }
    .field label { display: flex; gap: 6px; align-items: center; color: var(--muted); font-size: 12px; margin-bottom: 6px; }
    .tip {
      width: 18px; height: 18px; border-radius: 999px;
      border: 1px solid var(--border);
      display: inline-flex; align-items: center; justify-content: center;
      font-size: 12px; color: var(--muted);
      cursor: help;
      user-select: none;
    }
    input, select {
      width: 100%;
      padding: 10px 10px;
      border-radius: 12px;
      border: 1px solid var(--border);
      background: color-mix(in srgb, var(--panel2) 85%, transparent);
      color: var(--text);
      outline: none;
      transition: border-color .12s ease, box-shadow .12s ease, background .12s ease;
    }
    input:focus, select:focus {
      border-color: color-mix(in srgb, var(--accent) 55%, var(--border));
      box-shadow: 0 0 0 4px color-mix(in srgb, var(--accent) 16%, transparent);
    }
    input::placeholder { color: color-mix(in srgb, var(--muted) 70%, transparent); }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
    @media (max-width: 520px) { .row { grid-template-columns: 1fr; } }
    .kpis { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; }
    @media (max-width: 1020px) { .kpis { grid-template-columns: repeat(2, 1fr); } }
    .kpi {
      border: 1px solid var(--border);
      border-radius: 16px;
      background: color-mix(in srgb, var(--panel2) 70%, transparent);
      padding: 12px;
      min-height: 92px;
      transition: transform .12s ease;
    }
    .kpi:hover { transform: translateY(-1px); }
    .kpi .label { color: var(--muted); font-size: 12px; }
    .kpi .value { margin-top: 8px; font-size: 22px; letter-spacing: 0.2px; }
    .kpi .desc { margin-top: 6px; color: var(--muted); font-size: 12px; display:flex; gap:8px; align-items:center; }
    .badge-up, .badge-down {
      display:inline-flex; align-items:center; justify-content:center;
      width: 22px; height: 22px; border-radius: 10px;
      border: 1px solid var(--border);
      font-size: 12px;
    }
    .badge-up { background: color-mix(in srgb, var(--accent3) 18%, transparent); color: color-mix(in srgb, var(--accent3) 90%, var(--text)); }
    .badge-down { background: color-mix(in srgb, #ef4444 14%, transparent); color: color-mix(in srgb, #ef4444 90%, var(--text)); }
    .viz {
      display: grid;
      grid-template-columns: 1.35fr 1fr;
      gap: 12px;
      margin-top: 12px;
    }
    @media (max-width: 1020px) { .viz { grid-template-columns: 1fr; } }
    .chart {
      border: 1px solid var(--border);
      border-radius: 16px;
      background: color-mix(in srgb, var(--panel2) 70%, transparent);
      padding: 12px;
      box-shadow: var(--shadow2);
    }
    .chart h3 { margin: 0 0 10px; font-size: 13px; }
    canvas { width: 100%; height: 260px; display: block; }
    .map-wrap { width: 100%; display: grid; grid-template-columns: 1fr; gap: 10px; }
    .map {
      width: 100%;
      border-radius: 14px;
      border: 1px solid var(--border);
      background: color-mix(in srgb, var(--panel) 92%, transparent);
      overflow: hidden;
    }
    .map svg { width: 100%; height: 380px; display:block; }
    .legend { display:flex; gap: 10px; align-items:center; flex-wrap:wrap; color: var(--muted); font-size: 12px; }
    .grad { width: 160px; height: 10px; border-radius: 999px; border: 1px solid var(--border); background: linear-gradient(90deg, #e0f2fe, #7dd3fc, #38bdf8, #0284c7); }
    .insights { display: grid; gap: 8px; }
    .insight {
      padding: 10px 12px;
      border-radius: 14px;
      border: 1px solid var(--border);
      background: color-mix(in srgb, var(--panel2) 70%, transparent);
      color: var(--text);
      font-size: 13px;
      line-height: 1.4;
      box-shadow: var(--shadow2);
      transition: transform .12s ease;
    }
    .insight:hover { transform: translateY(-1px); }
    .table-wrap { border: 1px solid var(--border); border-radius: 16px; overflow: auto; max-height: 70vh; box-shadow: var(--shadow2); }
    table { border-collapse: collapse; width: 100%; min-width: 900px; background: color-mix(in srgb, var(--panel) 92%, transparent); }
    th, td { padding: 10px 10px; text-align: left; font-size: 12.5px; border-bottom: 1px solid var(--border); vertical-align: top; }
    th { position: sticky; top: 0; background: var(--panel); z-index: 2; font-size: 12px; }
    tbody tr:nth-child(even) { background: color-mix(in srgb, var(--panel2) 65%, transparent); }
    .footerbar { display:flex; gap: 10px; align-items:center; flex-wrap:wrap; justify-content: space-between; }
    .muted { color: var(--muted); font-size: 12px; }
    .toast {
      position: fixed;
      right: 16px;
      bottom: 16px;
      padding: 10px 12px;
      border-radius: 14px;
      border: 1px solid var(--border);
      background: color-mix(in srgb, var(--panel) 88%, transparent);
      color: var(--text);
      box-shadow: var(--shadow);
      display: none;
      max-width: 520px;
      font-size: 13px;
      z-index: 50;
    }
    .tooltip {
      position: fixed;
      pointer-events: none;
      background: color-mix(in srgb, var(--panel) 92%, transparent);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 8px 10px;
      box-shadow: var(--shadow);
      font-size: 12px;
      color: var(--text);
      display: none;
      z-index: 60;
      max-width: 280px;
    }
    .spinner {
      width: 16px; height: 16px;
      border-radius: 999px;
      border: 2px solid color-mix(in srgb, var(--muted) 35%, transparent);
      border-top-color: color-mix(in srgb, var(--accent) 85%, transparent);
      animation: spin 0.8s linear infinite;
    }
    @keyframes spin { to { transform: rotate(360deg); } }
    @media print {
      .header, .filters, .btn, .tip, .toast, .tooltip, .chat { display: none !important; }
      body { background: #ffffff !important; }
      .card { box-shadow: none !important; }
      .app { max-width: 100% !important; }
      canvas { height: 240px !important; }
      .map svg { height: 320px !important; }
    }
  </style>
</head>
<body data-theme="light">
  <div class="header">
    <div class="header-inner">
      <div class="hero">
        <div class="brand">
          <div class="logo"></div>
          <div class="titles">
            <h1 data-i18n="app.title">Enfermedades en Colombia</h1>
            <p data-i18n="app.subtitle">Explora la evolución de los casos por año, enfermedad y departamento. Diseñado para entenderse rápido, sin tecnicismos.</p>
          </div>
        </div>
        <div class="controls">
          <span class="pill" id="statusPill"><span class="spinner" id="spin" style="display:none;"></span><span id="statusText" data-i18n="status.ready">Listo</span></span>
          <button class="btn ghost" id="btnLang" title="Cambiar idioma">ES</button>
          <button class="btn ghost" id="btnTheme" title="Cambiar modo claro/oscuro" data-i18n="btn.theme">Modo</button>
          <a class="btn" id="btnExportFiltered" href="/download_filtered" data-i18n="btn.exportFiltered">Exportar (filtrado)</a>
          <button class="btn" id="btnPDF" title="Guardar como PDF (desde el navegador)" data-i18n="btn.pdf">PDF</button>
          <a class="btn" id="btnPowerBI" href="/download_powerbi" data-i18n="btn.powerbi">Power BI</a>
          <a class="btn primary" href="/download" data-i18n="btn.csvFull">CSV completo</a>
        </div>
      </div>
    </div>
  </div>

  <div class="app">
    <div class="nav" id="nav">
      <a href="#/inicio" id="navHome" data-i18n="nav.home">Inicio</a>
      <a href="#/enfermedades" id="navDiseases" data-i18n="nav.diseases">Enfermedades</a>
      <a href="#/dashboard" id="navDash" data-i18n="nav.dashboard">Dashboard</a>
    </div>

    <section class="page" id="page-home">
      <div class="card" style="margin-top: 14px;">
        <div class="hd">
          <h2 data-i18n="home.title">Inicio</h2>
          <div class="sub" data-i18n="home.sub">Portal educativo + análisis visual</div>
        </div>
        <div class="bd">
          <div class="home-grid">
            <div class="stack">
              <div class="kpis">
                <div class="kpi">
                  <div class="label" data-i18n="kpi.totalCases">Casos registrados</div>
                  <div class="value" id="homeKpiCases">—</div>
                  <div class="desc" data-i18n="kpi.totalDataset">Total del dataset</div>
                </div>
                <div class="kpi">
                  <div class="label" data-i18n="kpi.topDisease">Enfermedad más común</div>
                  <div class="value" id="homeKpiTopDis">—</div>
                  <div class="desc" data-i18n="kpi.byCases">Por número de casos</div>
                </div>
                <div class="kpi">
                  <div class="label" data-i18n="kpi.topRegion">Región más afectada</div>
                  <div class="value" id="homeKpiTopDept">—</div>
                  <div class="desc" data-i18n="kpi.deptMostCases">Departamento con más casos</div>
                </div>
                <div class="kpi">
                  <div class="label" data-i18n="kpi.changeOverTime">Cambio en el tiempo</div>
                  <div class="value" id="homeKpiVar">—</div>
                  <div class="desc" id="homeKpiVarDesc" data-i18n="kpi.overallTrend">Tendencia general</div>
                </div>
              </div>
              <div class="actions">
                <button class="cta" id="btnGoDash" data-i18n="home.ctaDash">Explorar Dashboard →</button>
                <button class="btn" id="btnGoDiseases" data-i18n="home.ctaGuide">Guía de enfermedades</button>
                <button class="btn ghost" id="btnScrollFAQ" data-i18n="home.ctaFaq">Preguntas frecuentes</button>
                <span class="pill" data-i18n="home.tip">Consejo: filtra por años y compara enfermedades</span>
              </div>

              <div class="feature-grid">
                <div class="feature" tabindex="0" role="button" data-go="dash-trend">
                  <div class="ico">↗</div>
                  <div>
                    <h4 data-i18n="home.feature.trends.title">Ver tendencias</h4>
                    <p data-i18n="home.feature.trends.desc">Identifica si los casos suben o bajan en el tiempo y compara enfermedades.</p>
                  </div>
                </div>
                <div class="feature" tabindex="0" role="button" data-go="dash-map">
                  <div class="ico">▦</div>
                  <div>
                    <h4 data-i18n="home.feature.regions.title">Explorar regiones</h4>
                    <p data-i18n="home.feature.regions.desc">Ubica departamentos con mayor incidencia y descubre patrones geográficos.</p>
                  </div>
                </div>
                <div class="feature" tabindex="0" role="button" data-go="learn">
                  <div class="ico">✓</div>
                  <div>
                    <h4 data-i18n="home.feature.learn.title">Aprender y prevenir</h4>
                    <p data-i18n="home.feature.learn.desc">Conoce síntomas comunes, prevención y cuándo consultar para actuar a tiempo.</p>
                  </div>
                </div>
              </div>

              <div class="panel">
                <h4 data-i18n="home.learnQuick.title">Aprender y prevenir (guía rápida)</h4>
                <ul id="homeGuideList"></ul>
                <div class="actions" style="margin-top: 10px;">
                  <button class="btn primary" id="btnLearnMore" data-i18n="home.learnQuick.more">Ver guía completa</button>
                  <button class="btn" id="btnAskIdeas" data-i18n="home.learnQuick.askIdeas">¿Qué puedo preguntar?</button>
                </div>
              </div>

              <div class="insights" id="homeInsights"></div>

              <div id="homeFAQ">
                <details class="faq" open>
                  <summary><strong data-i18n="home.faq1.q">¿Cómo interpretar este panel?</strong></summary>
                  <div class="muted" data-i18n="home.faq1.a">Las cifras muestran casos registrados en los datos. Usa el dashboard para filtrar por años, enfermedad y departamento. La tendencia te ayuda a ver si hay aumento o disminución.</div>
                </details>
                <details class="faq">
                  <summary><strong data-i18n="home.faq2.q">¿Qué significa “Cambio en el tiempo”?</strong></summary>
                  <div class="muted" data-i18n="home.faq2.a">Compara el primer y el último año disponibles en tu selección. Si seleccionas un solo año, no se calcula.</div>
                </details>
                <details class="faq">
                  <summary><strong data-i18n="home.faq3.q">¿De dónde salen los datos?</strong></summary>
                  <div class="muted" data-i18n="home.faq3.a">Provienen de fuentes públicas (datos.gov.co) integradas en un dataset maestro para análisis y visualización.</div>
                </details>
              </div>
            </div>
            <div class="stack">
              <div class="chart">
                <h3 data-i18n="home.preview.title">Vista previa: tendencia</h3>
                <canvas id="homeTrend"></canvas>
                <div class="chips" style="margin-top:10px;">
                  <span class="chip"><span style="width:10px;height:10px;border-radius:999px;background:#0284c7;display:inline-block;"></span> <strong>Dengue</strong></span>
                  <span class="chip"><span style="width:10px;height:10px;border-radius:999px;background:#10b981;display:inline-block;"></span> <strong>Zika</strong></span>
                  <span class="chip"><span style="width:10px;height:10px;border-radius:999px;background:#ef4444;display:inline-block;"></span> <strong>Chikungunya</strong></span>
                </div>
                <div class="muted" style="margin-top:8px;" data-i18n="home.preview.tip">Tip: selecciona varias enfermedades para comparar su comportamiento.</div>
              </div>

              <div class="chart">
                <h3 data-i18n="home.topMuni.title">Top municipios (casos)</h3>
                <canvas id="homeTopMuni"></canvas>
                <div class="muted" style="margin-top:8px;" data-i18n="home.topMuni.tip">Se actualiza con los mismos filtros del dashboard.</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>

    <section class="page" id="page-diseases">
      <div class="card" style="margin-top: 14px;">
        <div class="hd">
          <h2 data-i18n="dis.title">Enfermedades</h2>
          <div class="sub" data-i18n="dis.sub">Información clara para entender y prevenir</div>
        </div>
        <div class="bd">
          <div class="field" style="max-width: 560px;">
            <label data-i18n="dis.search.label">Buscador</label>
            <input id="diseaseSearch" placeholder="Escribe: dengue, zika, chikungunya…" data-i18n-placeholder="dis.search.ph" />
          </div>

          <div class="diseases-layout">
            <div>
              <div class="panel">
                <h4 data-i18n="dis.pick.title">Selecciona una enfermedad</h4>
                <div class="muted" data-i18n="dis.pick.desc">Verás una explicación sencilla y un resumen de datos para tu presentación.</div>
              </div>
              <div class="disease-list" id="diseaseList" style="margin-top: 12px;">
                <div class="disease-item" data-disease="DENGUE">
                  <h3><span class="tag">D</span> Dengue</h3>
                  <div class="muted" style="margin-top:6px;">Transmitida por mosquitos. Puede variar de leve a grave.</div>
                  <div class="chips">
                    <span class="chip"><span data-i18n="dis.cases">Casos</span>: <strong id="statDengueCases">—</strong></span>
                    <span class="chip"><span data-i18n="dis.change">Cambio</span>: <strong id="statDengueVar">—</strong></span>
                  </div>
                </div>
                <div class="disease-item" data-disease="ZIKA">
                  <h3><span class="tag">Z</span> Zika</h3>
                  <div class="muted" style="margin-top:6px;">Suele ser leve, pero requiere cuidado especial en embarazo.</div>
                  <div class="chips">
                    <span class="chip"><span data-i18n="dis.cases">Casos</span>: <strong id="statZikaCases">—</strong></span>
                    <span class="chip"><span data-i18n="dis.change">Cambio</span>: <strong id="statZikaVar">—</strong></span>
                  </div>
                </div>
                <div class="disease-item" data-disease="CHIKUNGUNYA">
                  <h3><span class="tag">C</span> Chikungunya</h3>
                  <div class="muted" style="margin-top:6px;">Dolor articular puede durar semanas. Prevención es clave.</div>
                  <div class="chips">
                    <span class="chip"><span data-i18n="dis.cases">Casos</span>: <strong id="statChikCases">—</strong></span>
                    <span class="chip"><span data-i18n="dis.change">Cambio</span>: <strong id="statChikVar">—</strong></span>
                  </div>
                </div>
              </div>

              <div class="panel compare-box">
                <h4 data-i18n="dis.compare.title">Comparar en el dashboard</h4>
                <div class="muted" data-i18n="dis.compare.desc">Elige 2 o 3 para compararlas en la gráfica de tendencia.</div>
                <div class="chips" id="compareChips" style="margin-top:10px;"></div>
                <div class="compare-actions">
                  <button class="btn primary" id="btnCompareGo" disabled data-i18n="dis.compare.go">Comparar ahora</button>
                  <button class="btn" id="btnCompareClear" data-i18n="dis.compare.clear">Limpiar</button>
                </div>
              </div>
            </div>

            <div class="card">
              <div class="hd">
                <h2 data-i18n="dis.detail.title">Detalle</h2>
                <div class="sub" data-i18n="dis.detail.sub">Educativo, simple y directo</div>
              </div>
              <div class="bd">
                <h3 class="detail-title" id="dTitle">—</h3>
                <p class="detail-lead" id="dLead" data-i18n="dis.detail.lead">Selecciona una enfermedad para ver información.</p>

                <div class="kpis" style="margin-top: 12px; grid-template-columns: repeat(3, 1fr);">
                  <div class="kpi" style="min-height: 82px;">
                    <div class="label" data-i18n="dis.detail.kpiCases">Casos (dataset)</div>
                    <div class="value" id="dCases">—</div>
                    <div class="desc" data-i18n="dis.detail.kpiCasesDesc">Total disponible</div>
                  </div>
                  <div class="kpi" style="min-height: 82px;">
                    <div class="label" data-i18n="kpi.changeOverTime">Cambio en el tiempo</div>
                    <div class="value" id="dVar">—</div>
                    <div class="desc" id="dVarDesc" data-i18n="dis.detail.kpiTrend">Tendencia</div>
                  </div>
                  <div class="kpi" style="min-height: 82px;">
                    <div class="label" data-i18n="kpi.topRegion">Región más afectada</div>
                    <div class="value" id="dTopDept">—</div>
                    <div class="desc" data-i18n="kpi.byCases">Por número de casos</div>
                  </div>
                </div>

                <div class="actions" style="margin-top: 12px;">
                  <button class="btn primary" id="dBtnView" data-i18n="dis.detail.viewDash">Ver en el Dashboard</button>
                  <button class="btn" id="dBtnAdd" data-i18n="dis.detail.addCompare">Añadir a comparación</button>
                </div>

                <div class="detail-cols">
                  <div class="panel">
                    <h4 data-i18n="dis.detail.symptoms">Síntomas comunes</h4>
                    <ul id="dSymptoms"></ul>
                  </div>
                  <div class="panel">
                    <h4 data-i18n="dis.detail.prevention">Prevención</h4>
                    <ul id="dPrevention"></ul>
                  </div>
                </div>

                <div class="panel" style="margin-top: 12px;">
                  <h4 data-i18n="dis.detail.when">Cuándo consultar</h4>
                  <ul id="dWhen"></ul>
                </div>

                <div class="panel" style="margin-top: 12px;">
                  <h4 data-i18n="dis.detail.noteTitle">Nota importante</h4>
                  <div class="muted" data-i18n="dis.detail.noteText">Este sitio es educativo y no reemplaza una consulta médica. Si tienes síntomas graves o persistentes, busca atención profesional.</div>
                </div>
              </div>
            </div>
          </div>

          <div class="panel" style="margin-top: 12px;">
            <h4 data-i18n="dis.faq.title">Preguntas frecuentes</h4>
            <details class="faq">
              <summary><strong data-i18n="dis.faq1.q">¿Qué significa “brote”?</strong></summary>
              <div class="muted" data-i18n="dis.faq1.a">Es una señal de alerta cuando los casos superan lo esperado para ese municipio y enfermedad. Sirve para orientar la atención, no para alarmar.</div>
            </details>
            <details class="faq">
              <summary><strong data-i18n="dis.faq2.q">¿Por qué algunas enfermedades suben en ciertas épocas?</strong></summary>
              <div class="muted" data-i18n="dis.faq2.a">La lluvia y la temperatura influyen en la presencia de mosquitos. También afectan factores como movilidad, prevención y acceso a servicios de salud.</div>
            </details>
            <details class="faq">
              <summary><strong data-i18n="dis.faq3.q">¿Qué puedo hacer en casa para reducir el riesgo?</strong></summary>
              <div class="muted" data-i18n="dis.faq3.a">Elimina agua estancada, lava recipientes, usa repelente, coloca mosquiteros y revisa patios y canaletas semanalmente.</div>
            </details>
            <details class="faq">
              <summary><strong data-i18n="dis.faq4.q">¿Qué significa “región más afectada” en el dashboard?</strong></summary>
              <div class="muted" data-i18n="dis.faq4.a">Es el departamento con más casos dentro de tu selección (filtros). Puedes cambiarlo al filtrar por años o enfermedades.</div>
            </details>
          </div>
        </div>
      </div>
    </section>

    <section class="page" id="page-dashboard">
      <div class="grid" style="margin-top: 14px;">
        <div class="card filters">
          <div class="hd">
            <h2>Filtros</h2>
            <div class="sub">Actualiza todo al instante</div>
          </div>
          <div class="bd">
            <div class="field">
              <label>Buscador <span class="tip" title="Busca por departamento, municipio o enfermedad">?</span></label>
              <input id="q" placeholder="Ej: DENGUE, ANTIOQUIA, BOGOTA" />
            </div>

            <div class="row">
              <div class="field">
                <label>Desde (año)</label>
                <select id="anoMin"><option value="">(cualquiera)</option></select>
              </div>
              <div class="field">
                <label>Hasta (año)</label>
                <select id="anoMax"><option value="">(cualquiera)</option></select>
              </div>
            </div>

            <div class="field">
              <label>Enfermedades (puedes elegir varias) <span class="tip" title="Selecciona 1 o varias para comparar en la tendencia">?</span></label>
              <select id="enfermedades" multiple size="5"></select>
              <div class="muted" style="margin-top:6px;">Tip: Ctrl/Shift para seleccionar varias en PC.</div>
            </div>

            <div class="field">
              <label>Departamento <span class="tip" title="También puedes hacer clic en el mapa para seleccionar un departamento">?</span></label>
              <select id="departamento"><option value="">(cualquiera)</option></select>
            </div>

            <div class="row">
              <div class="field">
                <label>Edad <span class="tip" title="Este dataset no incluye edad, por eso no se puede filtrar aún">?</span></label>
                <select disabled><option>No disponible</option></select>
              </div>
              <div class="field">
                <label>Género <span class="tip" title="Este dataset no incluye género, por eso no se puede filtrar aún">?</span></label>
                <select disabled><option>No disponible</option></select>
              </div>
            </div>

            <div class="footerbar" style="margin-top: 10px;">
              <button class="btn" id="btnReset">Reset</button>
              <button class="btn primary" id="btnApply">Aplicar</button>
            </div>

            <div class="muted" style="margin-top: 12px;">
              <div><strong>Fuente local:</strong> __CSV_PATH__</div>
            </div>
          </div>
        </div>

        <div>
          <div class="card">
            <div class="hd">
              <h2>Resumen</h2>
              <div class="sub">Lo más importante, en segundos</div>
            </div>
            <div class="bd">
              <div class="kpis">
                <div class="kpi">
                  <div class="label">Casos registrados</div>
                  <div class="value" id="kpiCases">—</div>
                  <div class="desc" id="kpiCasesDesc">Total en la selección</div>
                </div>
                <div class="kpi">
                  <div class="label">Enfermedad más común</div>
                  <div class="value" id="kpiTopDis">—</div>
                  <div class="desc">Por número de casos</div>
                </div>
                <div class="kpi">
                  <div class="label">Región más afectada</div>
                  <div class="value" id="kpiTopDept">—</div>
                  <div class="desc">Departamento con más casos</div>
                </div>
                <div class="kpi">
                  <div class="label">Cambio en el tiempo</div>
                  <div class="value" id="kpiVar">—</div>
                  <div class="desc" id="kpiVarDesc">Comparación por años disponibles</div>
                </div>
              </div>

              <div class="viz">
                <div class="chart">
                  <h3 id="dashTrend">Tendencia de casos</h3>
                  <canvas id="chartTrend"></canvas>
                  <div class="muted" id="trendHint" style="margin-top:8px;">Compara enfermedades y observa si suben o bajan.</div>
                </div>
                <div class="chart">
                  <h3>Distribución por enfermedad</h3>
                  <canvas id="chartDisease"></canvas>
                  <div class="muted" style="margin-top:8px;">Muestra las más frecuentes en tu selección.</div>
                </div>
              </div>

              <div class="viz" style="grid-template-columns: 1fr 1fr;">
                <div class="chart">
                  <h3 id="dashMap">Mapa (casos por departamento)</h3>
                  <div class="map-wrap">
                    <div class="map" id="mapBox"></div>
                    <div class="legend">
                      <span>Menos</span><span class="grad"></span><span>Más</span>
                      <span class="muted" id="mapHint"></span>
                    </div>
                  </div>
                </div>
                <div class="chart">
                  <h3>Top municipios</h3>
                  <canvas id="chartTopMuni"></canvas>
                  <div class="muted" style="margin-top:8px;">Los 8 municipios con más casos (en la selección).</div>
                </div>
              </div>
            </div>
          </div>

          <div class="card" style="margin-top: 14px;">
            <div class="hd">
              <h2>Insights automáticos</h2>
              <div class="sub">Conclusiones en lenguaje simple</div>
            </div>
            <div class="bd">
              <div class="insights" id="insights"></div>
            </div>
          </div>

          <div class="card chat" style="margin-top: 14px;">
            <div class="hd">
              <h2>Pregúntale al dashboard</h2>
              <div class="sub">Ej: “¿Qué enfermedad ha crecido más?” (escribe “ayuda” para ver más)</div>
            </div>
            <div class="bd">
              <div class="row" style="grid-template-columns: 1fr auto;">
                <input id="askInput" placeholder="Escribe tu pregunta…" />
                <button class="btn primary" id="btnAsk">Preguntar</button>
              </div>
              <div class="insight" id="askAnswer" style="margin-top:10px;">Escribe una pregunta para obtener una respuesta rápida.</div>
            </div>
          </div>

          <div class="card" style="margin-top: 14px;">
            <div class="hd">
              <h2>Tabla</h2>
              <div class="sub"><span class="pill" id="pillCount">Filas: —</span></div>
            </div>
            <div class="bd">
              <div class="footerbar" style="margin-bottom: 10px;">
                <div class="row" style="grid-template-columns: 140px auto auto 1fr; align-items:end; gap:10px;">
                  <div class="field" style="margin:0;">
                    <label>Por página</label>
                    <input id="limit" value="50" />
                  </div>
                  <button class="btn" id="btnPrev">Anterior</button>
                  <button class="btn" id="btnNext">Siguiente</button>
                  <div class="muted" id="pageInfo" style="align-self:center; justify-self:end;">—</div>
                </div>
              </div>
              <div class="table-wrap">
                <table id="table">
                  <thead><tr id="thead"></tr></thead>
                  <tbody id="tbody"></tbody>
                </table>
              </div>
            </div>
          </div>

        </div>
      </div>
    </section>
  </div>

  <div class="toast" id="toast"></div>
  <div class="tooltip" id="tooltip"></div>

  <div class="chat-mini" id="homeChatMini">
    <div class="bubble" id="homeChatOpen" title="Asistente de salud">💬</div>
  </div>
  <div class="chat-float" id="homeChat">
    <div class="hd">
      <div class="title">
        <span class="dot"></span>
        <div style="min-width:0;">
          <h3>Asistente de salud</h3>
          <div class="muted">Pregúntame sobre las enfermedades o sobre lo que ves en los datos</div>
        </div>
      </div>
      <div style="display:flex; gap:8px; align-items:center;">
        <button class="btn ghost" id="homeChatReset" title="Nueva conversación">Nuevo</button>
        <button class="btn ghost" id="homeChatClose" title="Cerrar">Cerrar</button>
      </div>
    </div>
    <div class="chat-body" id="homeChatBody">
      <div class="msg bot">Hola. Puedo ayudarte a entender <strong>dengue</strong>, <strong>zika</strong> y <strong>chikungunya</strong>, y también a interpretar tendencias por año y región. ¿Qué te gustaría saber?</div>
    </div>
    <div class="chat-tools">
      <div class="chat-row">
        <input id="homeAskInput" placeholder="Escribe tu pregunta…" />
        <button class="btn primary" id="homeAskSend">Enviar</button>
      </div>
      <div class="chat-quick">
        <button class="qbtn" data-q="¿Qué enfermedad ha crecido más en los últimos años?">Crecimiento</button>
        <button class="qbtn" data-q="¿Cuál es la región más afectada?">Región</button>
        <button class="qbtn" data-q="¿Cuáles son los síntomas del dengue?">Síntomas</button>
        <button class="qbtn" data-q="¿Cómo prevenir chikungunya?">Prevención</button>
      </div>
    </div>
  </div>

  <script>
    let offset = 0;
    let total = 0;
    let columns = [];
    let mapData = null;
    let homeChatMessages = [];

    function qs(id) { return document.getElementById(id); }

    function setLayoutVars() {
      const root = document.documentElement;
      const header = document.querySelector('.header');
      const nav = document.getElementById('nav');
      const h = header ? header.getBoundingClientRect().height : 0;
      const n = nav ? nav.getBoundingClientRect().height : 0;
      root.style.setProperty('--headerH', Math.round(h) + 'px');
      root.style.setProperty('--navH', Math.round(n) + 'px');
    }

    function toast(msg) {
      const el = qs('toast');
      el.textContent = msg;
      el.style.display = 'block';
      setTimeout(function() { el.style.display = 'none'; }, 4200);
    }

    function setStatus(text, loading) {
      qs('statusText').textContent = text;
      qs('spin').style.display = loading ? 'inline-block' : 'none';
    }

    async function fetchJSON(url) {
      const res = await fetch(url);
      const txt = await res.text();
      try { return JSON.parse(txt); } catch (e) { throw new Error(txt.slice(0, 250)); }
    }

    function setActiveNav(route) {
      const items = [
        { id: 'navHome', route: '/inicio' },
        { id: 'navDiseases', route: '/enfermedades' },
        { id: 'navDash', route: '/dashboard' },
      ];
      items.forEach(x => {
        const el = qs(x.id);
        if (!el) return;
        if (route === x.route) el.classList.add('active');
        else el.classList.remove('active');
      });
    }

    function showPage(route) {
      const r = route || '/inicio';
      const map = {
        '/inicio': 'page-home',
        '/enfermedades': 'page-diseases',
        '/dashboard': 'page-dashboard',
      };
      const target = map[r] || 'page-home';
      document.body.setAttribute('data-route', (r || '/inicio').replace('/', ''));
      ['page-home', 'page-diseases', 'page-dashboard'].forEach(id => {
        const el = qs(id);
        if (!el) return;
        if (id === target) el.classList.add('active');
        else el.classList.remove('active');
      });
      setActiveNav(r);
      setTimeout(function() {
        try { setHomeChatVisible(); } catch (e) {}
        try { scheduleRefresh(); } catch (e) {}
      }, 80);
    }

    function getRoute() {
      const h = (location.hash || '').replace(/^#/, '');
      if (!h) return '/inicio';
      if (h.startsWith('/')) return h;
      return '/inicio';
    }

    function debounce(fn, wait) {
      let t = null;
      return function() {
        const args = arguments;
        clearTimeout(t);
        t = setTimeout(function() { fn.apply(null, args); }, wait);
      }
    }

    function fmt(n) {
      if (n === null || n === undefined) return '—';
      const x = Number(n);
      if (!isFinite(x)) return '—';
      return x.toLocaleString();
    }

    function normalize(s) {
      return (s || '')
        .normalize('NFKD')
        .replace(/[\\u0300-\\u036f]/g, '')
        .toUpperCase()
        .replace(/[^A-Z0-9]+/g, ' ')
        .trim();
    }

    function getSelectedMulti(selectEl) {
      const out = [];
      Array.from(selectEl.options).forEach(o => { if (o.selected) out.push(o.value); });
      return out;
    }

    function getParams() {
      const enfermedades = getSelectedMulti(qs('enfermedades'));
      const p = {
        q: qs('q').value.trim(),
        departamento: (qs('departamento').value || '').trim(),
        enfermedad: enfermedades.join(','),
        ano_min: (qs('anoMin').value || '').trim(),
        ano_max: (qs('anoMax').value || '').trim(),
      };
      const clean = {};
      Object.keys(p).forEach(k => { if (p[k]) clean[k] = p[k]; });
      return clean;
    }

    function updateExportLink() {
      const params = new URLSearchParams(getParams());
      qs('btnExportFiltered').href = '/download_filtered?' + params.toString();
    }

    function setTheme(mode) {
      document.body.setAttribute('data-theme', mode);
      localStorage.setItem('theme', mode);
    }

    function initTheme() {
      const saved = localStorage.getItem('theme');
      if (saved === 'dark' || saved === 'light') setTheme(saved);
    }

    const I18N = {
      es: {
        'app.title': 'Enfermedades en Colombia',
        'app.subtitle': 'Explora la evolución de los casos por año, enfermedad y departamento. Diseñado para entenderse rápido, sin tecnicismos.',
        'status.ready': 'Listo',
        'btn.theme': 'Modo',
        'btn.exportFiltered': 'Exportar (filtrado)',
        'btn.pdf': 'PDF',
        'btn.powerbi': 'Power BI',
        'btn.csvFull': 'CSV completo',
        'nav.home': 'Inicio',
        'nav.diseases': 'Enfermedades',
        'nav.dashboard': 'Dashboard',
        'home.title': 'Inicio',
        'home.sub': 'Portal educativo + análisis visual',
        'kpi.totalCases': 'Casos registrados',
        'kpi.totalDataset': 'Total del dataset',
        'kpi.topDisease': 'Enfermedad más común',
        'kpi.byCases': 'Por número de casos',
        'kpi.topRegion': 'Región más afectada',
        'kpi.deptMostCases': 'Departamento con más casos',
        'kpi.changeOverTime': 'Cambio en el tiempo',
        'kpi.overallTrend': 'Tendencia general',
        'home.ctaDash': 'Explorar Dashboard →',
        'home.ctaGuide': 'Guía de enfermedades',
        'home.ctaFaq': 'Preguntas frecuentes',
        'home.tip': 'Consejo: filtra por años y compara enfermedades',
        'home.feature.trends.title': 'Ver tendencias',
        'home.feature.trends.desc': 'Identifica si los casos suben o bajan en el tiempo y compara enfermedades.',
        'home.feature.regions.title': 'Explorar regiones',
        'home.feature.regions.desc': 'Ubica departamentos con mayor incidencia y descubre patrones geográficos.',
        'home.feature.learn.title': 'Aprender y prevenir',
        'home.feature.learn.desc': 'Conoce síntomas comunes, prevención y cuándo consultar para actuar a tiempo.',
        'home.learnQuick.title': 'Aprender y prevenir (guía rápida)',
        'home.learnQuick.more': 'Ver guía completa',
        'home.learnQuick.askIdeas': '¿Qué puedo preguntar?',
        'home.faq1.q': '¿Cómo interpretar este panel?',
        'home.faq1.a': 'Las cifras muestran casos registrados en los datos. Usa el dashboard para filtrar por años, enfermedad y departamento. La tendencia te ayuda a ver si hay aumento o disminución.',
        'home.faq2.q': '¿Qué significa “Cambio en el tiempo”?',
        'home.faq2.a': 'Compara el primer y el último año disponibles en tu selección. Si seleccionas un solo año, no se calcula.',
        'home.faq3.q': '¿De dónde salen los datos?',
        'home.faq3.a': 'Provienen de fuentes públicas (datos.gov.co) integradas en un dataset maestro para análisis y visualización.',
        'home.preview.title': 'Vista previa: tendencia',
        'home.preview.tip': 'Tip: selecciona varias enfermedades para comparar su comportamiento.',
        'home.topMuni.title': 'Top municipios (casos)',
        'home.topMuni.tip': 'Se actualiza con los mismos filtros del dashboard.',
        'powerbi.missing': 'Aún no hay archivo Power BI (.pbix). Cuando lo tengas, colócalo en data/processed/reporte_powerbi.pbix o inicia el servidor con --pbix RUTA.',
        'powerbi.error': 'No se pudo verificar el archivo de Power BI.',
        'dis.title': 'Enfermedades',
        'dis.sub': 'Información clara para entender y prevenir',
        'dis.search.label': 'Buscador',
        'dis.search.ph': 'Escribe: dengue, zika, chikungunya…',
        'dis.pick.title': 'Selecciona una enfermedad',
        'dis.pick.desc': 'Verás una explicación sencilla y un resumen de datos para tu presentación.',
        'dis.cases': 'Casos',
        'dis.change': 'Cambio',
        'dis.compare.title': 'Comparar en el dashboard',
        'dis.compare.desc': 'Elige 2 o 3 para compararlas en la gráfica de tendencia.',
        'dis.compare.go': 'Comparar ahora',
        'dis.compare.clear': 'Limpiar',
        'dis.detail.title': 'Detalle',
        'dis.detail.sub': 'Educativo, simple y directo',
        'dis.detail.lead': 'Selecciona una enfermedad para ver información.',
        'dis.detail.kpiCases': 'Casos (dataset)',
        'dis.detail.kpiCasesDesc': 'Total disponible',
        'dis.detail.kpiTrend': 'Tendencia',
        'dis.detail.viewDash': 'Ver en el Dashboard',
        'dis.detail.addCompare': 'Añadir a comparación',
        'dis.detail.symptoms': 'Síntomas comunes',
        'dis.detail.prevention': 'Prevención',
        'dis.detail.when': 'Cuándo consultar',
        'dis.detail.noteTitle': 'Nota importante',
        'dis.detail.noteText': 'Este sitio es educativo y no reemplaza una consulta médica. Si tienes síntomas graves o persistentes, busca atención profesional.',
        'dis.faq.title': 'Preguntas frecuentes',
        'dis.faq1.q': '¿Qué significa “brote”?',
        'dis.faq1.a': 'Es una señal de alerta cuando los casos superan lo esperado para ese municipio y enfermedad. Sirve para orientar la atención, no para alarmar.',
        'dis.faq2.q': '¿Por qué algunas enfermedades suben en ciertas épocas?',
        'dis.faq2.a': 'La lluvia y la temperatura influyen en la presencia de mosquitos. También afectan factores como movilidad, prevención y acceso a servicios de salud.',
        'dis.faq3.q': '¿Qué puedo hacer en casa para reducir el riesgo?',
        'dis.faq3.a': 'Elimina agua estancada, lava recipientes, usa repelente, coloca mosquiteros y revisa patios y canaletas semanalmente.',
        'dis.faq4.q': '¿Qué significa “región más afectada” en el dashboard?',
        'dis.faq4.a': 'Es el departamento con más casos dentro de tu selección (filtros). Puedes cambiarlo al filtrar por años o enfermedades.',
      },
      en: {
        'app.title': 'Diseases in Colombia',
        'app.subtitle': 'Explore how cases evolve by year, disease, and department. Designed to be clear and non-technical.',
        'status.ready': 'Ready',
        'btn.theme': 'Theme',
        'btn.exportFiltered': 'Export (filtered)',
        'btn.pdf': 'PDF',
        'btn.powerbi': 'Power BI',
        'btn.csvFull': 'Full CSV',
        'nav.home': 'Home',
        'nav.diseases': 'Diseases',
        'nav.dashboard': 'Dashboard',
        'home.title': 'Home',
        'home.sub': 'Educational portal + visual insights',
        'kpi.totalCases': 'Reported cases',
        'kpi.totalDataset': 'Dataset total',
        'kpi.topDisease': 'Most common disease',
        'kpi.byCases': 'By number of cases',
        'kpi.topRegion': 'Most affected region',
        'kpi.deptMostCases': 'Department with most cases',
        'kpi.changeOverTime': 'Change over time',
        'kpi.overallTrend': 'Overall trend',
        'home.ctaDash': 'Explore Dashboard →',
        'home.ctaGuide': 'Disease guide',
        'home.ctaFaq': 'FAQ',
        'home.tip': 'Tip: filter by years and compare diseases',
        'home.feature.trends.title': 'See trends',
        'home.feature.trends.desc': 'Check whether cases go up or down over time and compare diseases.',
        'home.feature.regions.title': 'Explore regions',
        'home.feature.regions.desc': 'Identify departments with higher incidence and discover geographic patterns.',
        'home.feature.learn.title': 'Learn & prevent',
        'home.feature.learn.desc': 'Learn common symptoms, prevention, and when to seek care.',
        'home.learnQuick.title': 'Learn & prevent (quick guide)',
        'home.learnQuick.more': 'Open full guide',
        'home.learnQuick.askIdeas': 'What can I ask?',
        'home.faq1.q': 'How do I read this panel?',
        'home.faq1.a': 'Numbers reflect cases recorded in the dataset. Use the dashboard to filter by years, disease, and department. The trend helps you see increases or decreases.',
        'home.faq2.q': 'What does “Change over time” mean?',
        'home.faq2.a': 'It compares the first and last year in your selection. If you pick a single year, it cannot be computed.',
        'home.faq3.q': 'Where does the data come from?',
        'home.faq3.a': 'It comes from public sources (datos.gov.co) combined into a master dataset for analysis and visualization.',
        'home.preview.title': 'Preview: trend',
        'home.preview.tip': 'Tip: select multiple diseases to compare their behavior.',
        'home.topMuni.title': 'Top municipalities (cases)',
        'home.topMuni.tip': 'Updates with the same dashboard filters.',
        'powerbi.missing': 'Power BI file (.pbix) is not available yet. When you have it, place it in data/processed/reporte_powerbi.pbix or start the server with --pbix PATH.',
        'powerbi.error': 'Could not check the Power BI file.',
        'dis.title': 'Diseases',
        'dis.sub': 'Clear information to understand and prevent',
        'dis.search.label': 'Search',
        'dis.search.ph': 'Type: dengue, zika, chikungunya…',
        'dis.pick.title': 'Pick a disease',
        'dis.pick.desc': 'You will see a simple explanation and a data summary for your presentation.',
        'dis.cases': 'Cases',
        'dis.change': 'Change',
        'dis.compare.title': 'Compare in the dashboard',
        'dis.compare.desc': 'Choose 2–3 to compare them in the trend chart.',
        'dis.compare.go': 'Compare now',
        'dis.compare.clear': 'Clear',
        'dis.detail.title': 'Details',
        'dis.detail.sub': 'Educational, simple, and direct',
        'dis.detail.lead': 'Select a disease to view information.',
        'dis.detail.kpiCases': 'Cases (dataset)',
        'dis.detail.kpiCasesDesc': 'Total available',
        'dis.detail.kpiTrend': 'Trend',
        'dis.detail.viewDash': 'View in Dashboard',
        'dis.detail.addCompare': 'Add to comparison',
        'dis.detail.symptoms': 'Common symptoms',
        'dis.detail.prevention': 'Prevention',
        'dis.detail.when': 'When to seek care',
        'dis.detail.noteTitle': 'Important note',
        'dis.detail.noteText': 'This site is educational and does not replace medical advice. If you have severe or persistent symptoms, seek professional care.',
        'dis.faq.title': 'FAQ',
        'dis.faq1.q': 'What does “outbreak” mean?',
        'dis.faq1.a': 'It is an alert signal when cases exceed what is expected for that municipality and disease. It helps guide attention, not alarm.',
        'dis.faq2.q': 'Why do some diseases increase in certain seasons?',
        'dis.faq2.a': 'Rain and temperature affect mosquito presence. Mobility, prevention, and access to care can also influence trends.',
        'dis.faq3.q': 'What can I do at home to reduce risk?',
        'dis.faq3.a': 'Remove standing water, clean containers, use repellent, use bed nets/screens, and check patios and gutters weekly.',
        'dis.faq4.q': 'What does “most affected region” mean?',
        'dis.faq4.a': 'It is the department with the most cases within your current filters. It will change as you adjust years or diseases.',
      },
    };

    function getLang() {
      const saved = localStorage.getItem('lang');
      if (saved === 'en' || saved === 'es') return saved;
      const nav = (navigator.language || '').toLowerCase();
      return nav.startsWith('en') ? 'en' : 'es';
    }

    function setLang(lang) {
      localStorage.setItem('lang', lang);
      applyLang(lang);
    }

    function t(key) {
      const lang = getLang();
      return (I18N[lang] && I18N[lang][key]) ? I18N[lang][key] : (I18N.es[key] || key);
    }

    function applyLang(lang) {
      const l = (lang === 'en') ? 'en' : 'es';
      document.documentElement.lang = l;
      document.title = I18N[l]['app.title'] || document.title;

      Array.from(document.querySelectorAll('[data-i18n]')).forEach(el => {
        const key = el.getAttribute('data-i18n') || '';
        if (!key) return;
        el.textContent = (I18N[l][key] || I18N.es[key] || el.textContent);
      });
      Array.from(document.querySelectorAll('[data-i18n-placeholder]')).forEach(el => {
        const key = el.getAttribute('data-i18n-placeholder') || '';
        if (!key) return;
        el.setAttribute('placeholder', (I18N[l][key] || I18N.es[key] || el.getAttribute('placeholder') || ''));
      });

      const btn = document.getElementById('btnLang');
      if (btn) btn.textContent = (l === 'en') ? 'EN' : 'ES';

      const guide = document.getElementById('homeGuideList');
      if (guide) {
        const items = (l === 'en')
          ? [
              '<strong>Remove breeding sites:</strong> empty, cover, and clean containers with water.',
              '<strong>Protect your skin:</strong> use repellent and wear clothing that covers arms and legs.',
              '<strong>Protect your home:</strong> bed nets, window screens, and ventilation.',
              '<strong>Warning signs:</strong> bleeding, severe pain, or breathing difficulty → seek care.',
            ]
          : [
              '<strong>Evita criaderos:</strong> vacía, tapa y limpia recipientes con agua.',
              '<strong>Protege tu piel:</strong> usa repelente y ropa que cubra brazos y piernas.',
              '<strong>Protege tu casa:</strong> mosquiteros, mallas y ventilación.',
              '<strong>Señales de alarma:</strong> si hay sangrado, dolor intenso o dificultad respiratoria, busca atención.',
            ];
        guide.innerHTML = items.map(x => '<li>' + x + '</li>').join('');
      }

      const chat = document.getElementById('homeChat');
      if (chat) {
        const header = chat.querySelector('.hd .muted');
        if (header) header.textContent = (l === 'en')
          ? 'Ask me about diseases or what you see in the data'
          : 'Pregúntame sobre las enfermedades o sobre lo que ves en los datos';
      }
    }

    function initLang() {
      applyLang(getLang());
      const btn = document.getElementById('btnLang');
      if (btn) {
        btn.addEventListener('click', function() {
          const cur = getLang();
          setLang(cur === 'es' ? 'en' : 'es');
          try { scheduleRefresh(); } catch (e) {}
        });
      }
    }

    function canvasSize(canvas, height) {
      const dpr = window.devicePixelRatio || 1;
      const w = canvas.clientWidth;
      const h = height;
      canvas.width = Math.floor(w * dpr);
      canvas.height = Math.floor(h * dpr);
      const ctx = canvas.getContext('2d');
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      return { ctx, w, h };
    }

    function drawAxes(ctx, w, h, pad) {
      ctx.strokeStyle = 'rgba(15,23,42,0.12)';
      if (document.body.getAttribute('data-theme') === 'dark') ctx.strokeStyle = 'rgba(255,255,255,0.14)';
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(pad.l, pad.t);
      ctx.lineTo(pad.l, h - pad.b);
      ctx.lineTo(w - pad.r, h - pad.b);
      ctx.stroke();
    }

    function diseaseColor(name, idx) {
      const n = normalize(name);
      if (n.includes('CHIKUNGUNYA')) return '#ef4444';
      if (n.includes('DENGUE')) return '#0284c7';
      if (n.includes('ZIKA')) return '#10b981';
      const fallback = ['#0284c7', '#10b981', '#6366f1', '#f59e0b', '#a855f7'];
      return fallback[idx % fallback.length];
    }

    function drawLineMulti(canvas, labels, series) {
      const { ctx, w, h } = canvasSize(canvas, 260);
      ctx.clearRect(0, 0, w, h);
      ctx.font = '12px ui-sans-serif, system-ui, Segoe UI, Roboto, Arial';
      const pad = { l: 44, r: 12, t: 10, b: 26 };
      drawAxes(ctx, w, h, pad);
      const iw = w - pad.l - pad.r;
      const ih = h - pad.t - pad.b;
      const all = [];
      series.forEach(s => (s.data || []).forEach(v => all.push(Number(v) || 0)));
      const maxV = Math.max(1, ...all);
      const ticks = 4;

      for (let i = 0; i <= ticks; i++) {
        const t = i / ticks;
        const y = pad.t + ih - t * ih;
        ctx.strokeStyle = document.body.getAttribute('data-theme') === 'dark' ? 'rgba(255,255,255,0.08)' : 'rgba(15,23,42,0.08)';
        ctx.beginPath();
        ctx.moveTo(pad.l, y);
        ctx.lineTo(pad.l + iw, y);
        ctx.stroke();
        ctx.fillStyle = document.body.getAttribute('data-theme') === 'dark' ? 'rgba(255,255,255,0.55)' : 'rgba(15,23,42,0.55)';
        ctx.fillText(Math.round(t * maxV).toLocaleString(), 6, y + 4);
      }

      series.forEach((s, idx) => {
        const data = s.data || [];
        if (!data.length) return;
        ctx.strokeStyle = diseaseColor(s.name || '', idx);
        ctx.lineWidth = 2.25;
        ctx.beginPath();
        for (let i = 0; i < data.length; i++) {
          const x = pad.l + (i / Math.max(1, data.length - 1)) * iw;
          const v = Number(data[i]) || 0;
          const y = pad.t + ih - (v / maxV) * ih;
          if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
        }
        ctx.stroke();
      });

      const legendY = h - 8;
      let x = pad.l;
      series.slice(0, 5).forEach((s, idx) => {
        const label = String(s.name || '').slice(0, 18);
        ctx.fillStyle = diseaseColor(s.name || '', idx);
        ctx.fillRect(x, legendY - 9, 10, 10);
        ctx.fillStyle = document.body.getAttribute('data-theme') === 'dark' ? 'rgba(255,255,255,0.78)' : 'rgba(15,23,42,0.78)';
        ctx.fillText(label, x + 14, legendY);
        x += 14 + ctx.measureText(label).width + 14;
      });
    }

    function drawBars(canvas, items, color) {
      const { ctx, w, h } = canvasSize(canvas, 260);
      ctx.clearRect(0, 0, w, h);
      ctx.font = '12px ui-sans-serif, system-ui, Segoe UI, Roboto, Arial';
      const pad = { l: 12, r: 12, t: 10, b: 42 };
      const iw = w - pad.l - pad.r;
      const ih = h - pad.t - pad.b;
      const values = items.map(x => Number(x.value) || 0);
      const maxV = Math.max(1, ...values);
      const n = Math.max(1, items.length);
      const gap = 10;
      const barW = Math.max(18, (iw - gap * (n - 1)) / n);
      const baseFill = document.body.getAttribute('data-theme') === 'dark' ? 'rgba(255,255,255,0.10)' : 'rgba(2,8,23,0.06)';
      for (let i = 0; i < items.length; i++) {
        const x = pad.l + i * (barW + gap);
        const v = Number(items[i].value) || 0;
        const bh = (v / maxV) * ih;
        const y = pad.t + (ih - bh);
        ctx.fillStyle = baseFill;
        ctx.fillRect(x, pad.t, barW, ih);
        ctx.fillStyle = items[i].color || color || '#10b981';
        ctx.fillRect(x, y, barW, bh);
        ctx.save();
        ctx.translate(x + barW / 2, h - 10);
        ctx.rotate(-Math.PI / 6);
        ctx.textAlign = 'center';
        ctx.fillStyle = document.body.getAttribute('data-theme') === 'dark' ? 'rgba(255,255,255,0.75)' : 'rgba(15,23,42,0.70)';
        ctx.fillText(String(items[i].label).slice(0, 14), 0, 0);
        ctx.restore();
      }
    }

    function buildMap() {
      const box = qs('mapBox');
      box.innerHTML = '';
      if (!mapData || !mapData.map || !mapData.map.locations) {
        box.innerHTML = '<div class="muted" style="padding:10px;">Mapa no disponible. Verifica conexión a internet.</div>';
        return;
      }

      const viewBox = mapData.map.viewBox;
      const locations = mapData.map.locations;
      const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
      svg.setAttribute('viewBox', viewBox);
      svg.setAttribute('aria-label', 'Mapa de Colombia por departamentos');

      locations.forEach(loc => {
        const p = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        p.setAttribute('d', loc.path);
        p.setAttribute('data-name', loc.name);
        p.setAttribute('data-id', loc.id);
        p.style.fill = '#e0f2fe';
        p.style.stroke = document.body.getAttribute('data-theme') === 'dark' ? 'rgba(255,255,255,0.18)' : 'rgba(15,23,42,0.18)';
        p.style.strokeWidth = '0.8';
        p.style.cursor = 'pointer';
        p.addEventListener('mousemove', onMapHover);
        p.addEventListener('mouseleave', onMapLeave);
        p.addEventListener('click', function() {
          const name = String(loc.name || '');
          const depSel = qs('departamento');
          const targetNorm = normalize(name);
          const opts = Array.from(depSel.options).map(o => ({ val: o.value, norm: normalize(o.value) }));
          const found = opts.find(o =>
            o.norm === targetNorm ||
            (targetNorm.includes(o.norm) && o.norm.length > 4) ||
            (o.norm.includes(targetNorm) && targetNorm.length > 4)
          );
          if (found) {
            depSel.value = found.val;
            scheduleRefresh();
          } else {
            toast('No pude emparejar este departamento con el dataset: ' + name);
          }
        });
        svg.appendChild(p);
      });

      box.appendChild(svg);
    }

    function onMapHover(ev) {
      const tip = qs('tooltip');
      const el = ev.target;
      const name = el.getAttribute('data-name') || '';
      const cases = el.getAttribute('data-cases') || '0';
      tip.innerHTML = '<strong>' + name + '</strong><div class="muted">Casos: ' + fmt(cases) + '</div><div class="muted">Clic para filtrar</div>';
      tip.style.display = 'block';
      tip.style.left = (ev.clientX + 12) + 'px';
      tip.style.top = (ev.clientY + 12) + 'px';
    }

    function onMapLeave() {
      qs('tooltip').style.display = 'none';
    }

    function updateMapColors(rows) {
      const svg = qs('mapBox').querySelector('svg');
      if (!svg) return;
      const byDept = {};
      const alias = {
        'BOGOTA DC': 'BOGOTA',
        'BOGOTA D C': 'BOGOTA',
        'DISTRITO CAPITAL DE BOGOTA': 'BOGOTA',
        'NORTE DE SANTANDER': 'NORTH SANTANDER',
        'SAN ANDRES PROVIDENCIA Y SANTA CATALINA': 'SAN ANDRES Y PROVIDENCIA',
        'ARCHIPIELAGO DE SAN ANDRES PROVIDENCIA Y SANTA CATALINA': 'SAN ANDRES Y PROVIDENCIA'
      };
      (rows || []).forEach(r => {
        let k = normalize(r.departamento);
        if (alias[k]) k = alias[k];
        byDept[k] = (byDept[k] || 0) + (Number(r.casos) || 0);
      });
      const vals = Object.values(byDept);
      const maxV = Math.max(1, ...vals);

      const paths = svg.querySelectorAll('path');
      paths.forEach(p => {
        const name = p.getAttribute('data-name') || '';
        const key = normalize(name);
        const v = byDept[key] || 0;
        p.setAttribute('data-cases', String(v));
        const t = v / maxV;
        const c1 = [224, 242, 254];
        const c2 = [2, 132, 199];
        const rr = Math.round(c1[0] + (c2[0] - c1[0]) * t);
        const gg = Math.round(c1[1] + (c2[1] - c1[1]) * t);
        const bb = Math.round(c1[2] + (c2[2] - c1[2]) * t);
        p.style.fill = 'rgb(' + rr + ',' + gg + ',' + bb + ')';
      });

      qs('mapHint').textContent = 'Max: ' + fmt(maxV) + ' casos';
    }

    function renderSummary(sum) {
      qs('kpiCases').textContent = fmt(sum.total_cases);
      qs('kpiTopDis').textContent = sum.top_enfermedad || '—';
      qs('kpiTopDept').textContent = sum.top_departamento || '—';
      qs('pillCount').textContent = 'Filas: ' + fmt(sum.filtered_total_rows);

      const homeCases = document.getElementById('homeKpiCases');
      if (homeCases) homeCases.textContent = fmt(sum.total_cases);
      const homeTopDis = document.getElementById('homeKpiTopDis');
      if (homeTopDis) homeTopDis.textContent = sum.top_enfermedad || '—';
      const homeTopDept = document.getElementById('homeKpiTopDept');
      if (homeTopDept) homeTopDept.textContent = sum.top_departamento || '—';

      const v = sum.variation;
      if (v && v.pct !== null && v.pct !== undefined) {
        const dir = v.direction;
        const badge = dir === 'sube' ? '<span class="badge-up">↑</span>' : (dir === 'baja' ? '<span class="badge-down">↓</span>' : '<span class="badge-up">•</span>');
        const pctText = (dir === 'baja') ? ('-' + Math.abs(v.pct) + '%') : (v.pct + '%');
        qs('kpiVar').innerHTML = badge + ' ' + pctText;
        qs('kpiVarDesc').textContent = v.from_year + ' → ' + v.to_year;
        const hk = document.getElementById('homeKpiVar');
        if (hk) hk.innerHTML = badge + ' ' + pctText;
        const hkDesc = document.getElementById('homeKpiVarDesc');
        if (hkDesc) hkDesc.textContent = v.from_year + ' → ' + v.to_year;
      } else {
        qs('kpiVar').textContent = '—';
        qs('kpiVarDesc').textContent = 'Selecciona más de un año';
        const hk = document.getElementById('homeKpiVar');
        if (hk) hk.textContent = '—';
        const hkDesc = document.getElementById('homeKpiVarDesc');
        if (hkDesc) hkDesc.textContent = 'Selecciona más de un año';
      }

      const insights = qs('insights');
      insights.innerHTML = '';
      (sum.insights || []).slice(0, 4).forEach(t => {
        const div = document.createElement('div');
        div.className = 'insight';
        div.textContent = t;
        insights.appendChild(div);
      });
      if (!(sum.insights || []).length) {
        const div = document.createElement('div');
        div.className = 'insight';
        div.textContent = 'No hay suficientes datos con estos filtros. Prueba ampliar el rango de años o quitar filtros.';
        insights.appendChild(div);
      }

      const trend = sum.trend || { labels: [], series: [] };
      drawLineMulti(qs('chartTrend'), trend.labels || [], trend.series || []);
      const homeTrend = document.getElementById('homeTrend');
      if (homeTrend) drawLineMulti(homeTrend, trend.labels || [], trend.series || []);

      const homeTopMuni = document.getElementById('homeTopMuni');
      if (homeTopMuni) {
        const topHome = (sum.top_municipios || []).slice(0, 8).map(x => ({ label: x.municipio, value: Number(x.casos) || 0 }));
        drawBars(homeTopMuni, topHome, '#0284c7');
      }

      const homeInsights = document.getElementById('homeInsights');
      if (homeInsights) {
        homeInsights.innerHTML = '';
        (sum.insights || []).slice(0, 2).forEach(t => {
          const div = document.createElement('div');
          div.className = 'insight';
          div.textContent = t;
          homeInsights.appendChild(div);
        });
      }

      const dis = (sum.disease_stats || []).slice(0, 6).map((x, idx) => ({ label: x.enfermedad, value: Number(x.casos) || 0, color: diseaseColor(x.enfermedad, idx) }));
      drawBars(qs('chartDisease'), dis, '#10b981');

      const top = (sum.top_municipios || []).slice(0, 8).map(x => ({ label: x.municipio, value: Number(x.casos) || 0 }));
      drawBars(qs('chartTopMuni'), top, '#0284c7');
    }

    async function loadMeta() {
      const meta = await fetchJSON('/api/meta');
      columns = meta.columns || [];
      total = meta.total_rows || 0;
      const thead = qs('thead');
      thead.innerHTML = '';
      columns.forEach(c => {
        const th = document.createElement('th');
        th.textContent = c;
        thead.appendChild(th);
      });
    }

    async function loadValues() {
      const data = await fetchJSON('/api/values');
      const dep = qs('departamento');
      dep.innerHTML = '<option value="">(cualquiera)</option>';
      (data.departamentos || []).forEach(v => {
        const opt = document.createElement('option');
        opt.value = v;
        opt.textContent = v;
        dep.appendChild(opt);
      });

      const enf = qs('enfermedades');
      enf.innerHTML = '';
      (data.enfermedades || []).forEach(v => {
        const opt = document.createElement('option');
        opt.value = v;
        opt.textContent = v;
        enf.appendChild(opt);
      });

      const years = (data.anos || []);
      const yMin = qs('anoMin');
      const yMax = qs('anoMax');
      yMin.innerHTML = '<option value="">(cualquiera)</option>';
      yMax.innerHTML = '<option value="">(cualquiera)</option>';
      years.forEach(y => {
        const o1 = document.createElement('option');
        o1.value = String(y);
        o1.textContent = String(y);
        yMin.appendChild(o1);
        const o2 = document.createElement('option');
        o2.value = String(y);
        o2.textContent = String(y);
        yMax.appendChild(o2);
      });
      if (years.length) {
        yMin.value = String(years[0]);
        yMax.value = String(years[years.length - 1]);
      }
    }

    async function loadMap() {
      mapData = await fetchJSON('/api/map');
      buildMap();
    }

    function setPageInfo(limit) {
      const start = Math.min(offset + 1, total);
      const end = Math.min(offset + limit, total);
      qs('pageInfo').textContent = start.toLocaleString() + '–' + end.toLocaleString() + ' de ' + total.toLocaleString();
    }

    async function loadTable() {
      const limit = Math.max(1, Math.min(500, parseInt(qs('limit').value || '50', 10)));
      const p = getParams();
      p.offset = String(offset);
      p.limit = String(limit);
      const payload = await fetchJSON('/api/data?' + new URLSearchParams(p).toString());
      const rows = payload.rows || [];
      const filteredTotal = payload.filtered_total ?? total;
      total = filteredTotal;
      setPageInfo(limit);

      const tbody = qs('tbody');
      tbody.innerHTML = '';
      rows.forEach(r => {
        const tr = document.createElement('tr');
        columns.forEach(c => {
          const td = document.createElement('td');
          const v = r[c];
          td.textContent = (v === null || v === undefined) ? '' : String(v);
          tr.appendChild(td);
        });
        tbody.appendChild(tr);
      });
    }

    async function refreshAll() {
      setStatus('Actualizando…', true);
      updateExportLink();
      const p = getParams();
      const qs1 = new URLSearchParams(p).toString();
      const sum = await fetchJSON('/api/summary?' + qs1);
      renderSummary(sum);
      const geo = await fetchJSON('/api/geo?' + qs1);
      updateMapColors(geo.rows || []);
      offset = 0;
      await loadTable();
      setStatus('Listo', false);
    }

    const scheduleRefresh = debounce(function() {
      refreshAll().catch(e => { setStatus('Error', false); toast(e.message || String(e)); });
    }, 280);

    async function ask() {
      const q = qs('askInput').value.trim();
      if (!q) return;
      setStatus('Pensando…', true);
      const p = getParams();
      p.question = q;
      const res = await fetchJSON('/api/ask?' + new URLSearchParams(p).toString());
      qs('askAnswer').textContent = res.answer || 'No pude responder.';
      setStatus('Listo', false);
    }

    function resetAll() {
      qs('q').value = '';
      qs('departamento').value = '';
      Array.from(qs('enfermedades').options).forEach(o => { o.selected = false; });
      const minSel = qs('anoMin');
      const maxSel = qs('anoMax');
      if (minSel.options.length) minSel.selectedIndex = 0;
      if (maxSel.options.length) maxSel.selectedIndex = 0;
      qs('askInput').value = '';
      qs('askAnswer').textContent = 'Escribe una pregunta para obtener una respuesta rápida.';
      updateExportLink();
    }

    function setDiseaseSelection(names) {
      const wanted = new Set((names || []).map(x => String(x || '').trim()).filter(Boolean));
      Array.from(qs('enfermedades').options).forEach(o => { o.selected = wanted.has(o.value); });
    }

    function selectDiseaseAndGo(name) {
      setDiseaseSelection([name]);
      location.hash = '#/dashboard';
      showPage('/dashboard');
      offset = 0;
      scheduleRefresh();
    }

    function initDiseasePage() {
      const diseaseInfo = {
        DENGUE: {
          es: {
            title: 'Dengue',
            lead: 'Enfermedad viral transmitida por mosquitos. Puede presentarse como un cuadro leve o, en algunos casos, complicarse.',
            symptoms: ['Fiebre alta', 'Dolor de cabeza', 'Dolor detrás de los ojos', 'Dolor muscular y articular', 'Náuseas o malestar general'],
            prevention: ['Eliminar recipientes con agua estancada', 'Usar repelente', 'Instalar mosquiteros o mallas', 'Usar ropa que cubra la piel (especialmente al amanecer y atardecer)'],
            when: ['Fiebre persistente por más de 2 días', 'Dolor intenso o deshidratación', 'Sangrado, somnolencia o dificultad para respirar (urgencias)'],
          },
          en: {
            title: 'Dengue',
            lead: 'A mosquito-borne viral disease. It can be mild, but in some cases it may become severe.',
            symptoms: ['High fever', 'Headache', 'Pain behind the eyes', 'Muscle and joint pain', 'Nausea or general discomfort'],
            prevention: ['Remove standing water containers', 'Use repellent', 'Use bed nets or window screens', 'Wear clothing that covers skin (especially dawn and dusk)'],
            when: ['Fever lasting more than 2 days', 'Severe pain or dehydration', 'Bleeding, drowsiness, or breathing difficulty (urgent)'],
          },
        },
        ZIKA: {
          es: {
            title: 'Zika',
            lead: 'Virus transmitido por mosquitos. Suele ser leve, pero es importante en embarazo por posibles complicaciones.',
            symptoms: ['Sarpullido', 'Fiebre baja', 'Dolor articular', 'Ojos rojos (conjuntivitis)', 'Cansancio'],
            prevention: ['Evitar picaduras (repelente, ropa larga)', 'Eliminar criaderos de mosquitos', 'Consultar ante síntomas si estás embarazada o planeas estarlo'],
            when: ['Síntomas durante el embarazo', 'Fiebre con sarpullido que empeora', 'Cualquier señal de alarma o malestar intenso'],
          },
          en: {
            title: 'Zika',
            lead: 'A mosquito-borne virus. Symptoms are often mild, but it is especially important during pregnancy.',
            symptoms: ['Rash', 'Low fever', 'Joint pain', 'Red eyes (conjunctivitis)', 'Fatigue'],
            prevention: ['Avoid mosquito bites (repellent, long sleeves)', 'Remove breeding sites', 'Seek care if you are pregnant and have symptoms'],
            when: ['Symptoms during pregnancy', 'Worsening rash with fever', 'Any warning sign or severe discomfort'],
          },
        },
        CHIKUNGUNYA: {
          es: {
            title: 'Chikungunya',
            lead: 'Virus transmitido por mosquitos. Puede causar dolor articular fuerte que limita actividades diarias.',
            symptoms: ['Fiebre', 'Dolor articular intenso', 'Hinchazón articular', 'Dolor muscular', 'Dolor de cabeza'],
            prevention: ['Repelente y mosquiteros', 'Eliminar criaderos', 'Mantener patios y recipientes sin agua acumulada'],
            when: ['Dolor articular severo o prolongado', 'Fiebre alta persistente', 'Deshidratación o síntomas que empeoran'],
          },
          en: {
            title: 'Chikungunya',
            lead: 'A mosquito-borne virus that can cause intense joint pain that limits daily activities.',
            symptoms: ['Fever', 'Severe joint pain', 'Joint swelling', 'Muscle pain', 'Headache'],
            prevention: ['Use repellent and bed nets', 'Remove breeding sites', 'Keep patios and containers free of standing water'],
            when: ['Severe or prolonged joint pain', 'Persistent high fever', 'Dehydration or worsening symptoms'],
          },
        },
      };

      const state = {
        selected: 'DENGUE',
        compare: new Set(),
        cache: {},
      };

      function varText(v) {
        if (!v || v.pct === null || v.pct === undefined) return '—';
        const sign = (v.direction === 'baja') ? '-' : '';
        return sign + String(Math.abs(Number(v.pct) || 0)) + '%';
      }

      function varBadge(v) {
        if (!v || v.pct === null || v.pct === undefined) return '—';
        const dir = v.direction;
        const badge = dir === 'sube' ? '↑' : (dir === 'baja' ? '↓' : '•');
        const pctText = (dir === 'baja') ? ('-' + Math.abs(v.pct) + '%') : (v.pct + '%');
        return badge + ' ' + pctText;
      }

      async function loadDiseaseSummary(d) {
        if (state.cache[d]) return state.cache[d];
        const p = getParams();
        p.enfermedad = d;
        const res = await fetchJSON('/api/summary?' + new URLSearchParams(p).toString());
        state.cache[d] = res;
        return res;
      }

      function setActiveDisease(d) {
        state.selected = d;
        Array.from(document.querySelectorAll('#diseaseList .disease-item')).forEach(el => {
          el.classList.toggle('active', (el.getAttribute('data-disease') || '') === d);
        });
      }

      function fillList(id, items) {
        const el = document.getElementById(id);
        if (!el) return;
        el.innerHTML = '';
        (items || []).slice(0, 6).forEach(t => {
          const li = document.createElement('li');
          li.textContent = t;
          el.appendChild(li);
        });
      }

      function renderCompare() {
        const box = document.getElementById('compareChips');
        if (!box) return;
        box.innerHTML = '';
        const arr = Array.from(state.compare);
        if (!arr.length) {
          const m = document.createElement('div');
          m.className = 'muted';
          m.textContent = (getLang() === 'en') ? 'You have not selected diseases to compare yet.' : 'Aún no seleccionas enfermedades para comparar.';
          box.appendChild(m);
        } else {
          arr.forEach((d, idx) => {
            const chip = document.createElement('span');
            chip.className = 'chip';
            const dot = document.createElement('span');
            dot.style.width = '10px';
            dot.style.height = '10px';
            dot.style.borderRadius = '999px';
            dot.style.background = diseaseColor(d, idx);
            dot.style.display = 'inline-block';
            const s = document.createElement('strong');
            const lang = getLang();
            s.textContent = (diseaseInfo[d] && diseaseInfo[d][lang] ? diseaseInfo[d][lang].title : d);
            chip.appendChild(dot);
            chip.appendChild(s);
            chip.title = (getLang() === 'en') ? 'Click to remove' : 'Clic para quitar';
            chip.style.cursor = 'pointer';
            chip.addEventListener('click', function() {
              state.compare.delete(d);
              renderCompare();
            });
            box.appendChild(chip);
          });
        }
        const btn = document.getElementById('btnCompareGo');
        if (btn) btn.disabled = arr.length < 2;
      }

      async function renderDisease(d) {
        setActiveDisease(d);
        const lang = getLang();
        const info = (diseaseInfo[d] && diseaseInfo[d][lang]) ? diseaseInfo[d][lang] : null;
        const t = document.getElementById('dTitle');
        const lead = document.getElementById('dLead');
        if (t) t.textContent = info ? info.title : d;
        if (lead) lead.textContent = info ? info.lead : '';
        fillList('dSymptoms', info ? info.symptoms : []);
        fillList('dPrevention', info ? info.prevention : []);
        fillList('dWhen', info ? info.when : []);

        const viewBtn = document.getElementById('dBtnView');
        const addBtn = document.getElementById('dBtnAdd');
        if (viewBtn) viewBtn.onclick = function() { selectDiseaseAndGo(d); };
        if (addBtn) addBtn.onclick = function() {
          state.compare.add(d);
          renderCompare();
          toast((getLang() === 'en' ? 'Added: ' : 'Añadido: ') + (info ? info.title : d));
        };

        try {
          const sum = await loadDiseaseSummary(d);
          const cases = document.getElementById('dCases');
          const vEl = document.getElementById('dVar');
          const vDesc = document.getElementById('dVarDesc');
          const top = document.getElementById('dTopDept');
          if (cases) cases.textContent = fmt(sum.total_cases);
          if (vEl) vEl.textContent = varBadge(sum.variation);
          if (vDesc) vDesc.textContent = (sum.variation && sum.variation.from_year) ? (sum.variation.from_year + ' → ' + sum.variation.to_year) : 'Selecciona más de un año';
          if (top) top.textContent = sum.top_departamento || '—';
        } catch (e) {
          const cases = document.getElementById('dCases');
          if (cases) cases.textContent = '—';
        }
      }

      async function hydrateListStats() {
        const mapIds = {
          DENGUE: { c: 'statDengueCases', v: 'statDengueVar' },
          ZIKA: { c: 'statZikaCases', v: 'statZikaVar' },
          CHIKUNGUNYA: { c: 'statChikCases', v: 'statChikVar' },
        };
        for (const d of Object.keys(mapIds)) {
          try {
            const sum = await loadDiseaseSummary(d);
            const cases = document.getElementById(mapIds[d].c);
            const v = document.getElementById(mapIds[d].v);
            if (cases) cases.textContent = fmt(sum.total_cases);
            if (v) v.textContent = varText(sum.variation);
          } catch (e) {}
        }
      }

      Array.from(document.querySelectorAll('#diseaseList .disease-item')).forEach(el => {
        el.addEventListener('click', function() {
          const d = el.getAttribute('data-disease') || '';
          if (d) renderDisease(d);
        });
      });

      const search = document.getElementById('diseaseSearch');
      if (search) {
        search.addEventListener('input', function() {
          const q = normalize(search.value || '');
          Array.from(document.querySelectorAll('#diseaseList .disease-item')).forEach(el => {
            const d = normalize(el.getAttribute('data-disease') || '');
            const t = normalize(el.textContent || '');
            const ok = !q || d.includes(q) || t.includes(q);
            el.style.display = ok ? '' : 'none';
          });
        });
      }

      const btnGo = document.getElementById('btnCompareGo');
      if (btnGo) btnGo.addEventListener('click', function() {
        const arr = Array.from(state.compare);
        if (arr.length < 2) return;
        setDiseaseSelection(arr);
        location.hash = '#/dashboard';
        showPage('/dashboard');
        offset = 0;
        scheduleRefresh();
      });

      const btnClear = document.getElementById('btnCompareClear');
      if (btnClear) btnClear.addEventListener('click', function() { state.compare.clear(); renderCompare(); });

      renderCompare();
      hydrateListStats();
      renderDisease(state.selected);
    }

    function initRouting() {
      if (!location.hash) location.hash = '#/inicio';
      showPage(getRoute());
      window.addEventListener('hashchange', function() { showPage(getRoute()); });
      const go = document.getElementById('btnGoDash');
      if (go) go.addEventListener('click', function() { location.hash = '#/dashboard'; showPage('/dashboard'); });
      const goDis = document.getElementById('btnGoDiseases');
      if (goDis) goDis.addEventListener('click', function() { location.hash = '#/enfermedades'; showPage('/enfermedades'); });
      const faq = document.getElementById('btnScrollFAQ');
      if (faq) faq.addEventListener('click', function() {
        const el = document.getElementById('homeFAQ');
        if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
      });
    }

    function isHomeActive() {
      const el = document.getElementById('page-home');
      return !!(el && el.classList.contains('active'));
    }

    function setHomeChatVisible() {
      const chat = document.getElementById('homeChat');
      const mini = document.getElementById('homeChatMini');
      if (!chat || !mini) return;
      const open = localStorage.getItem('home_chat_open') === '1';
      if (!isHomeActive()) {
        chat.classList.remove('active');
        mini.classList.remove('active');
        return;
      }
      if (open) {
        chat.classList.add('active');
        mini.classList.remove('active');
      } else {
        chat.classList.remove('active');
        mini.classList.add('active');
      }
    }

    function addHomeMsg(kind, text) {
      const box = document.getElementById('homeChatBody');
      if (!box) return;
      const div = document.createElement('div');
      div.className = 'msg ' + (kind === 'user' ? 'user' : 'bot');
      div.textContent = text;
      box.appendChild(div);
      box.scrollTop = box.scrollHeight;
      return div;
    }

    function resetHomeChat() {
      homeChatMessages = [];
      const box = document.getElementById('homeChatBody');
      if (!box) return;
      box.innerHTML = '';
      const m = document.createElement('div');
      m.className = 'msg bot';
      m.textContent = (getLang() === 'en')
        ? 'Hi. I am your assistant. You can ask about health (dengue, zika, chikungunya) or about what you see in the data. What would you like to know?'
        : 'Hola. Soy tu asistente. Puedes preguntarme sobre salud (dengue, zika, chikungunya) o sobre lo que ves en los datos. ¿Qué te gustaría saber?';
      box.appendChild(m);
      box.scrollTop = box.scrollHeight;
    }

    async function askHome(question) {
      const q = String(question || '').trim();
      if (!q) return;
      addHomeMsg('user', q);
      homeChatMessages.push({ role: 'user', content: q });
      setStatus('Pensando…', true);
      const typing = addHomeMsg('bot', (getLang() === 'en') ? 'Typing…' : 'Escribiendo…');
      try {
        const body = {
          messages: homeChatMessages.slice(-16),
          params: getParams(),
        };
        const res = await fetch('/api/chat', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        const txt = await res.text();
        let payload = null;
        try { payload = JSON.parse(txt); } catch (e) { payload = { answer: txt }; }
        const a = (payload && payload.answer) ? String(payload.answer) : 'No pude responder.';
        if (typing) typing.textContent = a;
        else addHomeMsg('bot', a);
        homeChatMessages.push({ role: 'assistant', content: a });
      } catch (e) {
        if (typing) typing.textContent = 'Ocurrió un error al responder. Intenta de nuevo.';
        else addHomeMsg('bot', 'Ocurrió un error al responder. Intenta de nuevo.');
      } finally {
        setStatus('Listo', false);
      }
    }

    function initHomeChat() {
      const openBtn = document.getElementById('homeChatOpen');
      const closeBtn = document.getElementById('homeChatClose');
      const resetBtn = document.getElementById('homeChatReset');
      const sendBtn = document.getElementById('homeAskSend');
      const input = document.getElementById('homeAskInput');
      if (openBtn) openBtn.addEventListener('click', function() { localStorage.setItem('home_chat_open', '1'); setHomeChatVisible(); });
      if (closeBtn) closeBtn.addEventListener('click', function() { localStorage.setItem('home_chat_open', '0'); setHomeChatVisible(); });
      if (resetBtn) resetBtn.addEventListener('click', function() { resetHomeChat(); });
      if (sendBtn) sendBtn.addEventListener('click', function() { askHome(input ? input.value : ''); if (input) input.value = ''; });
      if (input) input.addEventListener('keydown', function(e) { if (e.key === 'Enter') { e.preventDefault(); if (sendBtn) sendBtn.click(); } });
      Array.from(document.querySelectorAll('.qbtn')).forEach(b => {
        b.addEventListener('click', function() {
          const q = b.getAttribute('data-q') || '';
          askHome(q);
        });
      });
      resetHomeChat();
      setHomeChatVisible();
    }

    function initHomeActions() {
      Array.from(document.querySelectorAll('.feature[data-go]')).forEach(el => {
        const go = el.getAttribute('data-go') || '';
        const run = function() {
          if (go === 'dash-trend') {
            location.hash = '#/dashboard';
            showPage('/dashboard');
            setTimeout(function() {
              const t = document.getElementById('dashTrend');
              if (t) t.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }, 240);
          } else if (go === 'dash-map') {
            location.hash = '#/dashboard';
            showPage('/dashboard');
            setTimeout(function() {
              const t = document.getElementById('dashMap');
              if (t) t.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }, 240);
          } else if (go === 'learn') {
            location.hash = '#/enfermedades';
            showPage('/enfermedades');
          }
        };
        el.addEventListener('click', run);
        el.addEventListener('keydown', function(e) { if (e.key === 'Enter') run(); });
      });

      const more = document.getElementById('btnLearnMore');
      if (more) more.addEventListener('click', function() { location.hash = '#/enfermedades'; showPage('/enfermedades'); });
      const ideas = document.getElementById('btnAskIdeas');
      if (ideas) ideas.addEventListener('click', function() { askHome('ayuda'); });
    }

    function initPowerBIButton() {
      const btn = document.getElementById('btnPowerBI');
      if (!btn) return;
      btn.addEventListener('click', async function(e) {
        e.preventDefault();
        try {
          const st = await fetchJSON('/api/powerbi');
          if (st && st.available) {
            window.location.href = '/download_powerbi';
            return;
          }
          const msg = (getLang() === 'en')
            ? (I18N.en['powerbi.missing'] || 'Power BI file not available yet.')
            : (I18N.es['powerbi.missing'] || 'Aún no hay archivo de Power BI disponible.');
          toast(msg);
        } catch (err) {
          const msg = (getLang() === 'en')
            ? (I18N.en['powerbi.error'] || 'Could not check Power BI file.')
            : (I18N.es['powerbi.error'] || 'No se pudo verificar el archivo de Power BI.');
          toast(msg);
        }
      });
    }

    qs('btnApply').addEventListener('click', function() { scheduleRefresh(); });
    qs('btnReset').addEventListener('click', function() { resetAll(); scheduleRefresh(); });
    qs('btnPDF').addEventListener('click', function() { window.print(); });
    qs('btnAsk').addEventListener('click', function() { ask().catch(e => toast(e.message || String(e))); });
    qs('askInput').addEventListener('keydown', function(e) { if (e.key === 'Enter') qs('btnAsk').click(); });

    ['q','anoMin','anoMax','departamento','enfermedades'].forEach(id => {
      qs(id).addEventListener('change', scheduleRefresh);
      if (id === 'q') qs(id).addEventListener('input', scheduleRefresh);
    });

    qs('btnPrev').addEventListener('click', async function() {
      const limit = Math.max(1, Math.min(500, parseInt(qs('limit').value || '50', 10)));
      offset = Math.max(0, offset - limit);
      await loadTable();
    });
    qs('btnNext').addEventListener('click', async function() {
      const limit = Math.max(1, Math.min(500, parseInt(qs('limit').value || '50', 10)));
      offset = offset + limit;
      await loadTable();
    });
    qs('limit').addEventListener('change', function() { offset = 0; scheduleRefresh(); });

    qs('btnTheme').addEventListener('click', function() {
      const cur = document.body.getAttribute('data-theme') || 'light';
      setTheme(cur === 'dark' ? 'light' : 'dark');
      buildMap();
      scheduleRefresh();
    });

    window.addEventListener('resize', debounce(function() { setLayoutVars(); scheduleRefresh(); }, 220));

    (async function() {
      try {
        initTheme();
        initLang();
        initPowerBIButton();
        setLayoutVars();
        initRouting();
        initDiseasePage();
        initHomeActions();
        initHomeChat();
        setStatus('Cargando…', true);
        await loadMeta();
        await loadValues();
        await loadMap();
        updateExportLink();
        await refreshAll();
      } catch (e) {
        setStatus('Error', false);
        toast(e.message || String(e));
      }
    })();
  </script>
</body>
</html>
"""
    html = html.replace("__CSV_PATH__", html_escape(csv_file.as_posix()))

    class Handler(BaseHTTPRequestHandler):
        def _send(self, status, content_type, body_bytes):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body_bytes)))
            self.end_headers()
            self.wfile.write(body_bytes)

        def do_POST(self):
            parsed = urlparse(self.path)
            path = parsed.path

            if path == "/api/chat":
                try:
                    length = int(self.headers.get("Content-Length") or "0")
                except Exception:
                    length = 0
                raw = self.rfile.read(length) if length > 0 else b""
                try:
                    payload = json.loads(raw.decode("utf-8") or "{}")
                except Exception:
                    status, data = to_json_bytes({"error": "JSON inválido"}, 400)
                    return self._send(status, "application/json; charset=utf-8", data)

                params = payload.get("params") or {}
                if not isinstance(params, dict):
                    params = {}
                messages = payload.get("messages") or []
                if not isinstance(messages, list):
                    messages = []
                filtered = apply_filters(df, params)
                answer = answer_chat(messages, filtered, params)
                status, data = to_json_bytes({"answer": answer}, 200)
                return self._send(status, "application/json; charset=utf-8", data)

            status, data = to_json_bytes({"error": "No encontrado"}, 404)
            return self._send(status, "application/json; charset=utf-8", data)

        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path

            if path == "/":
                body = html.encode("utf-8")
                return self._send(200, "text/html; charset=utf-8", body)

            if path == "/api/meta":
                payload = {
                    "columns": columns,
                    "total_rows": int(len(df)),
                    "sources": sources,
                    "csv_path": str(csv_file),
                }
                status, data = to_json_bytes(payload, 200)
                return self._send(status, "application/json; charset=utf-8", data)

            if path == "/api/values":
                payload = get_values()
                status, data = to_json_bytes(payload, 200)
                return self._send(status, "application/json; charset=utf-8", data)

            if path == "/api/map":
                payload = {"map": colombia_map}
                status, data = to_json_bytes(payload, 200)
                return self._send(status, "application/json; charset=utf-8", data)

            if path == "/api/summary":
                qs_params = parse_qs(parsed.query)
                params = {k: (v[0] if v else "") for k, v in qs_params.items()}
                filtered = apply_filters(df, params)
                payload = build_summary(filtered)
                status, data = to_json_bytes(payload, 200)
                return self._send(status, "application/json; charset=utf-8", data)

            if path == "/api/geo":
                qs_params = parse_qs(parsed.query)
                params = {k: (v[0] if v else "") for k, v in qs_params.items()}
                filtered = apply_filters(df, params)
                if {"departamento", "casos_totales"}.issubset(filtered.columns) and len(filtered):
                    g = (
                        filtered.groupby("departamento", dropna=False)["casos_totales"]
                        .sum()
                        .reset_index()
                        .sort_values("casos_totales", ascending=False)
                    )
                    rows = [{"departamento": str(r["departamento"]), "casos": int(r["casos_totales"])} for _, r in g.iterrows()]
                else:
                    rows = []
                payload = {"rows": rows}
                status, data = to_json_bytes(payload, 200)
                return self._send(status, "application/json; charset=utf-8", data)

            if path == "/api/data":
                qs_params = parse_qs(parsed.query)
                params = {k: (v[0] if v else "") for k, v in qs_params.items()}
                offset_req = parse_int(params.get("offset"), 0)
                limit_req = parse_int(params.get("limit"), 100)
                offset_req = max(0, offset_req)
                limit_req = max(1, min(5000, limit_req))

                filtered = apply_filters(df, params)
                filtered_total = int(len(filtered))

                page = filtered.iloc[offset_req: offset_req + limit_req]
                rows = page.to_dict(orient="records")
                payload = {
                    "columns": columns,
                    "rows": rows,
                    "offset": offset_req,
                    "limit": limit_req,
                    "filtered_total": filtered_total,
                }
                status, data = to_json_bytes(payload, 200)
                return self._send(status, "application/json; charset=utf-8", data)

            if path == "/api/ask":
                qs_params = parse_qs(parsed.query)
                params = {k: (v[0] if v else "") for k, v in qs_params.items()}
                question = (params.get("question") or "").strip()
                filtered = apply_filters(df, params)
                payload = {"answer": answer_question(question, filtered)}
                status, data = to_json_bytes(payload, 200)
                return self._send(status, "application/json; charset=utf-8", data)

            if path == "/api/powerbi":
                payload = {"available": bool(pbix_file.exists()), "filename": pbix_file.name}
                status, data = to_json_bytes(payload, 200)
                return self._send(status, "application/json; charset=utf-8", data)

            if path == "/download_filtered":
                qs_params = parse_qs(parsed.query)
                params = {k: (v[0] if v else "") for k, v in qs_params.items()}
                filtered = apply_filters(df, params)
                if len(filtered) > 300000:
                    status, data = to_json_bytes({"error": "La selección es muy grande para exportar. Usa filtros para reducirla."}, 413)
                    return self._send(status, "application/json; charset=utf-8", data)
                body = filtered.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                self.send_response(200)
                self.send_header("Content-Type", "text/csv; charset=utf-8")
                self.send_header("Content-Disposition", 'attachment; filename="dataset_filtrado.csv"')
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path == "/download":
                try:
                    body = csv_file.read_bytes()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/csv; charset=utf-8")
                    self.send_header("Content-Disposition", f'attachment; filename="{csv_file.name}"')
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                except Exception:
                    status, data = to_json_bytes({"error": "No se pudo descargar el archivo."}, 500)
                    return self._send(status, "application/json; charset=utf-8", data)

            if path == "/download_powerbi":
                if not pbix_file.exists():
                    status, data = to_json_bytes({"error": "Archivo Power BI no disponible."}, 404)
                    return self._send(status, "application/json; charset=utf-8", data)
                try:
                    body = pbix_file.read_bytes()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/octet-stream")
                    self.send_header("Content-Disposition", f'attachment; filename="{pbix_file.name}"')
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                except Exception:
                    status, data = to_json_bytes({"error": "No se pudo descargar el archivo."}, 500)
                    return self._send(status, "application/json; charset=utf-8", data)

            status, data = to_json_bytes({"error": "No encontrado"}, 404)
            return self._send(status, "application/json; charset=utf-8", data)

        def log_message(self, format, *args):
            return

    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Frontend listo: http://{host}:{port}/")
    print(f"CSV: {csv_file}")
    server.serve_forever()

def _parse_args(argv):
    p = argparse.ArgumentParser(add_help=True)
    p.add_argument("--serve", action="store_true")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--csv", default=None)
    p.add_argument("--pbix", default=None)
    return p.parse_args(argv)

if __name__ == "__main__":
    args = _parse_args(sys.argv[1:])
    if args.serve:
        run_frontend_server(host=args.host, port=args.port, csv_path=args.csv, pbix_path=args.pbix)
    else:
        main()
