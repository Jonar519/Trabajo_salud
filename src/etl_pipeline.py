import os
import sys
import re
import json
import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from pathlib import Path

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    _PYSPARK_IMPORT_ERROR = None
except Exception as e:
    SparkSession = None
    F = None
    Window = None
    _PYSPARK_IMPORT_ERROR = e


# ─────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────

def create_spark_session():
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

    (
        master_df
        .toPandas()
        .to_csv(output_path, index=False, encoding="utf-8-sig")
        # utf-8-sig adds a BOM so Excel on Windows opens accented characters correctly
    )

    print("\n  Pipeline completed successfully.\n")
    spark.stop()

def run_frontend_server(host="127.0.0.1", port=8000, csv_path=None):
    import pandas as pd

    src_dir = Path(__file__).resolve().parent
    project_root = src_dir.parent
    default_csv = project_root / "data" / "processed" / "dataset_maestro_epidemiologico.csv"
    csv_file = Path(csv_path) if csv_path else default_csv

    if not csv_file.exists():
        raise FileNotFoundError(
            f"No se encontró el CSV del dataset maestro en: {csv_file}\n"
            "Ejecuta primero el pipeline para generarlo."
        )

    df = pd.read_csv(csv_file)
    columns = list(df.columns)

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

        for col in ["departamento", "municipio", "enfermedad", "brote"]:
            v = (params.get(col) or "").strip()
            if v and col in df_out.columns:
                df_out = df_out[df_out[col].astype(str).str.upper() == v.upper()]

        ano = (params.get("ano") or "").strip()
        if ano and "ano" in df_out.columns:
            df_out = df_out[df_out["ano"] == parse_int(ano, -1)]

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

        return {
            "filtered_total_rows": total_rows,
            "total_cases": total_cases,
            "municipios": municipios,
            "brote_counts": brote_counts,
            "disease_stats": disease_stats,
            "weekly_cases": weekly,
            "top_municipios": top_munis,
            "metrics": metrics,
        }

    html = """<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Dataset Maestro Epidemiológico</title>
  <style>
    :root {
      --bg: #0b1220;
      --panel: rgba(255,255,255,0.06);
      --panel-2: rgba(255,255,255,0.09);
      --text: rgba(255,255,255,0.92);
      --muted: rgba(255,255,255,0.65);
      --border: rgba(255,255,255,0.12);
      --accent: #60a5fa;
      --accent-2: #34d399;
      --warn: #fbbf24;
      --danger: #fb7185;
      --shadow: 0 10px 30px rgba(0,0,0,0.35);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      color: var(--text);
      background:
        radial-gradient(1000px 700px at 20% 10%, rgba(96,165,250,0.20), transparent 60%),
        radial-gradient(900px 600px at 80% 0%, rgba(52,211,153,0.18), transparent 60%),
        radial-gradient(900px 600px at 50% 90%, rgba(251,113,133,0.10), transparent 60%),
        var(--bg);
      min-height: 100vh;
    }
    a { color: inherit; }
    .topbar {
      position: sticky;
      top: 0;
      z-index: 10;
      backdrop-filter: blur(10px);
      background: rgba(11,18,32,0.55);
      border-bottom: 1px solid var(--border);
    }
    .topbar-inner {
      max-width: 1200px;
      margin: 0 auto;
      padding: 16px 16px;
      display: flex;
      gap: 12px;
      align-items: center;
      justify-content: space-between;
    }
    .brand {
      display: flex;
      gap: 12px;
      align-items: center;
    }
    .logo {
      width: 40px;
      height: 40px;
      border-radius: 12px;
      background: linear-gradient(135deg, rgba(96,165,250,0.9), rgba(52,211,153,0.9));
      box-shadow: var(--shadow);
    }
    .title {
      line-height: 1.1;
    }
    .title h1 {
      font-size: 16px;
      margin: 0;
      letter-spacing: 0.2px;
    }
    .title .sub {
      font-size: 12px;
      color: var(--muted);
      margin-top: 2px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      max-width: 58vw;
    }
    .actions {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .btn {
      appearance: none;
      border: 1px solid var(--border);
      background: rgba(255,255,255,0.06);
      color: var(--text);
      padding: 9px 12px;
      border-radius: 12px;
      cursor: pointer;
      transition: transform 0.08s ease, background 0.08s ease, border-color 0.08s ease;
      font-size: 13px;
    }
    .btn:hover { background: rgba(255,255,255,0.09); border-color: rgba(255,255,255,0.18); }
    .btn:active { transform: translateY(1px); }
    .btn.primary {
      border-color: rgba(96,165,250,0.45);
      background: rgba(96,165,250,0.16);
    }
    .container {
      max-width: 100%;
      margin: 0 auto;
      padding: 14px 14px 22px;
      display: grid;
      grid-template-columns: 340px 1fr;
      gap: 14px;
      align-items: start;
    }
    @media (max-width: 980px) {
      .container { grid-template-columns: 1fr; }
      .title .sub { max-width: 90vw; }
    }
    .panel {
      border: 1px solid var(--border);
      background: var(--panel);
      border-radius: 16px;
      box-shadow: var(--shadow);
    }
    .panel .hd {
      padding: 14px 14px 10px;
      border-bottom: 1px solid rgba(255,255,255,0.08);
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 10px;
    }
    .panel .hd h2 {
      margin: 0;
      font-size: 13px;
      letter-spacing: 0.3px;
      text-transform: uppercase;
      color: rgba(255,255,255,0.88);
    }
    .panel .hd .hint {
      color: var(--muted);
      font-size: 12px;
    }
    .panel .bd { padding: 14px; }
    .field { margin-bottom: 10px; }
    .field label {
      display: block;
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 6px;
    }
    input, select {
      width: 100%;
      padding: 10px 10px;
      border-radius: 12px;
      border: 1px solid rgba(255,255,255,0.14);
      background: rgba(11,18,32,0.45);
      color: var(--text);
      outline: none;
    }
    input::placeholder { color: rgba(255,255,255,0.45); }
    .row { display: flex; gap: 10px; }
    .row .field { flex: 1; }
    .pill {
      display: inline-flex;
      gap: 8px;
      align-items: center;
      padding: 4px 10px;
      border-radius: 999px;
      border: 1px solid rgba(255,255,255,0.16);
      background: rgba(255,255,255,0.06);
      color: rgba(255,255,255,0.80);
      font-size: 12px;
      white-space: nowrap;
    }
    .muted { color: var(--muted); font-size: 12px; }
    .kpis {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 10px;
    }
    @media (max-width: 980px) { .kpis { grid-template-columns: repeat(2, 1fr); } }
    .kpi {
      border: 1px solid rgba(255,255,255,0.10);
      background: rgba(255,255,255,0.06);
      border-radius: 16px;
      padding: 12px;
      min-height: 88px;
    }
    .kpi .label { color: var(--muted); font-size: 12px; margin-bottom: 8px; }
    .kpi .value { font-size: 22px; letter-spacing: 0.2px; }
    .kpi .delta { font-size: 12px; color: rgba(255,255,255,0.72); margin-top: 6px; }
    .grid2 {
      display: grid;
      grid-template-columns: 1.4fr 1fr;
      gap: 12px;
      margin-top: 12px;
    }
    @media (max-width: 980px) { .grid2 { grid-template-columns: 1fr; } }
    .chart {
      padding: 12px;
      border-radius: 16px;
      border: 1px solid rgba(255,255,255,0.10);
      background: rgba(255,255,255,0.06);
    }
    .chart h3 { margin: 0 0 10px; font-size: 13px; color: rgba(255,255,255,0.86); }
    canvas { width: 100%; height: 240px; display: block; }
    .table-wrap { overflow: auto; max-height: 70vh; border-radius: 14px; border: 1px solid rgba(255,255,255,0.10); }
    table { border-collapse: collapse; width: 100%; min-width: 860px; background: rgba(11,18,32,0.32); }
    th, td { padding: 10px 10px; text-align: left; font-size: 12.5px; border-bottom: 1px solid rgba(255,255,255,0.06); vertical-align: top; }
    th { position: sticky; top: 0; z-index: 2; background: rgba(11,18,32,0.86); color: rgba(255,255,255,0.86); font-size: 12px; }
    tbody tr:nth-child(even) { background: rgba(255,255,255,0.03); }
    .toast {
      position: fixed;
      right: 16px;
      bottom: 16px;
      padding: 10px 12px;
      border-radius: 14px;
      border: 1px solid rgba(255,255,255,0.14);
      background: rgba(0,0,0,0.35);
      color: rgba(255,255,255,0.92);
      box-shadow: var(--shadow);
      display: none;
      max-width: 420px;
      font-size: 13px;
    }
  </style>
</head>
<body>
  <div class="topbar">
    <div class="topbar-inner">
      <div class="brand">
        <div class="logo"></div>
        <div class="title">
          <h1>Dataset Maestro Epidemiológico</h1>
          <div class="sub">Archivo: __CSV_PATH__</div>
        </div>
      </div>
      <div class="actions">
        <span class="pill" id="pillStatus">Listo</span>
        <a class="btn" href="/download">Descargar CSV</a>
        <button class="btn primary" id="btnApply">Aplicar filtros</button>
      </div>
    </div>
  </div>

  <div class="container">
    <div class="panel">
      <div class="hd">
        <h2>Filtros</h2>
        <div class="hint">Usa filtros para gráficos y tabla</div>
      </div>
      <div class="bd">
        <div class="field">
          <label>Buscar (departamento / municipio / enfermedad)</label>
          <input id="q" placeholder="Ej: BOGOTA, MEDELLIN, DENGUE" />
        </div>
        <div class="field">
          <label>Departamento</label>
          <select id="departamento">
            <option value="">(cualquiera)</option>
          </select>
        </div>
        <div class="field">
          <label>Municipio (texto exacto)</label>
          <input id="municipio" placeholder="Ej: BOGOTA" />
        </div>
        <div class="row">
          <div class="field">
            <label>Enfermedad</label>
            <select id="enfermedad">
              <option value="">(cualquiera)</option>
            </select>
          </div>
          <div class="field">
            <label>Brote</label>
            <select id="brote">
              <option value="">(cualquiera)</option>
              <option value="SI">SI</option>
              <option value="NO">NO</option>
            </select>
          </div>
        </div>
        <div class="row">
          <div class="field">
            <label>Año</label>
            <select id="ano">
              <option value="">(cualquiera)</option>
            </select>
          </div>
          <div class="field">
            <label>Semana</label>
            <input id="semana" placeholder="1–52" />
          </div>
        </div>
        <div class="row">
          <button class="btn" id="btnReset" style="flex:1;">Reset</button>
          <button class="btn primary" id="btnApply2" style="flex:1;">Aplicar</button>
        </div>
        <div class="muted" style="margin-top: 12px; line-height: 1.5;">
          <div class="pill" style="margin-bottom: 8px;">Definiciones</div>
          <div><span class="pill">brote</span> "SI" si casos_totales supera el percentil 75 histórico (por enfermedad + municipio).</div>
          <div style="margin-top: 6px;"><span class="pill">imputación</span> si falta clima/aire, se usa el municipio más cercano (≤ 50 km) con datos.</div>
          <div style="margin-top: 6px;"><span class="pill">vacunación</span> se muestra como "NO_REPORTA" en este pipeline.</div>
        </div>
        <div class="muted" style="margin-top: 10px;">
          <div id="meta">Cargando metadatos…</div>
          <div id="sources" style="margin-top: 8px;"></div>
        </div>
      </div>
    </div>

    <div>
      <div class="panel">
        <div class="hd">
          <h2>Resumen</h2>
          <div class="hint">KPIs + métricas promedio</div>
        </div>
        <div class="bd">
          <div class="kpis">
            <div class="kpi">
              <div class="label">Filas (filtradas)</div>
              <div class="value" id="kpiRows">—</div>
              <div class="delta" id="kpiCols">—</div>
            </div>
            <div class="kpi">
              <div class="label">Casos totales</div>
              <div class="value" id="kpiCases">—</div>
              <div class="delta" id="kpiCasesHint">Sum(casos_totales)</div>
            </div>
            <div class="kpi">
              <div class="label">Municipios</div>
              <div class="value" id="kpiMunis">—</div>
              <div class="delta" id="kpiBrote">Brote SI: —</div>
            </div>
            <div class="kpi">
              <div class="label">Promedios</div>
              <div class="value" id="kpiTemp">—</div>
              <div class="delta" id="kpiAire">—</div>
            </div>
          </div>

          <div class="grid2">
            <div class="chart">
              <h3>Casos por semana (últimas 52)</h3>
              <canvas id="chartTrend"></canvas>
            </div>
            <div class="chart">
              <h3>Brote (distribución)</h3>
              <canvas id="chartBrote"></canvas>
            </div>
          </div>

          <div class="grid2" style="grid-template-columns: 1fr 1fr; margin-top: 12px;">
            <div class="chart">
              <h3>Casos por enfermedad</h3>
              <canvas id="chartDisease"></canvas>
            </div>
            <div class="chart">
              <h3>Top municipios por casos</h3>
              <canvas id="chartTopMuni"></canvas>
            </div>
          </div>
        </div>
      </div>

      <div class="panel" style="margin-top: 14px;">
        <div class="hd">
          <h2>Tabla</h2>
          <div class="hint">
            <span class="pill" id="pillCount">Filas: —</span>
          </div>
        </div>
        <div class="bd">
          <div class="row" style="align-items:center; margin-bottom: 12px;">
            <div class="field" style="max-width: 140px; margin:0;">
              <label>Por página</label>
              <input id="limit" value="100" />
            </div>
            <button class="btn" id="btnPrev">Anterior</button>
            <button class="btn" id="btnNext">Siguiente</button>
            <div class="muted" id="pageInfo" style="margin-left:auto;">—</div>
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

  <div class="toast" id="toast"></div>

  <script>
    let offset = 0;
    let total = 0;
    let columns = [];
    let lastSummary = null;

    function qs(id) { return document.getElementById(id); }

    function setStatus(text) {
      qs('pillStatus').textContent = text;
    }

    function toast(msg) {
      const el = qs('toast');
      el.textContent = msg;
      el.style.display = 'block';
      setTimeout(function() { el.style.display = 'none'; }, 4000);
    }

    async function fetchJSON(url) {
      const res = await fetch(url);
      const txt = await res.text();
      try {
        return JSON.parse(txt);
      } catch (e) {
        throw new Error('Respuesta inválida del servidor: ' + txt.slice(0, 200));
      }
    }

    function getParams() {
      return {
        q: qs('q').value.trim(),
        departamento: (qs('departamento').value || '').trim(),
        municipio: qs('municipio').value.trim(),
        enfermedad: (qs('enfermedad').value || '').trim(),
        ano: (qs('ano').value || '').trim(),
        semana: qs('semana').value.trim(),
        brote: (qs('brote').value || '').trim(),
      };
    }

    async function loadMeta() {
      const meta = await fetchJSON('/api/meta');
      columns = meta.columns || [];
      total = meta.total_rows || 0;
      qs('pillCount').textContent = 'Filas: ' + total.toLocaleString();
      qs('kpiCols').textContent = 'Columnas: ' + columns.length;
      qs('meta').textContent = 'Columnas (' + columns.length + '): ' + columns.join(', ');

      const sourcesDiv = qs('sources');
      sourcesDiv.innerHTML = '';
      const title = document.createElement('div');
      title.innerHTML = '<strong>Fuentes integradas</strong>';
      sourcesDiv.appendChild(title);
      (meta.sources || []).forEach(s => {
        const el = document.createElement('div');
        el.style.marginTop = '6px';
        el.innerHTML = '<span class="pill">' + s.archivo + '</span> <span class="muted">' + s.tipo + ' — ' + s.nivel + '</span>';
        sourcesDiv.appendChild(el);
      });

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
      const enf = qs('enfermedad');
      const ano = qs('ano');

      function fillSelect(el, values) {
        const keep = el.value || '';
        const first = el.querySelector('option');
        el.innerHTML = '';
        el.appendChild(first);
        (values || []).forEach(v => {
          const opt = document.createElement('option');
          opt.value = v;
          opt.textContent = v;
          el.appendChild(opt);
        });
        el.value = keep;
      }

      fillSelect(dep, data.departamentos || []);
      fillSelect(enf, data.enfermedades || []);

      const anoKeep = ano.value || '';
      const firstAno = ano.querySelector('option');
      ano.innerHTML = '';
      ano.appendChild(firstAno);
      (data.anos || []).forEach(v => {
        const opt = document.createElement('option');
        opt.value = String(v);
        opt.textContent = String(v);
        ano.appendChild(opt);
      });
      ano.value = anoKeep;
    }

    function setPageInfo(limit) {
      const start = Math.min(offset + 1, total);
      const end = Math.min(offset + limit, total);
      qs('pageInfo').textContent = start.toLocaleString() + '–' + end.toLocaleString() + ' de ' + total.toLocaleString();
    }

    async function loadData() {
      const limit = Math.max(1, parseInt(qs('limit').value || '100', 10));
      const p = getParams();
      p.offset = String(offset);
      p.limit = String(limit);
      const params = new URLSearchParams(p);
      const payload = await fetchJSON('/api/data?' + params.toString());

      const rows = payload.rows || [];
      const filteredTotal = payload.filtered_total ?? total;
      qs('pillCount').textContent = 'Filas: ' + filteredTotal.toLocaleString();
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

    function fmt(n) {
      if (n === null || n === undefined) return '—';
      const x = Number(n);
      if (!isFinite(x)) return '—';
      return x.toLocaleString();
    }

    function canvasSize(canvas, height) {
      const dpr = window.devicePixelRatio || 1;
      const w = canvas.clientWidth;
      const h = height;
      canvas.width = Math.floor(w * dpr);
      canvas.height = Math.floor(h * dpr);
      const ctx = canvas.getContext('2d');
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      return ctx;
    }

    function clearCanvas(ctx, w, h) {
      ctx.clearRect(0, 0, w, h);
    }

    function drawCardLine(ctx, text, x, y, color) {
      ctx.fillStyle = color;
      ctx.fillText(text, x, y);
    }

    function drawLineChart(canvas, labels, values, color) {
      const ctx = canvasSize(canvas, 240);
      const w = canvas.clientWidth;
      const h = 240;
      clearCanvas(ctx, w, h);

      ctx.font = '12px ui-sans-serif, system-ui, Segoe UI, Roboto, Arial';
      ctx.strokeStyle = 'rgba(255,255,255,0.14)';
      ctx.fillStyle = 'rgba(255,255,255,0.75)';

      const padL = 38, padR = 12, padT = 10, padB = 24;
      const iw = w - padL - padR;
      const ih = h - padT - padB;

      const maxV = Math.max(1, ...values);
      const minV = 0;

      ctx.beginPath();
      ctx.moveTo(padL, padT);
      ctx.lineTo(padL, padT + ih);
      ctx.lineTo(padL + iw, padT + ih);
      ctx.stroke();

      const ticks = 4;
      for (let i = 0; i <= ticks; i++) {
        const t = i / ticks;
        const y = padT + ih - t * ih;
        ctx.strokeStyle = 'rgba(255,255,255,0.08)';
        ctx.beginPath();
        ctx.moveTo(padL, y);
        ctx.lineTo(padL + iw, y);
        ctx.stroke();
        ctx.fillStyle = 'rgba(255,255,255,0.55)';
        ctx.fillText(Math.round(minV + t * (maxV - minV)).toLocaleString(), 0, y + 4);
      }

      if (!values.length) return;

      ctx.strokeStyle = color || 'rgba(96,165,250,0.95)';
      ctx.lineWidth = 2;
      ctx.beginPath();
      for (let i = 0; i < values.length; i++) {
        const x = padL + (i / Math.max(1, values.length - 1)) * iw;
        const v = values[i];
        const y = padT + ih - ((v - minV) / (maxV - minV)) * ih;
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      }
      ctx.stroke();

      ctx.fillStyle = 'rgba(255,255,255,0.65)';
      const last = labels[labels.length - 1] || '';
      ctx.fillText(last, padL, h - 8);
    }

    function drawBarChart(canvas, items, color) {
      const ctx = canvasSize(canvas, 240);
      const w = canvas.clientWidth;
      const h = 240;
      clearCanvas(ctx, w, h);

      ctx.font = '12px ui-sans-serif, system-ui, Segoe UI, Roboto, Arial';
      const padL = 12, padR = 12, padT = 10, padB = 18;
      const iw = w - padL - padR;
      const ih = h - padT - padB;

      const values = items.map(x => x.value);
      const maxV = Math.max(1, ...values);
      const n = items.length || 1;
      const gap = 10;
      const barW = Math.max(18, (iw - gap * (n - 1)) / n);

      for (let i = 0; i < items.length; i++) {
        const x = padL + i * (barW + gap);
        const v = items[i].value;
        const bh = (v / maxV) * ih;
        const y = padT + (ih - bh);
        ctx.fillStyle = 'rgba(255,255,255,0.10)';
        ctx.fillRect(x, padT, barW, ih);
        ctx.fillStyle = color || 'rgba(52,211,153,0.90)';
        ctx.fillRect(x, y, barW, bh);
        ctx.fillStyle = 'rgba(255,255,255,0.75)';
        ctx.save();
        ctx.translate(x + barW / 2, h - 6);
        ctx.rotate(-Math.PI / 6);
        ctx.textAlign = 'center';
        ctx.fillText(items[i].label, 0, 0);
        ctx.restore();
      }
    }

    function drawDonut(canvas, items) {
      const ctx = canvasSize(canvas, 240);
      const w = canvas.clientWidth;
      const h = 240;
      clearCanvas(ctx, w, h);

      const cx = w / 2;
      const cy = h / 2;
      const r = Math.min(w, h) * 0.32;
      const r2 = r * 0.60;

      const total = items.reduce((a, b) => a + b.value, 0) || 1;
      let ang = -Math.PI / 2;

      const colors = ['rgba(96,165,250,0.95)', 'rgba(251,191,36,0.95)', 'rgba(251,113,133,0.90)', 'rgba(52,211,153,0.90)'];

      for (let i = 0; i < items.length; i++) {
        const frac = items[i].value / total;
        const ang2 = ang + frac * Math.PI * 2;
        ctx.beginPath();
        ctx.moveTo(cx, cy);
        ctx.fillStyle = colors[i % colors.length];
        ctx.arc(cx, cy, r, ang, ang2);
        ctx.closePath();
        ctx.fill();
        ang = ang2;
      }

      ctx.beginPath();
      ctx.fillStyle = 'rgba(11,18,32,0.85)';
      ctx.arc(cx, cy, r2, 0, Math.PI * 2);
      ctx.fill();

      ctx.fillStyle = 'rgba(255,255,255,0.86)';
      ctx.font = '600 18px ui-sans-serif, system-ui, Segoe UI, Roboto, Arial';
      ctx.textAlign = 'center';
      ctx.fillText(fmt(total), cx, cy + 6);

      ctx.textAlign = 'left';
      ctx.font = '12px ui-sans-serif, system-ui, Segoe UI, Roboto, Arial';
      for (let i = 0; i < items.length; i++) {
        const x = 12;
        const y = 18 + i * 18;
        ctx.fillStyle = colors[i % colors.length];
        ctx.fillRect(x, y - 10, 10, 10);
        ctx.fillStyle = 'rgba(255,255,255,0.75)';
        ctx.fillText(items[i].label + ': ' + fmt(items[i].value), x + 16, y);
      }
    }

    function renderSummary(sum) {
      lastSummary = sum;
      qs('kpiRows').textContent = fmt(sum.filtered_total_rows);
      qs('kpiCases').textContent = fmt(sum.total_cases);
      qs('kpiMunis').textContent = fmt(sum.municipios);

      const broteSi = (sum.brote_counts && (sum.brote_counts.SI || sum.brote_counts['SI'])) ? (sum.brote_counts.SI || sum.brote_counts['SI']) : 0;
      const pct = sum.filtered_total_rows ? Math.round((broteSi / sum.filtered_total_rows) * 100) : 0;
      qs('kpiBrote').textContent = 'Brote SI: ' + fmt(broteSi) + ' (' + pct + '%)';

      const t = sum.metrics && sum.metrics.temperatura_promedio !== null ? (sum.metrics.temperatura_promedio + ' °C') : '—';
      const p = sum.metrics && sum.metrics.precipitacion_promedio !== null ? (sum.metrics.precipitacion_promedio + ' mm') : '—';
      qs('kpiTemp').textContent = t;
      qs('kpiAire').textContent = 'Aire: ' + fmt(sum.metrics ? sum.metrics.calidad_aire_promedio : null) + ' | Prec: ' + p;

      const trendLabels = (sum.weekly_cases || []).map(x => x.label);
      const trendValues = (sum.weekly_cases || []).map(x => x.casos);
      drawLineChart(qs('chartTrend'), trendLabels, trendValues, 'rgba(96,165,250,0.95)');

      const broteItems = [];
      const bc = sum.brote_counts || {};
      Object.keys(bc).forEach(k => {
        broteItems.push({ label: String(k), value: Number(bc[k]) || 0 });
      });
      broteItems.sort((a,b) => b.value - a.value);
      drawDonut(qs('chartBrote'), broteItems);

      const dis = (sum.disease_stats || []).slice(0, 6).map(x => ({ label: x.enfermedad, value: Number(x.casos) || 0 }));
      drawBarChart(qs('chartDisease'), dis, 'rgba(52,211,153,0.90)');

      const top = (sum.top_municipios || []).slice(0, 8).map(x => ({ label: String(x.municipio), value: Number(x.casos) || 0 }));
      drawBarChart(qs('chartTopMuni'), top, 'rgba(251,191,36,0.90)');
    }

    async function loadSummary() {
      const p = getParams();
      const params = new URLSearchParams(p);
      const sum = await fetchJSON('/api/summary?' + params.toString());
      renderSummary(sum);
    }

    async function applyAll() {
      try {
        setStatus('Cargando…');
        offset = 0;
        await loadSummary();
        await loadData();
        setStatus('Listo');
      } catch (e) {
        setStatus('Error');
        toast(e.message || String(e));
      }
    }

    function resetAll() {
      qs('q').value = '';
      qs('departamento').value = '';
      qs('municipio').value = '';
      qs('enfermedad').value = '';
      qs('ano').value = '';
      qs('semana').value = '';
      qs('brote').value = '';
    }

    function redraw() {
      if (lastSummary) renderSummary(lastSummary);
    }

    qs('btnApply').addEventListener('click', applyAll);
    qs('btnApply2').addEventListener('click', applyAll);
    qs('btnReset').addEventListener('click', async function() {
      resetAll();
      offset = 0;
      try {
        await loadValues();
        await applyAll();
      } catch (e) {
        toast(e.message || String(e));
      }
    });
    qs('btnPrev').addEventListener('click', async function() {
      const limit = Math.max(1, parseInt(qs('limit').value || '100', 10));
      offset = Math.max(0, offset - limit);
      await loadData();
    });
    qs('btnNext').addEventListener('click', async function() {
      const limit = Math.max(1, parseInt(qs('limit').value || '100', 10));
      offset = offset + limit;
      await loadData();
    });
    qs('limit').addEventListener('change', async function() {
      offset = 0;
      await loadData();
    });
    window.addEventListener('resize', function() { redraw(); });

    (async () => {
      try {
        setStatus('Cargando…');
        await loadMeta();
        await loadValues();
        await applyAll();
      } catch (e) {
        setStatus('Error');
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

            if path == "/api/summary":
                qs_params = parse_qs(parsed.query)
                params = {k: (v[0] if v else "") for k, v in qs_params.items()}
                filtered = apply_filters(df, params)
                payload = build_summary(filtered)
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
    return p.parse_args(argv)

if __name__ == "__main__":
    args = _parse_args(sys.argv[1:])
    if args.serve:
        run_frontend_server(host=args.host, port=args.port, csv_path=args.csv)
    else:
        main()
