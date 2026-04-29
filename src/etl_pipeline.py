import os
import sys
import re
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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


if __name__ == "__main__":
    main()