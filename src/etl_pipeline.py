import os
import sys
import re
import argparse
import subprocess
from pathlib import Path

from frontend_server import run_frontend_server

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


    tmp_dir = str(Path(__file__).resolve().parent.parent / "spark_tmp")
    Path(tmp_dir).mkdir(parents=True, exist_ok=True)

    return (
        SparkSession.builder
        .appName("Epidemiological Master Dataset Pipeline")
        .master("local[2]")                                                                                         
        .config("spark.driver.memory",                  "4g")
        .config("spark.executor.memory",                "4g")
        .config("spark.memory.fraction",                "0.8")
        .config("spark.memory.storageFraction",         "0.3")
        .config("spark.driver.maxResultSize",           "2g")
        .config("spark.sql.shuffle.partitions",         "4")
        .config("spark.sql.adaptive.enabled",           "false")                                       
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


    char_map = {
        "Á": "A", "É": "E", "Í": "I", "Ó": "O", "Ú": "U", "Ñ": "N",
        "┴": "A", "═": "I", "Ë": "O", "Ð": "N", "Ý": "I", "¾": "O",
        "ß": "A", "Ú": "U", "×": "O", "¡": "A", "¢": "E", "£": "I", "¤": "O"
    }

    src_chars = "".join(char_map.keys())
    dst_chars = "".join(char_map.values())

    for col in columns:

        df = df.withColumn(
            col,
            F.upper(F.trim(F.translate(F.col(col), src_chars, dst_chars)))
        )

        df = df.withColumn(col, F.regexp_replace(F.col(col), r"^\*\s*", ""))
        df = df.withColumn(col, F.regexp_replace(F.col(col), r"\.\s*MUNICIPIO\s*DESCONOCIDO", ""))


        df = df.withColumn(col, F.regexp_replace(F.col(col), r"BOGOTA\s*,?\s*D\.?\s*C\.?", "BOGOTA"))
        df = df.withColumn(col, F.regexp_replace(F.col(col), r"SANTA MARTHA", "SANTA MARTA"))
        df = df.withColumn(col, F.regexp_replace(F.col(col), r"CARTAGENA DE INDIAS", "CARTAGENA"))


        df = df.withColumn(col, F.regexp_replace(F.col(col), r"\s+", " "))
        df = df.withColumn(col, F.trim(F.col(col)))

    return df


def get_geographic_mapping(spark, df_target, df_reference, max_dist_km=50):
    """
    Creates a mapping from target municipalities to their nearest reference municipality
    within a maximum distance.
    """

    ref_locs = df_reference.select(
        F.col("departamento").alias("ref_dept"),
        F.col("municipio").alias("ref_muni"),
        F.col("latitud").alias("ref_lat"),
        F.col("longitud").alias("ref_lon")
    ).distinct().filter(F.col("ref_lat").isNotNull() & F.col("ref_lon").isNotNull())


    target_locs = df_target.select(
        F.col("departamento").alias("target_dept"),
        F.col("municipio").alias("target_muni"),
        F.col("latitud").alias("target_lat"),
        F.col("longitud").alias("target_lon")
    ).distinct().filter(F.col("target_lat").isNotNull() & F.col("target_lon").isNotNull())



    distance_expr = """
        2 * 6371 * asin(sqrt(
            pow(sin(radians(ref_lat - target_lat) / 2), 2) +
            cos(radians(target_lat)) * cos(radians(ref_lat)) * pow(sin(radians(ref_lon - target_lon) / 2), 2)
        ))
    """

    mapping = target_locs.crossJoin(ref_locs)\
        .withColumn("dist_km", F.expr(distance_expr))\
        .filter(F.col("dist_km") <= max_dist_km)


    window_spec = Window.partitionBy("target_dept", "target_muni").orderBy("dist_km")

    mapping = mapping.withColumn("row_num", F.row_number().over(window_spec))\
        .filter(F.col("row_num") == 1)\
        .select(
            "target_dept", "target_muni",
            "ref_dept", "ref_muni", "dist_km"
        )

    return mapping




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


    df = normalize_text(df, ["departamento_ocurrencia", "municipio_ocurrencia", "nombre_evento"])


    df = df.filter(F.col("nombre_evento").isin(TARGET_DISEASES))

    df = df.select(
        F.col("ano").cast("int"),
        F.col("semana").cast("int"),
        F.col("departamento_ocurrencia").alias("departamento"),
        F.col("municipio_ocurrencia").alias("municipio"),
        F.col("nombre_evento").alias("enfermedad"),
        F.col("conteo").cast("int").alias("casos_totales"),
    )


    df = (
        df.groupBy("ano", "semana", "departamento", "municipio", "enfermedad")
        .agg(F.sum("casos_totales").alias("casos_totales"))
    )


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


    week_map = []
    for idx, month in enumerate(MONTH_COLS):
        start = idx * 4 + 1
        end = (idx + 1) * 4 if month != "dic" else 52
        for w in range(start, end + 1):
            week_map.append((month, w))

    df = spark.read.csv(path, header=True, inferSchema=True)
    df = normalize_column_names(df)


    df = normalize_text(df, ["departamento", "municipio", "par_metro"])


    stack_expr = (
        "stack(12, "
        + ", ".join([f"'{m}', {m}" for m in MONTH_COLS])
        + ") as (month_name, valor_clima)"
    )
    df_long = df.select(
        "departamento", "municipio", "par_metro", "latitud", "longitud",
        F.expr(stack_expr),
    )


    mapping_df = spark.createDataFrame(week_map, ["month_name", "semana"])
    df_weekly = df_long.join(mapping_df, on="month_name").drop("month_name")


    df_weekly = df_weekly.withColumn(
        "par_metro",
        F.when(F.col("par_metro").contains("TEMPERATURA MEDIA"), "temperatura_promedio")
         .when(F.col("par_metro").contains("PRECIPITACION"), "precipitacion_promedio")
         .otherwise(None),                                 
    ).filter(F.col("par_metro").isNotNull())



    df_agg = (
        df_weekly.groupBy("semana", "departamento", "municipio", "par_metro")
        .agg(
            F.avg("valor_clima").alias("valor_clima"),
            F.avg("latitud").alias("latitud"),
            F.avg("longitud").alias("longitud"),
        )
    )


    df_pivoted = (
        df_agg.groupBy("semana", "departamento", "municipio")
        .pivot("par_metro", ["temperatura_promedio", "precipitacion_promedio"])
        .agg(F.avg("valor_clima"))
    )


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

    POLLUTANTS  = ["PM2.5", "PM10", "O3", "NO2", "SO2", "CO", "PST"]
    TEMPERATURE = ["TAire", "TAire10", "TAire2"]

    df = spark.read.csv(path, header=True, inferSchema=True)
    df = normalize_column_names(df)






    df = (
        df.withColumnRenamed("nombre_del_departamento", "departamento")
          .withColumnRenamed("nombre_del_municipio", "municipio")
          .withColumnRenamed("a_o", "ano")
    )

    df = normalize_text(df, ["departamento", "municipio"])


    df_filtered = df.filter(F.col("variable").isin(POLLUTANTS + TEMPERATURE))


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


    df_weekly = expand_to_weeks(df_agg, "ano")

    validate_dataframe(df_weekly, "calidad_aire_promedio_anual", ["ano", "semana", "departamento", "municipio"])
    return df_weekly

















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






def apply_sanity_filters(df):
    _require_pyspark()
    """
    Removes rows with values that are physically impossible or highly 
    unlikely for the Colombian context (outlier cleaning).
    """

    df = df.filter((F.col("temperatura_promedio") >= 0) & (F.col("temperatura_promedio") <= 45))


    df = df.filter((F.col("precipitacion_promedio") >= 0) & (F.col("precipitacion_promedio") <= 1200))



    df = df.filter((F.col("calidad_aire_promedio") >= 0) & (F.col("calidad_aire_promedio") <= 500))



    df = df.filter((F.col("latitud") >= -5) & (F.col("latitud") <= 15))
    df = df.filter((F.col("longitud") >= -82) & (F.col("longitud") <= -65))


    df = df.filter(F.col("casos_totales") >= 0)

    return df






def main():
    _require_pyspark()
    spark = create_spark_session()












    SRC_DIR       = Path(__file__).resolve().parent                 
    PROJECT_ROOT  = SRC_DIR.parent                                            
    base_path     = str(PROJECT_ROOT / "data" / "raw") + "/"
    output_dir    = PROJECT_ROOT / "data" / "processed"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"  Project root : {PROJECT_ROOT}")
    print(f"  Raw data     : {base_path}")
    print(f"  Output dir   : {output_dir}\n")

    print("\n" + "=" * 60)
    print("  EPIDEMIOLOGICAL MASTER DATASET PIPELINE")
    print("=" * 60 + "\n")


    print("Step 1/5 — Processing individual datasets...\n")

    df_vsp   = process_vigilancia(spark, base_path + "vigilancia_salud_publica.csv")
    df_clima = process_clima(spark, base_path + "normales_climatologicas.csv")
    df_aire  = process_calidad_aire(spark, base_path + "calidad_aire_promedio_anual.csv")
    df_hosp  = process_prestadores(spark, base_path + "prestadores_sedes.csv")


    print("Step 2/5 — Joining datasets with geographic imputation...\n")



    df_geo_ref = df_clima.select("departamento", "municipio", "latitud", "longitud").distinct()\
        .union(df_aire.select("departamento", "municipio", 
                              F.col("latitud_aire").alias("latitud"), 
                              F.col("longitud_aire").alias("longitud")).distinct())\
        .groupBy("departamento", "municipio")\
        .agg(F.avg("latitud").alias("latitud"), F.avg("longitud").alias("longitud"))


    df_vsp_munis = df_vsp.select("departamento", "municipio").distinct()
    df_vsp_geo = df_vsp_munis.join(df_geo_ref, on=["departamento", "municipio"], how="left")


    print("     Finding nearest neighbors for climate data...")
    mapping_clima = get_geographic_mapping(spark, df_vsp_geo, df_clima)


    print("     Finding nearest neighbors for air quality data...")
    df_aire_geo = df_aire.select(
        "departamento", "municipio", 
        F.col("latitud_aire").alias("latitud"), 
        F.col("longitud_aire").alias("longitud")
    ).distinct()
    mapping_aire = get_geographic_mapping(spark, df_vsp_geo, df_aire_geo)


    master_df = df_vsp


    master_df = master_df.join(mapping_clima.select(
        F.col("target_dept").alias("departamento"),
        F.col("target_muni").alias("municipio"),
        F.col("ref_dept").alias("clima_dept"),
        F.col("ref_muni").alias("clima_muni")
    ), on=["departamento", "municipio"], how="left")


    master_df = master_df.withColumn("clima_dept", F.coalesce(F.col("clima_dept"), F.col("departamento")))\
                         .withColumn("clima_muni", F.coalesce(F.col("clima_muni"), F.col("municipio")))

    master_df = master_df.join(
        df_clima.withColumnRenamed("departamento", "clima_dept")
                .withColumnRenamed("municipio", "clima_muni")
                .withColumnRenamed("latitud", "latitud_clima")
                .withColumnRenamed("longitud", "longitud_clima"),
        on=["semana", "clima_dept", "clima_muni"], how="left"
    ).drop("clima_dept", "clima_muni")


    master_df = master_df.join(mapping_aire.select(
        F.col("target_dept").alias("departamento"),
        F.col("target_muni").alias("municipio"),
        F.col("ref_dept").alias("aire_dept"),
        F.col("ref_muni").alias("aire_muni")
    ), on=["departamento", "municipio"], how="left")

    master_df = master_df.withColumn("aire_dept", F.coalesce(F.col("aire_dept"), F.col("departamento")))\
                         .withColumn("aire_muni", F.coalesce(F.col("aire_muni"), F.col("municipio")))

    master_df = master_df.join(
        df_aire.withColumnRenamed("departamento", "aire_dept")
               .withColumnRenamed("municipio", "aire_muni"),
        on=["ano", "semana", "aire_dept", "aire_muni"], how="left"
    ).drop("aire_dept", "aire_muni")



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


    master_df = master_df.withColumn("vacunacion", F.lit("NO_REPORTA"))


    master_df = master_df.join(
        df_hosp, on=["departamento", "municipio"], how="left"
    )




    master_df.cache()
    master_df.count()                             


    print("Step 3/5 — Creating brote column...\n")
    master_df = create_brote_column(master_df, df_vsp)


    print("Step 4/5 — Finalizing schema and deduplication...\n")

    master_df = master_df.withColumn(
        "join_key",
        F.concat_ws("_", F.col("ano"), F.col("semana"), F.col("departamento"), F.col("municipio")),
    )


    master_df = master_df.fillna({"cantidad_hospitales": 0})

    FINAL_COLS = [
        "ano", "semana", "departamento", "municipio", "enfermedad",
        "casos_totales", "temperatura_promedio", "precipitacion_promedio",
        "calidad_aire_promedio", "vacunacion",
        "cantidad_hospitales", "latitud", "longitud", "brote",
    ]


    INT_COLS   = {"ano", "semana", "casos_totales", "brote", "cantidad_hospitales"}
    FLOAT_COLS = {
        "temperatura_promedio", "precipitacion_promedio",
        "calidad_aire_promedio", "latitud", "longitud",
    }

    for col in FINAL_COLS:
        if col not in master_df.columns:
            if col in INT_COLS:
                master_df = master_df.withColumn(col, F.lit(None).cast("int"))
            elif col in FLOAT_COLS:
                master_df = master_df.withColumn(col, F.lit(None).cast("double"))
            else:
                master_df = master_df.withColumn(col, F.lit(None).cast("string"))


    master_df = master_df.withColumn("temperatura_promedio", F.round("temperatura_promedio", 2))
    master_df = master_df.withColumn("precipitacion_promedio", F.round("precipitacion_promedio", 2))
    master_df = master_df.withColumn("calidad_aire_promedio", F.round("calidad_aire_promedio", 2))


    master_df = master_df.withColumn("latitud", F.round("latitud", 5))
    master_df = master_df.withColumn("longitud", F.round("longitud", 5))

    master_df = master_df.select(*FINAL_COLS)


    master_df = master_df.dropDuplicates()






    master_df.cache()
    total_rows = master_df.count()                                           


    print("Step 5/5 — Filtering incomplete and unreal records...\n")


    master_df = master_df.dropna()


    master_df = apply_sanity_filters(master_df)

    total_rows = master_df.count()

    print(f"{'='*60}")
    print(f"  VALIDATION: MASTER DATASET (CLEAN & COMPLETE RECORDS)")
    print(f"{'='*60}")
    print(f"  Rows total : {total_rows:,}")


    null_exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in FINAL_COLS]
    null_counts = master_df.agg(*null_exprs).collect()[0]

    print("\n  Null summary per column:")
    for col in FINAL_COLS:
        n = null_counts[col]
        pct = (n / total_rows * 100) if total_rows > 0 else 0
        print(f"    {col:<35} nulls={n:>7,}  ({pct:.1f}%)")


    print(f"\n  Brote distribution:")
    master_df.groupBy("brote").count().orderBy("brote").show()


    print("  Cases per disease:")
    master_df.groupBy("enfermedad").agg(
        F.sum("casos_totales").alias("total_casos"),
        F.count("*").alias("filas"),
    ).orderBy("total_casos", ascending=False).show()

    print(f"{'='*60}\n")












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
