import sys
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, desc, max as _max, current_timestamp, lit, when, lower
from pyspark.sql.types import IntegerType


spark = SparkSession.builder \
    .appName("TFL_Incremental_Analysis") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


HDFS_BASE = "/tmp/tfl_project_hadoop"
OUTPUT_BASE = "/tmp/tfl_project_hadoop/gold"
HIVE_DB = "tfl_db"

CHECK_COLUMN = "entry_exit_id"

WATERMARK_BASE = f"{OUTPUT_BASE}/_watermarks"
WATERMARK_PATH = f"{WATERMARK_BASE}/fact_passenger_entry_exit"
DIM_LINES_WATERMARK_PATH = f"{WATERMARK_BASE}/dim_lines_curated"
DIM_STATIONS_WATERMARK_PATH = f"{WATERMARK_BASE}/dim_stations_curated"


spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
spark.sql(f"USE {HIVE_DB}")


print("=" * 60)
print("TFL Incremental Data Analysis Pipeline")
print("=" * 60)


# ============================================================
# HDFS HELPER FUNCTIONS
# ============================================================

def get_hdfs_filesystem():
    return spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    )


def path_exists(path):
    fs = get_hdfs_filesystem()
    return fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path))


def delete_path(path):
    fs = get_hdfs_filesystem()
    hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(path)

    if fs.exists(hdfs_path):
        fs.delete(hdfs_path, True)


def rename_path(source_path, target_path):
    fs = get_hdfs_filesystem()

    source = spark._jvm.org.apache.hadoop.fs.Path(source_path)
    target = spark._jvm.org.apache.hadoop.fs.Path(target_path)

    if fs.exists(target):
        fs.delete(target, True)

    fs.rename(source, target)


# ============================================================
# WATERMARK FUNCTIONS
# ============================================================

def read_last_value(watermark_path, watermark_name="watermark"):
    """
    Reads the previous max value from an HDFS watermark path.
    If watermark does not exist, is empty, or contains blank value, returns 0.
    """
    if not path_exists(watermark_path):
        print(f"{watermark_name}: watermark path does not exist. Starting from 0.")
        return 0

    row = spark.read.text(watermark_path).first()

    if row is None:
        print(f"{watermark_name}: watermark file is empty. Starting from 0.")
        return 0

    value = row["value"]

    if value is None or value.strip() == "":
        print(f"{watermark_name}: watermark value is blank. Starting from 0.")
        return 0

    try:
        return int(value.strip())
    except ValueError:
        raise ValueError(f"{watermark_name}: invalid watermark value found: {value}")


def write_last_value(watermark_path, value):
    """
    Writes latest max value to an HDFS watermark path.
    This should only be called after the related append/write is successful.
    """
    temp_path = f"{watermark_path}_tmp_{uuid4().hex}"

    spark.createDataFrame([(str(int(value)),)], ["value"]) \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .text(temp_path)

    rename_path(temp_path, watermark_path)

    print(f"Watermark updated: {watermark_path}")
    print(f"New watermark value: {value}")


def get_target_table_max_value(target_table, target_column):
    """
    Reads the max ID already present in the existing Hive curated table.
    This is used as a safe starting watermark if no watermark file exists yet.
    """
    try:
        row = spark.sql(
            f"SELECT MAX({target_column}) AS max_value FROM {target_table}"
        ).first()

        if row is None or row["max_value"] is None:
            return 0

        return int(row["max_value"])

    except Exception as e:
        raise Exception(
            f"Unable to read existing max value from {target_table}.{target_column}. "
            f"Please confirm the Hive table exists and is readable. Original error: {e}"
        )


def read_dimension_last_value(watermark_path, watermark_name, target_table, target_column):
    """
    For dimension curated tables:
      1. If a watermark exists, use it.
      2. If no watermark exists, use MAX(id) from the existing Hive curated table.
         This avoids duplicating rows when the curated table already contains data.
    """
    if path_exists(watermark_path):
        return read_last_value(watermark_path, watermark_name)

    target_max_value = get_target_table_max_value(target_table, target_column)

    print(
        f"{watermark_name}: no watermark file found. "
        f"Using existing Hive target max {target_column} = {target_max_value} as starting point."
    )

    return target_max_value


# ============================================================
# SAFE PARQUET WRITE FUNCTION
# ============================================================

def write_parquet_replace(df, path):
    """
    Writes output to a temporary path first, then replaces final path.
    This avoids issues when reading and overwriting the same path.
    """
    temp_path = f"{path}_tmp_{uuid4().hex}"

    df.write.mode("overwrite").parquet(temp_path)

    rename_path(temp_path, path)

    print(f"Saved output to HDFS: {path}")


# ============================================================
# INCREMENTAL MERGE FUNCTION FOR AGGREGATED GOLD TABLES
# ============================================================

def merge_sum_gold(delta_df, table_name, group_cols, sum_cols):
    """
    Merges new delta aggregate with existing gold aggregate.

    Example:
    Existing:
        year = 2024, total_passengers = 1000

    Delta:
        year = 2024, total_passengers = 200

    Final:
        year = 2024, total_passengers = 1200
    """
    path = f"{OUTPUT_BASE}/{table_name}"

    if path_exists(path):
        existing_df = spark.read.parquet(path)
        combined_df = existing_df.unionByName(delta_df, allowMissingColumns=True)
    else:
        combined_df = delta_df

    merged_df = combined_df.groupBy(*group_cols).agg(
        *[_sum(c).alias(c) for c in sum_cols]
    )

    write_parquet_replace(merged_df, path)

    return merged_df



# ============================================================
# APPEND DIMENSION DATA INTO EXISTING HIVE CURATED TABLES
# ============================================================

def append_dim_lines_curated(dim_lines_df):
    """
    Appends only new dim_lines rows into the existing Hive table:
      tfl_db.dim_lines_curated

    Watermark column:
      line_id
    """
    target_table = f"{HIVE_DB}.dim_lines_curated"
    watermark_column = "line_id"

    last_value = read_dimension_last_value(
        DIM_LINES_WATERMARK_PATH,
        "dim_lines_curated",
        target_table,
        watermark_column
    )

    source_df = dim_lines_df.withColumn(
        watermark_column,
        col(watermark_column).cast("int")
    )

    delta_df = source_df.filter(col(watermark_column) > lit(last_value))

    new_last_value_row = delta_df.agg(
        _max(watermark_column).alias("new_last_value")
    ).first()

    new_last_value = new_last_value_row["new_last_value"]

    if new_last_value is None:
        print(f"{target_table}: no new rows to append.")
        # If the watermark file does not exist yet, create it from the existing target max.
        if not path_exists(DIM_LINES_WATERMARK_PATH):
            write_last_value(DIM_LINES_WATERMARK_PATH, last_value)
        return last_value, last_value, 0

    curated_df = delta_df.select(
    col("line_id").cast("int").alias("line_id"),
    col("line_name").cast("string").alias("line_name"),
    col("line_color").cast("string").alias("line_color"),
    when(
        lower(col("is_night_service").cast("string")).isin("true", "1", "yes", "y"),
        lit("Y")
    ).otherwise(lit("N")).alias("night_service_flag"),
    current_timestamp().alias("load_timestamp")
    )

    rows_to_append = curated_df.count()

    print("=" * 60)
    print("APPEND: dim_lines_curated")
    print(f"Target table        = {target_table}")
    print(f"Watermark column    = {watermark_column}")
    print(f"OLD_LAST_VALUE      = {last_value}")
    print(f"NEW_LAST_VALUE      = {new_last_value}")
    print(f"ROWS TO APPEND      = {rows_to_append}")
    print("=" * 60)

    curated_df.write \
        .mode("append") \
        .insertInto(target_table)

    write_last_value(DIM_LINES_WATERMARK_PATH, new_last_value)

    print(f"Append completed: {target_table}")

    return last_value, int(new_last_value), rows_to_append


def append_dim_stations_curated(dim_stations_df, dim_networks_df):
    """
    Appends only new dim_stations rows into the existing Hive table:
      tfl_db.dim_stations_curated

    Watermark column:
      station_id

    Source-to-target mapping:
      nlc_code   -> station_code
      is_active  -> active_status
      network_id -> joins dim_networks to bring network_name
    """
    target_table = f"{HIVE_DB}.dim_stations_curated"
    watermark_column = "station_id"

    last_value = read_dimension_last_value(
        DIM_STATIONS_WATERMARK_PATH,
        "dim_stations_curated",
        target_table,
        watermark_column
    )

    source_df = dim_stations_df.withColumn(
        watermark_column,
        col(watermark_column).cast("int")
    )

    delta_df = source_df.filter(col(watermark_column) > lit(last_value))

    new_last_value_row = delta_df.agg(
        _max(watermark_column).alias("new_last_value")
    ).first()

    new_last_value = new_last_value_row["new_last_value"]

    if new_last_value is None:
        print(f"{target_table}: no new rows to append.")
        # If the watermark file does not exist yet, create it from the existing target max.
        if not path_exists(DIM_STATIONS_WATERMARK_PATH):
            write_last_value(DIM_STATIONS_WATERMARK_PATH, last_value)
        return last_value, last_value, 0

    networks_for_join = dim_networks_df.select(
        col("network_id").cast("int").alias("network_id"),
        col("network_name").cast("string").alias("network_name")
    )

    curated_df = (
        delta_df
        .join(networks_for_join, "network_id", "left")
        .select(
            col("station_id").cast("int").alias("station_id"),
            col("nlc_code").cast("string").alias("station_code"),
            col("station_name").cast("string").alias("station_name"),
            col("network_id").cast("int").alias("network_id"),
            col("network_name").cast("string").alias("network_name"),
            col("has_london_underground").cast("string").alias("has_london_underground"),
            col("has_elizabeth_line").cast("string").alias("has_elizabeth_line"),
            col("has_overground").cast("string").alias("has_overground"),
            col("has_dlr").cast("string").alias("has_dlr"),
            col("has_night_tube").cast("string").alias("has_night_tube"),
            when(
                lower(col("is_active").cast("string")).isin("true", "1", "yes", "y", "active"),
                lit("Active")
            ).otherwise(lit("Inactive")).alias("active_status"),
            current_timestamp().alias("load_timestamp")
        )
    )

    rows_to_append = curated_df.count()

    print("=" * 60)
    print("APPEND: dim_stations_curated")
    print(f"Target table        = {target_table}")
    print(f"Watermark column    = {watermark_column}")
    print(f"OLD_LAST_VALUE      = {last_value}")
    print(f"NEW_LAST_VALUE      = {new_last_value}")
    print(f"ROWS TO APPEND      = {rows_to_append}")
    print("=" * 60)

    curated_df.write \
        .mode("append") \
        .insertInto(target_table)

    write_last_value(DIM_STATIONS_WATERMARK_PATH, new_last_value)

    print(f"Append completed: {target_table}")

    return last_value, int(new_last_value), rows_to_append


# ============================================================
# LOAD ALL TABLES FROM HDFS
# ============================================================

print("\nLoading tables from HDFS...")

dim_date = spark.read.option("header", "false").option("inferSchema", "true") \
    .csv(f"{HDFS_BASE}/dim_date_full_load") \
    .toDF(
        "date_id",
        "year",
        "quarter",
        "month",
        "is_annual",
        "period_label",
        "period_start",
        "period_end",
        "created_at"
    )

dim_lines = spark.read.option("header", "false").option("inferSchema", "true") \
    .csv(f"{HDFS_BASE}/dim_lines_full_load") \
    .toDF(
        "line_id",
        "line_name",
        "line_color",
        "is_night_service",
        "created_at",
        "updated_at"
    )

dim_networks = spark.read.option("header", "false").option("inferSchema", "true") \
    .csv(f"{HDFS_BASE}/dim_networks_full_load") \
    .toDF(
        "network_id",
        "network_name",
        "network_type",
        "created_at",
        "updated_at"
    )

dim_stations = spark.read.option("header", "false").option("inferSchema", "true") \
    .csv(f"{HDFS_BASE}/dim_stations_full_load") \
    .toDF(
        "station_id",
        "nlc_code",
        "station_name",
        "network_id",
        "has_london_underground",
        "has_elizabeth_line",
        "has_overground",
        "has_dlr",
        "has_night_tube",
        "is_active",
        "created_at",
        "updated_at"
    )

fact_pax = spark.read.option("header", "false").option("inferSchema", "true") \
    .csv(f"{HDFS_BASE}/fact_passenger_entry_exit_full_load") \
    .toDF(
        "entry_exit_id",
        "station_id",
        "date_id",
        "total_entry_exit",
        "estimated_entries",
        "estimated_exits",
        "record_type",
        "data_source",
        "created_at"
    )

fact_lines = spark.read.option("header", "false").option("inferSchema", "true") \
    .csv(f"{HDFS_BASE}/fact_station_lines_full_load") \
    .toDF(
        "station_line_id",
        "station_id",
        "line_id",
        "is_interchange",
        "effective_from",
        "effective_to",
        "created_at"
    )


print("All 6 source tables loaded successfully")


# ============================================================
# APPEND DIMENSION SOURCE DATA INTO EXISTING HIVE CURATED TABLES
# ============================================================

print("\nAppending dimension source data into existing Hive curated tables...")

DIM_LINES_OLD_LAST_VALUE, DIM_LINES_NEW_LAST_VALUE, DIM_LINES_ROWS_APPENDED = append_dim_lines_curated(
    dim_lines
)

DIM_STATIONS_OLD_LAST_VALUE, DIM_STATIONS_NEW_LAST_VALUE, DIM_STATIONS_ROWS_APPENDED = append_dim_stations_curated(
    dim_stations,
    dim_networks
)



# ============================================================
# APPLY INCREMENTAL FILTER ON MAIN FACT TABLE
# ============================================================

LAST_VALUE = read_last_value(WATERMARK_PATH, "fact_passenger_entry_exit")

fact_pax = fact_pax.withColumn(
    CHECK_COLUMN,
    col(CHECK_COLUMN).cast("long")
)

incremental_fact_pax = fact_pax.filter(
    col(CHECK_COLUMN) > LAST_VALUE
)

new_last_value_row = incremental_fact_pax.agg(
    _max(CHECK_COLUMN).alias("new_last_value")
).first()

NEW_LAST_VALUE = new_last_value_row["new_last_value"]

if NEW_LAST_VALUE is None:
    print("=" * 60)
    print("No new records found.")
    print(f"CHECK_COLUMN   = {CHECK_COLUMN}")
    print(f"LAST_VALUE     = {LAST_VALUE}")
    print("Pipeline stopped without updating gold outputs.")
    print("=" * 60)

    spark.stop()
    sys.exit(0)

fact_pax = incremental_fact_pax.cache()

print("=" * 60)
print("Incremental filter applied")
print(f"CHECK_COLUMN       = {CHECK_COLUMN}")
print(f"OLD_LAST_VALUE     = {LAST_VALUE}")
print(f"NEW_LAST_VALUE     = {NEW_LAST_VALUE}")
print(f"NEW RECORD COUNT   = {fact_pax.count()}")
print("=" * 60)


# ============================================================
# ANALYSIS 1: Top 10 Busiest Stations
# ============================================================

print("\n" + "=" * 60)
print("ANALYSIS 1: Top 10 Busiest Stations")
print("=" * 60)

station_passenger_delta = fact_pax \
    .join(dim_stations, "station_id") \
    .groupBy("station_name") \
    .agg(_sum("total_entry_exit").alias("total_passengers"))

station_passenger_totals = merge_sum_gold(
    station_passenger_delta,
    "gold_station_passenger_totals",
    ["station_name"],
    ["total_passengers"]
)

busiest_stations = station_passenger_totals \
    .orderBy(desc("total_passengers")) \
    .limit(10)

busiest_stations.show(truncate=False)

write_parquet_replace(
    busiest_stations,
    f"{OUTPUT_BASE}/gold_busiest_stations"
)


# ============================================================
# ANALYSIS 2: Passengers by Year
# ============================================================

print("\n" + "=" * 60)
print("ANALYSIS 2: Total Passengers by Year")
print("=" * 60)

passengers_by_year_delta = fact_pax \
    .join(dim_date, "date_id") \
    .groupBy("year") \
    .agg(_sum("total_entry_exit").alias("total_passengers"))

passengers_by_year = merge_sum_gold(
    passengers_by_year_delta,
    "gold_passengers_by_year",
    ["year"],
    ["total_passengers"]
)

passengers_by_year.orderBy("year").show(truncate=False)


# ============================================================
# ANALYSIS 3: Passengers by Tube Line
# ============================================================

print("\n" + "=" * 60)
print("ANALYSIS 3: Passengers by Tube Line")
print("=" * 60)

passengers_by_line_delta = fact_pax \
    .join(fact_lines, "station_id") \
    .join(dim_lines, "line_id") \
    .groupBy("line_name") \
    .agg(_sum("total_entry_exit").alias("total_passengers"))

passengers_by_line = merge_sum_gold(
    passengers_by_line_delta,
    "gold_passengers_by_line",
    ["line_name"],
    ["total_passengers"]
)

passengers_by_line.orderBy(desc("total_passengers")).show(truncate=False)


# ============================================================
# ANALYSIS 4: Passengers by Network
# ============================================================

print("\n" + "=" * 60)
print("ANALYSIS 4: Passengers by Network Type")
print("=" * 60)

passengers_by_network_delta = fact_pax \
    .join(dim_stations, "station_id") \
    .join(dim_networks, "network_id") \
    .groupBy("network_name", "network_type") \
    .agg(_sum("total_entry_exit").alias("total_passengers"))

passengers_by_network = merge_sum_gold(
    passengers_by_network_delta,
    "gold_passengers_by_network",
    ["network_name", "network_type"],
    ["total_passengers"]
)

passengers_by_network.orderBy(desc("total_passengers")).show(truncate=False)


# ============================================================
# ANALYSIS 5: Interchange Stations
# ============================================================

print("\n" + "=" * 60)
print("ANALYSIS 5: Top Interchange Stations")
print("=" * 60)

interchange_stations = fact_lines \
    .join(dim_stations, "station_id") \
    .groupBy("station_name") \
    .agg(count("line_id").alias("num_lines")) \
    .orderBy(desc("num_lines")) \
    .limit(15)

interchange_stations.show(truncate=False)

write_parquet_replace(
    interchange_stations,
    f"{OUTPUT_BASE}/gold_interchange_stations"
)


# ============================================================
# ANALYSIS 6: Quarterly Trend
# ============================================================

print("\n" + "=" * 60)
print("ANALYSIS 6: Passengers by Year and Quarter")
print("=" * 60)

quarterly_trend_delta = fact_pax \
    .join(dim_date, "date_id") \
    .select(
        col("year").cast(IntegerType()).alias("year"),
        col("quarter").cast(IntegerType()).alias("quarter"),
        col("total_entry_exit")
    ) \
    .groupBy("year", "quarter") \
    .agg(_sum("total_entry_exit").alias("total_passengers"))

quarterly_trend = merge_sum_gold(
    quarterly_trend_delta,
    "gold_quarterly_trend",
    ["year", "quarter"],
    ["total_passengers"]
)

quarterly_trend.orderBy("year", "quarter").show(truncate=False)


# ============================================================
# ANALYSIS 7: Night Tube Analysis
# ============================================================

print("\n" + "=" * 60)
print("ANALYSIS 7: Night Tube vs Regular Stations")
print("=" * 60)

night_tube_delta = fact_pax \
    .join(dim_stations, "station_id") \
    .groupBy("has_night_tube") \
    .agg(
        count("station_id").alias("num_records"),
        _sum("total_entry_exit").alias("total_passengers")
    )

night_tube_base = merge_sum_gold(
    night_tube_delta,
    "gold_night_tube_analysis_base",
    ["has_night_tube"],
    ["num_records", "total_passengers"]
)

night_tube_analysis = night_tube_base.withColumn(
    "avg_passengers_per_record",
    col("total_passengers") / col("num_records")
)

night_tube_analysis.show(truncate=False)

write_parquet_replace(
    night_tube_analysis,
    f"{OUTPUT_BASE}/gold_night_tube_analysis"
)


# ============================================================
# UPDATE WATERMARK ONLY AFTER SUCCESSFUL PROCESSING
# ============================================================

write_last_value(WATERMARK_PATH, NEW_LAST_VALUE)


# ============================================================
# SUMMARY
# ============================================================

print("\n" + "=" * 60)
print("INCREMENTAL PIPELINE COMPLETE")
print("=" * 60)
print(f"Database: {HIVE_DB}")
print(f"Watermark path: {WATERMARK_PATH}")
print(f"Old watermark value: {LAST_VALUE}")
print(f"New watermark value: {NEW_LAST_VALUE}")
print("")
print("Dimension curated appends:")
print(f"  dim_lines_curated    old watermark: {DIM_LINES_OLD_LAST_VALUE}, new watermark: {DIM_LINES_NEW_LAST_VALUE}, rows appended: {DIM_LINES_ROWS_APPENDED}")
print(f"  dim_stations_curated old watermark: {DIM_STATIONS_OLD_LAST_VALUE}, new watermark: {DIM_STATIONS_NEW_LAST_VALUE}, rows appended: {DIM_STATIONS_ROWS_APPENDED}")
print("Source tables:")
print("  dim_date")
print("  dim_lines")
print("  dim_networks")
print("  dim_stations")
print("  fact_passenger_entry_exit")
print("  fact_station_lines")
print("Gold outputs:")
print("  gold_busiest_stations")
print("  gold_station_passenger_totals")
print("  gold_passengers_by_year")
print("  gold_passengers_by_line")
print("  gold_passengers_by_network")
print("  gold_interchange_stations")
print("  gold_quarterly_trend")
print("  gold_night_tube_analysis")
print("  gold_night_tube_analysis_base")
print("=" * 60)

spark.stop()