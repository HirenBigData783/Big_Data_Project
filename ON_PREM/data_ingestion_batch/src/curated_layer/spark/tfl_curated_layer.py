from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# =====================================================
# Spark Session
# =====================================================

spark = SparkSession.builder \
    .appName("TFL Curated Layer") \
    .enableHiveSupport() \
    .getOrCreate()

print('=====================================================')
print('TFL Curated Layer')
print('Author: APARNA')
print('=====================================================')

db = "tfl_db"
base_path = "/tmp/aparna/tfl_proj/curated_data"

# =====================================================
# LOAD SOURCES
# =====================================================

networks = spark.table(f"{db}.dim_networks")
lines = spark.table(f"{db}.dim_lines")
stations = spark.table(f"{db}.dim_stations")
dates = spark.table(f"{db}.dim_date")
fact_station_lines = spark.table(f"{db}.fact_station_lines")
fact_passenger = spark.table(f"{db}.fact_passenger_entry_exit")

# =====================================================
# DIM_NETWORKS_CURATED
# =====================================================

dim_networks_curated = networks.select(
    col("network_id"),
    trim(col("network_name")).alias("network_name"),
    trim(col("network_type")).alias("network_type"),
    current_timestamp().alias("load_timestamp")
)

dim_networks_curated.write.mode("overwrite").parquet(f"{base_path}/dim_networks")

# =====================================================
# DIM_LINES_CURATED
# =====================================================

dim_lines_curated = lines.select(
    col("line_id"),
    trim(col("line_name")).alias("line_name"),
    upper(trim(col("line_color"))).alias("line_color"),
    when(col("is_night_service"), "Y").otherwise("N").alias("night_service_flag"),
    current_timestamp().alias("load_timestamp")
)

dim_lines_curated.write.mode("overwrite").parquet(f"{base_path}/dim_lines")

# =====================================================
# DIM_STATIONS_CURATED
# =====================================================

dim_stations_curated = stations.alias("s") \
    .join(networks.alias("n"), col("s.network_id") == col("n.network_id"), "left") \
    .select(
        col("s.station_id"),
        regexp_replace(col("s.nlc_code").cast("string"), r"\.0$", "").alias("station_code"),
        upper(trim(col("s.station_name"))).alias("station_name"),
        col("s.network_id"),
        col("n.network_name"),

        when(col("s.has_london_underground"), "Y").otherwise("N").alias("has_london_underground"),
        when(col("s.has_elizabeth_line"), "Y").otherwise("N").alias("has_elizabeth_line"),
        when(col("s.has_overground"), "Y").otherwise("N").alias("has_overground"),
        when(col("s.has_dlr"), "Y").otherwise("N").alias("has_dlr"),
        when(col("s.has_night_tube"), "Y").otherwise("N").alias("has_night_tube"),

        when(col("s.is_active"), "ACTIVE").otherwise("INACTIVE").alias("active_status"),
        current_timestamp().alias("load_timestamp")
    )

dim_stations_curated.write.mode("overwrite").parquet(f"{base_path}/dim_stations")

# =====================================================
# DIM_DATE_CURATED
# =====================================================

dim_date_curated = dates.select(
    col("date_id"),
    col("year"),
    coalesce(col("quarter"), lit(0)).alias("quarter"),
    coalesce(col("month"), lit(0)).alias("month"),
    when(col("is_annual"), "Y").otherwise("N").alias("is_annual"),
    trim(col("period_label")).alias("period_label"),
    col("period_start"),
    col("period_end"),
    current_timestamp().alias("load_timestamp")
)

dim_date_curated.write.mode("overwrite").parquet(f"{base_path}/dim_date")

# =====================================================
# FACT_STATION_LINES_CURATED
# =====================================================

fact_station_lines_curated = fact_station_lines.alias("f") \
    .join(stations.alias("s"), col("f.station_id") == col("s.station_id"), "left") \
    .join(lines.alias("l"), col("f.line_id") == col("l.line_id"), "left") \
    .select(
        col("f.station_line_id"),
        col("f.station_id"),
        col("s.station_name"),
        col("f.line_id"),
        col("l.line_name"),
        when(col("f.is_interchange"), "Y").otherwise("N").alias("is_interchange"),
        col("f.effective_from"),
        col("f.effective_to"),
        current_timestamp().alias("load_timestamp")
    )

fact_station_lines_curated.write.mode("overwrite") \
    .parquet(f"{base_path}/fact_station_lines")

# =====================================================
# FACT_PASSENGER_ENTRY_EXIT_CURATED
# =====================================================

fact_passenger_curated = fact_passenger.alias("f") \
    .join(stations.alias("s"), col("f.station_id") == col("s.station_id"), "left") \
    .join(dates.alias("d"), col("f.date_id") == col("d.date_id"), "left") \
    .select(
        col("f.entry_exit_id"),
        col("f.station_id"),
        col("s.station_name"),
        col("d.year"),
        col("f.total_entry_exit"),
        col("f.estimated_entries"),
        col("f.estimated_exits"),

        when(col("f.total_entry_exit") >= 10000000, "VERY HIGH")
        .when(col("f.total_entry_exit") >= 5000000, "HIGH")
        .when(col("f.total_entry_exit") >= 1000000, "MEDIUM")
        .otherwise("LOW").alias("passenger_volume_band"),

        trim(col("f.record_type")).alias("record_type"),
        trim(col("f.data_source")).alias("data_source"),
        current_timestamp().alias("load_timestamp")
    )

fact_passenger_curated.write.mode("overwrite") \
    .parquet(f"{base_path}/fact_passenger_entry_exit")

# =====================================================
# VALIDATION
# =====================================================

print("========== CURATED LAYER VALIDATION ==========")

tables = [
    "dim_networks",
    "dim_lines",
    "dim_stations",
    "dim_date",
    "fact_station_lines",
    "fact_passenger_entry_exit"
]

for t in tables:
    path = f"{base_path}/{t}"
    df = spark.read.parquet(path)
    print(f"{t}_curated -> {df.count()} rows")

print("========== TRANSFORMATION COMPLETE ==========")