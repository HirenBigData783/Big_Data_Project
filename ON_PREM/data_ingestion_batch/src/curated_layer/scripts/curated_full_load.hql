-- =====================================================
-- TFL DATA WAREHOUSE - CURATED TRANSFORMATION LAYER
-- =====================================================

-- Default parameters (can be overridden via beeline)
SET hivevar:DB_NAME=tfl_db;
SET hivevar:BASE_PATH=/tmp/aparna/tfl_proj/curated_data;

-- Use database
USE ${hivevar:DB_NAME};

-- =====================================================
-- DIM_NETWORKS_CURATED
-- =====================================================

CREATE EXTERNAL TABLE IF NOT EXISTS dim_networks_curated (
    network_id INT,
    network_name STRING,
    network_type STRING,
    load_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION '${hivevar:BASE_PATH}/dim_networks';

-- =====================================================
-- DIM_LINES_CURATED
-- =====================================================

CREATE EXTERNAL TABLE IF NOT EXISTS dim_lines_curated (
    line_id INT,
    line_name STRING,
    line_color STRING,
    night_service_flag STRING,
    load_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION '${hivevar:BASE_PATH}/dim_lines';

-- =====================================================
-- DIM_STATIONS_CURATED
-- =====================================================

CREATE EXTERNAL TABLE IF NOT EXISTS dim_stations_curated (
    station_id INT,
    station_code STRING,
    station_name STRING,
    network_id INT,
    network_name STRING,
    has_london_underground STRING,
    has_elizabeth_line STRING,
    has_overground STRING,
    has_dlr STRING,
    has_night_tube STRING,
    active_status STRING,
    load_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION '${hivevar:BASE_PATH}/dim_stations';

-- =====================================================
-- DIM_DATE_CURATED
-- =====================================================

CREATE EXTERNAL TABLE IF NOT EXISTS dim_date_curated (
    date_id INT,
    year INT,
    quarter INT,
    month INT,
    is_annual STRING,
    period_label STRING,
    period_start DATE,
    period_end DATE,
    load_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION '${hivevar:BASE_PATH}/dim_date';

-- =====================================================
-- FACT_STATION_LINES_CURATED
-- =====================================================

CREATE EXTERNAL TABLE IF NOT EXISTS fact_station_lines_curated (
    station_line_id INT,
    station_id INT,
    station_name STRING,
    line_id INT,
    line_name STRING,
    is_interchange STRING,
    effective_from DATE,
    effective_to DATE,
    load_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION '${hivevar:BASE_PATH}/fact_station_lines';

-- =====================================================
-- FACT_PASSENGER_ENTRY_EXIT_CURATED
-- =====================================================

CREATE EXTERNAL TABLE IF NOT EXISTS fact_passenger_entry_exit_curated (
    entry_exit_id INT,
    station_id INT,
    station_name STRING,
    year INT,
    total_entry_exit BIGINT,
    estimated_entries BIGINT,
    estimated_exits BIGINT,
    passenger_volume_band STRING,
    record_type STRING,
    data_source STRING,
    load_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION '${hivevar:BASE_PATH}/fact_passenger_entry_exit';

-- =====================================================
-- VALIDATION QUERIES
-- =====================================================

SELECT 'dim_networks_curated' AS table_name, COUNT(*) FROM dim_networks_curated;
SELECT 'dim_lines_curated' AS table_name, COUNT(*) FROM dim_lines_curated;
SELECT 'dim_stations_curated' AS table_name, COUNT(*) FROM dim_stations_curated;
SELECT 'dim_date_curated' AS table_name, COUNT(*) FROM dim_date_curated;
SELECT 'fact_station_lines_curated' AS table_name, COUNT(*) FROM fact_station_lines_curated;
SELECT 'fact_passenger_entry_exit_curated' AS table_name, COUNT(*) FROM fact_passenger_entry_exit_curated;