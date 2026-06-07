-- =====================================================
-- TFL DATA WAREHOUSE - HIVE ETL PIPELINE
-- =====================================================

!echo ==========================================
!echo STARTING TFL HIVE ETL PIPELINE
!echo ==========================================

-- =========================
-- STEP 1: DATABASE SETUP
-- =========================

!echo STEP 1: Creating database tfl_db

CREATE DATABASE IF NOT EXISTS tfl_db;

USE tfl_db;

!echo Database ready: tfl_db

-- =========================
-- STEP 2: DIMENSION TABLES
-- =========================

!echo STEP 2: Creating dimension tables

!echo Creating dim_networks
CREATE EXTERNAL TABLE IF NOT EXISTS dim_networks (
  network_id INT,
  network_name VARCHAR(100),
  network_type VARCHAR(50),
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/aparna/tfl_proj/tfl_data/dim_networks'
TBLPROPERTIES ("skip.header.line.count"="1");

!echo dim_networks created successfully

!echo Creating dim_lines
CREATE EXTERNAL TABLE IF NOT EXISTS dim_lines (
  line_id INT,
  line_name VARCHAR(100),
  line_color VARCHAR(7),
  is_night_service BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/aparna/tfl_proj/tfl_data/dim_lines'
TBLPROPERTIES ("skip.header.line.count"="1");

!echo dim_lines created successfully

!echo Creating dim_stations
CREATE EXTERNAL TABLE IF NOT EXISTS dim_stations (
  station_id INT,
  nlc_code VARCHAR(20),
  station_name VARCHAR(200),
  network_id INT,
  has_london_underground BOOLEAN,
  has_elizabeth_line BOOLEAN,
  has_overground BOOLEAN,
  has_dlr BOOLEAN,
  has_night_tube BOOLEAN,
  is_active BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/aparna/tfl_proj/tfl_data/dim_stations'
TBLPROPERTIES ("skip.header.line.count"="1");

!echo dim_stations created successfully

!echo Creating dim_date
CREATE EXTERNAL TABLE IF NOT EXISTS dim_date (
  date_id INT,
  year INT,
  quarter INT,
  month INT,
  is_annual BOOLEAN,
  period_label VARCHAR(50),
  period_start DATE,
  period_end DATE,
  created_at TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/aparna/tfl_proj/tfl_data/dim_date'
TBLPROPERTIES ("skip.header.line.count"="1");

!echo dim_date created successfully

-- =========================
-- STEP 3: FACT TABLES
-- =========================

!echo STEP 3: Creating fact tables

!echo Creating fact_station_lines
CREATE EXTERNAL TABLE IF NOT EXISTS fact_station_lines (
  station_line_id INT,
  station_id INT,
  line_id INT,
  is_interchange BOOLEAN,
  effective_from DATE,
  effective_to DATE,
  created_at TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/aparna/tfl_proj/tfl_data/fact_station_lines'
TBLPROPERTIES ("skip.header.line.count"="1");

!echo fact_station_lines created successfully

!echo Creating fact_passenger_entry_exit
CREATE EXTERNAL TABLE IF NOT EXISTS fact_passenger_entry_exit (
  entry_exit_id INT,
  station_id INT,
  date_id INT,
  total_entry_exit INT,
  estimated_entries INT,
  estimated_exits INT,
  record_type VARCHAR(20),
  data_source VARCHAR(20),
  created_at TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/aparna/tfl_proj/tfl_data/fact_passenger_entry_exit'
TBLPROPERTIES ("skip.header.line.count"="1");

!echo fact_passenger_entry_exit created successfully

-- =========================
-- STEP 4: VALIDATION
-- =========================

!echo STEP 4: Validating tables

SHOW TABLES;

!echo ============================================
!echo TFL HIVE ETL PIPELINE COMPLETED SUCCESSFULLY
!echo ============================================