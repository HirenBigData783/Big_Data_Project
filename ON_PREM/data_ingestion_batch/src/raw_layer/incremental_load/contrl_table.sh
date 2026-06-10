-- Connect to PostgreSQL
psql -U postgres -d tfl_db

-- Create control table
CREATE TABLE aparna.sqoop_control (
    table_name      VARCHAR(100) PRIMARY KEY,
    check_column    VARCHAR(100),
    last_value      BIGINT DEFAULT 0,
    last_run_time   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_row_count  INT DEFAULT 0,
    status          VARCHAR(20) DEFAULT 'PENDING'
);

-- Register all 6 tables
-- dim tables use INT PKs, fact_passenger uses BIGINT PK
INSERT INTO aparna.sqoop_control (table_name, check_column, last_value) VALUES
('dim_networks',              'network_id',    0),
('dim_lines',                 'line_id',       0),
('dim_stations',              'station_id',    0),
('fact_station_lines',        'station_line_id', 0),
('dim_date',                  'date_id',       0),
('fact_passenger_entry_exit', 'entry_exit_id', 0);

SELECT * FROM sqoop_control;

# Create base directory structure for all tables
hdfs dfs -mkdir -p /tmp/aparna/tfl_proj/raw/dim_networks
hdfs dfs -mkdir -p /tmp/aparna/tfl_proj/raw/dim_lines
hdfs dfs -mkdir -p /tmp/aparna/tfl_proj/raw/dim_stations
hdfs dfs -mkdir -p /tmp/aparna/tfl_proj/raw/fact_station_lines
hdfs dfs -mkdir -p /tmp/aparna/tfl_proj/raw/dim_date
hdfs dfs -mkdir -p /tmp/aparna/tfl_proj/raw/fact_passenger_entry_exit

# Verify
hdfs dfs -ls /tmp/aparna/tfl_proj/raw/


