```bash
#!/bin/bash

# ==========================================================
# raw_full_load.sh
# Imports FULL LOAD tables from PostgreSQL into HDFS using Sqoop
# ==========================================================

set -e

# ----------------------------------------------------------
# PostgreSQL Configuration
# ----------------------------------------------------------

PG_HOST="13.42.152.118"
PG_PORT="5432"
PG_USER="admin"
PG_PASSWORD="admin123"
PG_DB="testdb"
PG_SCHEMA="aparna"

export PGHOST="$PG_HOST"
export PGPORT="$PG_PORT"
export PGUSER="$PG_USER"
export PGPASSWORD="$PG_PASSWORD"
export PGDATABASE="$PG_DB"

# ----------------------------------------------------------
# HDFS Configuration
# ----------------------------------------------------------

HDFS_BASE="/tmp/tfl_project_hadoop"

# ----------------------------------------------------------
# JDBC Connection
# ----------------------------------------------------------

JDBC="jdbc:postgresql://${PG_HOST}:${PG_PORT}/${PG_DB}"

echo "=================================================="
echo "TfL FULL LOAD STARTED : $(date)"
echo "=================================================="

# ----------------------------------------------------------
# Format:
# source_table : full_load_table : split_column
# ----------------------------------------------------------

TABLES=(
"dim_networks:dim_networks_full_load:network_id"
"dim_lines:dim_lines_full_load:line_id"
"dim_stations:dim_stations_full_load:station_id"
"fact_station_lines:fact_station_lines_full_load:station_line_id"
"dim_date:dim_date_full_load:date_id"
"fact_passenger_entry_exit:fact_passenger_entry_exit_full_load:entry_exit_id"
)

# ----------------------------------------------------------
# Loop through tables
# ----------------------------------------------------------

for ENTRY in "${TABLES[@]}"
do

REAL_TABLE=$(echo "$ENTRY" | cut -d: -f1)
FULL_LOAD_TABLE=$(echo "$ENTRY" | cut -d: -f2)
CHECK_COL=$(echo "$ENTRY" | cut -d: -f3)

SCHEMA_TABLE="${PG_SCHEMA}.${FULL_LOAD_TABLE}"

echo ""
echo "--------------------------------------------------"
echo "Loading ${SCHEMA_TABLE}"
echo "Target : ${HDFS_BASE}/${REAL_TABLE}"
echo "--------------------------------------------------"

sqoop import \
  --connect "${JDBC}" \
  --username "${PG_USER}" \
  --password "${PG_PASSWORD}" \
  --query "SELECT * FROM ${SCHEMA_TABLE} WHERE \$CONDITIONS" \
  --target-dir "${HDFS_BASE}/${REAL_TABLE}" \
  --delete-target-dir \
  --fields-terminated-by ',' \
  --lines-terminated-by '\n' \
  --null-string '\\N' \
  --null-non-string '\\N' \
  -m 1

if [ $? -eq 0 ]; then

    MAX_VAL=$(psql -t -c "
        SELECT MAX(${CHECK_COL})
        FROM ${PG_SCHEMA}.${FULL_LOAD_TABLE};
    ")

    MAX_VAL=$(echo "$MAX_VAL" | xargs)

    ROW_COUNT=$(psql -t -c "
        SELECT COUNT(*)
        FROM ${PG_SCHEMA}.${FULL_LOAD_TABLE};
    ")

    ROW_COUNT=$(echo "$ROW_COUNT" | xargs)

    psql -c "
        UPDATE ${PG_SCHEMA}.sqoop_control
        SET
            last_value=$MAX_VAL,
            last_row_count=$ROW_COUNT,
            last_run_time=NOW(),
            status='SUCCESS'
        WHERE table_name='${REAL_TABLE}';
    "

    echo ""
    echo "SUCCESS : ${FULL_LOAD_TABLE}"
    echo "Rows Loaded : ${ROW_COUNT}"
    echo "Maximum ${CHECK_COL} : ${MAX_VAL}"

    hdfs dfs -ls "${HDFS_BASE}/${REAL_TABLE}"

else

    echo ""
    echo "FAILED : ${FULL_LOAD_TABLE}"

    psql -c "
        UPDATE ${PG_SCHEMA}.sqoop_control
        SET status='FAILED'
        WHERE table_name='${REAL_TABLE}';
    "

fi

done

echo ""
echo "=================================================="
echo "FULL LOAD COMPLETED : $(date)"
echo "=================================================="

echo ""
echo "Control Table Status"

psql -c "
SELECT
    table_name,
    last_value,
    last_row_count,
    status,
    last_run_time
FROM ${PG_SCHEMA}.sqoop_control
ORDER BY table_name;
"
```
