#!/bin/bash

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
# Jenkins Parameters
# ----------------------------------------------------------
REAL_TABLE="$1"
TARGET_DIR="$2"

if [ -z "$REAL_TABLE" ] || [ -z "$TARGET_DIR" ]; then
    echo "ERROR: Missing parameters"
    echo "Usage: $0 <table_name> <hdfs_target_dir>"
    exit 1
fi

# ----------------------------------------------------------
# Map real table to full-load source table and split column
# ----------------------------------------------------------
case "$REAL_TABLE" in
    dim_networks)
        FULL_LOAD_TABLE="dim_networks_full_load"
        CHECK_COL="network_id"
        ;;
    dim_lines)
        FULL_LOAD_TABLE="dim_lines_full_load"
        CHECK_COL="line_id"
        ;;
    dim_stations)
        FULL_LOAD_TABLE="dim_stations_full_load"
        CHECK_COL="station_id"
        ;;
    fact_station_lines)
        FULL_LOAD_TABLE="fact_station_lines_full_load"
        CHECK_COL="station_line_id"
        ;;
    dim_date)
        FULL_LOAD_TABLE="dim_date_full_load"
        CHECK_COL="date_id"
        ;;
    fact_passenger_entry_exit)
        FULL_LOAD_TABLE="fact_passenger_entry_exit_full_load"
        CHECK_COL="entry_exit_id"
        ;;
    *)
        echo "ERROR: Unknown table name: $REAL_TABLE"
        exit 1
        ;;
esac

SCHEMA_TABLE="${PG_SCHEMA}.${FULL_LOAD_TABLE}"
JDBC="jdbc:postgresql://${PG_HOST}:${PG_PORT}/${PG_DB}"

QUERY="SELECT * FROM ${SCHEMA_TABLE} WHERE \$CONDITIONS"

echo "=================================================="
echo "TfL FULL LOAD STARTED : $(date)"
echo "Real table       : ${REAL_TABLE}"
echo "Source table     : ${SCHEMA_TABLE}"
echo "Check column     : ${CHECK_COL}"
echo "Target directory : ${TARGET_DIR}"
echo "Sqoop query      : ${QUERY}"
echo "=================================================="

sqoop import \
   -Dmapreduce.framework.name=local \
  --connect "${JDBC}" \
  --username "${PG_USER}" \
  --password "${PG_PASSWORD}" \
  --query "${QUERY}" \
  --target-dir "${TARGET_DIR}" \
  --delete-target-dir \
  --fields-terminated-by ',' \
  --lines-terminated-by '\n' \
  --null-string '\\N' \
  --null-non-string '\\N' \
  --num-mappers 1

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
        last_value=${MAX_VAL},
        last_row_count=${ROW_COUNT},
        last_run_time=NOW(),
        status='SUCCESS'
    WHERE table_name='${REAL_TABLE}';
"

echo "=================================================="
echo "SUCCESS : ${FULL_LOAD_TABLE}"
echo "Rows Loaded : ${ROW_COUNT}"
echo "Maximum ${CHECK_COL} : ${MAX_VAL}"
echo "Target HDFS : ${TARGET_DIR}"
echo "=================================================="

hdfs dfs -ls "${TARGET_DIR}"

echo "Control Table Status"

psql -c "
SELECT
    table_name,
    last_value,
    last_row_count,
    status,
    last_run_time
FROM ${PG_SCHEMA}.sqoop_control
WHERE table_name='${REAL_TABLE}';
"