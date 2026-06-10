#!/bin/bash

# raw_full_load.sh

# Loads FULL LOAD tables from PostgreSQL schema 'aparna' into HDFS

set -e

# ── PostgreSQL Configuration ─────────────────────

PG_HOST="13.42.152.118"
PG_PORT="5432"
PG_USER="admin"
PG_PASSWORD="admin123"
PG_DB="testdb"
PG_SCHEMA="aparna"

# Export once for all psql commands

export PGHOST="$PG_HOST"
export PGPORT="$PG_PORT"
export PGUSER="$PG_USER"
export PGPASSWORD="$PG_PASSWORD"
export PGDATABASE="$PG_DB"

# ── HDFS Configuration ───────────────────────────

HDFS_BASE="/tmp/aparna/tfl_proj/raw_full_load"
#LOG_FILE="/var/log/tfl_full_load_$(date +%Y%m%d_%H%M%S).log"

# ── JDBC Connection ──────────────────────────────

JDBC="jdbc:postgresql://$PG_HOST:$PG_PORT/$PG_DB"

echo "============================================" #| tee -a "$LOG_FILE"
echo "TfL FULL LOAD Started: $(date)" #| tee -a "$LOG_FILE"
echo "============================================" #| tee -a "$LOG_FILE"

# Format:

# real_table_name:full_load_table_name:check_column

TABLES=(
"dim_networks:dim_networks_full_load:network_id"
"dim_lines:dim_lines_full_load:line_id"
"dim_stations:dim_stations_full_load:station_id"
"fact_station_lines:fact_station_lines_full_load:station_line_id"
"dim_date:dim_date_full_load:date_id"
"fact_passenger_entry_exit:fact_passenger_entry_exit_full_load:entry_exit_id"
)

for ENTRY in "${TABLES[@]}"; do

REAL_TABLE=$(echo "$ENTRY" | cut -d: -f1)
FULL_LOAD_TABLE=$(echo "$ENTRY" | cut -d: -f2)
CHECK_COL=$(echo "$ENTRY" | cut -d: -f3)

echo "" #| tee -a "$LOG_FILE"
echo "--------------------------------------------" #| tee -a "$LOG_FILE"
echo "Loading: $FULL_LOAD_TABLE → $HDFS_BASE/$REAL_TABLE" #| tee -a "$LOG_FILE"
echo "--------------------------------------------" #| tee -a "$LOG_FILE"

# ── Sqoop Import ─────────────────────────────
SCHEMA_TABLE="${PG_SCHEMA}.${FULL_LOAD_TABLE}"

sqoop import \
    --connect "${JDBC}" \
    --username "${PG_USER}" \
    --password "${PG_PASSWORD}" \
    --query "SELECT * FROM ${SCHEMA_TABLE} WHERE \$CONDITIONS" \
    #--split-by "${CHECK_COL}" \
    --target-dir "${HDFS_BASE}/${REAL_TABLE}" \
    --delete-target-dir \
    --fields-terminated-by ',' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    -m 1

if [ ${PIPESTATUS[0]} -eq 0 ]; then

    MAX_VAL=$(psql -t -c \
        "SELECT MAX(${CHECK_COL})
         FROM ${PG_SCHEMA}.${FULL_LOAD_TABLE};")
    MAX_VAL=$(echo "$MAX_VAL" | xargs)

    ROW_COUNT=$(psql -t -c \
        "SELECT COUNT(*)
         FROM ${PG_SCHEMA}.${FULL_LOAD_TABLE};")
    ROW_COUNT=$(echo "$ROW_COUNT" | xargs)

    psql -c "
        UPDATE $PG_SCHEMA.sqoop_control
        SET last_value = $MAX_VAL,
            last_row_count = $ROW_COUNT,
            last_run_time = NOW(),
            status = 'SUCCESS'
        WHERE table_name = '$REAL_TABLE';
    "

    echo "✓ SUCCESS: ${PG_SCHEMA}.${FULL_LOAD_TABLE} loaded" #| tee -a "$LOG_FILE"
    echo "  Rows: $ROW_COUNT" #| tee -a "$LOG_FILE"
    echo "  Max $CHECK_COL: $MAX_VAL" #| tee -a "$LOG_FILE"

    echo "  HDFS Files:" #| tee -a "$LOG_FILE"
    hdfs dfs -ls "$HDFS_BASE/$REAL_TABLE" #2>&1 | tee -a "$LOG_FILE"

else

    echo "✗ FAILED: ${PG_SCHEMA}.${FULL_LOAD_TABLE}" #| tee -a "$LOG_FILE"

    psql -c "
        UPDATE $PG_SCHEMA.sqoop_control
        SET status = 'FAILED'
        WHERE table_name = '$REAL_TABLE';
    "
fi

done

echo "" #| tee -a "$LOG_FILE"
echo "============================================" #| tee -a "$LOG_FILE"
echo "Full Load Completed: $(date)" #| tee -a "$LOG_FILE"
echo "============================================" #| tee -a "$LOG_FILE"

# ── Control Table Status ─────────────────────────

psql -c "
SELECT
table_name,
last_value,
last_row_count,
status
FROM $PG_SCHEMA.sqoop_control
ORDER BY table_name;
"