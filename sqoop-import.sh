#!/bin/bash
# raw_incremental_load.sh
# Incremental Load (Clean Version)

set -e

# ── Config ──────────────────────────────────────
PG_HOST="13.42.152.118"
PG_PORT="5432"
PG_USER="admin"
PG_PASSWORD="admin123"
PG_DB="testdb"
PG_SCHEMA="aparna"

HDFS_BASE="/tmp/tfl_project_hadoop"
HIVE_DB="tfl_db"

JDBC="jdbc:postgresql://$PG_HOST:$PG_PORT/$PG_DB"

# ── Export once (GLOBAL for psql + Sqoop) ──────
export PGPASSWORD="$PG_PASSWORD"
export PGHOST="$PG_HOST"
export PGPORT="$PG_PORT"
export PGUSER="$PG_USER"
export PGDATABASE="$PG_DB"

# ── Reusable PSQL function ─────────────────────
run_psql() {
    psql -t -A -c "$1" | xargs
}

echo "============================================"
echo "TfL INCREMENTAL LOAD Started: $(date)"
echo "============================================"

# ── Tables list ────────────────────────────────
TABLES=(
    "dim_networks:dim_networks_inc_load:network_id"
    "dim_lines:dim_lines_inc_load:line_id"
    "dim_stations:dim_stations_inc_load:station_id"
    "fact_station_lines:fact_station_lines_inc_load:station_line_id"
    "dim_date:dim_date_inc_load:date_id"
    "fact_passenger_entry_exit:fact_passenger_entry_exit_inc_load:entry_exit_id"
)

for ENTRY in "${TABLES[@]}"; do

    REAL_TABLE=$(echo $ENTRY | cut -d: -f1)
    INC_LOAD_TABLE=$(echo $ENTRY | cut -d: -f2)
    CHECK_COL=$(echo $ENTRY | cut -d: -f3)

    echo ""
    echo "--------------------------------------------"
    echo "Incremental: $REAL_TABLE"
    echo "--------------------------------------------"

    # ── Get last value ──────────────────────────
    LAST_VAL=$(run_psql "
        SELECT last_value
        FROM $PG_SCHEMA.sqoop_control
        WHERE table_name='$REAL_TABLE';
    ")

    echo "Last Value: $LAST_VAL"

    # ── Count new rows ──────────────────────────
    NEW_COUNT=$(run_psql "
        SELECT COUNT(*)
        FROM $PG_SCHEMA.$INC_LOAD_TABLE
        WHERE $CHECK_COL > $LAST_VAL;
    ")

    echo "New rows: $NEW_COUNT"

    # ── Skip if no data ─────────────────────────
    if [ "$NEW_COUNT" -eq 0 ]; then
        echo "No new data for $REAL_TABLE"

        run_psql "
            UPDATE $PG_SCHEMA.sqoop_control
            SET status='NO_NEW_DATA',
                last_run_time=NOW()
            WHERE table_name='$REAL_TABLE';
        "
        continue
    fi

    # ── Max value ───────────────────────────────
    MAX_VAL=$(run_psql "
        SELECT MAX($CHECK_COL)
        FROM $PG_SCHEMA.$INC_LOAD_TABLE;
    ")

    echo "Max Value: $MAX_VAL"

    # ── Sqoop Import ────────────────────────────

    SCHEMA_TABLE="${PG_SCHEMA}.${INC_LOAD_TABLE}"
    sqoop import \
        -D mapreduce.framework.name=local \
        --connect $JDBC \
        --username $PG_USER \
        --password $PG_PASSWORD \
        --query "SELECT * FROM ${PG_SCHEMA}.${INC_LOAD_TABLE} WHERE \$CONDITIONS AND ${CHECK_COL} > ${LAST_VAL}" \
        --split-by $CHECK_COL \
        --target-dir $HDFS_BASE/${REAL_TABLE}_inc_load \
        --append \
        -m 1

    # ── Update control table ────────────────────
    if [ $? -eq 0 ]; then

        run_psql "
            UPDATE $PG_SCHEMA.sqoop_control
            SET last_value=$MAX_VAL,
                last_row_count=$NEW_COUNT,
                last_run_time=NOW(),
                status='SUCCESS'
            WHERE table_name='$REAL_TABLE';
        "

        echo "✓ SUCCESS: $REAL_TABLE"

        hdfs dfs -ls $HDFS_BASE/$REAL_TABLE

        #hive -e "MSCK REPAIR TABLE $HIVE_DB.$REAL_TABLE;"
        beeline -u "jdbc:hive2://ip-172-31-12-74.eu-west-2.compute.internal:10000/default" \
-e "MSCK REPAIR TABLE ${HIVE_DB}.${REAL_TABLE};"

    else
        echo "✗ FAILED: $REAL_TABLE"

        run_psql "
            UPDATE $PG_SCHEMA.sqoop_control
            SET status='FAILED'
            WHERE table_name='$REAL_TABLE';
        "
    fi

done

echo "============================================"
echo "Incremental Load Completed: $(date)"
echo "============================================"

# ── Final status ───────────────────────────────
psql -c "
SELECT table_name,
       last_value,
       last_row_count,
       last_run_time,
       status
FROM $PG_SCHEMA.sqoop_control
ORDER BY table_name;
"
