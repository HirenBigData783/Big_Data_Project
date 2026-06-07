#!/bin/bash
set -euo pipefail

# =====================================================
# TFL SQOOP IMPORT PIPELINE
# =====================================================
# =============================================================================
# raw_full_load.sh
# =============================================================================
# Raw Layer — Full Load: PostgreSQL -> HDFS -> Hive External Table
#
# PURPOSE
# -------
# Performs a one-time full load of the TFL raw data from PostgreSQL
# into the HDFS bronze landing zone and creates (or recreates) the Hive
# external table on top of it.
#
# Run this script once during initial pipeline setup, or whenever a full
# data refresh of the raw layer is required. For ongoing daily updates
# use raw_incremental_load.sh instead.
#
# PIPELINE FLOW
# -------------
#   PostgreSQL (testdb)
#       |
#       | Sqoop import (--delete-target-dir, 1 mapper, textfile)
#       v
#   HDFS: /tmp/aparna/tfl_proj/tfl_data/
#       |
#       | Hive DDL (EXTERNAL TABLE, TEXTFILE, comma-delimited)
#       v
#   Hive: tfl_db
#
# PREREQUISITES
# -------------
#   - Sqoop, beeline, and hdfs CLI available on PATH
#   - PostgreSQL JDBC driver accessible by Sqoop
#   - HiveServer2 running at ip-172-31-12-74.eu-west-2.compute.internal:10000
#
# ENVIRONMENT VARIABLES (required)
# ---------------------------------
#   DB_HOST        PostgreSQL hostname / IP
#   DB_NAME        PostgreSQL database name
#   DB_USERNAME    PostgreSQL user
#   DB_PASSWORD    PostgreSQL password
#
# USAGE
# -----
#   export DB_HOST=<host> DB_NAME=<db> DB_USERNAME=<user> DB_PASSWORD=<pass>
#   bash raw_full_load.sh
#
# =============================================================================


# -----------------------------
# CONFIGURATION
# -----------------------------
export SQOOP_CONNECT="jdbc:postgresql://13.42.152.118:5432/testdb"
export SQOOP_USER="admin"
export SQOOP_PASS="admin123"
export TARGET_DIR="/tmp/aparna/tfl_proj/tfl_data"

LOG_FILE="sqoop_import_$(date +%F_%H%M%S).log"

# Redirect all output to log file AND console
exec > >(tee -a "$LOG_FILE") 2>&1

echo "=========================================="
echo "STARTING TFL SQOOP IMPORT PIPELINE"
echo "LOG FILE: $LOG_FILE"
echo "=========================================="

# -----------------------------
# FUNCTION: SQOOP IMPORT
# -----------------------------
run_sqoop_import () {
  TABLE_NAME=$1

  echo ""
  echo "------------------------------------------"
  echo "STARTING IMPORT: $TABLE_NAME"
  echo "TARGET PATH: $TARGET_DIR/$TABLE_NAME"
  echo "------------------------------------------"

  sqoop import \
    -D mapreduce.framework.name=local \
    --connect "$SQOOP_CONNECT" \
    --username "$SQOOP_USER" \
    --password "$SQOOP_PASS" \
    --table "$TABLE_NAME" \
    --target-dir "$TARGET_DIR/$TABLE_NAME" \
    -m 1

  echo "SUCCESS: $TABLE_NAME imported successfully"
}

# -----------------------------
# STEP 1: DIMENSION TABLES
# -----------------------------
echo ""
echo "=========================================="
echo "STEP 1: IMPORTING DIMENSION TABLES"
echo "=========================================="

run_sqoop_import "dim_networks"
run_sqoop_import "dim_lines"
run_sqoop_import "dim_date"
run_sqoop_import "dim_stations"

# -----------------------------
# STEP 2: FACT TABLES
# -----------------------------
echo ""
echo "=========================================="
echo "STEP 2: IMPORTING FACT TABLES"
echo "=========================================="

run_sqoop_import "fact_station_lines"
run_sqoop_import "fact_passenger_entry_exit"

# -----------------------------
# COMPLETION
# -----------------------------
echo ""
echo "=========================================="
echo "ALL SQOOP IMPORTS COMPLETED SUCCESSFULLY"
echo "=========================================="