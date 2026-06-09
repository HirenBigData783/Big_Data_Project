#!/bin/bash

#################################################
# PostgreSQL Connection
#################################################

PGHOST="13.42.152.118"
PGUSER="admin"
PGPASSWORD="admin123"
PGDATABASE="testdb"
PGSCHEMA="aparna"

#################################################
# Split Percentage
#################################################

SPLIT_PERCENT=0.7

#################################################
# Function: Split Table
#################################################

split_table() {

    TABLE_NAME=$1
    PK_COLUMN=$2

    echo "======================================="
    echo "Splitting table: ${TABLE_NAME}"
    echo "Primary Key: ${PK_COLUMN}"
    echo "======================================="

    PGPASSWORD="$PGPASSWORD" psql -h ${PGHOST} -U ${PGUSER} -d ${PGDATABASE} <<EOF

DROP TABLE IF EXISTS ${PGSCHEMA}.${TABLE_NAME}_full_load;
DROP TABLE IF EXISTS ${PGSCHEMA}.${TABLE_NAME}_inc_load;

CREATE TABLE ${PGSCHEMA}.${TABLE_NAME}_full_load AS
SELECT *
FROM ${TABLE_NAME}
WHERE ${PK_COLUMN} <= (
    SELECT CEIL(COUNT(*) * ${SPLIT_PERCENT})
    FROM ${TABLE_NAME}
);

CREATE TABLE ${PGSCHEMA}.${TABLE_NAME}_inc_load AS
SELECT *
FROM ${TABLE_NAME}
WHERE ${PK_COLUMN} > (
    SELECT CEIL(COUNT(*) * ${SPLIT_PERCENT})
    FROM ${TABLE_NAME}
);

SELECT '${TABLE_NAME} TOTAL' AS info,
       COUNT(*)
FROM ${TABLE_NAME}

UNION ALL

SELECT '${TABLE_NAME} FULL_LOAD',
       COUNT(*)
FROM ${PGSCHEMA}.${TABLE_NAME}_full_load

UNION ALL

SELECT '${TABLE_NAME} INC_LOAD',
       COUNT(*)
FROM ${PGSCHEMA}.${TABLE_NAME}_inc_load;

EOF

}

#################################################
# Execute Splits
#################################################

split_table "dim_networks" "network_id"

split_table "dim_lines" "line_id"

split_table "dim_stations" "station_id"

split_table "fact_station_lines" "station_line_id"

split_table "dim_date" "date_id"

split_table "fact_passenger_entry_exit" "entry_exit_id"

echo "======================================="
echo "All tables successfully split"
echo "======================================="