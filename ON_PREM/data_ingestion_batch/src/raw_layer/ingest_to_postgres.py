"""
TfL Data Loader - Injest CSV data into PostgreSQL
Handles schema creation and data import with error handling
"""

import pandas as pd
import psycopg2
from psycopg2 import sql
from pathlib import Path
import sys

# Configuration
DB_CONFIG = {
    'host': '13.42.152.118',
    'port': 5432,
    'database': 'testdb',
    'user': 'admin',
    'password': 'admin123'
}

DATA_DIR = Path(__file__).parent.parent / 'Data' / 'normalized'
SQL_DIR = Path(__file__).parent

# Table loading order (respects foreign key dependencies)
LOAD_ORDER = [
    'dim_networks',
    'dim_lines',
    'dim_stations',
    'dim_date',
    'fact_station_lines',
    'fact_passenger_entry_exit'
]

def connect_postgres():
    """Connect to PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print(f"✓ Connected to PostgreSQL: {DB_CONFIG['database']}")
        return conn
    except psycopg2.Error as e:
        print(f"✗ Database connection failed: {e}")
        sys.exit(1)

def create_database_if_not_exists():
    """Create database if it doesn't exist"""
    try:
        # Connect to default postgres database
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database='postgres',
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Check if database exists
        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (DB_CONFIG['database'],)
        )
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(
                sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(DB_CONFIG['database'])
                )
            )
            print(f"✓ Created database: {DB_CONFIG['database']}")
        else:
            print(f"✓ Database already exists: {DB_CONFIG['database']}")

        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"✗ Database creation failed: {e}")
        sys.exit(1)

def execute_schema_script(conn):
    """Execute the schema creation SQL script"""
    try:
        schema_file = SQL_DIR / 'create_postgres_schema.sql'

        if not schema_file.exists():
            print(f"✗ Schema file not found: {schema_file}")
            return False

        with open(schema_file, 'r') as f:
            schema_sql = f.read()

        cursor = conn.cursor()
        cursor.execute(schema_sql)
        conn.commit()
        cursor.close()

        print("✓ Schema created successfully")
        return True

    except psycopg2.Error as e:
        print(f"✗ Schema creation failed: {e}")
        conn.rollback()
        return False

def load_csv_to_table(conn, table_name, csv_file):
    """Load CSV file into PostgreSQL table"""
    try:
        # Read CSV with empty strings kept as empty
        df = pd.read_csv(csv_file, keep_default_na=False, na_values=[''])

        # Replace NaN and empty strings with None for proper NULL handling
        df = df.replace({'': None, 'nan': None, 'NaN': None})
        df = df.where(pd.notna(df), None)

        # Prepare insert statement
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Insert data
        cursor = conn.cursor()
        for idx, row in df.iterrows():
            # Convert empty strings to None
            row_data = [None if (pd.isna(val) or val == '' or val == 'nan') else val for val in row]
            cursor.execute(insert_query, tuple(row_data))

        conn.commit()
        cursor.close()

        print(f"   ✓ {table_name}: {len(df)} records loaded")
        return True

    except Exception as e:
        print(f"   ✗ {table_name}: Failed - {e}")
        conn.rollback()
        return False

def verify_data_load(conn):
    """Verify data was loaded correctly"""
    cursor = conn.cursor()

    print("\n" + "=" * 80)
    print("DATA VERIFICATION")
    print("=" * 80)

    for table in LOAD_ORDER:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table:35} : {count:>10,} records")

    cursor.close()

def run_sample_queries(conn):
    """Run sample queries to demonstrate the data model"""
    cursor = conn.cursor()

    print("\n" + "=" * 80)
    print("SAMPLE QUERIES")
    print("=" * 80)

    # Query 1: Top 10 busiest stations in 2019
    print("\n1. Top 10 Busiest Stations in 2019:")
    print("-" * 80)
    query1 = """
    SELECT
        s.station_name,
        SUM(f.total_entry_exit) as total_passengers
    FROM fact_passenger_entry_exit f
    JOIN dim_stations s ON f.station_id = s.station_id
    JOIN dim_date d ON f.date_id = d.date_id
    WHERE d.year = 2019
    GROUP BY s.station_name
    ORDER BY total_passengers DESC
    LIMIT 10;
    """
    cursor.execute(query1)
    results = cursor.fetchall()
    for idx, (station, passengers) in enumerate(results, 1):
        print(f"   {idx:2}. {station:40} {passengers:>15,}")

    # Query 2: Lines with most stations
    print("\n2. Lines by Number of Stations:")
    print("-" * 80)
    query2 = """
    SELECT
        line_name,
        number_of_stations,
        interchange_stations,
        night_tube_stations
    FROM vw_line_stats
    ORDER BY number_of_stations DESC;
    """
    cursor.execute(query2)
    results = cursor.fetchall()
    print(f"   {'Line Name':30} {'Stations':>10} {'Interchanges':>15} {'Night Tube':>12}")
    print(f"   {'-'*30} {'-'*10} {'-'*15} {'-'*12}")
    for line, stations, interchanges, night in results:
        print(f"   {line:30} {stations:>10} {interchanges or 0:>15} {night or 0:>12}")

    # Query 3: Passenger growth 2017 vs 2019
    print("\n3. Stations with Highest Growth (2017 vs 2019):")
    print("-" * 80)
    query3 = """
    WITH stats AS (
        SELECT
            s.station_name,
            MAX(CASE WHEN d.year = 2017 THEN f.total_entry_exit END) as passengers_2017,
            MAX(CASE WHEN d.year = 2019 THEN f.total_entry_exit END) as passengers_2019
        FROM fact_passenger_entry_exit f
        JOIN dim_stations s ON f.station_id = s.station_id
        JOIN dim_date d ON f.date_id = d.date_id
        WHERE d.year IN (2017, 2019)
        GROUP BY s.station_name
        HAVING MAX(CASE WHEN d.year = 2017 THEN f.total_entry_exit END) > 0
           AND MAX(CASE WHEN d.year = 2019 THEN f.total_entry_exit END) > 0
    )
    SELECT
        station_name,
        passengers_2017,
        passengers_2019,
        passengers_2019 - passengers_2017 as growth,
        ROUND((passengers_2019::numeric - passengers_2017::numeric) / passengers_2017::numeric * 100, 2) as growth_pct
    FROM stats
    ORDER BY growth DESC
    LIMIT 10;
    """
    cursor.execute(query3)
    results = cursor.fetchall()
    print(f"   {'Station':35} {'2017':>15} {'2019':>15} {'Growth':>15} {'%':>8}")
    print(f"   {'-'*35} {'-'*15} {'-'*15} {'-'*15} {'-'*8}")
    for station, p2017, p2019, growth, pct in results:
        print(f"   {station:35} {p2017:>15,} {p2019:>15,} {growth:>15,} {pct:>7.1f}%")

    cursor.close()

def main():
    """Main execution function"""
    print("=" * 80)
    print("TfL Data Loader - PostgreSQL Import")
    print("=" * 80)

    # Step 1: Create database if needed
    print("\n[1/5] Checking database...")
    create_database_if_not_exists()

    # Step 2: Connect to database
    print("\n[2/5] Connecting to database...")
    conn = connect_postgres()

    # Step 3: Create schema
    print("\n[3/5] Creating schema...")
    if not execute_schema_script(conn):
        print("✗ Schema creation failed. Exiting.")
        sys.exit(1)

    # Step 4: Load data
    print("\n[4/5] Loading data...")
    success_count = 0
    for table in LOAD_ORDER:
        csv_file = DATA_DIR / f"{table}.csv"
        if csv_file.exists():
            if load_csv_to_table(conn, table, csv_file):
                success_count += 1
        else:
            print(f"   ✗ {table}: CSV file not found - {csv_file}")

    print(f"\n   Loaded {success_count}/{len(LOAD_ORDER)} tables successfully")

    # Step 5: Verify and sample queries
    print("\n[5/5] Verification and sample queries...")
    verify_data_load(conn)
    run_sample_queries(conn)

    # Close connection
    conn.close()

    print("\n" + "=" * 80)
    print("✓ Data load complete!")
    print("=" * 80)
    print(f"\nConnection details:")
    print(f"  Host     : {DB_CONFIG['host']}")
    print(f"  Port     : {DB_CONFIG['port']}")
    print(f"  Database : {DB_CONFIG['database']}")
    print(f"  User     : {DB_CONFIG['user']}")
    print("\nConnect with:")
    print(f"  psql -h {DB_CONFIG['host']} -p {DB_CONFIG['port']} -U {DB_CONFIG['user']} -d {DB_CONFIG['database']}")
    print("=" * 80)

if __name__ == "__main__":
    main()