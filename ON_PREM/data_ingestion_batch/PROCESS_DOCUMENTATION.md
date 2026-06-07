# TfL Data Warehouse - Complete Process Documentation

## 📋 Table of Contents
1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Step-by-Step Process](#step-by-step-process)
4. [Data Flow](#data-flow)
5. [Technologies Used](#technologies-used)
6. [Deployment](#deployment)
7. [Usage Examples](#usage-examples)
8. [Troubleshooting](#troubleshooting)

---

## 🎯 Project Overview

This project demonstrates a complete **data engineering pipeline** that transforms raw Transport for London (TfL) passenger entry/exit data into a production-ready **star schema data warehouse** in PostgreSQL.

### Business Problem
- Raw TfL data is spread across multiple Excel files (2007-2021)
- Data is denormalized and not optimized for analytics
- No proper relationships or data integrity constraints
- Difficult to query and generate insights

### Solution
- Built a normalized **star schema data model**
- Automated data transformation pipeline
- Production-ready PostgreSQL deployment
- Ready for BI tools and Sqoop export to Hadoop/HDFS

### Results
- ✅ 5,812 records organized into 6 tables
- ✅ Proper foreign key relationships
- ✅ 10 performance indexes
- ✅ 4 pre-built analytical views
- ✅ Sub-second query performance

---

## 🏗️ Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                │
├─────────────────────────────────────────────────────────────────────┤
│  • TfL Station Data (Excel/CSV)                                     │
│  • Multi-year Entry/Exit Statistics (2007-2021)                     │
│  • Station Metadata (Lines, Network, Zones)                         │
└──────────────────────┬──────────────────────────────────────────────┘
                       │
                       │ Extract
                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    TRANSFORMATION LAYER                             │
├─────────────────────────────────────────────────────────────────────┤
│  Python Script: data_modeling.py                                    │
│  • Read raw Excel/CSV files                                         │
│  • Normalize to 3rd Normal Form (3NF)                               │
│  • Create dimension tables (networks, lines, stations, dates)       │
│  • Create bridge table (station-line relationships)                 │
│  • Create fact table (passenger entry/exit)                         │
│  • Generate primary/foreign keys                                    │
│  • Export to CSV files                                              │
└──────────────────────┬──────────────────────────────────────────────┘
                       │
                       │ Transform
                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     NORMALIZED CSV FILES                            │
├─────────────────────────────────────────────────────────────────────┤
│  1. dim_networks.csv (1 record)                                     │
│  2. dim_lines.csv (14 records)                                      │
│  3. dim_stations.csv (436 records)                                  │
│  4. dim_date.csv (15 records)                                       │
│  5. fact_station_lines.csv (575 records)                            │
│  6. fact_passenger_entry_exit.csv (4,771 records)                   │
└──────────────────────┬──────────────────────────────────────────────┘
                       │
                       │ Load
                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    POSTGRESQL DATABASE                              │
├─────────────────────────────────────────────────────────────────────┤
│  Schema: create_postgres_schema.sql                                 │
│  Loader: load_to_postgres.py                                        │
│                                                                     │
│  Tables Created:                                                    │
│  • dim_networks (dimension)                                         │
│  • dim_lines (dimension)                                            │
│  • dim_stations (dimension)                                         │
│  • dim_date (dimension)                                             │
│  • fact_station_lines (bridge)                                      │
│  • fact_passenger_entry_exit (fact)                                 │
│                                                                     │
│  Features:                                                          │
│  • Foreign key constraints                                          │
│  • Primary keys on all tables                                       │
│  • 10 performance indexes                                           │
│  • 4 analytical views                                               │
└──────────────────────┬──────────────────────────────────────────────┘
                       │
                       │ Export
                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    DOWNSTREAM SYSTEMS                               │
├─────────────────────────────────────────────────────────────────────┤
│  • Apache Sqoop → HDFS/Hadoop                                       │
│  • Tableau/PowerBI → Dashboards                                     │
│  • Python/R → Analytics                                             │
│  • Apache Spark → Big Data Processing                               │
└─────────────────────────────────────────────────────────────────────┘
```

### Star Schema Design

```
                    ┌─────────────────┐
                    │  dim_networks   │
                    │  PK: network_id │
                    └────────┬────────┘
                             │ 1
                             │
                             │ Many
                    ┌────────▼────────┐
         ┌──────────┤  dim_stations   │◄─────────┐
         │          │  PK: station_id │          │
         │          │  FK: network_id │          │
         │          └────────┬────────┘          │
         │                   │                   │
         │ Many              │ Many              │ Many
┌────────▼────────┐    ┌─────▼──────────┐ ┌─────▼────────┐
│   dim_lines     │◄───┤ fact_station_  │ │  dim_date    │
│  PK: line_id    │    │     lines      │ │ PK: date_id  │
└─────────────────┘    │  (Bridge)      │ └──────────────┘
                       │  PK: station_  │
                       │      line_id   │
                       │  FK: station_id│
                       │  FK: line_id   │
                       └────────────────┘
                              
                       ┌────────────────┐
                       │ fact_passenger_│
                       │   entry_exit   │
                       │  PK: entry_    │
                       │      exit_id   │
                       │  FK: station_id│◄──── dim_stations
                       │  FK: date_id   │◄──── dim_date
                       └────────────────┘
```

---

## 📝 Step-by-Step Process

### Phase 1: Data Acquisition

**Step 1.1: Obtain TfL Data**
```bash
# Original source: Transport for London Open Data
# https://tfl.gov.uk/corporate/open-data

# For this project, data was cloned from:
git clone https://github.com/AparnaAmonkar22/TFL_PROJECT.git temp_tfl_data
cp -r temp_tfl_data/data/Data/* Data/
```

**Input Files:**
- `TfL_stations.csv` - Station master data with lines and services
- `AnnualisedEntryExit_2017.xlsx` - 2017 passenger data
- `AnnualisedEntryExit_2018.xlsx` - 2018 passenger data
- `AnnualisedEntryExit_2019.xlsx` - 2019 passenger data
- `AC2020_AnnualisedEntryExit.xlsx` - 2020 passenger data
- `AC2021_AnnualisedEntryExit.xlsx` - 2021 passenger data
- `multi-year-station-entry-and-exit-figures.xlsx` - Historical data

### Phase 2: Data Transformation

**Step 2.1: Environment Setup**
```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install pandas openpyxl psycopg2-binary numpy
```

**Step 2.2: Data Modeling**

Run the transformation script:
```bash
python src/data_modeling.py
```

**What it does:**
1. **Reads raw TfL data** from Excel/CSV files
2. **Creates dimension tables:**
   - `dim_networks` - Extracts unique network types
   - `dim_lines` - Extracts tube/rail lines with official colors
   - `dim_stations` - Normalizes station master data
   - `dim_date` - Creates time dimension (2007-2021)

3. **Creates bridge table:**
   - `fact_station_lines` - Many-to-many station-line relationships
   - Identifies interchange stations (multiple lines)
   - Tracks temporal validity (effective_from, effective_to)

4. **Creates fact table:**
   - `fact_passenger_entry_exit` - Main fact table
   - One row per station per year
   - Stores total entry/exit counts
   - Links to dimensions via foreign keys

5. **Generates primary/foreign keys:**
   - Auto-incrementing surrogate keys for all tables
   - Foreign key references maintained
   - NULL handling for optional relationships

6. **Exports to CSV:**
   - 6 normalized CSV files in `Data/normalized/`
   - Proper NULL representation (empty strings)
   - Ready for database import

**Output:**
```
Data/normalized/
├── dim_networks.csv (1 record)
├── dim_lines.csv (14 records)
├── dim_stations.csv (436 records)
├── dim_date.csv (15 records)
├── fact_station_lines.csv (575 records)
├── fact_passenger_entry_exit.csv (4,771 records)
└── DATA_DICTIONARY.txt
```

### Phase 3: Database Schema Creation

**Step 3.1: PostgreSQL Schema Design**

File: `src/create_postgres_schema.sql`

**Schema Features:**
- **Primary Keys**: All tables have surrogate primary keys
- **Foreign Keys**: All relationships enforced
- **Indexes**: 10 performance indexes on join columns
- **Constraints**: NOT NULL, UNIQUE where appropriate
- **Audit Columns**: created_at, updated_at timestamps
- **Views**: 4 analytical views for common queries

**Key Design Decisions:**

1. **Surrogate Keys vs Natural Keys:**
   - Used auto-incrementing integers for all primary keys
   - Faster joins, simpler foreign key management
   - Natural keys (NLC codes, station names) kept as attributes

2. **Bridge Table Pattern:**
   - `fact_station_lines` implements many-to-many
   - Allows stations to serve multiple lines
   - Temporal tracking for changes over time

3. **Star Schema vs Snowflake:**
   - Chose star schema (denormalized dimensions)
   - Optimized for query performance
   - Sacrifices some storage for speed

4. **Date Dimension:**
   - Currently annual granularity (2007-2021)
   - Designed to extend to monthly/daily
   - Supports fiscal year analysis

### Phase 4: Data Loading

**Step 4.1: Configure Database Connection**

Edit `src/load_to_postgres.py`:
```python
DB_CONFIG = {
    'host': '13.42.152.118',  # Your PostgreSQL host
    'port': 5432,
    'database': 'testdb',     # Your database name
    'user': 'admin',          # Your username
    'password': 'admin123'    # Your password
}
```

**Step 4.2: Run Data Loader**
```bash
python src/load_to_postgres.py
```

**Loading Process:**
1. **Connects to PostgreSQL** (creates database if needed)
2. **Drops existing tables** (if any) to ensure clean state
3. **Creates schema** from SQL file
4. **Loads data in correct order** (respects foreign keys):
   - dim_networks (parent)
   - dim_lines (parent)
   - dim_stations (child of networks)
   - dim_date (parent)
   - fact_station_lines (child of stations and lines)
   - fact_passenger_entry_exit (child of stations and dates)
5. **Verifies data integrity**
6. **Runs sample queries** to demonstrate functionality

**Data Validation:**
- Check for orphaned foreign keys
- Verify row counts match source
- Test views and indexes
- Run sample analytical queries

### Phase 5: Verification

**Step 5.1: Connect to Database**
```bash
psql -h 13.42.152.118 -p 5432 -U admin -d testdb
```

**Step 5.2: Verify Tables**
```sql
-- List all tables
\dt

-- Check record counts
SELECT 
    'dim_networks' as table_name, COUNT(*) FROM dim_networks
UNION ALL
SELECT 'dim_lines', COUNT(*) FROM dim_lines
UNION ALL
SELECT 'dim_stations', COUNT(*) FROM dim_stations
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'fact_station_lines', COUNT(*) FROM fact_station_lines
UNION ALL
SELECT 'fact_passenger_entry_exit', COUNT(*) FROM fact_passenger_entry_exit;
```

**Step 5.3: Test Sample Queries**
```sql
-- Top 10 busiest stations in 2019
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
```

---

## 🔄 Data Flow

### Raw Data → Normalized CSV → PostgreSQL → Downstream

```
┌──────────────────────┐
│  Raw Excel/CSV       │
│  • TfL_stations.csv  │
│  • Entry/Exit        │
│    2007-2021         │
└──────┬───────────────┘
       │
       │ [1] Extract
       │     • pandas.read_csv()
       │     • pandas.read_excel()
       ▼
┌──────────────────────┐
│  Python DataFrames   │
│  • In-memory data    │
│  • Type conversion   │
│  • NULL handling     │
└──────┬───────────────┘
       │
       │ [2] Transform
       │     • Normalize to 3NF
       │     • Create dimensions
       │     • Generate keys
       │     • Build relationships
       ▼
┌──────────────────────┐
│  Normalized CSV      │
│  • 6 CSV files       │
│  • Primary keys      │
│  • Foreign keys      │
└──────┬───────────────┘
       │
       │ [3] Load
       │     • psycopg2.connect()
       │     • CREATE TABLE
       │     • INSERT data
       │     • CREATE INDEX
       ▼
┌──────────────────────┐
│  PostgreSQL Tables   │
│  • Enforced FK       │
│  • Indexed columns   │
│  • Analytical views  │
└──────┬───────────────┘
       │
       │ [4] Export
       │     • Sqoop to HDFS
       │     • BI tools
       │     • Analytics
       ▼
┌──────────────────────┐
│  Downstream Systems  │
│  • Hadoop/Spark      │
│  • Tableau/PowerBI   │
│  • Python/R          │
└──────────────────────┘
```

---

## 🛠️ Technologies Used

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Data Storage** | PostgreSQL 15 | Production database |
| **Programming** | Python 3.8+ | Data transformation |
| **Data Processing** | pandas, numpy | ETL operations |
| **Database Driver** | psycopg2 | PostgreSQL connectivity |
| **Version Control** | Git, GitHub | Source control |
| **Big Data** | Apache Sqoop | HDFS export |
| **Documentation** | Markdown | Technical docs |
| **Automation** | Bash scripts | Setup automation |

### Python Libraries

```python
pandas==2.1.4          # Data manipulation
openpyxl==3.1.2        # Excel file handling
psycopg2-binary==2.9.9 # PostgreSQL adapter
numpy==1.26.3          # Numerical operations
```

---

## 🚀 Deployment

### Local Development Deployment

```bash
# 1. Clone repository
git clone https://github.com/uttamraj9/TFL_Project_Demo.git
cd TFL_Project_Demo

# 2. Run automated setup
./setup.sh

# 3. Configure database connection
vim src/load_to_postgres.py

# 4. Load data
python src/load_to_postgres.py
```

### Production Deployment

**Prerequisites:**
- PostgreSQL 12+ running and accessible
- Network connectivity to database server
- Python 3.8+ installed
- Sufficient database permissions (CREATE, INSERT, INDEX)

**Deployment Steps:**

1. **Prepare Environment:**
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2. **Configure Connection:**
```python
# Edit src/load_to_postgres.py
DB_CONFIG = {
    'host': 'production-db.example.com',
    'port': 5432,
    'database': 'tfl_prod',
    'user': 'tfl_user',
    'password': 'secure_password'
}
```

3. **Verify Connectivity:**
```bash
psql -h production-db.example.com -U tfl_user -d tfl_prod -c "SELECT version();"
```

4. **Run Data Modeling:**
```bash
python src/data_modeling.py
```

5. **Load to Database:**
```bash
python src/load_to_postgres.py
```

6. **Verify Deployment:**
```sql
-- Run verification queries
SELECT COUNT(*) FROM dim_stations;
SELECT * FROM vw_busiest_stations WHERE year=2019 LIMIT 10;
```

### AWS Deployment

**Using Amazon RDS PostgreSQL:**

```bash
# 1. Create RDS PostgreSQL instance
aws rds create-db-instance \
  --db-instance-identifier tfl-datawarehouse \
  --db-instance-class db.t3.medium \
  --engine postgres \
  --master-username admin \
  --master-user-password YourPassword \
  --allocated-storage 20

# 2. Get endpoint
aws rds describe-db-instances \
  --db-instance-identifier tfl-datawarehouse \
  --query 'DBInstances[0].Endpoint.Address'

# 3. Update connection string
DB_CONFIG = {
    'host': 'tfl-datawarehouse.xyz.us-east-1.rds.amazonaws.com',
    'port': 5432,
    'database': 'testdb',
    'user': 'admin',
    'password': 'YourPassword'
}

# 4. Load data
python src/load_to_postgres.py
```

---

## 📊 Usage Examples

### Example 1: Top Stations Analysis

```sql
-- Find top 10 busiest stations per year
SELECT
    d.year,
    s.station_name,
    SUM(f.total_entry_exit) as passengers,
    RANK() OVER (PARTITION BY d.year ORDER BY SUM(f.total_entry_exit) DESC) as rank
FROM fact_passenger_entry_exit f
JOIN dim_stations s ON f.station_id = s.station_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year, s.station_name
QUALIFY rank <= 10
ORDER BY d.year DESC, rank;
```

### Example 2: Line Coverage Analysis

```sql
-- Analyze line coverage and interchange stations
SELECT
    l.line_name,
    l.line_color,
    COUNT(DISTINCT sl.station_id) as total_stations,
    COUNT(DISTINCT CASE WHEN sl.is_interchange THEN sl.station_id END) as interchange_count,
    COUNT(DISTINCT CASE WHEN s.has_night_tube THEN sl.station_id END) as night_tube_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN s.has_night_tube THEN sl.station_id END)::numeric 
        / COUNT(DISTINCT sl.station_id) * 100, 
        2
    ) as night_tube_coverage_pct
FROM dim_lines l
JOIN fact_station_lines sl ON l.line_id = sl.line_id
JOIN dim_stations s ON sl.station_id = s.station_id
GROUP BY l.line_name, l.line_color
ORDER BY total_stations DESC;
```

### Example 3: Growth Trend Analysis

```sql
-- Calculate year-over-year growth by station
WITH yearly_passengers AS (
    SELECT
        s.station_id,
        s.station_name,
        d.year,
        SUM(f.total_entry_exit) as passengers
    FROM fact_passenger_entry_exit f
    JOIN dim_stations s ON f.station_id = s.station_id
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY s.station_id, s.station_name, d.year
)
SELECT
    station_name,
    year,
    passengers,
    LAG(passengers) OVER (PARTITION BY station_id ORDER BY year) as prev_year_passengers,
    passengers - LAG(passengers) OVER (PARTITION BY station_id ORDER BY year) as growth,
    ROUND(
        (passengers - LAG(passengers) OVER (PARTITION BY station_id ORDER BY year))::numeric
        / NULLIF(LAG(passengers) OVER (PARTITION BY station_id ORDER BY year), 0) * 100,
        2
    ) as growth_pct
FROM yearly_passengers
WHERE year >= 2017
ORDER BY station_name, year;
```

### Example 4: Sqoop Export to HDFS

```bash
# Export all TfL tables to HDFS
sqoop import-all-tables \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin \
  --password admin123 \
  --warehouse-dir /user/hadoop/tfl_warehouse \
  --exclude-tables 'pg_*,sql_*,information_schema.*' \
  --as-parquetfile \
  --compression-codec snappy \
  --m 4

# Export specific fact table with partitioning
sqoop import \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin \
  --password admin123 \
  --table fact_passenger_entry_exit \
  --target-dir /user/hadoop/tfl_warehouse/fact_passenger \
  --split-by entry_exit_id \
  --as-parquetfile \
  --m 8
```

### Example 5: Python Analytics

```python
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

# Connect to database
conn = psycopg2.connect(
    host='13.42.152.118',
    port=5432,
    database='testdb',
    user='admin',
    password='admin123'
)

# Load data
query = """
SELECT 
    d.year,
    SUM(f.total_entry_exit) as total_passengers
FROM fact_passenger_entry_exit f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year
ORDER BY d.year
"""

df = pd.read_sql(query, conn)

# Visualize trend
plt.figure(figsize=(12, 6))
plt.plot(df['year'], df['total_passengers'] / 1e9, marker='o')
plt.title('TfL Annual Passenger Volume (2007-2021)')
plt.xlabel('Year')
plt.ylabel('Total Passengers (Billions)')
plt.grid(True)
plt.savefig('tfl_trend.png')
```

---

## 🔧 Troubleshooting

### Issue 1: Connection Failed

**Symptom:**
```
psycopg2.OperationalError: could not connect to server
```

**Solutions:**
```bash
# Check PostgreSQL is running
pg_isready -h 13.42.152.118 -p 5432

# Test network connectivity
telnet 13.42.152.118 5432

# Verify firewall rules
sudo iptables -L | grep 5432

# Check PostgreSQL config
cat /etc/postgresql/15/main/postgresql.conf | grep listen_addresses
```

### Issue 2: Foreign Key Violation

**Symptom:**
```
psycopg2.IntegrityError: insert or update on table "fact_passenger_entry_exit" 
violates foreign key constraint
```

**Solutions:**
```bash
# Drop and recreate tables in correct order
python -c "
import psycopg2
conn = psycopg2.connect(...)
cursor = conn.cursor()
tables = ['fact_passenger_entry_exit', 'fact_station_lines', 'dim_stations', 
          'dim_lines', 'dim_networks', 'dim_date']
for table in tables:
    cursor.execute(f'DROP TABLE IF EXISTS {table} CASCADE')
conn.commit()
"

# Re-run data load
python src/load_to_postgres.py
```

### Issue 3: Duplicate Key Error

**Symptom:**
```
duplicate key value violates unique constraint "dim_stations_nlc_code_key"
```

**Solution:**
```sql
-- Remove UNIQUE constraint from nlc_code
ALTER TABLE dim_stations DROP CONSTRAINT IF EXISTS dim_stations_nlc_code_key;
```

### Issue 4: Out of Memory

**Symptom:**
```
MemoryError: Unable to allocate array
```

**Solutions:**
```python
# Use chunked reading for large files
chunk_size = 10000
for chunk in pd.read_csv('large_file.csv', chunksize=chunk_size):
    process_chunk(chunk)

# Or increase available memory
export PYTHONHASHSEED=0
ulimit -v unlimited
```

---

## 📚 Additional Resources

### Documentation Files
- `README.md` - Full project documentation
- `QUICKSTART.md` - 5-minute setup guide
- `DEPLOYMENT_SUCCESS.md` - Deployment summary
- `PROJECT_OVERVIEW.txt` - Visual overview
- `Data/normalized/DATA_DICTIONARY.txt` - Data dictionary
- `src/er_diagram.sql` - ER diagram

### External Resources
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Apache Sqoop Guide](https://sqoop.apache.org/docs/)
- [TfL Open Data](https://tfl.gov.uk/corporate/open-data)
- [Star Schema Tutorial](https://www.kimballgroup.com/)

### GitHub Repository
🔗 https://github.com/uttamraj9/TFL_Project_Demo

---

## 📧 Support

For issues or questions:
1. Check this documentation
2. Review the troubleshooting section
3. Check the GitHub repository issues
4. Review sample queries in README.md

---

**Built with ❤️ for data engineering excellence**

*Last Updated: 2026-05-29*
