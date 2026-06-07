# TfL Data Warehouse Project

A complete data engineering project that transforms Transport for London (TfL) station entry/exit data into a normalized star schema data warehouse ready for PostgreSQL.

## 📊 Project Overview

This project takes raw TfL passenger data and creates a professional data warehouse with:
- **Normalized star schema** with fact and dimension tables
- **Proper primary and foreign key relationships**
- **CSV files ready for database import**
- **Automated PostgreSQL loading scripts**
- **Pre-built analytical views and sample queries**

## 🏗️ Data Model

### Star Schema Architecture

```
                    ┌─────────────────┐
                    │  dim_networks   │
                    │  (1 record)     │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
         ┌──────────│  dim_stations   │◄─────────┐
         │          │  (436 records)  │          │
         │          └────────┬────────┘          │
         │                   │                   │
┌────────▼─────────┐    ┌────▼────────────┐     │
│    dim_lines     │◄───│ fact_station_   │     │
│   (14 records)   │    │     lines       │     │
└──────────────────┘    │  (575 records)  │     │
                        └─────────────────┘     │
         ┌──────────────────────────────────────┘
         │
┌────────▼──────────────────┐      ┌─────────────────┐
│ fact_passenger_entry_exit │◄─────│    dim_date     │
│     (4,771 records)       │      │  (15 records)   │
└───────────────────────────┘      └─────────────────┘
```

### Tables

| Table | Type | Records | Description |
|-------|------|---------|-------------|
| `dim_networks` | Dimension | 1 | Network types (Underground, Rail) |
| `dim_lines` | Dimension | 14 | Individual tube/rail lines with colors |
| `dim_stations` | Dimension | 436 | Station master data with attributes |
| `dim_date` | Dimension | 15 | Date dimension (2007-2021 annual) |
| `fact_station_lines` | Bridge | 575 | Many-to-many station-line relationships |
| `fact_passenger_entry_exit` | Fact | 4,771 | Passenger entry/exit statistics |

## 📁 Project Structure

```
TFL_Project_Demo/
├── Data/
│   ├── normalized/              # Generated normalized CSV files
│   │   ├── dim_networks.csv
│   │   ├── dim_lines.csv
│   │   ├── dim_stations.csv
│   │   ├── dim_date.csv
│   │   ├── fact_station_lines.csv
│   │   ├── fact_passenger_entry_exit.csv
│   │   └── DATA_DICTIONARY.txt
│   ├── Geodata/                 # Geographic data
│   ├── Tube maps/               # Tube map images
│   └── *.xlsx                   # Raw TfL data files
├── src/
│   ├── data_modeling.py         # Python script to create normalized tables
│   ├── create_postgres_schema.sql  # PostgreSQL schema DDL
│   └── load_to_postgres.py      # Python script to load data into PostgreSQL
├── test/
└── README.md
```

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- PostgreSQL 12+
- Virtual environment (included)

### Step 1: Generate Normalized CSV Files

```bash
# Activate virtual environment
source venv/bin/activate

# Run data modeling script
python src/data_modeling.py
```

Output: 6 normalized CSV files in `Data/normalized/`

### Step 2: Setup PostgreSQL

```bash
# Make sure PostgreSQL is running
# macOS with Homebrew:
brew services start postgresql

# Or manually:
pg_ctl -D /usr/local/var/postgres start
```

### Step 3: Load Data into PostgreSQL

```bash
# Install psycopg2 if not already installed
pip install psycopg2-binary

# Edit database credentials in src/load_to_postgres.py
# Then run the loader:
python src/load_to_postgres.py
```

This will:
1. Create database `tfl_datawarehouse` (if doesn't exist)
2. Create all tables with proper relationships
3. Load data from CSV files
4. Create analytical views
5. Run sample queries

### Step 4: Connect and Query

```bash
# Connect to database
psql -h localhost -p 5432 -U postgres -d tfl_datawarehouse

# Or use the connection string
postgresql://postgres:postgres@localhost:5432/tfl_datawarehouse
```

## 📊 Database Configuration

Edit the following in `src/load_to_postgres.py`:

```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'tfl_datawarehouse',
    'user': 'postgres',
    'password': 'YOUR_PASSWORD'  # Change this!
}
```

## 🔍 Sample Queries

### 1. Top 10 Busiest Stations (2019)

```sql
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

### 2. Stations by Line

```sql
SELECT
    l.line_name,
    COUNT(DISTINCT sl.station_id) as station_count,
    STRING_AGG(s.station_name, ', ' ORDER BY s.station_name) as stations
FROM dim_lines l
JOIN fact_station_lines sl ON l.line_id = sl.line_id
JOIN dim_stations s ON sl.station_id = s.station_id
WHERE sl.effective_to IS NULL
GROUP BY l.line_name
ORDER BY station_count DESC;
```

### 3. Year-over-Year Growth

```sql
SELECT
    s.station_name,
    d.year,
    SUM(f.total_entry_exit) as passengers,
    LAG(SUM(f.total_entry_exit)) OVER (PARTITION BY s.station_id ORDER BY d.year) as prev_year,
    ROUND(
        (SUM(f.total_entry_exit) - LAG(SUM(f.total_entry_exit)) OVER (PARTITION BY s.station_id ORDER BY d.year))
        / NULLIF(LAG(SUM(f.total_entry_exit)) OVER (PARTITION BY s.station_id ORDER BY d.year), 0) * 100,
        2
    ) as growth_pct
FROM fact_passenger_entry_exit f
JOIN dim_stations s ON f.station_id = s.station_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.is_annual = TRUE
GROUP BY s.station_id, s.station_name, d.year
ORDER BY s.station_name, d.year;
```

### 4. Interchange Stations

```sql
SELECT
    s.station_name,
    COUNT(DISTINCT sl.line_id) as number_of_lines,
    STRING_AGG(l.line_name, ', ' ORDER BY l.line_name) as lines
FROM dim_stations s
JOIN fact_station_lines sl ON s.station_id = sl.station_id
JOIN dim_lines l ON sl.line_id = l.line_id
WHERE sl.is_interchange = TRUE
GROUP BY s.station_name
ORDER BY number_of_lines DESC, s.station_name;
```

### 5. Night Tube Coverage

```sql
SELECT
    l.line_name,
    COUNT(DISTINCT s.station_id) as total_stations,
    COUNT(DISTINCT CASE WHEN s.has_night_tube THEN s.station_id END) as night_tube_stations,
    ROUND(
        COUNT(DISTINCT CASE WHEN s.has_night_tube THEN s.station_id END)::numeric
        / COUNT(DISTINCT s.station_id) * 100,
        2
    ) as coverage_pct
FROM dim_lines l
JOIN fact_station_lines sl ON l.line_id = sl.line_id
JOIN dim_stations s ON sl.station_id = s.station_id
GROUP BY l.line_name
ORDER BY coverage_pct DESC;
```

## 📋 Pre-built Views

The schema includes 4 analytical views:

1. **`vw_station_summary`** - Complete station details with line information
2. **`vw_annual_passenger_stats`** - Annual passenger statistics by station
3. **`vw_busiest_stations`** - Ranked busiest stations by year
4. **`vw_line_stats`** - Line statistics (stations, interchanges, night service)

### Example View Usage

```sql
-- Use the station summary view
SELECT * FROM vw_station_summary
WHERE has_night_tube = TRUE
ORDER BY number_of_lines DESC;

-- Use the busiest stations view
SELECT * FROM vw_busiest_stations
WHERE year = 2019 AND rank_by_year <= 20;
```

## 🔗 Relationships

### Foreign Key Constraints

- `dim_stations.network_id` → `dim_networks.network_id`
- `fact_station_lines.station_id` → `dim_stations.station_id`
- `fact_station_lines.line_id` → `dim_lines.line_id`
- `fact_passenger_entry_exit.station_id` → `dim_stations.station_id`
- `fact_passenger_entry_exit.date_id` → `dim_date.date_id`

### Indexes

Performance indexes are automatically created on:
- All foreign keys
- Frequently queried columns (station_name, line_name, year)
- Composite indexes for common join patterns

## 📈 Data Quality

- **Primary Keys**: All tables have proper primary keys
- **Foreign Keys**: All relationships enforced with FK constraints
- **Unique Constraints**: Prevent duplicate records
- **NULL Handling**: Proper NULL constraints on required fields
- **Data Types**: Appropriate data types for each column
- **Timestamps**: Audit columns (created_at, updated_at) on all tables

## 🛠️ Extending the Model

### Add Monthly Data

To extend the model with monthly granularity:

1. Add monthly records to `dim_date`:
```sql
INSERT INTO dim_date (date_id, year, quarter, month, is_annual, period_label, period_start, period_end)
VALUES (100, 2022, 1, 1, FALSE, '2022-01 January', '2022-01-01', '2022-01-31');
```

2. Load monthly passenger data into `fact_passenger_entry_exit`

### Add Geographic Data

Add latitude/longitude to stations:

```sql
ALTER TABLE dim_stations
ADD COLUMN latitude DECIMAL(10, 8),
ADD COLUMN longitude DECIMAL(11, 8);
```

### Add Station Zones

Create a zone dimension:

```sql
CREATE TABLE dim_zones (
    zone_id INTEGER PRIMARY KEY,
    zone_number VARCHAR(10),
    zone_description TEXT
);

ALTER TABLE dim_stations
ADD COLUMN zone_id INTEGER REFERENCES dim_zones(zone_id);
```

## 🧪 Testing

Verify data integrity:

```sql
-- Check for orphaned records
SELECT 'fact_passenger_entry_exit' as table_name, COUNT(*) as orphaned
FROM fact_passenger_entry_exit f
LEFT JOIN dim_stations s ON f.station_id = s.station_id
WHERE s.station_id IS NULL
UNION ALL
SELECT 'fact_station_lines', COUNT(*)
FROM fact_station_lines f
LEFT JOIN dim_stations s ON f.station_id = s.station_id
WHERE s.station_id IS NULL;

-- Validate passenger counts
SELECT
    year,
    SUM(total_entry_exit) as total,
    AVG(total_entry_exit) as average,
    MIN(total_entry_exit) as minimum,
    MAX(total_entry_exit) as maximum
FROM fact_passenger_entry_exit f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY year
ORDER BY year;
```

## 📝 License

This project uses open data from Transport for London (TfL).

## 🤝 Contributing

Feel free to fork and extend this project. Common extensions:
- Add real-time API data ingestion
- Create dbt models for transformations
- Build dashboards with Tableau/PowerBI
- Add geospatial analysis with PostGIS
- Implement data quality checks with Great Expectations

## 📞 Support

For issues or questions:
1. Check the DATA_DICTIONARY.txt in Data/normalized/
2. Review sample queries in this README
3. Examine the schema in src/create_postgres_schema.sql

---

**Built with ❤️ for data engineering learning and practice**
