# 🚀 TfL Data Warehouse - Quick Start Guide

## 5-Minute Setup

### Step 1: Run Setup Script
```bash
chmod +x setup.sh
./setup.sh
```

This will:
- ✅ Create virtual environment
- ✅ Install dependencies
- ✅ Generate normalized CSV files
- ✅ Check PostgreSQL status

### Step 2: Configure Database

Edit `src/load_to_postgres.py` line 13-19:

```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'tfl_datawarehouse',
    'user': 'postgres',
    'password': 'YOUR_PASSWORD_HERE'  # ← Change this!
}
```

### Step 3: Start PostgreSQL

**macOS (Homebrew):**
```bash
brew services start postgresql
```

**Linux (systemd):**
```bash
sudo systemctl start postgresql
```

**Manual:**
```bash
pg_ctl -D /usr/local/var/postgres start
```

### Step 4: Load Data

```bash
source venv/bin/activate
python src/load_to_postgres.py
```

### Step 5: Connect & Query

```bash
psql -h localhost -U postgres -d tfl_datawarehouse
```

## 📊 Your First Query

```sql
-- Top 10 busiest stations in 2019
SELECT
    s.station_name,
    f.total_entry_exit as passengers
FROM fact_passenger_entry_exit f
JOIN dim_stations s ON f.station_id = s.station_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year = 2019
ORDER BY passengers DESC
LIMIT 10;
```

## 🗺️ What You Have Now

### 6 Tables Created:
```
dim_networks             (1 record)
dim_lines               (14 records)
dim_stations           (436 records)
dim_date                (15 records)
fact_station_lines     (575 records)
fact_passenger_entry_exit (4,771 records)
```

### 4 Analytical Views:
- `vw_station_summary` - Complete station details
- `vw_annual_passenger_stats` - Annual statistics
- `vw_busiest_stations` - Ranked by year
- `vw_line_stats` - Line coverage stats

### 10 Performance Indexes
All foreign keys and frequently queried columns

## 🔍 Useful Queries

### List All Stations on Piccadilly Line
```sql
SELECT s.station_name
FROM dim_stations s
JOIN fact_station_lines sl ON s.station_id = sl.station_id
JOIN dim_lines l ON sl.line_id = l.line_id
WHERE l.line_name = 'Piccadilly'
ORDER BY s.station_name;
```

### Night Tube Stations
```sql
SELECT station_name, lines_served
FROM vw_station_summary
WHERE has_night_tube = TRUE
ORDER BY number_of_lines DESC;
```

### Year-over-Year Growth
```sql
SELECT
    s.station_name,
    d.year,
    f.total_entry_exit,
    LAG(f.total_entry_exit) OVER (PARTITION BY s.station_id ORDER BY d.year) as prev_year,
    ROUND(
        (f.total_entry_exit - LAG(f.total_entry_exit) OVER (PARTITION BY s.station_id ORDER BY d.year))::numeric
        / NULLIF(LAG(f.total_entry_exit) OVER (PARTITION BY s.station_id ORDER BY d.year), 0) * 100,
        2
    ) as growth_pct
FROM fact_passenger_entry_exit f
JOIN dim_stations s ON f.station_id = s.station_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE s.station_name = 'King''s Cross St. Pancras'
ORDER BY d.year;
```

### Interchange Hubs (Multi-Line Stations)
```sql
SELECT
    station_name,
    number_of_lines,
    lines_served
FROM vw_station_summary
WHERE number_of_lines >= 3
ORDER BY number_of_lines DESC, station_name;
```

## 🛠️ Troubleshooting

### PostgreSQL Not Running?
```bash
# Check status
pg_isready

# Start it
brew services start postgresql  # macOS
sudo systemctl start postgresql # Linux
```

### Can't Connect to Database?
```bash
# Check PostgreSQL is listening
netstat -an | grep 5432

# Try connecting to postgres database first
psql -U postgres -d postgres
```

### Import Errors?
```bash
# Install missing packages
source venv/bin/activate
pip install -r requirements.txt
```

### Data Not Loading?
```sql
-- Check if tables exist
\dt

-- Check record counts
SELECT 'dim_stations' as table_name, COUNT(*) FROM dim_stations
UNION ALL
SELECT 'fact_passenger_entry_exit', COUNT(*) FROM fact_passenger_entry_exit;
```

## 📁 File Locations

- **Raw Data**: `Data/*.xlsx`
- **Normalized CSVs**: `Data/normalized/*.csv`
- **Python Scripts**: `src/*.py`
- **SQL Scripts**: `src/*.sql`
- **Documentation**: `README.md`, `Data/normalized/DATA_DICTIONARY.txt`

## 🎯 Next Steps

1. **Explore the Views**
   ```sql
   SELECT * FROM vw_station_summary LIMIT 10;
   ```

2. **Check Data Quality**
   ```sql
   -- Find stations with no passenger data
   SELECT s.station_name
   FROM dim_stations s
   LEFT JOIN fact_passenger_entry_exit f ON s.station_id = f.station_id
   WHERE f.entry_exit_id IS NULL;
   ```

3. **Analyze Trends**
   ```sql
   -- Passenger trends over time
   SELECT year, SUM(total_entry_exit) as total
   FROM fact_passenger_entry_exit f
   JOIN dim_date d ON f.date_id = d.date_id
   GROUP BY year
   ORDER BY year;
   ```

4. **Build Dashboards**
   - Connect Tableau/PowerBI to PostgreSQL
   - Use connection string: `postgresql://postgres:password@localhost:5432/tfl_datawarehouse`

## 📚 Learn More

- **Full Documentation**: See `README.md`
- **Data Dictionary**: See `Data/normalized/DATA_DICTIONARY.txt`
- **ER Diagram**: See `src/er_diagram.sql`
- **Schema Details**: See `src/create_postgres_schema.sql`

## 💡 Pro Tips

1. **Use Views for Complex Queries**
   Views are pre-optimized and easier to use than writing joins every time.

2. **Indexes Speed Up Queries**
   All foreign keys are automatically indexed. Query by `station_id`, `date_id`, or `line_id` for fast results.

3. **Window Functions for Analytics**
   Use `LAG()`, `LEAD()`, `RANK()`, `ROW_NUMBER()` for trend analysis.

4. **Export Results**
   ```sql
   \copy (SELECT * FROM vw_busiest_stations WHERE year=2019) TO 'results.csv' CSV HEADER
   ```

---

**Need Help?** Check `README.md` for detailed documentation!
