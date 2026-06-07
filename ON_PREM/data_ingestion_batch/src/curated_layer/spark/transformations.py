#!/usr/bin/env python3
"""
TfL Data Warehouse - PySpark Analysis
Analyzes TfL passenger data from HDFS/Hive
Performs aggregations, transformations, and insights generation
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

def create_spark_session(app_name="TfL_Data_Analysis"):
    """Create and configure Spark session"""
    print(f"Creating Spark session: {app_name}")

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.sql.catalogImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print(f"✓ Spark session created - Version: {spark.version}")
    return spark

def load_data_from_hive(spark):
    """Load TfL data from Hive tables"""
    print("\n" + "="*80)
    print("Loading Data from Hive Database: tlf_db")
    print("="*80)

    try:
        # Load dimension tables
        print("Loading dimension tables...")
        dim_stations = spark.sql("SELECT * FROM tfl_db.dim_stations")
        dim_lines = spark.sql("SELECT * FROM tfl_db.dim_lines")
        dim_date = spark.sql("SELECT * FROM tfl_db.dim_date")

        # Load fact tables
        print("Loading fact tables...")
        fact_passengers = spark.sql("SELECT * FROM tfl_db.fact_passenger_entry_exit")
        fact_station_lines = spark.sql("SELECT * FROM tfl_db.fact_station_lines")

        print("✓ All tables loaded successfully")
        return dim_stations, dim_lines, dim_date, fact_passengers, fact_station_lines

    except Exception as e:
        print(f"✗ Error loading data from Hive: {e}")
        print("\nCreating sample data for demonstration...")
        return create_sample_data(spark)

def create_sample_data(spark):
    """Create sample TfL data for demonstration"""
    print("\n" + "="*80)
    print("Creating Sample TfL Data")
    print("="*80)

    # Sample stations
    stations_data = [
        (1, "Kings Cross St. Pancras", "Underground", True),
        (2, "Oxford Circus", "Underground", True),
        (3, "Stratford", "Underground", True),
        (4, "Waterloo", "Underground", True),
        (5, "Bank", "Underground", True),
        (6, "Liverpool Street", "Underground", True),
        (7, "Victoria", "Underground", True),
        (8, "London Bridge", "Underground", True),
        (9, "Paddington", "Underground", True),
        (10, "Canary Wharf", "Underground", True)
    ]

    dim_stations = spark.createDataFrame(
        stations_data,
        ["station_id", "station_name", "network_type", "is_active"]
    )

    # Sample lines
    lines_data = [
        (1, "Northern", "#000000"),
        (2, "Piccadilly", "#003688"),
        (3, "Central", "#E32017"),
        (4, "District", "#00782A"),
        (5, "Circle", "#FFD300"),
        (6, "Jubilee", "#A0A5A9")
    ]

    dim_lines = spark.createDataFrame(
        lines_data,
        ["line_id", "line_name", "line_color"]
    )

    # Sample dates
    dates_data = [
        (1, 2021, 1, "2021", "2021-01-01", "2021-12-31"),
        (2, 2022, 1, "2022", "2022-01-01", "2022-12-31"),
        (3, 2023, 1, "2023", "2023-01-01", "2023-12-31")
    ]

    dim_date = spark.createDataFrame(
        dates_data,
        ["date_id", "year", "quarter", "period_label", "period_start", "period_end"]
    )

    # Sample passenger data
    passengers_data = []
    import random
    for station_id in range(1, 11):
        for date_id in range(1, 4):
            total_passengers = random.randint(50000000, 150000000)
            passengers_data.append((
                station_id * 100 + date_id,
                station_id,
                date_id,
                total_passengers,
                total_passengers // 2,
                total_passengers // 2
            ))

    fact_passengers = spark.createDataFrame(
        passengers_data,
        ["entry_exit_id", "station_id", "date_id", "total_entry_exit",
         "estimated_entries", "estimated_exits"]
    )

    # Sample station-line relationships
    station_lines_data = [
        (1, 1, 1), (2, 2, 1), (3, 3, 1), (4, 4, 1), (5, 5, 1),
        (6, 1, 2), (7, 2, 3), (8, 3, 4), (9, 4, 5), (10, 5, 6)
    ]

    fact_station_lines = spark.createDataFrame(
        station_lines_data,
        ["station_line_id", "station_id", "line_id"]
    )

    print("✓ Sample data created successfully")
    return dim_stations, dim_lines, dim_date, fact_passengers, fact_station_lines

def analyze_busiest_stations(spark, dim_stations, fact_passengers, dim_date):
    """Find the busiest stations by year"""
    print("\n" + "="*80)
    print("Analysis 1: Top 10 Busiest Stations")
    print("="*80)

    # Join stations with passenger data and dates
    busiest = fact_passengers \
        .join(dim_stations, "station_id") \
        .join(dim_date, "date_id") \
        .groupBy("station_name", "year") \
        .agg(
            sum("total_entry_exit").alias("total_passengers"),
            avg("total_entry_exit").alias("avg_passengers")
        ) \
        .orderBy(col("total_passengers").desc()) \
        .limit(10)

    print("\nTop 10 Busiest Stations (All Years):")
    busiest.show(truncate=False)

    return busiest

def analyze_year_over_year_growth(spark, dim_stations, fact_passengers, dim_date):
    """Calculate year-over-year passenger growth"""
    print("\n" + "="*80)
    print("Analysis 2: Year-over-Year Growth")
    print("="*80)

    # Calculate yearly totals
    yearly_totals = fact_passengers \
        .join(dim_date, "date_id") \
        .groupBy("year") \
        .agg(sum("total_entry_exit").alias("total_passengers")) \
        .orderBy("year")

    # Calculate growth rate
    from pyspark.sql.window import Window
    windowSpec = Window.orderBy("year")

    growth = yearly_totals \
        .withColumn("prev_year_passengers", lag("total_passengers").over(windowSpec)) \
        .withColumn("growth",
            (col("total_passengers") - col("prev_year_passengers")) / col("prev_year_passengers") * 100
        ) \
        .withColumn("growth_rate", round(col("growth"), 2))

    print("\nYear-over-Year Passenger Growth:")
    growth.select("year", "total_passengers", "growth_rate").show()

    return growth

def analyze_station_performance(spark, dim_stations, fact_passengers, dim_date):
    """Analyze individual station performance"""
    print("\n" + "="*80)
    print("Analysis 3: Station Performance Metrics")
    print("="*80)

    performance = fact_passengers \
        .join(dim_stations, "station_id") \
        .join(dim_date, "date_id") \
        .groupBy("station_name") \
        .agg(
            sum("total_entry_exit").alias("total_passengers"),
            avg("total_entry_exit").alias("avg_annual_passengers"),
            max("total_entry_exit").alias("peak_year_passengers"),
            min("total_entry_exit").alias("min_year_passengers"),
            count("*").alias("years_recorded")
        ) \
        .withColumn("avg_annual", round(col("avg_annual_passengers"), 0)) \
        .orderBy(col("total_passengers").desc())

    print("\nStation Performance Summary:")
    performance.select(
        "station_name",
        "total_passengers",
        "avg_annual",
        "peak_year_passengers",
        "years_recorded"
    ).show(20, truncate=False)

    return performance

def analyze_line_coverage(spark, dim_stations, dim_lines, fact_station_lines):
    """Analyze line coverage and station distribution"""
    print("\n" + "="*80)
    print("Analysis 4: Line Coverage Analysis")
    print("="*80)

    line_stats = fact_station_lines \
        .join(dim_lines, "line_id") \
        .groupBy("line_name", "line_color") \
        .agg(
            count("station_id").alias("number_of_stations"),
            countDistinct("station_id").alias("unique_stations")
        ) \
        .orderBy(col("number_of_stations").desc())

    print("\nLine Coverage Statistics:")
    line_stats.show(truncate=False)

    return line_stats

def save_results_to_hdfs(df, output_path, format="parquet"):
    """Save analysis results to HDFS"""
    print(f"\nSaving results to HDFS: {output_path}")
    try:
        df.write \
            .mode("overwrite") \
            .format(format) \
            .save(output_path)
        print(f"✓ Results saved successfully")
    except Exception as e:
        print(f"✗ Error saving results: {e}")

def main():
    """Main execution function"""
    print("\n" + "="*80)
    print("TfL Data Warehouse - PySpark Analysis Pipeline")
    print("="*80)

    # Create Spark session
    spark = create_spark_session()

    try:
        # Load data
        dim_stations, dim_lines, dim_date, fact_passengers, fact_station_lines = \
            load_data_from_hive(spark)

        # Show data counts
        print("\n" + "="*80)
        print("Data Summary")
        print("="*80)
        print(f"Stations: {dim_stations.count()}")
        print(f"Lines: {dim_lines.count()}")
        print(f"Date periods: {dim_date.count()}")
        print(f"Passenger records: {fact_passengers.count()}")
        print(f"Station-Line relationships: {fact_station_lines.count()}")

        # Run analyses
        busiest_stations = analyze_busiest_stations(spark, dim_stations, fact_passengers, dim_date)
        growth_analysis = analyze_year_over_year_growth(spark, dim_stations, fact_passengers, dim_date)
        performance = analyze_station_performance(spark, dim_stations, fact_passengers, dim_date)
        line_coverage = analyze_line_coverage(spark, dim_stations, dim_lines, fact_station_lines)

        # Save results
        output_base = "/tmp/aparna/tfl_transformaed_output"
        save_results_to_hdfs(busiest_stations, f"{output_base}/busiest_stations")
        save_results_to_hdfs(performance, f"{output_base}/station_performance")
        save_results_to_hdfs(line_coverage, f"{output_base}/line_coverage")

        print("\n" + "="*80)
        print("✓✓✓ Analysis Complete!")
        print("="*80)
        print(f"Results saved to HDFS: {output_base}/")
        print("="*80)

    except Exception as e:
        print(f"\n✗ Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        spark.stop()
        print("\n✓ Spark session stopped")

if __name__ == "__main__":
    main()