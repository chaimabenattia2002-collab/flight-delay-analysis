"""
Flight Delay Analysis using Apache Spark and SQL
Big Data Acquisition Project

This project analyzes flight delay patterns across airlines and routes
using PySpark and Spark SQL for distributed data processing.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, when, 
    round as spark_round, 
    sum as spark_sum, 
    isnull
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FlightDelayAnalysis") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

print("=" * 60)
print("FLIGHT DELAY ANALYSIS - SPARK & SQL PROJECT")
print("=" * 60)

# Define schema for flight data
schema = StructType([
    StructField("flight_id", StringType(), True),
    StructField("airline", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("delay_minutes", IntegerType(), True)
])

# Read CSV file
print("\n📂 Loading data from CSV file...")
flights_df = spark.read.csv(
    "flight_delays.csv",
    header=True,
    schema=schema
)

# Method 2: With inferSchema (alternative - automatically detects types)
# flights_df = spark.read.csv("flight_delays.csv", header=True, inferSchema=True)

# Method 3: Read from different path if needed
# flights_df = spark.read.csv("/path/to/your/flight_delays.csv", header=True, schema=schema)

print("✅ Data loaded successfully!")

# Register as temporary SQL table
flights_df.createOrReplaceTempView("flights")

print("\n📊 Dataset Overview:")
total_count = flights_df.count()
print(f"Total Flights: {total_count}")
print("\nFirst 10 records:")
flights_df.show(10)

# Verify data quality
print("\n🔍 Data Quality Check:")
flights_df.printSchema()
print(f"\nNull values check:")
flights_df.select([spark_sum(isnull(c).cast("int")).alias(c) for c in flights_df.columns]).show()

print("\n" + "=" * 60)
print("ANALYSIS 1: AVERAGE DELAY BY AIRLINE")
print("=" * 60)

# Using Spark SQL
avg_delay_sql = spark.sql("""
    SELECT 
        airline,
        ROUND(AVG(delay_minutes), 2) as avg_delay,
        COUNT(*) as total_flights,
        ROUND(SUM(delay_minutes), 2) as total_delay_minutes
    FROM flights
    GROUP BY airline
    ORDER BY avg_delay DESC
""")

print("\n✈️ Average Delay by Airline (SQL):")
avg_delay_sql.show()

# Using DataFrame API
avg_delay_df = flights_df.groupBy("airline") \
    .agg(
        spark_round(avg("delay_minutes"), 2).alias("avg_delay"),
        count("*").alias("total_flights"),
        spark_round(spark_sum("delay_minutes"), 2).alias("total_delay_minutes")
    ) \
    .orderBy(col("avg_delay").desc())

print("\n" + "=" * 60)
print("ANALYSIS 2: ROUTES WITH HIGHEST DELAYS")
print("=" * 60)

# Using SQL
route_delays_sql = spark.sql("""
    SELECT 
        origin,
        destination,
        ROUND(AVG(delay_minutes), 2) as avg_delay,
        COUNT(*) as flight_count,
        MAX(delay_minutes) as max_delay
    FROM flights
    GROUP BY origin, destination
    HAVING COUNT(*) >= 1
    ORDER BY avg_delay DESC
    LIMIT 10
""")

print("\n🛫 Top Routes by Average Delay:")
route_delays_sql.show()

print("\n" + "=" * 60)
print("ANALYSIS 3: ON-TIME PERFORMANCE")
print("=" * 60)

# Flights are considered on-time if delay <= 15 minutes
ontime_analysis = spark.sql("""
    SELECT 
        airline,
        COUNT(*) as total_flights,
        SUM(CASE WHEN delay_minutes <= 15 THEN 1 ELSE 0 END) 
                            as ontime_flights,
        ROUND(100.0 * SUM(CASE WHEN delay_minutes <= 15 THEN 1 ELSE 0 END)
                             / COUNT(*), 2) as ontime_percentage
    FROM flights
    GROUP BY airline
    ORDER BY ontime_percentage DESC
""")

print("\n⏰ On-Time Performance by Airline (≤15 min delay):")
ontime_analysis.show()

# Overall on-time percentage
overall_ontime = spark.sql("""
    SELECT 
        COUNT(*) as total_flights,
        SUM(CASE WHEN delay_minutes <= 15 THEN 1 ELSE 0 END) as ontime_flights,
        ROUND(100.0 * SUM(CASE WHEN delay_minutes <= 15 THEN 1 ELSE 0 END) / COUNT(*), 2) as ontime_percentage
    FROM flights
""")

print("\n📈 Overall On-Time Performance:")
overall_ontime.show()

print("\n" + "=" * 60)
print("ANALYSIS 4: CITIES CAUSING MOST DELAYS")
print("=" * 60)

# Analyze delays by origin city
origin_delays = spark.sql("""
    SELECT 
        origin as city,
        COUNT(*) as departing_flights,
        ROUND(AVG(delay_minutes), 2) as avg_delay,
        SUM(delay_minutes) as total_delay_minutes,
        MAX(delay_minutes) as max_delay
    FROM flights
    GROUP BY origin
    ORDER BY avg_delay DESC
""")

print("\n🏙️ Delays by Origin City:")
origin_delays.show()

# Analyze delays by destination city
dest_delays = spark.sql("""
    SELECT 
        destination as city,
        COUNT(*) as arriving_flights,
        ROUND(AVG(delay_minutes), 2) as avg_delay,
        SUM(delay_minutes) as total_delay_minutes
    FROM flights
    GROUP BY destination
    ORDER BY avg_delay DESC
""")
print("\n🏙️ Delays by Destination City:")
dest_delays.show()

print("\n" + "=" * 60)
print("BONUS ANALYSIS: DELAY SEVERITY CATEGORIES")
print("=" * 60)

# Categorize delays
delay_categories = spark.sql("""
    SELECT 
        airline,
        SUM(CASE WHEN delay_minutes = 0 THEN 1 ELSE 0 END) as no_delay,
        SUM(CASE WHEN delay_minutes > 0 AND delay_minutes <= 15 THEN 1 ELSE 0 END)
                              as minor_delay,
        SUM(CASE WHEN delay_minutes > 15 AND delay_minutes <= 60 THEN 1 ELSE 0 END) 
                             as moderate_delay,
        SUM(CASE WHEN delay_minutes > 60 THEN 1 ELSE 0 END) as major_delay
    FROM flights
    GROUP BY airline
    ORDER BY airline
""")
print("\n📊 Delay Severity Distribution by Airline:")
delay_categories.show()

print("\n" + "=" * 60)
print("KEY INSIGHTS & STORYTELLING")
print("=" * 60)

# Generate insights
airline_stats = avg_delay_df.collect()

if airline_stats:
    worst_airline = airline_stats[0]
    best_airline = airline_stats[-1]
    
    print(f"\n🔍 Key Findings:")
    print(f"   • {worst_airline['airline']} has the highest average delay: {worst_airline['avg_delay']} minutes")
    print(f"   • {best_airline['airline']} has the lowest average delay: {best_airline['avg_delay']} minutes")
    
    total_flights = sum([row['total_flights'] for row in airline_stats])
    print(f"   • Total flights analyzed: {total_flights}")
    
    ontime_data = ontime_analysis.collect()
    if ontime_data:
        best_ontime = ontime_data[0]
        print(f"   • Best on-time performance: {best_ontime['airline']} at {best_ontime['ontime_percentage']}%")

print("\n💡 Business Recommendations:")
print("   1. Airlines with high delays should investigate operational bottlenecks")
print("   2. Routes with consistent delays may need schedule adjustments")
print("   3. Cities causing delays might need infrastructure improvements")
print("   4. Consider weather patterns and airport capacity in high-delay cities")

print("\n" + "=" * 60)
print("EXPORT RESULTS")
print("=" * 60)

# Save results to CSV files using Pandas (Windows-compatible)
import os

output_dir = "output"
print(f"\n💾 Saving analysis results to '{output_dir}/' directory...")

try:
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Convert Spark DataFrames to Pandas and save as CSV
    avg_delay_df.toPandas().to_csv(
        f"{output_dir}/avg_delay_by_airline.csv", index=False
    )
    print("✅ Saved: avg_delay_by_airline.csv")
    
    route_delays_sql.toPandas().to_csv(
        f"{output_dir}/route_delays.csv", index=False
    )
    print("✅ Saved: route_delays.csv")
    
    ontime_analysis.toPandas().to_csv(
        f"{output_dir}/ontime_performance.csv", index=False
    )
    print("✅ Saved: ontime_performance.csv")
    
    origin_delays.toPandas().to_csv(
        f"{output_dir}/origin_delays.csv", index=False
    )
    print("✅ Saved: origin_delays.csv")
    
    dest_delays.toPandas().to_csv(
        f"{output_dir}/destination_delays.csv", index=False
    )
    print("✅ Saved: destination_delays.csv")
    
    delay_categories.toPandas().to_csv(
        f"{output_dir}/delay_categories.csv", index=False
    )
    print("✅ Saved: delay_categories.csv")
    
    print(f"\n📁 All results saved to '{output_dir}/' directory!")
    print(f"📍 Full path: {os.path.abspath(output_dir)}")
    
except Exception as e:
    print(f"⚠️ Error saving files: {e}")
    print("Make sure you have write permissions in the current directory")

print("\n✅ Analysis Complete!")
print("\n" + "=" * 60)
print("📋 FILES GENERATED:")
print("=" * 60)
print("Input:  flight_delays.csv (1000+ records)")
print("Output: output/avg_delay_by_airline/")
print("        output/route_delays/")
print("        output/ontime_performance/")
print("        output/origin_delays/")
print("        output/destination_delays/")
print("=" * 60)

# Stop Spark session
print("\n🛑 Stopping Spark session...")
spark.stop()
print("✅ Spark session closed successfully!")