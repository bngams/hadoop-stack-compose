#!/usr/bin/env python3
"""
ETL Script C: Load diagnoses table
Reads hospitalisations.csv and creates the diagnoses dimension table with deduplicated diagnostic codes
"""

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, first
from pyspark.sql.window import Window

def create_spark_session():
    """Initialize Spark session with Hive support"""
    return SparkSession.builder \
        .appName("ETL_C_Diagnoses") \
        .config("spark.sql.warehouse.dir", "/opt/hive/data/warehouse") \
        .config("hive.metastore.uris", "thrift://metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()

def main():
    print(f"[{datetime.now()}] Starting ETL process for diagnoses table...")

    # Initialize Spark
    spark = create_spark_session()

    # Step 1: Read hospitalisations staging table
    print(f"[{datetime.now()}] Reading hospitalisations_staging table...")
    staging_df = spark.sql("""
        SELECT
            Code_diagnostic,
            Suite_diagnostic_consultation,
            Num_Hospitalisation
        FROM hospitalisations_staging
        WHERE Code_diagnostic IS NOT NULL
            AND Code_diagnostic != ''
    """)

    print(f"[{datetime.now()}] Found {staging_df.count()} records with diagnostic codes")

    # Step 2: Extract 4-character diagnostic codes and deduplicate
    print(f"[{datetime.now()}] Extracting and deduplicating diagnostic codes...")

    # Create window specification to get the first description for each diagnosis code
    window_spec = Window.partitionBy("diagnosis_id").orderBy("Num_Hospitalisation")

    diagnoses_df = staging_df \
        .withColumn("diagnosis_id", substring(col("Code_diagnostic"), 1, 4)) \
        .select(
            col("diagnosis_id"),
            col("Suite_diagnostic_consultation").alias("diagnosis_name"),
            col("Num_Hospitalisation")
        ) \
        .withColumn(
            "first_name",
            first(col("diagnosis_name")).over(window_spec)
        ) \
        .select("diagnosis_id", "first_name") \
        .distinct() \
        .withColumnRenamed("first_name", "name")

    unique_count = diagnoses_df.count()
    print(f"[{datetime.now()}] Found {unique_count} unique diagnostic codes")

    # Step 3: Create diagnoses table if not exists
    print(f"[{datetime.now()}] Creating diagnoses table if not exists...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS diagnoses (
            id STRING,
            name STRING
        ) STORED AS ORC
    """)

    # Step 4: Load data into diagnoses table
    print(f"[{datetime.now()}] Loading data into diagnoses table...")
    diagnoses_df.createOrReplaceTempView("diagnoses_temp")

    spark.sql("""
        INSERT OVERWRITE TABLE diagnoses
        SELECT
            diagnosis_id as id,
            name
        FROM diagnoses_temp
        ORDER BY id
    """)

    # Step 5: Validation
    count = spark.sql("SELECT COUNT(*) as cnt FROM diagnoses").collect()[0]['cnt']
    print(f"[{datetime.now()}] Successfully loaded {count} diagnoses")

    # Show sample records
    print(f"[{datetime.now()}] Sample records:")
    spark.sql("SELECT * FROM diagnoses LIMIT 20").show(truncate=False)

    # Show diagnostic code prefix distribution
    print(f"[{datetime.now()}] Diagnostic code prefix distribution (top 10):")
    spark.sql("""
        SELECT
            SUBSTRING(id, 1, 1) as code_prefix,
            COUNT(*) as count
        FROM diagnoses
        GROUP BY SUBSTRING(id, 1, 1)
        ORDER BY count DESC
        LIMIT 10
    """).show(truncate=False)

    # Check for orphan diagnoses in hospitalisations
    print(f"[{datetime.now()}] Checking for orphan diagnoses in hospitalisations table...")
    orphan_check = spark.sql("""
        SELECT COUNT(*) AS orphan_count
        FROM hospitalisations h
        LEFT JOIN diagnoses d ON h.diagnosis_id = d.id
        WHERE d.id IS NULL
    """)

    if orphan_check.count() > 0:
        orphan_count = orphan_check.collect()[0]['orphan_count']
        print(f"[{datetime.now()}] Hospitalisations with missing diagnosis_id: {orphan_count}")
        if orphan_count > 0:
            print(f"[{datetime.now()}] WARNING: Found {orphan_count} hospitalisations with missing diagnoses!")
    else:
        print(f"[{datetime.now()}] No orphan diagnoses found - data integrity is good!")

    spark.stop()
    print(f"[{datetime.now()}] ETL process completed successfully!")

if __name__ == "__main__":
    main()
