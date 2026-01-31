#!/usr/bin/env python3
"""
ETL Script B: Load hospitalisations table
Reads hospitalisations.csv and creates the hospitalisations fact table with date transformations

Usage:
    python b_load_hospitalisations.py [--hive-host HOSTNAME] [--warehouse-dir DIR]

Environment Variables:
    HIVE_METASTORE_HOST: Hive metastore hostname (default: metastore)
    HIVE_WAREHOUSE_DIR: Hive warehouse directory (default: /opt/hive/data/warehouse)
"""

import os
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_unixtime, unix_timestamp, date_add, year,
    substring, to_date, expr
)

def create_spark_session(hive_host="metastore", warehouse_dir="/opt/hive/data/warehouse"):
    """Initialize Spark session with Hive support

    Args:
        hive_host: Hive metastore hostname (default: metastore)
        warehouse_dir: Hive warehouse directory (default: /opt/hive/data/warehouse)
    """
    metastore_uri = f"thrift://{hive_host}:9083"
    print(f"Connecting to Hive metastore: {metastore_uri}")
    print(f"Warehouse directory: {warehouse_dir}")

    return SparkSession.builder \
        .appName("ETL_B_Hospitalisations") \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .config("hive.metastore.uris", metastore_uri) \
        .enableHiveSupport() \
        .getOrCreate()

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='ETL Script B: Load hospitalisations table')
    parser.add_argument('--hive-host',
                        default=os.getenv('HIVE_METASTORE_HOST', 'metastore'),
                        help='Hive metastore hostname (default: metastore, or HIVE_METASTORE_HOST env var)')
    parser.add_argument('--warehouse-dir',
                        default=os.getenv('HIVE_WAREHOUSE_DIR', '/opt/hive/data/warehouse'),
                        help='Hive warehouse directory (default: /opt/hive/data/warehouse, or HIVE_WAREHOUSE_DIR env var)')
    args = parser.parse_args()

    print(f"[{datetime.now()}] Starting ETL process for hospitalisations table...")

    # Initialize Spark
    spark = create_spark_session(hive_host=args.hive_host, warehouse_dir=args.warehouse_dir)

    # Enable dynamic partitioning
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    # Step 1: Read hospitalisations staging table
    print(f"[{datetime.now()}] Reading hospitalisations_staging table...")
    staging_df = spark.sql("""
        SELECT
            Num_Hospitalisation,
            Id_patient,
            identifiant_organisation,
            Code_diagnostic,
            Date_Entree,
            Jour_Hospitalisation
        FROM hospitalisations_staging
        WHERE Id_patient IS NOT NULL
            AND identifiant_organisation IS NOT NULL
            AND Date_Entree IS NOT NULL
            AND Date_Entree != ''
    """)

    print(f"[{datetime.now()}] Found {staging_df.count()} valid hospitalisations")

    # Step 2: Transform data
    print(f"[{datetime.now()}] Transforming dates and extracting fields...")

    hospitalisations_transformed = staging_df \
        .withColumn(
            "in_date",
            to_date(from_unixtime(unix_timestamp(col("Date_Entree"), "dd/MM/yyyy")))
        ) \
        .withColumn(
            "out_date",
            expr("date_add(in_date, Jour_Hospitalisation)")
        ) \
        .withColumn(
            "patient_id",
            col("Id_patient")
        ) \
        .withColumn(
            "facility_id",
            col("identifiant_organisation")
        ) \
        .withColumn(
            "diagnosis_id",
            substring(col("Code_diagnostic"), 1, 4)
        ) \
        .withColumn(
            "year",
            year(col("in_date"))
        ) \
        .select(
            "in_date",
            "out_date",
            "patient_id",
            "facility_id",
            "diagnosis_id",
            "year"
        )

    # Step 3: Create hospitalisations table if not exists
    print(f"[{datetime.now()}] Creating hospitalisations table if not exists...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS hospitalisations (
            in_date DATE,
            out_date DATE,
            patient_id INT,
            facility_id STRING,
            diagnosis_id STRING
        )
        PARTITIONED BY (year INT)
        STORED AS ORC
    """)

    # Step 4: Load data into hospitalisations table
    print(f"[{datetime.now()}] Loading data into hospitalisations table with dynamic partitioning...")
    hospitalisations_transformed.createOrReplaceTempView("hospitalisations_temp")

    spark.sql("""
        INSERT OVERWRITE TABLE hospitalisations PARTITION(year)
        SELECT
            in_date,
            out_date,
            patient_id,
            facility_id,
            diagnosis_id,
            year
        FROM hospitalisations_temp
    """)

    # Step 5: Validation
    print(f"[{datetime.now()}] Running validation checks...")

    # Total count
    count = spark.sql("SELECT COUNT(*) as cnt FROM hospitalisations").collect()[0]['cnt']
    print(f"[{datetime.now()}] Successfully loaded {count} hospitalisations")

    # Show sample records
    print(f"[{datetime.now()}] Sample records:")
    spark.sql("SELECT * FROM hospitalisations LIMIT 10").show(truncate=False)

    # Year partition distribution
    print(f"[{datetime.now()}] Year partition distribution:")
    spark.sql("""
        SELECT year, COUNT(*) as count
        FROM hospitalisations
        GROUP BY year
        ORDER BY year
    """).show(truncate=False)

    # Check for invalid dates
    invalid_dates = spark.sql("""
        SELECT COUNT(*) as invalid_count
        FROM hospitalisations
        WHERE out_date < in_date OR out_date IS NULL OR in_date IS NULL
    """).collect()[0]['invalid_count']
    print(f"[{datetime.now()}] Invalid dates: {invalid_dates}")

    # Check for orphan facilities
    orphan_facilities = spark.sql("""
        SELECT COUNT(*) AS orphan_count
        FROM hospitalisations h
        LEFT JOIN facilities f ON h.facility_id = f.id
        WHERE f.id IS NULL
    """).collect()[0]['orphan_count']
    print(f"[{datetime.now()}] Hospitalisations with invalid facility_id: {orphan_facilities}")

    if orphan_facilities > 0:
        print(f"[{datetime.now()}] WARNING: Found {orphan_facilities} hospitalisations with missing facilities!")
        print(f"[{datetime.now()}] Sample orphan facility IDs:")
        spark.sql("""
            SELECT DISTINCT h.facility_id
            FROM hospitalisations h
            LEFT JOIN facilities f ON h.facility_id = f.id
            WHERE f.id IS NULL
            LIMIT 10
        """).show(truncate=False)

    spark.stop()
    print(f"[{datetime.now()}] ETL process completed successfully!")

if __name__ == "__main__":
    main()
