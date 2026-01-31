#!/usr/bin/env python3
"""
ETL Script A: Load facilities table
Reads etablissement_sante.csv and creates the facilities dimension table with region mapping

Usage:
    python a_load_facilities.py [--hive-host HOSTNAME] [--warehouse-dir DIR]

Environment Variables:
    HIVE_METASTORE_HOST: Hive metastore hostname (default: metastore)
    HIVE_WAREHOUSE_DIR: Hive warehouse directory (default: /opt/hive/data/warehouse)
"""

import os
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, coalesce, lit, when

# Département to Région mapping (all 101 French départements)
DEPT_REGION_MAPPING = {
    '01': 'Auvergne-Rhône-Alpes', '02': 'Hauts-de-France', '03': 'Auvergne-Rhône-Alpes',
    '04': "Provence-Alpes-Côte d'Azur", '05': "Provence-Alpes-Côte d'Azur",
    '06': "Provence-Alpes-Côte d'Azur", '07': 'Auvergne-Rhône-Alpes',
    '08': 'Grand Est', '09': 'Occitanie', '10': 'Grand Est',
    '11': 'Occitanie', '12': 'Occitanie', '13': "Provence-Alpes-Côte d'Azur",
    '14': 'Normandie', '15': 'Auvergne-Rhône-Alpes', '16': 'Nouvelle-Aquitaine',
    '17': 'Nouvelle-Aquitaine', '18': 'Centre-Val de Loire', '19': 'Nouvelle-Aquitaine',
    '21': 'Bourgogne-Franche-Comté', '22': 'Bretagne', '23': 'Nouvelle-Aquitaine',
    '24': 'Nouvelle-Aquitaine', '25': 'Bourgogne-Franche-Comté', '26': 'Auvergne-Rhône-Alpes',
    '27': 'Normandie', '28': 'Centre-Val de Loire', '29': 'Bretagne',
    '2A': 'Corse', '2B': 'Corse', '30': 'Occitanie',
    '31': 'Occitanie', '32': 'Occitanie', '33': 'Nouvelle-Aquitaine',
    '34': 'Occitanie', '35': 'Bretagne', '36': 'Centre-Val de Loire',
    '37': 'Centre-Val de Loire', '38': 'Auvergne-Rhône-Alpes', '39': 'Bourgogne-Franche-Comté',
    '40': 'Nouvelle-Aquitaine', '41': 'Centre-Val de Loire', '42': 'Auvergne-Rhône-Alpes',
    '43': 'Auvergne-Rhône-Alpes', '44': 'Pays de la Loire', '45': 'Centre-Val de Loire',
    '46': 'Occitanie', '47': 'Nouvelle-Aquitaine', '48': 'Occitanie',
    '49': 'Pays de la Loire', '50': 'Normandie', '51': 'Grand Est',
    '52': 'Grand Est', '53': 'Pays de la Loire', '54': 'Grand Est',
    '55': 'Grand Est', '56': 'Bretagne', '57': 'Grand Est',
    '58': 'Bourgogne-Franche-Comté', '59': 'Hauts-de-France', '60': 'Hauts-de-France',
    '61': 'Normandie', '62': 'Hauts-de-France', '63': 'Auvergne-Rhône-Alpes',
    '64': 'Nouvelle-Aquitaine', '65': 'Occitanie', '66': 'Occitanie',
    '67': 'Grand Est', '68': 'Grand Est', '69': 'Auvergne-Rhône-Alpes',
    '70': 'Bourgogne-Franche-Comté', '71': 'Bourgogne-Franche-Comté', '72': 'Pays de la Loire',
    '73': 'Auvergne-Rhône-Alpes', '74': 'Auvergne-Rhône-Alpes', '75': 'Île-de-France',
    '76': 'Normandie', '77': 'Île-de-France', '78': 'Île-de-France',
    '79': 'Nouvelle-Aquitaine', '80': 'Hauts-de-France', '81': 'Occitanie',
    '82': 'Occitanie', '83': "Provence-Alpes-Côte d'Azur", '84': "Provence-Alpes-Côte d'Azur",
    '85': 'Pays de la Loire', '86': 'Nouvelle-Aquitaine', '87': 'Nouvelle-Aquitaine',
    '88': 'Grand Est', '89': 'Bourgogne-Franche-Comté', '90': 'Bourgogne-Franche-Comté',
    '91': 'Île-de-France', '92': 'Île-de-France', '93': 'Île-de-France',
    '94': 'Île-de-France', '95': 'Île-de-France',
    '971': 'Guadeloupe', '972': 'Martinique', '973': 'Guyane',
    '974': 'La Réunion', '976': 'Mayotte'
}

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
        .appName("ETL_A_Facilities") \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .config("hive.metastore.uris", metastore_uri) \
        .enableHiveSupport() \
        .getOrCreate()

def extract_dept_code(postal_code):
    """Extract département code from postal code"""
    if not postal_code or len(postal_code) < 2:
        return None

    # Handle Corse (2A, 2B)
    if postal_code.startswith('20'):
        if postal_code[:3] in ['200', '201']:
            return '2A'
        else:
            return '2B'

    # Handle DOM-TOM (3 digits)
    if postal_code.startswith('97') or postal_code.startswith('98'):
        return postal_code[:3]

    # Standard métropole (2 digits)
    return postal_code[:2]

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='ETL Script A: Load facilities table')
    parser.add_argument('--hive-host',
                        default=os.getenv('HIVE_METASTORE_HOST', 'metastore'),
                        help='Hive metastore hostname (default: metastore, or HIVE_METASTORE_HOST env var)')
    parser.add_argument('--warehouse-dir',
                        default=os.getenv('HIVE_WAREHOUSE_DIR', '/opt/hive/data/warehouse'),
                        help='Hive warehouse directory (default: /opt/hive/data/warehouse, or HIVE_WAREHOUSE_DIR env var)')
    args = parser.parse_args()

    print(f"[{datetime.now()}] Starting ETL process for facilities table...")

    # Initialize Spark
    spark = create_spark_session(hive_host=args.hive_host, warehouse_dir=args.warehouse_dir)

    # Step 1: Read etablissement_sante staging table
    print(f"[{datetime.now()}] Reading etablissement_sante_staging table...")
    etablissement_df = spark.sql("""
        SELECT DISTINCT
            identifiant_organisation,
            raison_sociale_site,
            code_postal
        FROM etablissement_sante_staging
        WHERE identifiant_organisation IS NOT NULL
            AND identifiant_organisation != ''
            AND raison_sociale_site IS NOT NULL
    """)

    print(f"[{datetime.now()}] Found {etablissement_df.count()} distinct facilities")

    # Step 2: Create département-région mapping DataFrame
    print(f"[{datetime.now()}] Creating département-région mapping...")
    mapping_data = [(dept, region) for dept, region in DEPT_REGION_MAPPING.items()]
    mapping_df = spark.createDataFrame(mapping_data, ["dept_code", "region"])

    # Step 3: Extract département code and join with region mapping
    print(f"[{datetime.now()}] Mapping départements to régions...")

    # Register UDF for département extraction
    from pyspark.sql.types import StringType
    from pyspark.sql.functions import udf

    extract_dept_udf = udf(extract_dept_code, StringType())

    facilities_with_dept = etablissement_df.withColumn(
        "dept_code",
        extract_dept_udf(col("code_postal"))
    )

    # Join with mapping
    facilities_final = facilities_with_dept.alias("f") \
        .join(mapping_df.alias("m"), col("f.dept_code") == col("m.dept_code"), "left") \
        .select(
            col("f.identifiant_organisation").alias("id"),
            col("f.raison_sociale_site").alias("name"),
            coalesce(col("m.region"), lit("Unknown")).alias("region")
        )

    # Step 4: Create facilities table if not exists
    print(f"[{datetime.now()}] Creating facilities table if not exists...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS facilities (
            id STRING,
            name STRING,
            region STRING
        ) STORED AS ORC
    """)

    # Step 5: Load data into facilities table
    print(f"[{datetime.now()}] Loading data into facilities table...")
    facilities_final.createOrReplaceTempView("facilities_temp")

    spark.sql("""
        INSERT OVERWRITE TABLE facilities
        SELECT * FROM facilities_temp
    """)

    # Step 6: Validation
    count = spark.sql("SELECT COUNT(*) as cnt FROM facilities").collect()[0]['cnt']
    print(f"[{datetime.now()}] Successfully loaded {count} facilities")

    # Show sample records
    print(f"[{datetime.now()}] Sample records:")
    spark.sql("SELECT * FROM facilities LIMIT 10").show(truncate=False)

    # Region distribution
    print(f"[{datetime.now()}] Region distribution:")
    spark.sql("""
        SELECT region, COUNT(*) as count
        FROM facilities
        GROUP BY region
        ORDER BY count DESC
    """).show(truncate=False)

    spark.stop()
    print(f"[{datetime.now()}] ETL process completed successfully!")

if __name__ == "__main__":
    main()
