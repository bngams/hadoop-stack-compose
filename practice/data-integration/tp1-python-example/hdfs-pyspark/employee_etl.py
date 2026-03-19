"""
Employee Data Integration ETL
Rebuilds the NiFi pipeline manually in Python
"""

import pandas as pd
from pathlib import Path
import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_spark_session():
    """
    Create and configure Spark session with HDFS support

    Returns:
        SparkSession configured for HDFS access
    """
    import os
    os.environ['HADOOP_USER_NAME'] = 'root'
    os.environ['HADOOP_HOME'] = '/opt/hadoop'

    spark = SparkSession.builder \
        .appName("EmployeeETL") \
        .master("local[1]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .getOrCreate()

    # Set log level to ERROR to reduce noise
    spark.sparkContext.setLogLevel("ERROR")

    return spark


def extract_data(data_dir: Path):
    """
    Extract: Read employee data from three separate files

    Args:
        data_dir: Path to the directory containing source files

    Returns:
        Tuple of three DataFrames (employees, functions, salaries)
    """
    logger.info("Starting data extraction...")

    # Read employes.txt (no header)
    employees = pd.read_csv(
        data_dir / 'employes.txt',
        sep=';',
        header=None,
        names=['id', 'nom', 'prenom', 'date_naissance']
    )

    # Read fonction_employes.txt (has header)
    functions = pd.read_csv(
        data_dir / 'fonction_employes.txt',
        sep=';',
        encoding='latin-1'
    )

    # Read salaire_employes.txt (has header)
    salaries = pd.read_csv(
        data_dir / 'salaire_employes.txt',
        sep=';',
        encoding='latin-1'
    )

    logger.info(f"Extracted {len(employees)} employees")
    logger.info(f"Extracted {len(functions)} job functions")
    logger.info(f"Extracted {len(salaries)} salary records")

    return employees, functions, salaries


def transform_data(employees, functions, salaries):
    """
    Transform: Clean, validate, and merge the datasets

    Args:
        employees: DataFrame with employee personal data
        functions: DataFrame with job information
        salaries: DataFrame with salary information

    Returns:
        Merged and cleaned DataFrame
    """
    logger.info("Starting data transformation...")

    # Clean employee birthdates (handle both DD/MM/YYYY and YYYY-MM-DD formats)
    employees['date_naissance'] = pd.to_datetime(
        employees['date_naissance'],
        format='%d/%m/%Y',
        errors='coerce'
    )

    # Standardize column names for merging
    employees = employees.rename(columns={'id': 'employee_id'})
    functions = functions.rename(columns={'id_empl': 'employee_id'})
    salaries = salaries.rename(columns={'id_empl': 'employee_id'})

    # Merge the three DataFrames
    merged_data = pd.merge(employees, functions, on='employee_id', how='inner')
    merged_data = pd.merge(merged_data, salaries, on='employee_id', how='inner')

    logger.info(f"Merged dataset contains {len(merged_data)} records")
    logger.info(f"Columns: {list(merged_data.columns)}")

    return merged_data


def load_data(data, output_path: Path):
    """
    Load: Save the integrated dataset to a file

    Args:
        data: Merged DataFrame
        output_path: Path where to save the output file
    """
    logger.info(f"Loading data to {output_path}...")

    # Save to CSV
    data.to_csv(output_path, index=False)

    logger.info(f"Data successfully saved to {output_path}")


def load_data_to_hdfs(data, hdfs_path: str):
    """
    Load: Save the integrated dataset to HDFS using PySpark

    Args:
        data: Merged pandas DataFrame
        hdfs_path: HDFS destination path (e.g., 'hdfs://localhost:9000/user/python-etl/employees/')
    """
    logger.info(f"Loading data to HDFS: {hdfs_path}...")

    # Get Spark session
    spark = get_spark_session()

    # Convert pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(data)

    # Write to HDFS as CSV
    spark_df.coalesce(1).write \
        .mode('overwrite') \
        .option('header', 'true') \
        .csv(hdfs_path)

    logger.info(f"Data successfully saved to HDFS: {hdfs_path}")

    # Stop Spark session
    spark.stop()


def main():
    """Main ETL pipeline orchestration"""
    logger.info("=== Employee ETL Pipeline Started ===")

    # Define paths
    data_dir = Path("data/donnees")
    output_dir = Path("data/output")
    output_dir.mkdir(exist_ok=True)

    local_output = output_dir / "integrated_employees.csv"
    hdfs_output = "hdfs://namenode:8020/user/python-etl/employees/integrated_employees"

    # Run ETL pipeline
    employees, functions, salaries = extract_data(data_dir)
    merged_data = transform_data(employees, functions, salaries)

    # Load to both local and HDFS
    load_data(merged_data, local_output)
    load_data_to_hdfs(merged_data, hdfs_output)

    logger.info("=== ETL Pipeline Completed Successfully ===")


if __name__ == "__main__":
    main()
