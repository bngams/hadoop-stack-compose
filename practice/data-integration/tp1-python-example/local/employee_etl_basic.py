"""
Employee Data Integration ETL (Basic Version)
Rebuilds the NiFi pipeline manually in Python - Local output only
"""

import pandas as pd
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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


def main():
    """Main ETL pipeline orchestration"""
    logger.info("=== Employee ETL Pipeline Started ===")

    # Define paths (relative to project root, 3 levels up from local/)
    base_dir = Path(__file__).parent.parent.parent.parent
    data_dir = base_dir / "data/donnees"
    output_dir = base_dir / "data/output"
    output_dir.mkdir(exist_ok=True)

    output_file = output_dir / "integrated_employees.csv"

    # Run ETL pipeline
    employees, functions, salaries = extract_data(data_dir)
    merged_data = transform_data(employees, functions, salaries)
    load_data(merged_data, output_file)

    logger.info("=== ETL Pipeline Completed Successfully ===")


if __name__ == "__main__":
    main()
