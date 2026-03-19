"""
Employee Data Integration ETL with HDFS Support
Uses WebHDFS REST API for HDFS operations
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


def load_data_to_hdfs(data, hdfs_path: str, hdfs_host='localhost', hdfs_port=9870, user='root'):
    """
    Load: Save the integrated dataset to HDFS using WebHDFS REST API

    Args:
        data: Merged pandas DataFrame
        hdfs_path: HDFS destination path (e.g., '/user/python-etl/employees/integrated_employees.csv')
        hdfs_host: HDFS namenode host (default: localhost)
        hdfs_port: WebHDFS port (default: 9870 for Hadoop 3.x)
        user: HDFS user (default: root)
    """
    import requests

    logger.info(f"Loading data to HDFS via WebHDFS: http://{hdfs_host}:{hdfs_port}{hdfs_path}...")

    try:
        base_url = f"http://{hdfs_host}:{hdfs_port}/webhdfs/v1"

        # Ensure parent directory exists
        parent_dir = '/'.join(hdfs_path.split('/')[:-1])
        if parent_dir:
            logger.info(f"Creating directory: {parent_dir}")
            mkdir_url = f"{base_url}{parent_dir}?op=MKDIRS&user.name={user}"
            requests.put(mkdir_url)

        # Convert DataFrame to CSV bytes
        csv_content = data.to_csv(index=False).encode('utf-8')

        # Step 1: Get redirect location from namenode
        create_url = f"{base_url}{hdfs_path}?op=CREATE&user.name={user}&overwrite=true&noredirect=true"
        response = requests.put(create_url, allow_redirects=False)

        # Handle both 307 redirect (header) and 200 OK (JSON body) responses
        datanode_url = None
        if response.status_code == 307:
            datanode_url = response.headers['Location']
        elif response.status_code == 200:
            datanode_url = response.json()['Location']
        else:
            raise Exception(f"Unexpected response: {response.status_code} - {response.text}")

        # Replace internal docker hostnames with localhost mapped ports
        datanode_url = datanode_url.replace('datanode1:9864', 'localhost:9864')
        datanode_url = datanode_url.replace('datanode2:9864', 'localhost:9865')
        datanode_url = datanode_url.replace('datanode3:9866', 'localhost:9866')

        logger.info(f"Writing to datanode: {datanode_url}")

        # Step 2: Write data to datanode
        response = requests.put(datanode_url, data=csv_content,
                               headers={'Content-Type': 'application/octet-stream'})
        response.raise_for_status()

        logger.info(f"Data successfully saved to HDFS: {hdfs_path}")

    except Exception as e:
        logger.error(f"Failed to write to HDFS: {str(e)}")
        logger.warning("Make sure HDFS is running and WebHDFS is enabled")
        logger.warning("You may need to add datanode hostnames to /etc/hosts")
        raise


def main():
    """Main ETL pipeline orchestration"""
    logger.info("=== Employee ETL Pipeline Started ===")

    # Define paths relative to script location
    # Script is in practice/data-integration/tp1-python-example/
    # We need to go up 3 levels to reach the root
    root_dir = Path(__file__).resolve().parent.parent.parent.parent
    data_dir = root_dir / "data" / "donnees"
    output_dir = root_dir / "data" / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    local_output = output_dir / "integrated_employees.csv"
    hdfs_output = "/user/python-etl/employees/integrated_employees.csv"

    # Run ETL pipeline
    employees, functions, salaries = extract_data(data_dir)
    merged_data = transform_data(employees, functions, salaries)

    # Load to both local and HDFS
    load_data(merged_data, local_output)
    load_data_to_hdfs(merged_data, hdfs_output)

    logger.info("=== ETL Pipeline Completed Successfully ===")


if __name__ == "__main__":
    main()
