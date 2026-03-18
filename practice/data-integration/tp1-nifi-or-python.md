# TP1-bis - NiFi vs Python: Manual ETL Implementation

## Objectives

- Understand the low-level mechanics of ETL pipelines by coding them manually
- Compare Python's programmatic approach with NiFi's visual flow
- Learn when to use Python scripts vs industrial ETL tools
- Integrate Python ETL output with HDFS

**Duration:** 2-3 hours
**Difficulty:** Intermediate
**Prerequisites:** Python basics, completed [TP2 - NiFi Use Case](tp1-nifi-use-case.md)

## Important Context: Python vs NiFi

### ⚠️ Python Does NOT Replace NiFi

Both tools serve different purposes:

| Aspect | Python | NiFi |
|--------|--------|------|
| **Use Case** | Custom logic, prototyping, one-off scripts | Production ETL, real-time ingestion, monitoring |
| **Control** | Full programmatic control | Configuration-based |
| **Monitoring** | Manual logging, external tools | Built-in data provenance, UI monitoring |
| **Scalability** | Requires custom distribution logic | Built-in clustering, load balancing |
| **Speed** | Fast for small-medium datasets | Optimized for high-throughput streams |
| **Learning Curve** | Lower (if you know Python) | Steeper (processor ecosystem) |

### When to Choose Each

**Use Python when:**
- 🧪 You need quick prototyping or exploratory analysis
- 🎯 Your transformation logic is highly custom or complex
- 📊 You're integrating with ML models or scientific libraries
- 🔬 You need low-level control over every step

**Use NiFi when:**
- ⚡ You need real-time, continuous data ingestion
- 📂 You're processing file-based workflows at scale
- 👀 You want visual monitoring and data lineage tracking
- 🔌 You need plug-and-play connectors (Kafka, HDFS, databases, APIs)
- 👥 Non-developers need to maintain the pipeline

### Real-World Practice

**Many teams use both:**
- Python for data science notebooks and custom transformations
- NiFi for production data pipelines and orchestration
- Airflow to schedule and coordinate both

---

## Step 1 — Manual ETL with Python

We'll rebuild the employee data integration from [TP2](tp1-nifi-use-case.md) using pure Python.

### Environment Setup

#### Option 1: Using `uv` (Recommended)

[uv](https://github.com/astral-sh/uv) is a fast Python package installer.

```bash
# Install uv (macOS/Linux)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create a virtual environment
uv venv

# Activate it
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate   # Windows

# Install dependencies
uv pip install pandas
```

#### Option 2: Using Standard `pip`

```bash
# Create virtual environment
python3 -m venv .venv

# Activate it
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate   # Windows

# Install pandas
pip install pandas
```

**Resources:**
- [Python Installation Guide](https://www.python.org/downloads/)
- [uv Documentation](https://github.com/astral-sh/uv)
- [pip User Guide](https://pip.pypa.io/en/stable/user_guide/)

### Create the ETL Script

Create a new file: `practice/data-integration/employee_etl.py`

```python
"""
Employee Data Integration ETL
Rebuilds the NiFi pipeline manually in Python
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

    # TODO: Read employes.txt
    # Hint: Use pd.read_csv() with sep=';', header=None
    # Columns: ['id', 'nom', 'prenom', 'date_naissance']
    employees = None  # Replace with your code

    # TODO: Read fonction_employes.txt
    # Hint: This file HAS a header row
    # Columns: ['id_empl', 'année-embauche', 'Fonction']
    functions = None  # Replace with your code

    # TODO: Read salaire_employes.txt
    # Hint: This file also has a header
    # Columns: ['Sexe', 'Secteur', 'Salaire', 'Code_postal', 'id_empl']
    salaries = None  # Replace with your code

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

    # TODO: Clean employee birthdates
    # Hint: Some dates are in DD/MM/YYYY, some in YYYY-MM-DD
    # Use pd.to_datetime() with format parameter or errors='coerce'
    # employees['date_naissance'] = ...

    # TODO: Standardize column names for merging
    # Hint: Rename 'id' to 'employee_id' in employees
    # Rename 'id_empl' to 'employee_id' in functions and salaries

    # TODO: Merge the three DataFrames
    # Hint: Use pd.merge() twice
    # First merge employees + functions on 'employee_id'
    # Then merge result + salaries on 'employee_id'
    merged_data = None  # Replace with your code

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

    # TODO: Save to CSV
    # Hint: Use data.to_csv() with index=False

    logger.info(f"Data successfully saved to {output_path}")


def main():
    """Main ETL pipeline orchestration"""
    logger.info("=== Employee ETL Pipeline Started ===")

    # Define paths
    data_dir = Path("data/donnees")
    output_dir = Path("data/output")
    output_dir.mkdir(exist_ok=True)

    output_file = output_dir / "integrated_employees.csv"

    # Run ETL pipeline
    employees, functions, salaries = extract_data(data_dir)
    merged_data = transform_data(employees, functions, salaries)
    load_data(merged_data, output_file)

    logger.info("=== ETL Pipeline Completed Successfully ===")


if __name__ == "__main__":
    main()
```

### Your Task: Complete the TODOs

Fill in the missing code sections marked with `# TODO:` comments.

**Hints:**

1. **Reading files with pandas:**
   ```python
   df = pd.read_csv('file.txt', sep=';', header=None, names=['col1', 'col2'])
   ```

2. **Handling dates:**
   ```python
   df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y', errors='coerce')
   ```

3. **Merging DataFrames:**
   ```python
   merged = pd.merge(df1, df2, on='common_column', how='inner')
   ```

### Run the Script

```bash
# Make sure you're in the project root directory
cd /path/to/hadoop-stack-compose

# Activate virtual environment
source .venv/bin/activate

# Run the ETL
python practice/data-integration/employee_etl.py
```

**Expected Output:**
```
2026-03-18 10:30:00 - INFO - === Employee ETL Pipeline Started ===
2026-03-18 10:30:00 - INFO - Starting data extraction...
2026-03-18 10:30:00 - INFO - Extracted 100 employees
2026-03-18 10:30:00 - INFO - Extracted 100 job functions
2026-03-18 10:30:00 - INFO - Extracted 100 salary records
2026-03-18 10:30:01 - INFO - Starting data transformation...
2026-03-18 10:30:01 - INFO - Merged dataset contains 100 records
2026-03-18 10:30:01 - INFO - Loading data to data/output/integrated_employees.csv...
2026-03-18 10:30:01 - INFO - === ETL Pipeline Completed Successfully ===
```

### Verify the Output

```bash
# Check the file was created
ls -lh data/output/integrated_employees.csv

# View first few lines
head -20 data/output/integrated_employees.csv
```

---

## Step 2 — Push Output to HDFS

Now let's integrate our Python ETL with Hadoop's distributed file system.

### Why HDFS?

- **Scalability**: Store petabytes of data across clusters
- **Integration**: Works with Spark, Hive, and other big data tools
- **Durability**: Data is replicated across nodes
- **Industry Standard**: Used in production data lakes

### Start HDFS Services

```bash
# Start the Hadoop stack
docker-compose up -d namenode datanode

# Verify services are running
docker-compose ps namenode datanode
```

### Create HDFS Output Directory

```bash
# Create directory in HDFS
docker exec -it namenode hdfs dfs -mkdir -p /user/python-etl/employees

# Set permissions
docker exec -it namenode hdfs dfs -chmod 755 /user/python-etl/employees

# Verify
docker exec -it namenode hdfs dfs -ls /user/python-etl/
```

### Update the ETL Script for HDFS

Add HDFS support to `employee_etl.py`:

```python
import subprocess

def load_to_hdfs(data, hdfs_path: str):
    """
    Load data to HDFS using Hadoop CLI

    Args:
        data: DataFrame to save
        hdfs_path: HDFS destination path (e.g., '/user/python-etl/employees/output.csv')
    """
    logger.info(f"Loading data to HDFS: {hdfs_path}...")

    # Save to temporary local file
    temp_file = Path("/tmp/integrated_employees.csv")
    data.to_csv(temp_file, index=False)

    # TODO: Copy file into namenode container
    # Hint: Use subprocess.run() with docker cp command
    # subprocess.run(['docker', 'cp', str(temp_file), 'namenode:/tmp/'])

    # TODO: Put file into HDFS
    # Hint: Use docker exec to run hdfs dfs -put command
    # subprocess.run(['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-put', ...])

    # Clean up temp file
    temp_file.unlink()

    logger.info(f"Data successfully uploaded to HDFS: {hdfs_path}")


def main():
    """Main ETL pipeline orchestration"""
    logger.info("=== Employee ETL Pipeline Started ===")

    # Define paths
    data_dir = Path("data/donnees")
    output_dir = Path("data/output")
    output_dir.mkdir(exist_ok=True)

    local_output = output_dir / "integrated_employees.csv"
    hdfs_output = "/user/python-etl/employees/integrated_employees.csv"

    # Run ETL pipeline
    employees, functions, salaries = extract_data(data_dir)
    merged_data = transform_data(employees, functions, salaries)

    # Load to both local and HDFS
    load_data(merged_data, local_output)
    load_to_hdfs(merged_data, hdfs_output)

    logger.info("=== ETL Pipeline Completed Successfully ===")
```

### Verify HDFS Upload

```bash
# List files in HDFS
docker exec -it namenode hdfs dfs -ls /user/python-etl/employees/

# View content
docker exec -it namenode hdfs dfs -cat /user/python-etl/employees/integrated_employees.csv | head -20
```

---

## Reflection: Python vs NiFi

### What You Built in Python

- ✅ Simple, readable code
- ✅ Full control over transformation logic
- ✅ Easy to version control (Git)
- ✅ Works well for batch processing

### What NiFi Provides

- ✅ Visual monitoring of data flows
- ✅ Built-in error handling and retry logic
- ✅ Data provenance tracking (where did each record come from?)
- ✅ Real-time processing capabilities
- ✅ Back-pressure handling for high-volume streams
- ✅ No code deployment required (configuration-based)

### Key Differences

| Feature | Python Script | NiFi Flow |
|---------|--------------|-----------|
| **Development** | Write code | Drag-and-drop UI |
| **Debugging** | Print/log statements | Data provenance viewer |
| **Monitoring** | External tools (Prometheus, etc.) | Built-in UI |
| **Scheduling** | Cron or Airflow | Built-in scheduling |
| **Error Recovery** | Manual try/except | Built-in queues, retry |
| **Scalability** | Manual threading/multiprocessing | Built-in clustering |

---

## Key Takeaways

✅ **Python gives you low-level control** for custom logic and complex transformations
✅ **NiFi is an industrial tool** for production ETL with monitoring and reliability
✅ **Both have their place** in a modern data stack
✅ **Real teams use both**: Python for custom code, NiFi for data pipelines

---

## Next Steps

Continue to [TP1-ter - Orchestration with Airflow](tp1-nifi-python-orchestration-with-airflow.md) to learn how to schedule and coordinate both approaches.

---

## Additional Challenges

1. **Add Data Validation**:
   - Check for missing values in critical fields
   - Validate date ranges (birthdates should be before hire dates)
   - Write rejected records to a separate file

2. **Add Incremental Loading**:
   - Track which files you've already processed
   - Only process new or updated files
   - Append new records to HDFS instead of overwriting

3. **Add Performance Metrics**:
   - Time each ETL stage
   - Count records processed per second
   - Log memory usage

4. **Add Data Quality Report**:
   - Generate summary statistics
   - Count null values per column
   - Detect outliers in salary data

---

## Resources

- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Python Logging Guide](https://docs.python.org/3/howto/logging.html)
- [HDFS Commands Reference](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/FileSystemShell.html)
- [uv Package Manager](https://github.com/astral-sh/uv)
