# Employee ETL with HDFS Upload (PySpark)

This script performs ETL (Extract, Transform, Load) on employee data and uploads the result to HDFS using PySpark.

## Features

- ✅ Extracts data from 3 CSV files (employees, functions, salaries)
- ✅ Transforms and merges the datasets
- ✅ Saves to local CSV file
- ✅ Uploads to HDFS using PySpark

## Prerequisites

- Docker with `apache/spark:4.1.1-java21-python3` image
- Hadoop namenode container running on the `hadoop` network
- HDFS accessible at `namenode:8020`

**Note:** The script automatically creates the required HDFS directories (`/user/python-etl/employees/`). No manual HDFS preparation needed!

## Usage

### Run with Docker

From the project root directory:

```bash
docker run --rm --user root \
  --network hadoop \
  -v "$(pwd):/app" \
  -w /app \
  -e HADOOP_USER_NAME=root \
  -e PYSPARK_PYTHON=python3 \
  apache/spark:4.1.1-java21-python3 \
  bash -c "pip3 install --quiet pandas && /opt/spark/bin/spark-submit --master local[1] practice/data-integration/tp1-python-example/hdfs-pyspark/employee_etl.py"
```

### What It Does

1. **Extract**: Reads employee data from:
   - `data/donnees/employes.txt` (personal data)
   - `data/donnees/fonction_employes.txt` (job functions)
   - `data/donnees/salaire_employes.txt` (salary data)

2. **Transform**:
   - Cleans date formats
   - Standardizes column names
   - Merges all datasets on `employee_id`

3. **Load**:
   - Saves to local file: `data/output/integrated_employees.csv`
   - Uploads to HDFS: `hdfs://namenode:8020/user/python-etl/employees/integrated_employees/`

## Verify Results

### Check Local Output

```bash
cat data/output/integrated_employees.csv | head -5
```

### Check HDFS Output

```bash
# List files
docker exec namenode hdfs dfs -ls /user/python-etl/employees/integrated_employees/

# View data
docker exec namenode hdfs dfs -cat "/user/python-etl/employees/integrated_employees/part-*.csv" | head -5
```

## Configuration

The script uses the following HDFS configuration:

- **Namenode host**: `namenode` (container name)
- **HDFS port**: `8020` (not 9000)
- **Default FS**: `hdfs://namenode:8020`

### Important Notes

- The script must run inside a Docker container on the same network as the namenode
- PySpark requires Java 21 to avoid compatibility issues
- The container runs as `root` user to match HDFS permissions
- Pandas is installed at runtime in the container

## Troubleshooting

### Connection Refused Error

If you get `Connection refused` errors:
- Check namenode is running: `docker ps | grep namenode`
- Verify network: `docker network ls | grep hadoop`
- Ensure correct port (8020, not 9000)

### Module Not Found: pyspark

- Use `spark-submit` command, not plain `python3`
- Ensure using the Spark container image

### Permission Denied

- Run container with `--user root`
- Set `HADOOP_USER_NAME=root` environment variable

## Output Format

The data is saved in CSV format with the following columns:

- `employee_id`
- `nom` (last name)
- `prenom` (first name)
- `date_naissance` (birthdate)
- `année-embauche` (hire year)
- `Fonction` (job function)
- `Sexe` (gender)
- `Secteur` (sector)
- `Salaire` (salary)
- `Code_postal` (postal code)
