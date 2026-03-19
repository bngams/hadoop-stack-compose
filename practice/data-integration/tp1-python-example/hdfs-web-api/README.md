# Employee ETL with HDFS WebHDFS API

Python ETL pipeline demonstrating data integration using WebHDFS REST API.

## Setup

```bash
pip install -r requirements.txt
```

## Run

```bash
python3 employee_etl_hdfs.py
```

Integrates employee data from CSV files and uploads to HDFS at `/user/python-etl/employees/integrated_employees.csv`
