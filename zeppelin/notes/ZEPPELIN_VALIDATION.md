# Zeppelin + Spark Validation Guide

This guide provides step-by-step tests to validate your Zeppelin and Spark setup.

## Prerequisites

Start the custom Zeppelin stack:
```bash
docker-compose --profile zeppelin-custom up -d
```

Access Zeppelin at: **http://localhost:8080**

---

## Step 1: Basic Scala Spark Test

Create a new notebook and test basic Spark functionality.

### Test 1.1: Print Hello from Spark
```scala
%spark
println("Hello from Spark!")
```

**Expected Output:** `Hello from Spark!`

### Test 1.2: Check Spark Version
```scala
%spark
println(s"Spark version: ${spark.version}")
println(s"Scala version: ${scala.util.Properties.versionString}")
```

**Expected Output:**
- Spark version: 3.5.0
- Scala version: version 2.12.x

### Test 1.3: Create Simple DataFrame
```scala
%spark
val data = Seq(
  ("Alice", 25),
  ("Bob", 30),
  ("Charlie", 35)
)

val df = spark.createDataFrame(data).toDF("name", "age")
df.show()
```

**Expected Output:** A table showing 3 rows with names and ages

### Test 1.4: Basic DataFrame Operations
```scala
%spark
val df = spark.createDataFrame(Seq(
  ("Alice", 25, "USA"),
  ("Bob", 30, "Canada"),
  ("Charlie", 35, "USA")
)).toDF("name", "age", "country")

df.printSchema()
df.filter($"age" > 25).show()
df.groupBy("country").count().show()
```

**Expected Output:** Schema, filtered results, and country counts

---

## Step 2: PySpark Tests

### Test 2.1: Basic Python

```
%spark.conf

# Reduce executor memory to fit available resources
spark.executor.memory 1g
spark.driver.memory 1g
```


```python
%pyspark
print("Hello from PySpark!")
print(f"Python version: {sys.version}")
```

**Expected Output:** Hello message and Python version (3.8.x)



### Test 2.2: Create PySpark DataFrame
```python
%pyspark
data = [
    (1, "andy", 20, "USA"),
    (2, "jeff", 23, "China"),
    (3, "james", 18, "USA")
]

df = spark.createDataFrame(data).toDF("id", "name", "age", "country")
df.printSchema()
df.show()
```

**Expected Output:** Schema and data table

### Test 2.3: PySpark DataFrame Operations
```python
%pyspark
df = spark.createDataFrame([
    (1, "andy", 20, "USA"),
    (2, "jeff", 23, "China"),
    (3, "james", 18, "USA")
]).toDF("id", "name", "age", "country")

# Filter
print("Adults (age >= 20):")
df.filter(df.age >= 20).show()

# Group by
print("Count by country:")
df.groupBy("country").count().show()

# Add column
from pyspark.sql.functions import col
df.withColumn("age_plus_10", col("age") + 10).show()
```

**Expected Output:** Filtered data, grouped counts, and new column

### Test 2.4: Pandas Integration
```python
%pyspark
import pandas as pd

# Create PySpark DataFrame
spark_df = spark.createDataFrame([
    (1, "andy", 20),
    (2, "jeff", 23),
    (3, "james", 18)
]).toDF("id", "name", "age")

# Convert to Pandas
pandas_df = spark_df.toPandas()
print(type(pandas_df))
print(pandas_df)

# Convert back to Spark
spark_df2 = spark.createDataFrame(pandas_df)
spark_df2.show()
```

**Expected Output:** Pandas DataFrame type and conversions

---

## Step 3: Read Data from CSV Files

### Test 3.1: Read CSV File
```python
%pyspark
# List available files
import os
files = os.listdir('/opt/zeppelin/csv')
print("Available files:", files)
```

**Expected Output:** List of CSV files in the directory

### Test 3.2: Read and Process CSV
```python
%pyspark
# Read one of the CSV files (adjust filename based on what you have)
df = spark.read.option("header", "false").option("delimiter", "\t").csv("/opt/zeppelin/csv/employes.txt")
df.show(10)
df.printSchema()
```

**Expected Output:** CSV data displayed

### Test 3.3: Read CSV with Schema
```python
%pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define schema (adjust based on your file structure)
schema = StructType([
    StructField("col1", StringType(), True),
    StructField("col2", StringType(), True)
])

df = spark.read.option("delimiter", "\t").csv("/opt/zeppelin/csv/employes.txt", schema=schema)
df.show()
```

**Expected Output:** CSV data with proper schema

---

## Step 4: HDFS Integration

### Test 4.1: Check HDFS Connection
```scala
%spark
val conf = spark.sparkContext.hadoopConfiguration
val fs = org.apache.hadoop.fs.FileSystem.get(conf)
println(s"HDFS URI: ${fs.getUri}")
```

**Expected Output:** HDFS URI (hdfs://namenode:8020)

### Test 4.2: List HDFS Files (if any)
```python
%pyspark
# Try to list HDFS root
try:
    df = spark.read.text("hdfs://namenode:8020/")
    df.show()
except Exception as e:
    print(f"HDFS listing error (expected if empty): {e}")
```

**Expected Output:** HDFS file listing or error if empty

### Test 4.3: Write to HDFS
```python
%pyspark
# Create sample data and write to HDFS
df = spark.createDataFrame([
    (1, "test1"),
    (2, "test2"),
    (3, "test3")
]).toDF("id", "value")

# Write to HDFS
df.write.mode("overwrite").csv("hdfs://namenode:8020/user/test_data.csv")
print("Data written to HDFS")

# Read it back
df_read = spark.read.csv("hdfs://namenode:8020/user/test_data.csv")
df_read.show()
```

**Expected Output:** Data written and read from HDFS

---

## Step 5: SparkSQL Tests

### Test 5.1: SQL Queries
```sql
%sql
CREATE OR REPLACE TEMP VIEW people AS
SELECT * FROM VALUES
  (1, 'Alice', 25),
  (2, 'Bob', 30),
  (3, 'Charlie', 35)
AS people(id, name, age)
```

```sql
%sql
SELECT * FROM people WHERE age > 25
```

**Expected Output:** Filtered results

### Test 5.2: Complex SQL
```sql
%sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  (1, 'Product A', 100),
  (2, 'Product B', 200),
  (3, 'Product A', 150),
  (4, 'Product B', 250)
AS sales(id, product, amount);

SELECT product, SUM(amount) as total_sales, AVG(amount) as avg_sales
FROM sales
GROUP BY product
ORDER BY total_sales DESC
```

**Expected Output:** Aggregated sales data

---

## Step 6: Advanced PySpark Features

### Test 6.1: Window Functions
```python
%pyspark
from pyspark.sql import Window
from pyspark.sql.functions import row_number, rank, dense_rank

data = [
    ("Alice", "Sales", 5000),
    ("Bob", "Sales", 6000),
    ("Charlie", "IT", 7000),
    ("David", "IT", 7000),
    ("Eve", "Sales", 5500)
]

df = spark.createDataFrame(data).toDF("name", "department", "salary")

windowSpec = Window.partitionBy("department").orderBy(df.salary.desc())

df.withColumn("rank", rank().over(windowSpec)) \
  .withColumn("dense_rank", dense_rank().over(windowSpec)) \
  .show()
```

**Expected Output:** Ranked salaries by department

### Test 6.2: UDF (User Defined Functions)
```python
%pyspark
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# Define UDF
def square(x):
    return x * x

square_udf = udf(square, IntegerType())

df = spark.createDataFrame([(1,), (2,), (3,), (4,), (5,)]).toDF("number")
df.withColumn("squared", square_udf("number")).show()
```

**Expected Output:** Numbers with their squares

---

## Step 7: Spark Master Integration

### Test 7.1: Check Spark Master Connection
```scala
%spark
println(s"Spark Master: ${spark.sparkContext.master}")
println(s"Application ID: ${spark.sparkContext.applicationId}")
println(s"Application Name: ${spark.sparkContext.appName}")
```

**Expected Output:**
- Spark Master: spark://spark-master:7077
- Application ID and Name

### Test 7.2: Check Executors
```scala
%spark
val executors = spark.sparkContext.getExecutorMemoryStatus
executors.foreach { case (id, (maxMem, remainingMem)) =>
  println(s"Executor $id: Max Memory = ${maxMem / (1024*1024)} MB, Remaining = ${remainingMem / (1024*1024)} MB")
}
```

**Expected Output:** Executor memory information

---

## Step 8: HDFS Integration

### Test 8.1: Check HDFS Connection

First method: 

```python
%pyspark
# List HDFS directories
hadoop = spark._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

print("=== HDFS Root Directory ===")
status = fs.listStatus(hadoop.fs.Path("/"))
for file_status in status:
    print(file_status.getPath().toString())
```

Second method: 

```python
%pyspark
# Read the CSV file from HDFS
df = spark.read.option("header", "true").csv("hdfs://namenode:8020/user/python-etl/employees/integrated_employees")
df.show(10)
```

---

## Troubleshooting

### Check Logs
```bash
# Zeppelin logs
docker logs zeppelin-custom

# Spark Master logs
docker logs spark-master

# Spark Worker logs
docker logs spark-worker
```

### Access Spark UI
- **Spark Master UI:** http://localhost:8082
- **Spark Worker UI:** http://localhost:8083
- **Zeppelin UI:** http://localhost:8080

### Common Issues

1. **Python not found:** Ensure `PYSPARK_PYTHON=python3` is set
2. **HDFS connection failed:** Check namenode is running: `docker ps | grep namenode`
3. **No executors:** Check spark-worker is running: `docker ps | grep spark-worker`
4. **Out of memory:** Adjust executor memory in `%spark.conf` cell

---

## Summary

✅ **Step 1:** Basic Spark (Scala) - DataFrame creation and operations
✅ **Step 2:** PySpark - Python integration and Pandas conversion
✅ **Step 3:** CSV Files - Reading local files
✅ **Step 4:** HDFS - Distributed file system integration
✅ **Step 5:** SparkSQL - SQL queries on DataFrames
✅ **Step 6:** Advanced Features - Window functions and UDFs
✅ **Step 7:** Cluster Mode - Spark Master/Worker integration

If all tests pass, your Zeppelin + Spark setup is fully operational! 🎉
