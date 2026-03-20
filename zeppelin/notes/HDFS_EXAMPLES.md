# HDFS Access Examples in PySpark

## ❌ This WON'T work for listing directories:

```python
%pyspark
# This will FAIL - can't read a directory as text
try:
    df = spark.read.text("hdfs://namenode:8020/")
    df.show()
except Exception as e:
    print(f"Error: {e}")
```

**Why it fails:** `spark.read.text()` expects a file, not a directory. You'll get an error like:
```
Path hdfs://namenode:8020/ is not a file
```

---

## ✅ CORRECT Ways to List HDFS Directories in PySpark

### Method 1: Using Hadoop FileSystem API (Recommended)

```python
%pyspark
# Access Hadoop FileSystem API
hadoop = spark._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

# List root directory
print("=== HDFS Root Directory ===")
path = hadoop.fs.Path("/")
status = fs.listStatus(path)

for file_status in status:
    name = file_status.getPath().getName()
    full_path = file_status.getPath().toString()
    is_dir = "DIR " if file_status.isDirectory() else "FILE"
    size = file_status.getLen()
    print(f"{is_dir:6} {name:20} {size:>10} bytes  ->  {full_path}")
```

**Output:**
```
DIR    opt                        0 bytes  ->  hdfs://namenode:8020/opt
DIR    tmp                        0 bytes  ->  hdfs://namenode:8020/tmp
DIR    user                       0 bytes  ->  hdfs://namenode:8020/user
```

---

### Method 2: Recursive Directory Listing

```python
%pyspark
def list_hdfs_directory(path_str, recursive=False):
    """List HDFS directory contents"""
    hadoop = spark._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = hadoop.fs.Path(path_str)

    print(f"\n=== Listing: {path_str} ===")

    if not fs.exists(path):
        print(f"Path does not exist: {path_str}")
        return

    if recursive:
        status = fs.listFiles(path, True)
        while status.hasNext():
            file_status = status.next()
            name = file_status.getPath().toString()
            size = file_status.getLen()
            print(f"FILE: {name} ({size} bytes)")
    else:
        status = fs.listStatus(path)
        for file_status in status:
            name = file_status.getPath().toString()
            is_dir = "DIR " if file_status.isDirectory() else "FILE"
            size = file_status.getLen()
            print(f"{is_dir}: {name} ({size} bytes)")

# Usage
list_hdfs_directory("/", recursive=False)
list_hdfs_directory("/user", recursive=False)
list_hdfs_directory("/user/python-etl/employees", recursive=True)
```

---

### Method 3: Check if Path Exists

```python
%pyspark
def hdfs_path_exists(path_str):
    """Check if HDFS path exists"""
    hadoop = spark._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = hadoop.fs.Path(path_str)
    return fs.exists(path)

# Check various paths
paths_to_check = [
    "/",
    "/user",
    "/user/python-etl",
    "/user/python-etl/employees/integrated_employees",
    "/nonexistent"
]

for path in paths_to_check:
    exists = hdfs_path_exists(path)
    status = "✅ EXISTS" if exists else "❌ NOT FOUND"
    print(f"{status}: {path}")
```

---

## ✅ Reading Actual Files from HDFS

Once you know the file paths, you can read them:

### Read CSV from HDFS

```python
%pyspark
# Read CSV file from HDFS
df = spark.read.option("header", "true").csv("hdfs://namenode:8020/user/python-etl/employees/integrated_employees")

print("=== Schema ===")
df.printSchema()

print("\n=== Data Sample ===")
df.show(10)

print(f"\n=== Total Rows: {df.count()} ===")
```

### Read Text File from HDFS

```python
%pyspark
# Read text file (this works for actual files, not directories)
df = spark.read.text("hdfs://namenode:8020/user/python-etl/employees/integrated_employees/part-00000-*.csv")

print("=== First 5 lines ===")
df.show(5, truncate=False)
```

### Read with Wildcard

```python
%pyspark
# Read all CSV files in a directory
df = spark.read.option("header", "true").csv("hdfs://namenode:8020/user/python-etl/employees/integrated_employees/*.csv")

print(f"Total rows from all files: {df.count()}")
df.show(10)
```

---

## Comparison: HDFS CLI vs PySpark

### HDFS Command (in terminal):
```bash
docker exec namenode hdfs dfs -ls /
docker exec namenode hdfs dfs -ls -R /user
docker exec namenode hdfs dfs -cat /user/python-etl/employees/integrated_employees/part-00000-*.csv | head -10
```

### PySpark Equivalent:

```python
%pyspark
# List directory
hadoop = spark._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
status = fs.listStatus(hadoop.fs.Path("/"))
for s in status:
    print(s.getPath().toString())

# Read file contents
df = spark.read.text("hdfs://namenode:8020/user/python-etl/employees/integrated_employees/part-00000-*.csv")
df.show(10, truncate=False)
```

---

## Summary

| Task | ❌ Won't Work | ✅ Use This Instead |
|------|---------------|---------------------|
| List directory | `spark.read.text("hdfs://path/")` | `fs.listStatus(hadoop.fs.Path("/"))` |
| Check if exists | Try/catch on read | `fs.exists(path)` |
| Read file | N/A | `spark.read.text("hdfs://path/file.txt")` |
| Read CSV | N/A | `spark.read.csv("hdfs://path/file.csv")` |

---

## Try This in Your Notebook

```python
%pyspark
# Complete example - list and read
hadoop = spark._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

# 1. List HDFS root
print("=== HDFS Root ===")
for file_status in fs.listStatus(hadoop.fs.Path("/")):
    print(file_status.getPath().toString())

# 2. List user directories
print("\n=== User Directories ===")
for file_status in fs.listStatus(hadoop.fs.Path("/user")):
    print(file_status.getPath().toString())

# 3. Read actual data
print("\n=== Reading Employee Data ===")
df = spark.read.option("header", "true").csv("hdfs://namenode:8020/user/python-etl/employees/integrated_employees")
df.show(5)
```
