# TP3 - EDA from HDFS Using Zeppelin & PySpark

## Objectives

- Read data directly from HDFS using PySpark
- Perform Exploratory Data Analysis on distributed data
- Use Zeppelin notebooks for interactive analysis
- Visualize data with Zeppelin's built-in charts

**Duration:** 30-40 minutes
**Difficulty:** Intermediate
**Prerequisites:** Data loaded in HDFS (from TP1 or TP2), basic Python/SQL knowledge

---

## Why Zeppelin for EDA?

**Apache Zeppelin** is a web-based notebook for interactive data analytics, similar to Jupyter but designed for big data.

### Advantages for HDFS Data

✅ **Native HDFS integration** - Read data directly from HDFS without copying
✅ **PySpark built-in** - No setup required, works out of the box
✅ **Interactive visualizations** - Built-in charts (bar, line, pie, scatter)
✅ **Multiple interpreters** - PySpark, SQL, Shell in one notebook
✅ **Big data ready** - Handles datasets too large for pandas

---

## Architecture

```
HDFS (/user/python-etl/employees/)
    ↓
Zeppelin Notebook (PySpark)
    ↓
Interactive Analysis & Visualizations
```

---

## Step 1 — Start Zeppelin

### Start the Stack

```bash
# Start Zeppelin with dependencies
docker-compose --profile zeppelin up -d

# Check Zeppelin is running
docker-compose ps zeppelin

# Wait for healthcheck (may take 1-2 minutes)
docker-compose logs -f zeppelin
```

### Access Zeppelin UI

Open your browser: **http://localhost:8080**

You should see the Zeppelin welcome page.

---

## Step 2 — Verify HDFS Data

Before starting, ensure your data is in HDFS:

```bash
# Check if data exists
docker exec -it namenode hdfs dfs -ls /user/python-etl/employees/

# Preview the data
docker exec -it namenode hdfs dfs -cat /user/python-etl/employees/integrated_employees.csv | head -20
```

**Expected output:**
```
Found 1 items
-rw-r--r--   3 root supergroup      12345 2026-03-19 /user/python-etl/employees/integrated_employees.csv
```

If you don't have data in HDFS yet, run the Python ETL from TP1 first:
```bash
cd practice/data-integration/tp1-python-example
...
```

---

## Step 3 — Create a New Notebook

1. In Zeppelin UI, click **"Create new note"**
2. **Note Name**: `Employee EDA from HDFS`
3. **Default Interpreter**: `spark` (PySpark)
4. Click **Create**

---

## Step 4 — Load Data from HDFS

In the first cell (paragraph), enter this code:

```python
%pyspark

# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, min, max, desc
import matplotlib.pyplot as plt
import seaborn as sns

# Initialize Spark session (if not already initialized)
spark = SparkSession.builder \
    .appName("Employee EDA") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

print("✅ Spark session initialized")
print(f"Spark version: {spark.version}")
```

**Click the ▶️ (Run) button** or press **Shift + Enter**

### Load CSV from HDFS

In the next cell:

```python
%pyspark

# Load data from HDFS
hdfs_path = "hdfs://namenode:8020/user/python-etl/employees/integrated_employees.csv"

# Read CSV with header inference
df = spark.read.csv(
    hdfs_path,
    header=True,
    inferSchema=True,
    sep=','
)

print(f"✅ Data loaded from HDFS: {df.count()} records")
print(f"Columns: {df.columns}")

# Show first few rows
df.show(5, truncate=False)
```

**Expected output:**
```
✅ Data loaded from HDFS: 32 records
Columns: ['employee_id', 'nom', 'prenom', 'date_naissance', 'année-embauche', 'Fonction', 'Sexe', 'Secteur', 'Salaire', 'Code_postal']

+----------+----------+-----------+---------------+---------------+-------------+----+--------+-------------+-----------+
|employee_id|nom       |prenom     |date_naissance |année-embauche |Fonction     |Sexe|Secteur |Salaire      |Code_postal|
+----------+----------+-----------+---------------+---------------+-------------+----+--------+-------------+-----------+
|1         |RAU       |PAULETTE...|03/03/1989     |2015           |aide-soignant|F   |sante   |20000euro/an |59000      |
...
```

---

## Step 5 — Data Inspection & Quality Check

```python
%pyspark

print("=" * 60)
print("DATA SCHEMA")
print("=" * 60)
df.printSchema()

print("\n" + "=" * 60)
print("DATA SUMMARY")
print("=" * 60)
print(f"Total records: {df.count()}")
print(f"Total columns: {len(df.columns)}")

# Check for null values
print("\n" + "=" * 60)
print("MISSING VALUES")
print("=" * 60)

from pyspark.sql.functions import col, sum as spark_sum

# Count nulls per column
null_counts = df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
])

null_counts.show()
```

---

## Step 6 — Data Cleaning & Transformation

Clean the salary column to extract numeric values:

```python
%pyspark

from pyspark.sql.functions import regexp_extract

# Extract numeric salary (remove "euro/an" text)
df_cleaned = df.withColumn(
    "Salaire_numeric",
    regexp_extract(col("Salaire"), r'(\d+)', 1).cast("int")
)

# Show the transformation
df_cleaned.select("nom", "Salaire", "Salaire_numeric").show(5)

print("✅ Salary column cleaned")
```

---

## Step 7 — Basic Statistics

```python
%pyspark

print("=" * 60)
print("💰 SALARY STATISTICS")
print("=" * 60)

# Compute salary statistics
salary_stats = df_cleaned.select(
    avg("Salaire_numeric").alias("avg_salary"),
    min("Salaire_numeric").alias("min_salary"),
    max("Salaire_numeric").alias("max_salary")
).collect()[0]

print(f"Average salary: {salary_stats.avg_salary:,.0f} €")
print(f"Min salary: {salary_stats.min_salary:,.0f} €")
print(f"Max salary: {salary_stats.max_salary:,.0f} €")

# More detailed summary
df_cleaned.select("Salaire_numeric").summary().show()

print("\n" + "=" * 60)
print("👥 GENDER DISTRIBUTION")
print("=" * 60)

df_cleaned.groupBy("Sexe").count().orderBy(desc("count")).show()

print("\n" + "=" * 60)
print("🏢 TOP 5 SECTORS")
print("=" * 60)

df_cleaned.groupBy("Secteur").count().orderBy(desc("count")).limit(5).show(truncate=False)

print("\n" + "=" * 60)
print("💼 TOP 5 JOB FUNCTIONS")
print("=" * 60)

df_cleaned.groupBy("Fonction").count().orderBy(desc("count")).limit(5).show(truncate=False)
```

---

## Step 8 — Visualizations with Zeppelin

### Using Zeppelin's Built-in Charts

Zeppelin can automatically visualize PySpark DataFrames!

```python
%pyspark

# Prepare data for visualization
print("Salary by Sector:")

# Calculate average salary by sector
salary_by_sector = df_cleaned.groupBy("Secteur") \
    .agg(
        avg("Salaire_numeric").alias("avg_salary"),
        count("*").alias("count")
    ) \
    .orderBy(desc("avg_salary"))

# Register as temp view for SQL queries
salary_by_sector.createOrReplaceTempView("salary_by_sector")

# Show the data
salary_by_sector.show(10, truncate=False)
```

**After running this cell:**
1. Click the **bar chart icon** (📊) above the output
2. In **Settings**, drag `Secteur` to **Keys** and `avg_salary` to **Values**
3. Zeppelin will render an interactive bar chart!

### Using SQL for Visualization

```sql
%sql

SELECT
    Secteur,
    AVG(Salaire_numeric) as avg_salary,
    COUNT(*) as employee_count
FROM employee_data
GROUP BY Secteur
HAVING COUNT(*) >= 2
ORDER BY avg_salary DESC
```

**To enable SQL queries**, first register the DataFrame:

```python
%pyspark

# Register DataFrame as SQL table
df_cleaned.createOrReplaceTempView("employee_data")

print("✅ DataFrame registered as SQL table 'employee_data'")
```

Now run the SQL cell above and use Zeppelin's chart options.

---

## Step 9 — Advanced Analysis with Matplotlib

For custom visualizations:

```python
%pyspark

import matplotlib.pyplot as plt
import seaborn as sns

# Collect data to driver (safe for small datasets)
salary_data = df_cleaned.select("Salaire_numeric").toPandas()

# Create histogram
plt.figure(figsize=(10, 6))
plt.hist(salary_data['Salaire_numeric'], bins=15, color='skyblue', edgecolor='black')
plt.xlabel('Salary (€/year)', fontsize=12)
plt.ylabel('Number of Employees', fontsize=12)
plt.title('Salary Distribution (from HDFS)', fontsize=14, fontweight='bold')
plt.axvline(salary_data['Salaire_numeric'].mean(), color='red', linestyle='--',
            label=f'Mean: {salary_data["Salaire_numeric"].mean():,.0f}€')
plt.legend()
plt.grid(axis='y', alpha=0.3)

# Display in Zeppelin
z.show(plt)
```

### Gender Distribution Pie Chart

```python
%pyspark

# Collect gender counts
gender_data = df_cleaned.groupBy("Sexe").count().toPandas()

# Create pie chart
plt.figure(figsize=(8, 8))
plt.pie(
    gender_data['count'],
    labels=['Female' if x == 'F' else 'Male' for x in gender_data['Sexe']],
    autopct='%1.1f%%',
    colors=['#ff9999', '#66b3ff'],
    startangle=90
)
plt.title('Gender Distribution (from HDFS)', fontsize=14, fontweight='bold')

z.show(plt)
```

---

## Step 10 — Key Question: Salary by Sector

```python
%pyspark

print("=" * 60)
print("KEY QUESTION: Do salaries vary by sector?")
print("=" * 60)

# Average salary by sector
salary_analysis = df_cleaned.groupBy("Secteur") \
    .agg(
        avg("Salaire_numeric").alias("avg_salary"),
        min("Salaire_numeric").alias("min_salary"),
        max("Salaire_numeric").alias("max_salary"),
        count("*").alias("employee_count")
    ) \
    .orderBy(desc("avg_salary"))

print("\nAverage Salary by Sector:")
salary_analysis.show(10, truncate=False)

# Visualize (sectors with 2+ employees)
sector_salary_plot = salary_analysis.filter(col("employee_count") >= 2).toPandas()

plt.figure(figsize=(12, 6))
plt.barh(sector_salary_plot['Secteur'], sector_salary_plot['avg_salary'], color='teal')
plt.xlabel('Average Salary (€/year)', fontsize=12, fontweight='bold')
plt.ylabel('Sector', fontsize=12, fontweight='bold')
plt.title('Average Salary by Sector (Sectors with 2+ Employees)', fontsize=14, fontweight='bold')
plt.gca().invert_yaxis()
plt.grid(axis='x', alpha=0.3)
plt.tight_layout()

z.show(plt)

# Key insights
highest = salary_analysis.first()
print(f"\n📊 KEY INSIGHT:")
print(f"Highest paying sector: {highest['Secteur']} ({highest['avg_salary']:,.0f} €)")
```

---

## Step 11 — Export Results Back to HDFS

Save your analysis results back to HDFS:

```python
%pyspark

# Save the salary analysis to HDFS
output_path = "hdfs://namenode:8020/user/python-etl/analysis_results/salary_by_sector"

salary_analysis.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print(f"✅ Analysis results saved to HDFS: {output_path}")
```

Verify the output:
```bash
docker exec -it namenode hdfs dfs -ls /user/python-etl/analysis_results/salary_by_sector/
docker exec -it namenode hdfs dfs -cat /user/python-etl/analysis_results/salary_by_sector/*.csv | head -20
```

---

## Step 12 — Summary Report

Create a final summary cell:

```python
%pyspark

# Collect summary statistics
total_employees = df_cleaned.count()
avg_salary = df_cleaned.select(avg("Salaire_numeric")).collect()[0][0]
median_salary_approx = df_cleaned.approxQuantile("Salaire_numeric", [0.5], 0.01)[0]
gender_dist = df_cleaned.groupBy("Sexe").count().collect()
total_sectors = df_cleaned.select("Secteur").distinct().count()

print("=" * 60)
print("EDA SUMMARY REPORT (FROM HDFS)")
print("=" * 60)

print(f"""
📋 DATASET SUMMARY:
   • Data source: HDFS (/user/python-etl/employees/)
   • Total employees: {total_employees}
   • Processing engine: PySpark {spark.version}

💰 SALARY INSIGHTS:
   • Average salary: {avg_salary:,.0f} €/year
   • Median salary (approx): {median_salary_approx:,.0f} €/year

👥 DEMOGRAPHICS:
   • Female: {[g['count'] for g in gender_dist if g['Sexe'] == 'F'][0]}
   • Male: {[g['count'] for g in gender_dist if g['Sexe'] == 'M'][0]}

🏢 SECTORS:
   • Total unique sectors: {total_sectors}

✅ ANALYSIS COMPLETE
   • Results exported to HDFS
   • Ready for downstream processing (Hive, Spark jobs, etc.)
""")

print("=" * 60)
```

---

## Comparison: Pandas vs PySpark EDA

| Aspect | Pandas (TP2) | PySpark (TP3) |
|--------|-------------|---------------|
| **Data Source** | Local files | HDFS (distributed) |
| **Data Size** | Small-medium (< 1GB) | Large (GB to TB) |
| **Processing** | Single machine | Distributed cluster |
| **Syntax** | `df.groupby()` | `df.groupBy()` |
| **Visualizations** | Matplotlib directly | Zeppelin charts + Matplotlib |
| **Use Case** | Quick local analysis | Production big data |
| **Lazy Evaluation** | No | Yes (optimized execution) |

---

## Key Takeaways

✅ **PySpark reads HDFS natively** - No need to download data locally
✅ **Zeppelin provides interactive notebooks** - Similar to Jupyter for big data
✅ **Built-in visualizations** - Quick charts without coding
✅ **Scalable analysis** - Same code works on 100 rows or 100 million rows
✅ **Results exportable** - Save back to HDFS for downstream jobs

---

## Next Steps

After completing this EDA from HDFS:
1. Use Hive to create SQL tables from your HDFS data
2. Build Spark jobs for automated analysis
3. Create dashboards with Hue or Superset
4. Integrate with Airflow for scheduled EDA reports

---

## Additional Challenges

1. **Partitioned Output**:
   - Save analysis results partitioned by sector
   - Use `.partitionBy("Secteur")` when writing

2. **Time Series Analysis**:
   - Parse `date_naissance` and calculate age distribution
   - Analyze hiring trends by year

3. **Join with New Data**:
   - Load another dataset from HDFS
   - Perform distributed joins with PySpark

4. **Create Hive Table**:
   - Register your DataFrame as a Hive table
   - Query with SQL from Hue or other tools

---

## Troubleshooting

**Issue**: Zeppelin won't start
- **Solution**: Check logs: `docker-compose logs zeppelin`
- Ensure dependencies are running: `docker-compose ps namenode resourcemanager`

**Issue**: "File not found" when reading from HDFS
- **Solution**: Verify HDFS path: `docker exec namenode hdfs dfs -ls /user/python-etl/employees/`
- Use full HDFS URI: `hdfs://namenode:8020/path/to/file`

**Issue**: Charts don't display in Zeppelin
- **Solution**: Use `z.show(plt)` for matplotlib plots
- For DataFrame visualization, register as temp table and use `%sql`

**Issue**: Out of memory errors
- **Solution**: Increase Spark driver memory in Zeppelin interpreter settings
- Or use `.limit(1000)` to analyze a sample

---

## Resources

- [Apache Zeppelin Documentation](https://zeppelin.apache.org/docs/latest/)
- [PySpark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [PySpark DataFrame Guide](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
- [Zeppelin Visualization Guide](https://zeppelin.apache.org/docs/latest/usage/display_system/basic.html)
