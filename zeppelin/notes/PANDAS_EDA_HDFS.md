# Pandas EDA from HDFS Files

This guide shows how to perform Exploratory Data Analysis (EDA) with pandas on CSV files stored in HDFS using Zeppelin.

## Available Files in HDFS

```bash
/user/nifi/satisfaction/ESATIS48H_MCO_recueil2017_donnees.csv (204KB)
/user/nifi/satisfaction/ete-ortho-ipaqss-2017-2018-donnees.csv (54KB)
```

---

## Step 1: Read CSV from HDFS into Spark DataFrame

**Why:** Load distributed data from HDFS for analysis
**Conclusion:** Verify file structure, delimiter (`;`), and row count are correct

```python
%pyspark
# Read the first satisfaction survey file
df_esatis = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://namenode:8020/user/nifi/satisfaction/ESATIS48H_MCO_recueil2017_donnees.csv")

print("=== ESATIS48H Dataset ===")
df_esatis.printSchema()
print(f"Total rows: {df_esatis.count()}")
df_esatis.show(5)
```

```python
%pyspark
# Read the second satisfaction survey file
df_ete_ortho = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://namenode:8020/user/nifi/satisfaction/ete-ortho-ipaqss-2017-2018-donnees.csv")

print("=== ETE-ORTHO Dataset ===")
df_ete_ortho.printSchema()
print(f"Total rows: {df_ete_ortho.count()}")
df_ete_ortho.show(5)
```

---

## Step 2: Convert Spark DataFrame to Pandas

**Why:** Use pandas for rich EDA capabilities (describe, corr, plotting)
**Conclusion:** Check dataset size - ensure it fits in memory before converting

```python
%pyspark
import pandas as pd

# Convert to pandas for detailed EDA
pdf_esatis = df_esatis.toPandas()
pdf_ete_ortho = df_ete_ortho.toPandas()

print(f"ESATIS48H shape: {pdf_esatis.shape}")
print(f"ETE-ORTHO shape: {pdf_ete_ortho.shape}")
```

---

## Step 3: Basic EDA - Dataset Overview

**Why:** Understand data structure, types, and initial quality
**Conclusion:** Identify column names, data types, and shape for next steps

### ESATIS48H Dataset

```python
%pyspark
import pandas as pd

# Display info
print("=== Dataset Info ===")
print(pdf_esatis.info())

print("\n=== First 10 rows ===")
print(pdf_esatis.head(10))

print("\n=== Column names ===")
print(pdf_esatis.columns.tolist())
```

```python
%pyspark
# Data types and missing values
print("=== Data Types ===")
print(pdf_esatis.dtypes)

print("\n=== Missing Values ===")
missing = pdf_esatis.isnull().sum()
print(missing[missing > 0])

print("\n=== Missing Values Percentage ===")
missing_pct = (pdf_esatis.isnull().sum() / len(pdf_esatis)) * 100
print(missing_pct[missing_pct > 0].sort_values(ascending=False))
```

**Conclusion:** Note which columns have high missing % - may need imputation or removal

### ETE-ORTHO Dataset

```python
%pyspark
# Display info
print("=== Dataset Info ===")
print(pdf_ete_ortho.info())

print("\n=== First 10 rows ===")
print(pdf_ete_ortho.head(10))

print("\n=== Column names ===")
print(pdf_ete_ortho.columns.tolist())
```

---

## Step 4: Descriptive Statistics

**Why:** Understand numeric distributions, ranges, and outliers
**Conclusion:** Check if values are in expected ranges; identify outliers needing investigation

### ESATIS48H - Numeric Columns

```python
%pyspark
# Convert numeric columns (currently strings due to CSV)
numeric_cols = ['nb_rep_score_all_rea_ajust', 'score_all_rea_ajust',
                'score_accueil_rea_ajust', 'score_PECinf_rea_ajust',
                'score_PECmed_rea_ajust', 'score_chambre_rea_ajust',
                'score_repas_rea_ajust', 'score_sortie_rea_ajust']

for col in numeric_cols:
    if col in pdf_esatis.columns:
        pdf_esatis[col] = pd.to_numeric(pdf_esatis[col], errors='coerce')

print("=== Descriptive Statistics ===")
print(pdf_esatis[numeric_cols].describe())
```

### ETE-ORTHO - Numeric Columns

```python
%pyspark
# Convert numeric columns
numeric_cols_ete = ['ete_ortho_cible_etbt', 'ete_ortho_obs_etbt',
                    'ete_ortho_att_etbt', 'ete_ortho_etbt']

for col in numeric_cols_ete:
    if col in pdf_ete_ortho.columns:
        pdf_ete_ortho[col] = pd.to_numeric(pdf_ete_ortho[col], errors='coerce')

print("=== Descriptive Statistics ===")
print(pdf_ete_ortho[numeric_cols_ete].describe())
```

---

## Step 5: Categorical Analysis

**Why:** Understand category distributions and imbalances
**Conclusion:** Note dominant categories and rare values for filtering or grouping

### ESATIS48H - Categorical Variables

```python
%pyspark
# Region distribution
print("=== Distribution by Region ===")
print(pdf_esatis['region'].value_counts())

print("\n=== Distribution by Classement ===")
print(pdf_esatis['classement'].value_counts())

print("\n=== Distribution by Evolution ===")
print(pdf_esatis['evolution'].value_counts())

print("\n=== Distribution by Participation ===")
print(pdf_esatis['participation'].value_counts())
```

### ETE-ORTHO - Categorical Variables

```python
%pyspark
# Region distribution
print("=== Distribution by Region ===")
print(pdf_ete_ortho['region'].value_counts())

print("\n=== Distribution by Position Seuil ===")
print(pdf_ete_ortho['ete_ortho_pos_seuil_etbt'].value_counts())
```

---

## Step 6: Data Quality Analysis

**Why:** Detect duplicates and data integrity issues
**Conclusion:** Remove or investigate duplicates; ensure FINESS codes are unique per facility

```python
%pyspark
# Check for duplicates
print("=== ESATIS48H Duplicates ===")
print(f"Duplicate rows: {pdf_esatis.duplicated().sum()}")
print(f"Duplicate FINESS codes: {pdf_esatis['finess'].duplicated().sum()}")

print("\n=== ETE-ORTHO Duplicates ===")
print(f"Duplicate rows: {pdf_ete_ortho.duplicated().sum()}")
print(f"Duplicate FINESS codes: {pdf_ete_ortho['finess'].duplicated().sum()}")
```

```python
%pyspark
# Unique values in key columns
print("=== ESATIS48H Unique Values ===")
for col in ['region', 'participation', 'classement', 'evolution']:
    print(f"{col}: {pdf_esatis[col].nunique()} unique values")

print("\n=== ETE-ORTHO Unique Values ===")
for col in ['region', 'ete_ortho_pos_seuil_etbt']:
    print(f"{col}: {pdf_ete_ortho[col].nunique()} unique values")
```

---

## Step 7: Score Analysis (ESATIS48H)

**Why:** Analyze hospital satisfaction scores across dimensions
**Conclusion:** Identify best/worst performers and regional patterns for quality improvement

```python
%pyspark
# Filter out non-numeric scores (like 'DI', 'NR')
pdf_esatis_clean = pdf_esatis[pd.to_numeric(pdf_esatis['score_all_rea_ajust'], errors='coerce').notna()].copy()

print(f"Records with valid scores: {len(pdf_esatis_clean)}")

# Score statistics by classement
print("\n=== Average Score by Classement ===")
print(pdf_esatis_clean.groupby('classement')['score_all_rea_ajust'].agg(['mean', 'median', 'count']))
```

```python
%pyspark
# Score distribution by region
print("=== Average Scores by Region ===")
region_scores = pdf_esatis_clean.groupby('region').agg({
    'score_all_rea_ajust': 'mean',
    'score_accueil_rea_ajust': 'mean',
    'score_PECinf_rea_ajust': 'mean',
    'score_PECmed_rea_ajust': 'mean',
    'score_chambre_rea_ajust': 'mean',
    'score_repas_rea_ajust': 'mean',
    'score_sortie_rea_ajust': 'mean'
}).round(2)

print(region_scores)
```

---

## Step 8: Correlation Analysis

**Why:** Find relationships between satisfaction dimensions
**Conclusion:** Strong correlations reveal linked quality aspects; weak ones show independent factors

```python
%pyspark
# Correlation between different scores
score_cols = ['score_all_rea_ajust', 'score_accueil_rea_ajust',
              'score_PECinf_rea_ajust', 'score_PECmed_rea_ajust',
              'score_chambre_rea_ajust', 'score_repas_rea_ajust',
              'score_sortie_rea_ajust']

correlation_matrix = pdf_esatis_clean[score_cols].corr()

print("=== Correlation Matrix ===")
print(correlation_matrix.round(2))
```

---

## Step 9: Simple Visualizations (Text-based)

**Why:** Visualize score distributions without graphics libraries
**Conclusion:** Check if scores are normally distributed or skewed; detect clustering

```python
%pyspark
# Create simple histogram using text
def text_histogram(series, bins=10):
    counts, bin_edges = pd.cut(series, bins=bins, retbins=True)
    freq = counts.value_counts().sort_index()

    max_count = freq.max()
    for interval, count in freq.items():
        bar_length = int((count / max_count) * 50)
        print(f"{interval}: {'█' * bar_length} ({count})")

print("=== Score Distribution (ESATIS48H) ===")
text_histogram(pdf_esatis_clean['score_all_rea_ajust'].astype(float))
```

---

## Step 10: Cross-Dataset Analysis

**Why:** Combine satisfaction and clinical quality data by facility
**Conclusion:** Analyze if clinical quality (orthopedic events) correlates with patient satisfaction

```python
%pyspark
# Merge datasets on FINESS code
pdf_merged = pd.merge(pdf_esatis, pdf_ete_ortho, on='finess', how='inner', suffixes=('_esatis', '_ete'))

print(f"=== Merged Dataset ===")
print(f"Total merged records: {len(pdf_merged)}")
print(f"Common facilities: {pdf_merged['finess'].nunique()}")

print("\n=== Sample of merged data ===")
# Note: Use 'rs_finess' not 'rs_esatis' - that's the actual column name
print(pdf_merged[['finess', 'rs_finess', 'region_esatis', 'score_all_rea_ajust',
                   'ete_ortho_obs_etbt', 'ete_ortho_pos_seuil_etbt']].head(10))

# Optional: Correlation analysis
pdf_merged['score_numeric'] = pd.to_numeric(pdf_merged['score_all_rea_ajust'], errors='coerce')
pdf_merged['ete_numeric'] = pd.to_numeric(pdf_merged['ete_ortho_obs_etbt'], errors='coerce')

print("\n=== Correlation between satisfaction and clinical quality ===")
correlation = pdf_merged[['score_numeric', 'ete_numeric']].corr()
print(correlation)
```

---

## Step 11: Export Results

**Why:** Save analysis for reports or further processing
**Conclusion:** Store cleaned data and summaries in HDFS for downstream ML or dashboards

```python
%pyspark
# Save analysis results back to HDFS
summary_stats = pdf_esatis_clean.describe()

# Convert summary to Spark DataFrame and save
summary_df = spark.createDataFrame(summary_stats.reset_index())
summary_df.write.mode("overwrite").csv("hdfs://namenode:8020/user/analysis/esatis_summary", header=True)

print("Summary statistics saved to HDFS: /user/analysis/esatis_summary")
```

```python
%pyspark
# Save cleaned data
df_cleaned = spark.createDataFrame(pdf_esatis_clean)
df_cleaned.write.mode("overwrite").parquet("hdfs://namenode:8020/user/analysis/esatis_cleaned")

print("Cleaned data saved to HDFS: /user/analysis/esatis_cleaned")
```

---

## Key Findings Summary

### Dataset Overview
- **ESATIS48H:** 1,157 hospitals, 23 columns - Patient satisfaction survey data
- **ETE-ORTHO:** 740 hospitals, 8 columns - Orthopedic adverse events quality indicators
- Both use FINESS codes for facility identification
- Only 623/1157 (54%) hospitals have valid satisfaction scores in ESATIS48H

### Data Quality Issues

**ESATIS48H:**
- 46% missing values in all score columns (534 hospitals marked as "DI" - Data Insufficient)
- 139 duplicate FINESS codes - indicates multi-site facilities or data quality issue
- All columns loaded as strings - require type conversion

**ETE-ORTHO:**
- No missing values, no duplicates - clean dataset
- All columns loaded as strings - require type conversion

### Score Insights (ESATIS48H)

**Performance Distribution:**
- Class A (excellent): 73 hospitals, avg score 78.9
- Class B (good): 181 hospitals, avg score 75.4
- Class C (average): 230 hospitals, avg score 72.3
- Class D (poor): 139 hospitals, avg score 68.5
- Scores range: 55.2 - 82.5 (out of 100)
- Distribution is **left-skewed** - most hospitals perform well (71-77 range)

**Regional Performance:**
- Best: Bretagne (75.03), Pays de la Loire (74.73), Nouvelle Aquitaine (74.70)
- Worst: Guyane (64.86), Martinique (68.61), Océan Indien (68.74)
- Overseas territories systematically underperform mainland France

**Satisfaction Dimensions (weakest to strongest):**
1. Meals: 63.2 avg (lowest satisfaction)
2. Discharge process: 63.2 avg
3. Room quality: 67.8 avg
4. Patient care by staff: 76.2 avg
5. Medical care: 77.5 avg (highest satisfaction)

### Correlations

**Strong correlations (>0.80):**
- Overall score ↔ Reception: 0.88
- Overall score ↔ Nursing care: 0.88
- Overall score ↔ Medical care: 0.84

**Weak correlations (<0.60):**
- Meals ↔ Room quality: 0.43
- Meals ↔ Discharge: 0.25 (weakest)

**Insight:** Medical aspects (care quality) drive overall satisfaction more than comfort aspects (meals, rooms)

### Clinical Quality (ETE-ORTHO)

**Orthopedic Events:**
- 93% of hospitals (685/740) rated "B" (good quality - within expected thresholds)
- Only 8 hospitals rated "A" (exceptional - significantly below expected events)
- 47 hospitals rated "C" (concerning - above expected adverse events)
- Most hospitals (75th percentile) had 1.15 or fewer events per 100 cases

### Cross-Dataset Potential

- 740 hospitals in ETE-ORTHO vs 1,157 in ESATIS - expect ~640 matches
- Can analyze: Does clinical quality (adverse events) correlate with patient satisfaction?
- Hypothesis: Hospitals with fewer complications should have higher satisfaction

### Recommendations

**Data Quality:**
1. Investigate 139 duplicate FINESS codes in ESATIS48H
2. Understand why 46% of hospitals lack satisfaction scores
3. Standardize region names (inconsistent encoding: "Rh�ne" vs proper UTF-8)

**Analysis Next Steps:**
1. Merge datasets to correlate clinical quality with satisfaction
2. Focus improvement efforts on: meal quality, discharge process, overseas territories
3. Investigate why Guyane/Martinique underperform (cultural, resource, or methodology issues?)
4. Time-series analysis if multi-year data available (evolution column hints at trends)

**Business Actions:**
- Hospitals with Class C/D satisfaction + high adverse events = priority intervention targets
- Share best practices from Bretagne/Pays de la Loire regions
- Overseas territories need tailored satisfaction measurement approach

---

## Step 12: Create Star Schema Data Model in Hive

**Why:** Transform cleaned data into dimensional model for efficient BI/analytics queries
**Conclusion:** Enable fast aggregations by region, facility, time - ready for dashboards

### Data Model Design

**Star Schema:**
- **Fact Table:** `fact_hospital_quality` - Satisfaction scores + clinical metrics
- **Dimensions:**
  - `dim_establishment` - Hospital details (FINESS, name)
  - `dim_location` - Geographic hierarchy (region, department)
  - `dim_date` - Survey period (year, quarter, month)
  - `dim_quality_classification` - Performance categories (A/B/C/D ratings)

### Step 12.1: Create Hive Database

```python
%pyspark
# Create database for healthcare analytics
spark.sql("CREATE DATABASE IF NOT EXISTS healthcare_analytics")
spark.sql("USE healthcare_analytics")

print("Database created and selected: healthcare_analytics")
spark.sql("SHOW DATABASES").show()
```

### Step 12.2: Create Dimension Tables

**⚠️ Common Issue - Empty Tables After `saveAsTable()`**

If you see tables created but `COUNT(*)` returns 0, use this reliable pattern:
1. Explicitly create database
2. Write with explicit HDFS path using `.option("path", ...)`
3. Force metastore refresh with `REFRESH TABLE`
4. Verify count after creation

This ensures Hive metastore properly registers the data location.

#### Dimension: Location

```python
%pyspark
from pyspark.sql.functions import monotonically_increasing_id, trim

# Extract unique regions from both datasets
regions_esatis = df_esatis.select("region").distinct()
regions_ete = df_ete_ortho.select("region").distinct()

# Union and deduplicate
dim_location = regions_esatis.union(regions_ete).distinct() \
    .withColumnRenamed("region", "region_name") \
    .withColumn("location_id", monotonically_increasing_id())

# Clean region names
dim_location = dim_location.withColumn("region_name_clean", trim(dim_location.region_name))

# Verify BEFORE writing
count = dim_location.count()
print(f"Count before write: {count}")
dim_location.show()

# 1. Ensure database exists
spark.sql("CREATE DATABASE IF NOT EXISTS healthcare_analytics")

# 2. Write with explicit path
dim_location.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", "/user/hive/warehouse/healthcare_analytics.db/dim_location") \
    .saveAsTable("healthcare_analytics.dim_location")

# 3. Force metastore refresh
spark.sql("REFRESH TABLE healthcare_analytics.dim_location")
spark.catalog.refreshTable("healthcare_analytics.dim_location")

# 4. Verify
final_count = spark.table("healthcare_analytics.dim_location").count()
print(f"✓ Created: dim_location ({final_count} rows)")
```

#### Dimension: Establishment

```python
%pyspark
from pyspark.sql.functions import coalesce, lit, monotonically_increasing_id

# Create establishment dimension from ESATIS (more complete)
dim_establishment = df_esatis.select(
    "finess",
    "rs_finess",
    "finess_geo",
    "rs_finess_geo",
    "region",
    "participation"
).distinct()

# Add surrogate key
dim_establishment = dim_establishment.withColumn("establishment_id", monotonically_increasing_id())

# Reorder columns
dim_establishment = dim_establishment.select(
    "establishment_id",
    "finess",
    "rs_finess",
    "finess_geo",
    "rs_finess_geo",
    "region",
    "participation"
)

print(f"Total establishments before write: {dim_establishment.count()}")
dim_establishment.show(5)

# Write with explicit path and refresh
dim_establishment.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", "/user/hive/warehouse/healthcare_analytics.db/dim_establishment") \
    .saveAsTable("healthcare_analytics.dim_establishment")

spark.sql("REFRESH TABLE healthcare_analytics.dim_establishment")
spark.catalog.refreshTable("healthcare_analytics.dim_establishment")

final_count = spark.table("healthcare_analytics.dim_establishment").count()
print(f"✓ Created: dim_establishment ({final_count} rows)")
```

#### Dimension: Quality Classification

```python
%pyspark
# Create quality classification dimension
quality_data = [
    (1, "A", "Excellent", "Score 75-100, Top performers", "green"),
    (2, "B", "Good", "Score 70-75, Above average", "lightgreen"),
    (3, "C", "Average", "Score 65-70, Meets standards", "yellow"),
    (4, "D", "Poor", "Score below 65, Needs improvement", "orange"),
    (5, "DI", "Data Insufficient", "Not enough responses", "gray"),
    (6, "NR", "Not Reported", "Did not participate", "gray")
]

dim_quality = spark.createDataFrame(quality_data,
    ["classification_id", "classification_code", "classification_label",
     "description", "color_code"])

dim_quality.show()

# Write with explicit path and refresh
dim_quality.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", "/user/hive/warehouse/healthcare_analytics.db/dim_quality_classification") \
    .saveAsTable("healthcare_analytics.dim_quality_classification")

spark.sql("REFRESH TABLE healthcare_analytics.dim_quality_classification")
spark.catalog.refreshTable("healthcare_analytics.dim_quality_classification")

final_count = spark.table("healthcare_analytics.dim_quality_classification").count()
print(f"✓ Created: dim_quality_classification ({final_count} rows)")
```

#### Dimension: Date

```python
%pyspark
from datetime import datetime

# Create date dimension for 2017 survey
date_data = [
    (1, 2017, "2017", 1, "Q1", "2017-Q1"),
    (2, 2017, "2017", 2, "Q2", "2017-Q2"),
    (3, 2017, "2017", 3, "Q3", "2017-Q3"),
    (4, 2017, "2017", 4, "Q4", "2017-Q4"),
    (5, 2018, "2018", 1, "Q1", "2018-Q1"),
    (6, 2018, "2018", 2, "Q2", "2018-Q2"),
]

dim_date = spark.createDataFrame(date_data,
    ["date_id", "year", "year_label", "quarter", "quarter_label", "period"])

dim_date.show()

# Write with explicit path and refresh
dim_date.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", "/user/hive/warehouse/healthcare_analytics.db/dim_date") \
    .saveAsTable("healthcare_analytics.dim_date")

spark.sql("REFRESH TABLE healthcare_analytics.dim_date")
spark.catalog.refreshTable("healthcare_analytics.dim_date")

final_count = spark.table("healthcare_analytics.dim_date").count()
print(f"✓ Created: dim_date ({final_count} rows)")
```

### Step 12.3: Create Fact Table

```python
%pyspark
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import DoubleType, IntegerType

# Prepare ESATIS data with proper types
df_esatis_typed = df_esatis.withColumn(
    "score_all", col("score_all_rea_ajust").cast(DoubleType())
).withColumn(
    "score_accueil", col("score_accueil_rea_ajust").cast(DoubleType())
).withColumn(
    "score_nursing", col("score_PECinf_rea_ajust").cast(DoubleType())
).withColumn(
    "score_medical", col("score_PECmed_rea_ajust").cast(DoubleType())
).withColumn(
    "score_room", col("score_chambre_rea_ajust").cast(DoubleType())
).withColumn(
    "score_meals", col("score_repas_rea_ajust").cast(DoubleType())
).withColumn(
    "score_discharge", col("score_sortie_rea_ajust").cast(DoubleType())
).withColumn(
    "nb_responses", col("nb_rep_score_all_rea_ajust").cast(IntegerType())
)

# Prepare ETE-ORTHO data with proper types
df_ete_typed = df_ete_ortho.withColumn(
    "ortho_target", col("ete_ortho_cible_etbt").cast(IntegerType())
).withColumn(
    "ortho_observed", col("ete_ortho_obs_etbt").cast(IntegerType())
).withColumn(
    "ortho_expected", col("ete_ortho_att_etbt").cast(DoubleType())
).withColumn(
    "ortho_ratio", col("ete_ortho_etbt").cast(DoubleType())
)

# Merge satisfaction and clinical quality data
fact_hospital_quality = df_esatis_typed.join(
    df_ete_typed,
    on="finess",
    how="outer"
).select(
    # Keys
    col("finess").alias("establishment_finess"),
    col("region").alias("location_region"),
    lit(1).alias("date_id"),  # Survey year 2017
    col("classement").alias("classification_code"),

    # Satisfaction metrics
    "score_all",
    "score_accueil",
    "score_nursing",
    "score_medical",
    "score_room",
    "score_meals",
    "score_discharge",
    "nb_responses",

    # Clinical quality metrics
    "ortho_target",
    "ortho_observed",
    "ortho_expected",
    "ortho_ratio",
    col("ete_ortho_pos_seuil_etbt").alias("ortho_quality_level"),

    # Metadata
    col("evolution").alias("trend"),
    col("Depot").alias("data_submitted")
)

print(f"Fact table records before write: {fact_hospital_quality.count()}")
fact_hospital_quality.show(5)

# Write with explicit path, partitioning, and refresh
fact_hospital_quality.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("location_region") \
    .option("path", "/user/hive/warehouse/healthcare_analytics.db/fact_hospital_quality") \
    .saveAsTable("healthcare_analytics.fact_hospital_quality")

# Force metastore refresh
spark.sql("REFRESH TABLE healthcare_analytics.fact_hospital_quality")
spark.catalog.refreshTable("healthcare_analytics.fact_hospital_quality")

# Verify
final_count = spark.table("healthcare_analytics.fact_hospital_quality").count()
print(f"✓ Created: fact_hospital_quality ({final_count} rows, partitioned by region)")
```

### Step 12.4: Verify Hive Tables

```python
%pyspark
# Show all tables
print("=== Healthcare Analytics Tables ===")
spark.sql("SHOW TABLES IN healthcare_analytics").show()

# Table statistics
print("\n=== Table Row Counts ===")
tables = ["dim_location", "dim_establishment", "dim_quality_classification",
          "dim_date", "fact_hospital_quality"]

for table in tables:
    count = spark.sql(f"SELECT COUNT(*) as count FROM healthcare_analytics.{table}").collect()[0][0]
    print(f"{table}: {count:,} rows")
```

### Step 12.5: Sample Analytics Queries

#### Query 1: Average Satisfaction by Region

```sql
%sql
SELECT
    l.region_name,
    COUNT(DISTINCT f.establishment_finess) as hospital_count,
    ROUND(AVG(f.score_all), 2) as avg_satisfaction,
    ROUND(AVG(f.score_meals), 2) as avg_meals_score,
    ROUND(AVG(f.ortho_ratio), 2) as avg_ortho_adverse_events
FROM healthcare_analytics.fact_hospital_quality f
LEFT JOIN healthcare_analytics.dim_location l
    ON f.location_region = l.region_name
WHERE f.score_all IS NOT NULL
GROUP BY l.region_name
ORDER BY avg_satisfaction DESC
LIMIT 10
```

#### Query 2: Quality Classification Distribution

```sql
%sql
SELECT
    q.classification_label,
    COUNT(*) as hospital_count,
    ROUND(AVG(f.score_all), 2) as avg_score,
    ROUND(AVG(f.nb_responses), 0) as avg_responses
FROM healthcare_analytics.fact_hospital_quality f
JOIN healthcare_analytics.dim_quality_classification q
    ON f.classification_code = q.classification_code
GROUP BY q.classification_label, q.classification_id
ORDER BY q.classification_id
```

#### Query 3: Hospitals Needing Attention (Low Satisfaction + High Adverse Events)

```sql
%sql
SELECT
    e.rs_finess as hospital_name,
    e.region,
    f.score_all as satisfaction_score,
    f.ortho_observed as adverse_events_count,
    f.ortho_ratio as adverse_events_ratio,
    f.classification_code as quality_class,
    f.ortho_quality_level
FROM healthcare_analytics.fact_hospital_quality f
JOIN healthcare_analytics.dim_establishment e
    ON f.establishment_finess = e.finess
WHERE f.score_all < 70
  AND f.ortho_observed > 3
ORDER BY f.score_all ASC, f.ortho_observed DESC
LIMIT 20
```

#### Query 4: Best Performers by Dimension

```sql
%sql
SELECT
    e.rs_finess as hospital_name,
    e.region,
    f.score_all,
    f.score_accueil as reception,
    f.score_nursing,
    f.score_medical,
    f.score_room,
    f.score_meals,
    f.score_discharge
FROM healthcare_analytics.fact_hospital_quality f
JOIN healthcare_analytics.dim_establishment e
    ON f.establishment_finess = e.finess
WHERE f.classification_code = 'A'
ORDER BY f.score_all DESC
LIMIT 10
```

### Step 12.6: Access Tables from Command Line

```bash
# Connect to HiveServer2
docker exec -it hive-hs2 beeline -u jdbc:hive2://localhost:10000

# Run queries
USE healthcare_analytics;
SHOW TABLES;
SELECT COUNT(*) FROM fact_hospital_quality;
```

---

## Troubleshooting Hive Tables

### Issue: Tables Created but COUNT(*) Returns 0

**Symptoms:**
- `SHOW TABLES` displays the table
- `SELECT COUNT(*) FROM table` returns 0
- But you know data was written

**Root Cause:** Hive metastore not properly registered with data location

**Solution:** Use the reliable pattern shown above:
```python
%pyspark
# Always use this pattern for saveAsTable
your_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", "/user/hive/warehouse/database.db/table_name") \
    .saveAsTable("database.table_name")

# Then force refresh
spark.sql("REFRESH TABLE database.table_name")
spark.catalog.refreshTable("database.table_name")

# Verify
print(spark.table("database.table_name").count())
```

### Issue: SQL Queries Return Empty Results

**Symptoms:**
- Table has data (PySpark count works)
- SQL `SELECT * FROM table` returns no rows

**Solution:**
```sql
%sql
-- Repair table partitions
MSCK REPAIR TABLE healthcare_analytics.fact_hospital_quality;

-- Refresh table metadata
REFRESH TABLE healthcare_analytics.fact_hospital_quality;

-- Then query
SELECT COUNT(*) FROM healthcare_analytics.fact_hospital_quality;
```

### Issue: Column Name Mismatch in Merged Data

**Symptoms:**
- `KeyError: 'rs_esatis' not in index`
- Columns have different names after merge

**Solution:**
Check actual column names after merge:
```python
%pyspark
print(pdf_merged.columns.tolist())
```

Common mistakes:
- ESATIS has `rs_finess` not `rs_esatis`
- After merge with suffixes, `region` becomes `region_esatis` and `region_ete`

### Issue: Database Not Found

**Symptoms:**
- `Database 'healthcare_analytics' not found`

**Solution:**
```python
%pyspark
# Always create database first
spark.sql("CREATE DATABASE IF NOT EXISTS healthcare_analytics")
spark.sql("USE healthcare_analytics")
```

---

## Data Architecture Summary

### Bronze → Silver → Gold Pipeline

**Bronze (Raw):**
- `/user/nifi/satisfaction/*.csv` - Original CSV files in HDFS

**Silver (Cleaned):**
- `/user/analysis/esatis_cleaned` - Cleaned parquet files
- Type conversions, null handling, deduplication applied

**Gold (Data Warehouse):**
- `healthcare_analytics.*` - Hive tables in star schema
- Optimized for analytics with partitioning and proper typing
- Ready for BI tools (Superset, Tableau, Power BI)

### Benefits of This Model

1. **Performance:** Partitioned by region for fast queries
2. **Maintainability:** Clear separation of facts and dimensions
3. **Scalability:** Star schema works with billions of rows
4. **Flexibility:** Easy to add new dimensions (hospital type, department, etc.)
5. **BI-Ready:** Can connect Hive to any SQL-based visualization tool
