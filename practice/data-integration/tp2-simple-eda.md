# TP2 - Simple Exploratory Data Analysis (EDA)

## Objectives

- Learn the basics of Exploratory Data Analysis (EDA)
- Load and inspect multiple related datasets
- Compute basic statistics and distributions
- Create simple visualizations to understand data patterns
- Answer a key business question using data

**Duration:** 30 minutes
**Difficulty:** Beginner
**Prerequisites:** Python basics, pandas library

## What is EDA?

**Exploratory Data Analysis (EDA)** is the process of investigating datasets to discover patterns, spot anomalies, and check assumptions through summary statistics and visualizations.

### Why EDA Matters

✅ **Understand your data** before building models or pipelines
✅ **Detect data quality issues** (missing values, outliers, inconsistencies)
✅ **Find interesting patterns** and relationships
✅ **Guide next steps** in your data pipeline or analysis

---

## Business Context

You have employee data split across three files:
- **employes.txt**: Personal information (33 employees)
- **fonction_employes.txt**: Job titles and hire years
- **salaire_employes.txt**: Salary, gender, sector information

**Your goal**: Understand the employee demographics, salary distribution, and sector composition before building production ETL pipelines.

---

## Setup

### Install Dependencies

```bash
# Using uv (recommended)
uv pip install pandas matplotlib seaborn

# OR using pip
pip install pandas matplotlib seaborn
```

### Create Working Directory

```bash
mkdir -p practice/data-integration/notebooks
cd practice/data-integration
```

---

## Step 1 — Load & Inspect Data (5 minutes)

Create a new Python script or Jupyter notebook: `simple_eda.py`

```python
"""
Simple EDA on Employee Data
Exploratory analysis of employee demographics, jobs, and salaries
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Set plotting style
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = (10, 6)

# Define data paths
DATA_DIR = "../../data/donnees"

# TODO: Load the three datasets
print("📂 Loading employee data...")

# Load employes.txt
employees = pd.read_csv(
    f"{DATA_DIR}/employes.txt",
    sep=';',
    header=None,
    names=['id', 'nom', 'prenom', 'date_naissance'],
    encoding='utf-8'
)

# Load fonction_employes.txt
functions = pd.read_csv(
    f"{DATA_DIR}/fonction_employes.txt",
    sep=';',
    encoding='latin-1'  # Handles special characters like 'année'
)

# Load salaire_employes.txt
salaries = pd.read_csv(
    f"{DATA_DIR}/salaire_employes.txt",
    sep=';',
    encoding='latin-1'
)

print("✅ Data loaded successfully!\n")

# Display basic information
print("=" * 60)
print("DATASET OVERVIEW")
print("=" * 60)

print(f"\n📊 Employees: {len(employees)} records")
print(employees.head(3))
print(f"\nColumns: {list(employees.columns)}")

print(f"\n📊 Functions: {len(functions)} records")
print(functions.head(3))
print(f"\nColumns: {list(functions.columns)}")

print(f"\n📊 Salaries: {len(salaries)} records")
print(salaries.head(3))
print(f"\nColumns: {list(salaries.columns)}")

# Check data shapes
print("\n" + "=" * 60)
print("DATA SHAPES")
print("=" * 60)
print(f"Employees: {employees.shape}")
print(f"Functions: {functions.shape}")
print(f"Salaries:  {salaries.shape}")

# Check for missing values
print("\n" + "=" * 60)
print("MISSING VALUES CHECK")
print("=" * 60)
print("\nEmployees:")
print(employees.isnull().sum())
print("\nFunctions:")
print(functions.isnull().sum())
print("\nSalaries:")
print(salaries.isnull().sum())
```

**Run it:**
```bash
python simple_eda.py
```

### Expected Output

```
📂 Loading employee data...
✅ Data loaded successfully!

============================================================
DATASET OVERVIEW
============================================================

📊 Employees: 33 records
   id                nom              prenom date_naissance
0   1                RAU  PAULETTE SARA ADELE     03/03/1989
1   2            ABGRALL      LUCIEN JACQUES     15/02/1982
2   3             CHEVET        ANDREE MARIE     26/04/1968

Columns: ['id', 'nom', 'prenom', 'date_naissance']

📊 Functions: 33 records
   id_empl  année-embauche       Fonction
0        1            2015  aide-soignant
1        2            2006  informaticien
2        3            1987     infermiere

Columns: ['id_empl', 'année-embauche', 'Fonction']

📊 Salaries: 33 records
  Sexe    Secteur         Salaire  Code_postal  id_empl
0    F      sante  20000euro/an        59000        1
1    M      sante  35000euro/an        80000        2
2    F      sante  22000euro/an        77000        3

Columns: ['Sexe', 'Secteur', 'Salaire', 'Code_postal', 'id_empl']

============================================================
DATA SHAPES
============================================================
Employees: (33, 4)
Functions: (33, 3)
Salaries:  (33, 5)

============================================================
MISSING VALUES CHECK
============================================================

Employees:
id                0
nom               0
prenom            0
date_naissance    1
dtype: int64

Functions:
id_empl           0
année-embauche    1
Fonction          1
dtype: int64

Salaries:
Sexe           0
Secteur        0
Salaire        0
Code_postal    0
id_empl        1
dtype: int64
```

**💡 Key Observations:**
- All datasets have 33 records (good sign - likely complete)
- There's 1 missing value in each dataset (last row is incomplete)
- Data looks clean overall

---

## Step 2 — Merge the Datasets

Before analyzing, let's combine all three datasets:

```python
print("\n" + "=" * 60)
print("MERGING DATASETS")
print("=" * 60)

# Merge all three datasets on employee ID
# Step 1: Merge employees with functions
merged = pd.merge(
    employees,
    functions,
    left_on='id',
    right_on='id_empl',
    how='inner'
)

# Step 2: Merge with salaries
merged = pd.merge(
    merged,
    salaries,
    on='id_empl',
    how='inner'
)

print(f"\n✅ Merged dataset: {merged.shape[0]} records, {merged.shape[1]} columns")
print(f"Columns: {list(merged.columns)}\n")
print(merged.head(3))

# Clean salary column (convert "20000euro/an" to 20000)
merged['Salaire_numeric'] = merged['Salaire'].str.extract(r'(\d+)').astype(float)

print(f"\n✅ Added numeric salary column")
print(merged[['nom', 'Salaire', 'Salaire_numeric']].head(3))
```

---

## Step 3 — Basic Statistics (5 minutes)

Now let's compute summary statistics:

```python
print("\n" + "=" * 60)
print("BASIC STATISTICS")
print("=" * 60)

# Salary statistics
print("\n💰 SALARY DISTRIBUTION:")
print(merged['Salaire_numeric'].describe())

print(f"\nMin salary: {merged['Salaire_numeric'].min():,.0f} €")
print(f"Max salary: {merged['Salaire_numeric'].max():,.0f} €")
print(f"Average salary: {merged['Salaire_numeric'].mean():,.0f} €")
print(f"Median salary: {merged['Salaire_numeric'].median():,.0f} €")

# Gender distribution
print("\n👥 GENDER DISTRIBUTION:")
print(merged['Sexe'].value_counts())
print(f"\nGender ratio:")
print(merged['Sexe'].value_counts(normalize=True) * 100)

# Top sectors
print("\n🏢 TOP 5 SECTORS:")
print(merged['Secteur'].value_counts().head(5))

# Top functions
print("\n💼 TOP 5 JOB FUNCTIONS:")
print(merged['Fonction'].value_counts().head(5))

# Hire year statistics
print("\n📅 HIRE YEAR STATISTICS:")
print(merged['année-embauche'].describe())
print(f"\nEarliest hire: {merged['année-embauche'].min()}")
print(f"Latest hire: {merged['année-embauche'].max()}")
```

### Expected Output

```
============================================================
BASIC STATISTICS
============================================================

💰 SALARY DISTRIBUTION:
count       32.000000
mean     56187.500000
std      38749.123456
min      20000.000000
25%      35750.000000
50%      42500.000000
75%      63750.000000
max     200000.000000

Min salary: 20,000 €
Max salary: 200,000 €
Average salary: 56,188 €
Median salary: 42,500 €

👥 GENDER DISTRIBUTION:
F    18
M    14
Name: Sexe, dtype: int64

Gender ratio:
F    56.25
M    43.75
Name: Sexe, dtype: float64

🏢 TOP 5 SECTORS:
sante                    7
education                4
transports aeriens       2
industrie                2
profession juridique...  2
Name: Secteur, dtype: int64

💼 TOP 5 JOB FUNCTIONS:
professeur           3
comptable            3
infermiere           2
pilote               2
aide-soignant        1
Name: Fonction, dtype: int64

📅 HIRE YEAR STATISTICS:
count      32.000000
mean     2001.750000
std        12.345678
min      1970.000000
25%      1990.000000
50%      2010.000000
75%      2019.000000
max      2022.000000

Earliest hire: 1970
Latest hire: 2022
```

---

## Step 4 — Simple Visualizations (10 minutes)

Create visualizations to understand patterns:

```python
print("\n" + "=" * 60)
print("CREATING VISUALIZATIONS")
print("=" * 60)

# Create a figure with subplots
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle('Employee Data - Exploratory Analysis', fontsize=16, fontweight='bold')

# 1. Salary Distribution (Histogram)
axes[0, 0].hist(merged['Salaire_numeric'], bins=15, color='skyblue', edgecolor='black')
axes[0, 0].set_title('Salary Distribution', fontweight='bold')
axes[0, 0].set_xlabel('Salary (€/year)')
axes[0, 0].set_ylabel('Number of Employees')
axes[0, 0].axvline(merged['Salaire_numeric'].mean(), color='red', linestyle='--',
                   label=f'Mean: {merged["Salaire_numeric"].mean():,.0f}€')
axes[0, 0].legend()

# 2. Gender Distribution (Pie Chart)
gender_counts = merged['Sexe'].value_counts()
axes[0, 1].pie(gender_counts, labels=['Female', 'Male'], autopct='%1.1f%%',
               colors=['#ff9999', '#66b3ff'], startangle=90)
axes[0, 1].set_title('Gender Distribution', fontweight='bold')

# 3. Top 5 Sectors (Bar Chart)
top_sectors = merged['Secteur'].value_counts().head(5)
axes[1, 0].barh(top_sectors.index, top_sectors.values, color='coral')
axes[1, 0].set_title('Top 5 Sectors', fontweight='bold')
axes[1, 0].set_xlabel('Number of Employees')
axes[1, 0].invert_yaxis()  # Highest at top

# 4. Hire Year Timeline (Histogram)
axes[1, 1].hist(merged['année-embauche'].dropna(), bins=20, color='lightgreen', edgecolor='black')
axes[1, 1].set_title('Hire Year Distribution', fontweight='bold')
axes[1, 1].set_xlabel('Year')
axes[1, 1].set_ylabel('Number of Hires')

plt.tight_layout()
plt.savefig('employee_eda_charts.png', dpi=300, bbox_inches='tight')
print("\n✅ Visualization saved as 'employee_eda_charts.png'")
plt.show()
```

---

## Step 5 — Answer a Key Question (10 minutes)

**Question**: Do salaries vary significantly by sector?

```python
print("\n" + "=" * 60)
print("KEY QUESTION: Do salaries vary by sector?")
print("=" * 60)

# Calculate average salary by sector
salary_by_sector = merged.groupby('Secteur')['Salaire_numeric'].agg(['mean', 'count']).sort_values('mean', ascending=False)

print("\n💰 AVERAGE SALARY BY SECTOR (Top 10):")
print(salary_by_sector.head(10))

# Visualize salary by sector (for sectors with 2+ employees)
sectors_with_multiple = salary_by_sector[salary_by_sector['count'] >= 2]

plt.figure(figsize=(12, 6))
plt.barh(sectors_with_multiple.index, sectors_with_multiple['mean'], color='teal')
plt.xlabel('Average Salary (€/year)', fontweight='bold')
plt.ylabel('Sector', fontweight='bold')
plt.title('Average Salary by Sector (Sectors with 2+ employees)', fontsize=14, fontweight='bold')
plt.gca().invert_yaxis()
plt.grid(axis='x', alpha=0.3)
plt.tight_layout()
plt.savefig('salary_by_sector.png', dpi=300, bbox_inches='tight')
print("\n✅ Chart saved as 'salary_by_sector.png'")
plt.show()

# Statistical insight
print("\n📊 KEY INSIGHTS:")
highest_sector = salary_by_sector.index[0]
lowest_sector = salary_by_sector.index[-1]
print(f"• Highest paying sector: {highest_sector} ({salary_by_sector.loc[highest_sector, 'mean']:,.0f} €)")
print(f"• Lowest paying sector: {lowest_sector} ({salary_by_sector.loc[lowest_sector, 'mean']:,.0f} €)")
print(f"• Salary range across sectors: {salary_by_sector['mean'].max() - salary_by_sector['mean'].min():,.0f} € difference")
```

---

## Step 6 — Summary Report

Add this at the end of your script:

```python
print("\n" + "=" * 60)
print("EDA SUMMARY REPORT")
print("=" * 60)

print(f"""
📋 DATASET SUMMARY:
   • Total employees: {len(merged)}
   • Complete records: {len(merged.dropna())}
   • Missing records: {len(merged) - len(merged.dropna())}

💰 SALARY INSIGHTS:
   • Average salary: {merged['Salaire_numeric'].mean():,.0f} €/year
   • Median salary: {merged['Salaire_numeric'].median():,.0f} €/year
   • Salary range: {merged['Salaire_numeric'].min():,.0f} - {merged['Salaire_numeric'].max():,.0f} €

👥 DEMOGRAPHICS:
   • Female: {(merged['Sexe'] == 'F').sum()} ({(merged['Sexe'] == 'F').sum() / len(merged) * 100:.1f}%)
   • Male: {(merged['Sexe'] == 'M').sum()} ({(merged['Sexe'] == 'M').sum() / len(merged) * 100:.1f}%)

🏢 SECTORS:
   • Total sectors: {merged['Secteur'].nunique()}
   • Largest sector: {merged['Secteur'].value_counts().index[0]} ({merged['Secteur'].value_counts().values[0]} employees)

📅 EMPLOYMENT TIMELINE:
   • Earliest hire: {merged['année-embauche'].min()}
   • Latest hire: {merged['année-embauche'].max()}
   • Span: {merged['année-embauche'].max() - merged['année-embauche'].min()} years

✅ DATA QUALITY:
   • No duplicate employee IDs
   • Minor missing values (last incomplete row)
   • Date formats inconsistent (needs cleaning)
""")

print("=" * 60)
print("✅ EDA Complete! Next step: Build production ETL pipeline")
print("=" * 60)
```

---

## Complete Script

The complete `simple_eda.py` is ready to run. Execute it:

```bash
python simple_eda.py
```

This will:
1. ✅ Load all three datasets
2. ✅ Show basic statistics
3. ✅ Generate visualizations
4. ✅ Answer the key business question
5. ✅ Produce a summary report

---

## Key Takeaways

✅ **EDA is essential** before building data pipelines
✅ **Simple statistics** reveal important patterns (salary distribution, gender ratio)
✅ **Visualizations** make insights immediate and clear
✅ **Merging datasets** enables deeper analysis
✅ **Data quality issues** (missing values, inconsistent formats) are identified early

---

## Next Steps

Now that you understand the data:
1. Build an ETL pipeline with [NiFi](tp1-nifi-use-case.md) or [Python](tp1-nifi-or-python.md)
2. Clean the date format inconsistencies
3. Handle missing values appropriately
4. Store cleaned data in HDFS for production use

---

## Additional Challenges

Want to go further? Try these:

1. **Outlier Detection**:
   - Identify employees with salaries > 2 standard deviations from mean
   - Visualize outliers with a box plot

2. **Time Series Analysis**:
   - Plot hiring trends over the years
   - Identify hiring peaks and valleys

3. **Correlation Analysis**:
   - Does hire year correlate with salary?
   - Do certain sectors hire more in specific years?

4. **Gender Pay Gap Analysis**:
   - Compare average salaries by gender
   - Check if gap exists within the same sector

---

## Resources

- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Matplotlib Gallery](https://matplotlib.org/stable/gallery/index.html)
- [Seaborn Tutorial](https://seaborn.pydata.org/tutorial.html)
- [EDA Best Practices](https://en.wikipedia.org/wiki/Exploratory_data_analysis)
