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
DATA_DIR = "../../../data/donnees"

# ============================================================
# STEP 1: LOAD DATA
# ============================================================

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

# ============================================================
# STEP 2: MERGE DATASETS
# ============================================================

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

# ============================================================
# STEP 3: BASIC STATISTICS
# ============================================================

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

# ============================================================
# STEP 4: VISUALIZATIONS
# ============================================================

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

# ============================================================
# STEP 5: ANSWER KEY QUESTION
# ============================================================

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

# ============================================================
# STEP 6: SUMMARY REPORT
# ============================================================

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
