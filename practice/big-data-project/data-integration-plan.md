# Data Integration Plan - Healthcare Data Warehouse

## 1. Data Sources Analysis

### hospitalisations.csv (2,479 records)

**File Location:** `../CESI-BigData-FiseA42526/data/hospitalisation/hospitalisations.csv`

**Delimiter:** `;` (semicolon)

**Columns:**
| Column Name | Type | Description | Example |
|-------------|------|-------------|---------|
| `Num_Hospitalisation` | INT | Unique hospitalization ID | 10546 |
| `Id_patient` | INT | Patient identifier | 29620 |
| `identifiant_organisation` | STRING | FINESS facility ID | F010000107 |
| `Code_diagnostic` | STRING | CIM-10 diagnostic code | S02800 |
| `Suite_diagnostic_consultation` | STRING | Diagnosis description | Fracture de l'alveole dentaire, fermees |
| `Date_Entree` | STRING | Admission date (DD/MM/YYYY) | 27/09/2017 |
| `Jour_Hospitalisation` | INT | Length of stay (days) | 17 |

---

### etablissement_sante.csv (416,665 records)

**File Location:** `../CESI-BigData-FiseA42526/data/etablissement_sante/etablissement_sante.csv`

**Delimiter:** `;` (semicolon)

**Key Columns:**
| Column Name | Type | Description | Example |
|-------------|------|-------------|---------|
| `identifiant_organisation` | STRING | FINESS facility ID | F010000024 |
| `raison_sociale_site` | STRING | Facility name | CH DE FLEYRIAT |
| `adresse` | STRING | Full address | 900 Route DE PARIS 01440 Viriat |
| `code_postal` | STRING | Postal code | 01440 |
| `commune` | STRING | City | Viriat |
| `code_commune` | STRING | INSEE city code | 01451 |
| `siren_site` | STRING | SIREN number | 010000024 |
| `siret_site` | STRING | SIRET number | 26010004500012 |
| `telephone` | STRING | Phone number | 0474454647 |
| `email` | STRING | Email | dirg@ch-bourg01.fr |

---

## 2. Target Hive Schema Mapping

### A. facilities table
```sql
CREATE TABLE IF NOT EXISTS facilities (
  id STRING,
  name STRING,
  region STRING
) STORED AS ORC;
```

**Mapping:**
```
Source: etablissement_sante.csv
├─ id               ← identifiant_organisation
├─ name             ← raison_sociale_site
└─ region           ← DERIVED (from code_postal/code_commune)
```

**Transformation Required:**
- Extract département code from `code_postal` (first 2 digits)
- Map département → région using lookup table

---

### B. hospitalisations table
```sql
CREATE TABLE IF NOT EXISTS hospitalisations (
  in_date DATE,
  out_date DATE,
  patient_id INT,
  facility_id STRING,
  diagnosis_id STRING
) PARTITIONED BY (year INT) STORED AS ORC;
```

**Mapping:**
```
Source: hospitalisations.csv
├─ in_date          ← CONVERT Date_Entree (DD/MM/YYYY → YYYY-MM-DD)
├─ out_date         ← CALCULATE (in_date + Jour_Hospitalisation days)
├─ patient_id       ← Id_patient
├─ facility_id      ← identifiant_organisation
├─ diagnosis_id     ← SUBSTRING(Code_diagnostic, 1, 4)
└─ year (partition) ← YEAR(in_date)
```

**Transformation Required:**
- Date format conversion: French `DD/MM/YYYY` → ISO `YYYY-MM-DD`
- Calculate `out_date` using date arithmetic
- Normalize diagnostic code to 4-character prefix
- Extract year for partitioning

---

### C. diagnoses table (supplementary)
```sql
CREATE TABLE IF NOT EXISTS diagnoses (
  id STRING,
  name STRING
) STORED AS ORC;
```

**Mapping:**
```
Source: hospitalisations.csv (deduplicated)
├─ id               ← SUBSTRING(Code_diagnostic, 1, 4)
└─ name             ← Suite_diagnostic_consultation
```

**Transformation Required:**
- Extract unique diagnostic codes
- Truncate to 4-character prefix (e.g., "S02800" → "S028")
- Deduplicate and aggregate descriptions

---

## 3. ETL Pipeline Architecture

### Phase 1: Data Upload to HDFS

```bash
# Upload raw data to HDFS
hdfs dfs -mkdir -p /data/raw/csv/hospitalisation
hdfs dfs -mkdir -p /data/raw/csv/etablissement_sante

hdfs dfs -put hospitalisations.csv /data/raw/csv/hospitalisation/
hdfs dfs -put etablissement_sante.csv /data/raw/csv/etablissement_sante/
```

---

### Phase 2: Create Staging Tables

```sql
-- Staging table for hospitalisations
CREATE EXTERNAL TABLE IF NOT EXISTS hospitalisations_staging (
  Num_Hospitalisation INT,
  Id_patient INT,
  identifiant_organisation STRING,
  Code_diagnostic STRING,
  Suite_diagnostic_consultation STRING,
  Date_Entree STRING,
  Jour_Hospitalisation INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/data/raw/csv/hospitalisation/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Staging table for etablissement_sante
CREATE EXTERNAL TABLE IF NOT EXISTS etablissement_sante_staging (
  adresse STRING,
  cedex STRING,
  code_commune STRING,
  code_postal STRING,
  commune STRING,
  complement_destinataire STRING,
  complement_point_geographique STRING,
  email STRING,
  enseigne_commerciale_site STRING,
  finess_etablissement_juridique STRING,
  finess_site STRING,
  identifiant_organisation STRING,
  indice_repetition_voie STRING,
  mention_distribution STRING,
  numero_voie STRING,
  pays STRING,
  raison_sociale_site STRING,
  siren_site STRING,
  siret_site STRING,
  telecopie STRING,
  telephone STRING,
  telephone_2 STRING,
  type_voie STRING,
  voie STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/data/raw/csv/etablissement_sante/'
TBLPROPERTIES ("skip.header.line.count"="1");
```

---

### Phase 3: Transformation & Load

#### 3.1 Load facilities table

```sql
-- Create département → région mapping table
CREATE TABLE IF NOT EXISTS dept_region_mapping (
  dept_code STRING,
  region STRING
) STORED AS ORC;

-- Insert département mappings (sample)
INSERT INTO dept_region_mapping VALUES
('01', 'Auvergne-Rhône-Alpes'),
('02', 'Hauts-de-France'),
('03', 'Auvergne-Rhône-Alpes'),
('04', 'Provence-Alpes-Côte d''Azur'),
('05', 'Provence-Alpes-Côte d''Azur'),
('06', 'Provence-Alpes-Côte d''Azur'),
('07', 'Auvergne-Rhône-Alpes'),
('08', 'Grand Est'),
('09', 'Occitanie'),
-- ... add all 101 départements
('75', 'Île-de-France'),
('92', 'Île-de-France'),
('93', 'Île-de-France'),
('94', 'Île-de-France'),
('95', 'Île-de-France');

-- Load facilities with region mapping
INSERT INTO TABLE facilities
SELECT
  est.identifiant_organisation AS id,
  est.raison_sociale_site AS name,
  COALESCE(drm.region, 'Unknown') AS region
FROM (
  SELECT DISTINCT
    identifiant_organisation,
    raison_sociale_site,
    code_postal
  FROM etablissement_sante_staging
  WHERE identifiant_organisation IS NOT NULL
    AND identifiant_organisation != ''
) est
LEFT JOIN dept_region_mapping drm
  ON SUBSTRING(est.code_postal, 1, 2) = drm.dept_code;
```

#### 3.2 Load diagnoses table

```sql
INSERT INTO TABLE diagnoses
SELECT DISTINCT
  SUBSTRING(Code_diagnostic, 1, 4) AS id,
  FIRST_VALUE(Suite_diagnostic_consultation) OVER (
    PARTITION BY SUBSTRING(Code_diagnostic, 1, 4)
    ORDER BY Num_Hospitalisation
  ) AS name
FROM hospitalisations_staging
WHERE Code_diagnostic IS NOT NULL;
```

#### 3.3 Load hospitalisations table

```sql
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE hospitalisations PARTITION(year)
SELECT
  -- Convert French date format to DATE
  FROM_UNIXTIME(
    UNIX_TIMESTAMP(Date_Entree, 'dd/MM/yyyy'),
    'yyyy-MM-dd'
  ) AS in_date,

  -- Calculate out_date by adding duration
  DATE_ADD(
    FROM_UNIXTIME(UNIX_TIMESTAMP(Date_Entree, 'dd/MM/yyyy'), 'yyyy-MM-dd'),
    Jour_Hospitalisation
  ) AS out_date,

  Id_patient AS patient_id,
  identifiant_organisation AS facility_id,
  SUBSTRING(Code_diagnostic, 1, 4) AS diagnosis_id,

  -- Extract year for partitioning
  YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(Date_Entree, 'dd/MM/yyyy'))) AS year
FROM hospitalisations_staging
WHERE Id_patient IS NOT NULL
  AND identifiant_organisation IS NOT NULL
  AND Date_Entree IS NOT NULL;
```

---

## 4. Data Quality Checks

### 4.1 Validate row counts
```sql
-- Check staging vs target counts
SELECT 'hospitalisations_staging' AS table_name, COUNT(*) AS row_count FROM hospitalisations_staging
UNION ALL
SELECT 'hospitalisations', COUNT(*) FROM hospitalisations
UNION ALL
SELECT 'etablissement_sante_staging', COUNT(*) FROM etablissement_sante_staging
UNION ALL
SELECT 'facilities', COUNT(*) FROM facilities;
```

### 4.2 Check for orphan records
```sql
-- Hospitalisations with invalid facility_id
SELECT COUNT(*) AS orphan_facility_count
FROM hospitalisations h
LEFT JOIN facilities f ON h.facility_id = f.id
WHERE f.id IS NULL;

-- Hospitalisations with invalid diagnosis_id
SELECT COUNT(*) AS orphan_diagnosis_count
FROM hospitalisations h
LEFT JOIN diagnoses d ON h.diagnosis_id = d.id
WHERE d.id IS NULL;
```

### 4.3 Validate date ranges
```sql
-- Check for invalid date calculations
SELECT
  COUNT(*) AS invalid_dates
FROM hospitalisations
WHERE out_date < in_date OR out_date IS NULL OR in_date IS NULL;

-- Check year partition distribution
SELECT year, COUNT(*) AS count
FROM hospitalisations
GROUP BY year
ORDER BY year;
```

---

## 5. Key Challenges & Solutions

### Challenge 1: Region Derivation
**Problem:** `etablissement_sante` has postal codes but no region field

**Solution:**
- Create département → région mapping table
- Extract département code from postal code (first 2 digits)
- LEFT JOIN to handle edge cases (DOM-TOM, special codes)

---

### Challenge 2: French Date Format
**Problem:** Source dates are `DD/MM/YYYY` format

**Solution:**
```sql
FROM_UNIXTIME(UNIX_TIMESTAMP(Date_Entree, 'dd/MM/yyyy'), 'yyyy-MM-dd')
```

---

### Challenge 3: Missing out_date
**Problem:** Only admission date + duration (days) provided

**Solution:**
```sql
DATE_ADD(in_date, Jour_Hospitalisation)
```

---

### Challenge 4: Diagnostic Code Normalization
**Problem:** Source has full CIM-10 codes (5+ chars), target needs 4-char prefix

**Solution:**
```sql
SUBSTRING(Code_diagnostic, 1, 4)
```

---

### Challenge 5: Large Facility Dataset
**Problem:** 416K facility records (includes historical/duplicate entries)

**Solution:**
- Use `SELECT DISTINCT` on `identifiant_organisation`
- Filter out NULL/empty IDs
- Consider most recent record if temporal data exists

---

## 6. Execution Checklist

- [ ] Upload CSV files to HDFS
- [ ] Create staging tables
- [ ] Create département-région mapping table
- [ ] Populate mapping table with all 101 départements
- [ ] Load facilities dimension table
- [ ] Load diagnoses dimension table
- [ ] Load hospitalisations fact table with dynamic partitioning
- [ ] Run data quality validation queries
- [ ] Document orphan records and data issues
- [ ] Create views for common analytical queries
- [ ] Set up incremental load process (if needed)

---

## 7. Next Steps

1. **Complete département mapping**: Full list of 101 French départements → 18 regions
2. **Handle special cases**:
   - Corse (2A, 2B)
   - DOM-TOM (97x, 98x)
   - Monaco (98000)
3. **Incremental loads**: Design UPSERT strategy for new hospitalizations
4. **Performance tuning**:
   - Bucketing on `facility_id`
   - Indexing on `patient_id`
5. **Data lineage**: Track ETL execution metadata
