# Simple EDA Example

This folder contains a complete implementation of the Simple EDA tutorial.

## Quick Start

### 1. Install Dependencies

```bash
# Using uv (recommended)
uv pip install pandas matplotlib seaborn

# OR using pip
pip install pandas matplotlib seaborn
```

### 2. Run the EDA Script

```bash
cd practice/data-integration/tp2-eda-example
python simple_eda.py
```

## What This Does

The script will:
1. ✅ Load employee data from three CSV files
2. ✅ Display basic statistics and data quality checks
3. ✅ Merge the datasets
4. ✅ Generate visualizations (saved as PNG files)
5. ✅ Answer the key question: "Do salaries vary by sector?"
6. ✅ Print a comprehensive summary report

## Output Files

After running the script, you'll find:
- `employee_eda_charts.png` - 4-panel visualization dashboard
- `salary_by_sector.png` - Bar chart comparing salaries across sectors

## Expected Runtime

⏱️ ~30 seconds (including chart display)

## Troubleshooting

**Issue**: `ModuleNotFoundError: No module named 'pandas'`
- **Solution**: Install dependencies with `pip install pandas matplotlib seaborn`

**Issue**: Charts don't display
- **Solution**: The charts are saved as PNG files even if display fails. Check the current directory.

**Issue**: `FileNotFoundError` for data files
- **Solution**: Make sure you're running from the correct directory (`practice/data-integration/tp2-eda-example`)

## Next Steps

After completing this example:
1. Read the full tutorial: [tp2-simple-eda.md](../tp2-simple-eda.md)
2. Try the additional challenges in the tutorial
3. Build an ETL pipeline with the cleaned insights
