# NURSING HOME INVESTMENT RESEARCH 🏥

## By Minh (Robin) Nguyen ☀️

## Introduction
This research project investigates the financial performance and investment potential of the U.S. nursing home industry, using a near-complete dataset (2015–2021) from the U.S. Department of Health and Human Services.
- The dataset covers over 95% of all licensed nursing home providers, offering a robust foundation for industry-wide analysis.
- The primary objective is to assess whether this sector presents a viable opportunity for investment by examining key operational and financial metrics.

[Interact with my Tableau dashboard here](https://public.tableau.com/app/profile/minh.nguyen5432/viz/nursing_dashboard/FinancialPerformance)

---

## 📂 Data Access

Due to the large size of the files, all data is hosted externally:

🔗 [Access the dataset on Google Drive](https://drive.google.com/drive/folders/1tA6BRJbabY_oJSWi-kBEqZZwCEJ2o9Sp)

---

## 🏛 Data Structure
- 2 main tables: provider info and cost report
- Each table has 7 files, each of them represent a whole year of data from 2017 - 2021
- Each files has approximately 89 columns
- Please refer to the data dictionary in the data access folder to understand the meta data

---

## Data Cleaning Methodology To Ensure Tidy Data 🧹

### Create a function to standardize column name
- Lower all column name
- Change picked columns (19 out of 89 columns)
- Add the year column, which extracted out of the file name

Then loop all files through this function, repeat for table provider info

```python

for key in raw_provider_info:
    raw_provider_info[key].columns = raw_provider_info[key].columns.str.lower()

def create_provider_info_tables(raw_provider_info):
    tables = {}
    
    for key, df in raw_provider_info.items():
        year = key.split('_')[-1]
        
        # List the columns to keep
        # Adjust these column names to match actual data
        columns_to_keep = [
            'provnum', 'federal provider number',
            'provname', 'provider name',
            'address', 'provider address',
            'city', 'provider city',
            'state', 'provider state',
            'year'
        ]
        
        # Only keep columns that exist in the dataframe
        valid_columns = [col for col in columns_to_keep if col in df.columns]
        
        # Create new table with only the columns you need
        if valid_columns:
            tables[f'provider_basic_{year}'] = df[valid_columns].copy()
    
    return tables
```

### Union all files across provider info and cost report table, then merge both into 1 file

```python
nursing_merge = union_provider_info.merge(union_cost_report, 
                                          left_on=['provnum', 'year'], 
                                          right_on=['provider_ccn', 'year'], 
                                          how='right')

```

### Same sample, the fiscal year in each report is different
- Take all value/day in fiscal year * 365
- Compute average (using group by) and deal with missing value (fiscal period = 0)
```python
# Column to adjust
adjust_cols = ['gross_revenue','net_income','net_patient_revenue','total_costs','total_salaries_adjusted','total_income','less_total_operating_expense','net_income_from_patients','other_revenue','snf_admissions_total',
              'total_bed_days_available','total_days_total']

# Adjust value to column
nursing_merge['fiscal_period_days'] = (nursing_merge['fiscal_year_end_date'] - nursing_merge['fiscal_year_begin_date']).dt.days

for col in adjust_cols:
    nursing_merge[col + '_annualized'] = nursing_merge[col] * 365 / nursing_merge['fiscal_period_days']

def consolidate_annual_reports(df: pd.DataFrame) -> pd.DataFrame:
    """
    Consolidates multiple annual reports for the same provider in the same year by:
    - Averaging financial/cost report columns
    - Taking the first value for all other columns
    
    Args:
        df: DataFrame containing hospital annual report data
        
    Returns:
        DataFrame with one row per provider per year
    """
    # Define columns to average (financial metrics)
    financial_cols = [
        'gross_revenue', 'inpatient_revenue', 'net_income', 'net_patient_revenue',
        'total_costs', 'total_income', 'total_salaries_adjusted',
        'gross_revenue_annualized', 'inpatient_revenue_annualized', 
        'net_income_annualized', 'net_patient_revenue_annualized',
        'total_costs_annualized', 'total_salaries_adjusted_annualized', 
        'total_income_annualized', 'less_total_operating_expense','less_total_operating_expense_annualized',
        'net_income_from_patients','net_income_from_patients_annualized','other_revenue','other_revenue_annualized',
        'snf_admissions_total','snf_admissions_total_annualized',
        'total_days_total','total_days_total_annualized',
        'total_bed_days_available','total_bed_days_available_annualized'
    ]
    
    # Create aggregation dictionary - first filter to only include columns that exist in df
    financial_cols = [col for col in financial_cols if col in df.columns]
    
    # Build aggregation dictionary dynamically
    agg_dict = {}
    for col in df.columns:
        if col in financial_cols:
            agg_dict[col] = 'mean'
        elif col not in ['provider_ccn', 'year']:  # Exclude groupby columns
            agg_dict[col] = 'first'
    
    # Group by provider and year, then apply aggregation
    result_df = df.groupby(['provider_ccn', 'year']).agg(agg_dict).reset_index()
    
    return result_df
```

## Key Findings 🔑

### Positive Income Growth Rate
![Income Growth Rate](https://github.com/minhnbnguyen/project3_nursing_home_research/blob/main/graph/IR_income%20growth%20rate.png)
![Income Structure](https://github.com/minhnbnguyen/project3_nursing_home_research/blob/main/graph/FP_income_structure.png)
- This industry witnesses very positive income growth rate, even during Covid-19
- Despite the negative operating growth rate starting since the pandemic, this industry is still witnessing a positive net income growth. This is due to the Government fund serves as a strong source of income.

## Final Suggestions 🧐
- Potential industry to invest into, with guaranteed stream of income from both market demand and government fund
- Focus on metropolitan area like New York or California
