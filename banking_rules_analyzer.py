import pandas as pd
import re
from collections import Counter
from datetime import datetime

# Read the dataset
df = pd.read_csv(r'.\global-prediction\uploads\Comprehensive_Banking_Database[1].csv')

print("=" * 80)
print("CORE BANKING DOMAIN DATA OBSERVATION & BUSINESS RULES INFERENCE REPORT")
print("=" * 80)
print(f"\nDataset: Comprehensive_Banking_Database")
print(f"Total Records: {len(df):,}")
print(f"Total Banking Domain Columns Identified: {len(df.columns)}\n")

def infer_pattern(value):
    """Infer pattern from a value"""
    if pd.isna(value):
        return None
    s = str(value)
    if re.match(r'^[0-9]+$', s):
        return "digits only"
    elif re.match(r'^[A-Za-z]+$', s):
        return "letters only"
    elif re.match(r'^[A-Za-z0-9]+$', s):
        return "letters and digits"
    elif re.match(r'^[A-Za-z0-9._-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$', s):
        return "email format"
    elif re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', s):
        return "date format (MM/DD/YYYY)"
    elif re.match(r'^-?\d+\.?\d*$', s):
        return "numeric"
    return "mixed/alphanumeric"

def analyze_column(col_name, series):
    """Analyze a single column and infer banking business rules"""
    analysis = {
        'column': col_name,
        'total_records': len(series),
        'null_count': series.isnull().sum(),
        'null_percentage': (series.isnull().sum() / len(series)) * 100,
        'unique_count': series.nunique(),
        'unique_ratio': series.nunique() / len(series),
        'duplicate_count': len(series) - series.nunique()
    }
    
    non_null = series.dropna()
    
    if len(non_null) > 0:
        # Pattern analysis
        patterns = [infer_pattern(val) for val in non_null.head(1000)]
        pattern_counts = Counter(patterns)
        most_common_pattern = pattern_counts.most_common(1)[0][0] if pattern_counts else "unknown"
        pattern_percentage = (pattern_counts[most_common_pattern] / len(patterns)) * 100 if patterns else 0
        
        # Length analysis (for string columns)
        if series.dtype == 'object':
            lengths = [len(str(val)) for val in non_null]
            analysis['min_length'] = min(lengths)
            analysis['max_length'] = max(lengths)
            analysis['avg_length'] = sum(lengths) / len(lengths)
            most_common_length = Counter(lengths).most_common(1)[0][0]
            analysis['most_common_length'] = most_common_length
        else:
            analysis['min_length'] = None
            analysis['max_length'] = None
        
        # Numeric analysis
        if series.dtype in ['int64', 'float64']:
            analysis['min_value'] = float(non_null.min())
            analysis['max_value'] = float(non_null.max())
            analysis['avg_value'] = float(non_null.mean())
        else:
            analysis['min_value'] = None
            analysis['max_value'] = None
        
        # Value domain analysis
        value_counts = non_null.value_counts()
        top_values = value_counts.head(5).to_dict()
        analysis['top_values'] = top_values
        analysis['most_common_pattern'] = most_common_pattern
        analysis['pattern_consistency'] = pattern_percentage
        
        # Sample values
        analysis['sample_values'] = non_null.head(3).tolist()
    
    return analysis

# Analyze all columns
banking_columns = []
for col in df.columns:
    analysis = analyze_column(col, df[col])
    banking_columns.append(analysis)

# Generate banking-style report
print("\n" + "=" * 80)
print("BUSINESS RULES OBSERVATION BY BANKING DOMAIN")
print("=" * 80 + "\n")

for col_analysis in banking_columns:
    col = col_analysis['column']
    print(f"\n{'-' * 80}")
    print(f"COLUMN: {col}")
    print(f"{'-' * 80}")
    
    # Uniqueness behavior
    unique_ratio = col_analysis['unique_ratio']
    if unique_ratio == 1.0:
        print(f"✔ Data indicates: All {col_analysis['total_records']:,} values are unique")
    elif unique_ratio >= 0.95:
        print(f"✔ Data indicates: Nearly all values are unique ({unique_ratio*100:.1f}% unique)")
    elif unique_ratio >= 0.8:
        print(f"⚠ Data indicates: Most values are unique ({unique_ratio*100:.1f}% unique, {col_analysis['duplicate_count']} duplicates observed)")
    elif unique_ratio < 0.1:
        print(f"⚠ Data indicates: Limited value set - only {col_analysis['unique_count']} distinct values across {col_analysis['total_records']:,} records")
    else:
        print(f"⚠ Data indicates: Moderate uniqueness ({unique_ratio*100:.1f}% unique, {col_analysis['duplicate_count']} duplicates observed)")
    
    # Null behavior
    null_pct = col_analysis['null_percentage']
    if null_pct == 0:
        print(f"✔ Data indicates: No missing values observed")
    elif null_pct < 1:
        print(f"⚠ Data indicates: Rare missing values ({null_pct:.2f}% missing, {col_analysis['null_count']} records)")
    elif null_pct < 10:
        print(f"⚠ Data indicates: Some missing values ({null_pct:.2f}% missing, {col_analysis['null_count']} records)")
    else:
        print(f"🚩 Data indicates: Frequent missing values ({null_pct:.2f}% missing, {col_analysis['null_count']} records)")
    
    # Length behavior (for text fields)
    if col_analysis['min_length'] is not None:
        if col_analysis['min_length'] == col_analysis['max_length']:
            print(f"✔ Data indicates: Consistent length - all values are {col_analysis['min_length']} characters")
        elif col_analysis['max_length'] - col_analysis['min_length'] <= 2:
            print(f"✔ Data indicates: Very consistent length - ranges from {col_analysis['min_length']} to {col_analysis['max_length']} characters (most common: {col_analysis['most_common_length']})")
        else:
            print(f"⚠ Data indicates: Variable length - ranges from {col_analysis['min_length']} to {col_analysis['max_length']} characters (most common: {col_analysis['most_common_length']})")
    
    # Pattern behavior
    if col_analysis.get('most_common_pattern'):
        pattern_pct = col_analysis.get('pattern_consistency', 0)
        pattern = col_analysis['most_common_pattern']
        if pattern_pct >= 95:
            print(f"✔ Data indicates: Strong pattern consistency - {pattern_pct:.1f}% follow '{pattern}' pattern")
        elif pattern_pct >= 80:
            print(f"⚠ Data indicates: Mostly consistent pattern - {pattern_pct:.1f}% follow '{pattern}' pattern")
        else:
            print(f"⚠ Data indicates: Mixed patterns - most common is '{pattern}' ({pattern_pct:.1f}%)")
    
    # Numeric value behavior
    if col_analysis.get('min_value') is not None:
        min_val = col_analysis['min_value']
        max_val = col_analysis['max_value']
        avg_val = col_analysis['avg_value']
        if 'Balance' in col or 'Amount' in col or 'Limit' in col:
            if min_val < 0:
                print(f"🚩 Data indicates: Negative values observed - minimum value is {min_val:,.2f}")
            else:
                print(f"✔ Data indicates: All values are non-negative (range: {min_val:,.2f} to {max_val:,.2f})")
        print(f"✔ Data indicates: Value range from {min_val:,.2f} to {max_val:,.2f} (average: {avg_val:,.2f})")
    
    # Value domain behavior
    if col_analysis.get('top_values'):
        top_vals = col_analysis['top_values']
        if len(top_vals) <= 5 and unique_ratio < 0.1:
            print(f"✔ Data indicates: Limited domain - values are from a controlled set: {list(top_vals.keys())[:5]}")
        elif len(top_vals) <= 10:
            print(f"⚠ Data indicates: Moderately controlled domain - top values: {list(top_vals.keys())[:3]}")
    
    # Special handling for Customer ID and Account-like fields
    if 'Customer ID' in col or 'customer' in col.lower() and 'id' in col.lower():
        if unique_ratio < 1.0:
            print(f"🚩 Banking Risk: Duplicate Customer IDs detected - may cause ambiguity in customer identification")
    
    if 'Account' in col and ('Number' in col or 'ID' in col):
        if unique_ratio < 1.0:
            print(f"🚩 Banking Risk: Duplicate Account identifiers detected - may cause transaction routing errors")
    
    if 'Transaction' in col and 'ID' in col:
        if unique_ratio < 1.0:
            print(f"🚩 Banking Risk: Duplicate Transaction IDs detected - may cause audit and reconciliation issues")
    
    # Balance consistency check
    if 'Balance' in col:
        non_null = df[col].dropna()
        if non_null.dtype in ['int64', 'float64']:
            negative_count = (non_null < 0).sum()
            if negative_count > 0:
                print(f"🚩 Banking Risk: {negative_count} records show negative balances - requires business review for overdraft policies")

print(f"\n\n{'=' * 80}")
print("REPORT SUMMARY")
print(f"{'=' * 80}")
print(f"\nTotal Banking Domain Columns Analyzed: {len(banking_columns)}")
print(f"Columns with Complete Data: {sum(1 for c in banking_columns if c['null_percentage'] == 0)}")
print(f"Columns Requiring Attention: {sum(1 for c in banking_columns if c['null_percentage'] > 0 or c['unique_ratio'] < 0.95 or (c.get('min_value') is not None and 'Balance' in c['column'] and c['min_value'] < 0))}")
print(f"\nReport generated from actual data observations only.")
print(f"No assumptions or hardcoded banking standards were applied.\n")
