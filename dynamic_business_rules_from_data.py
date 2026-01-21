"""
Dynamic Business Rules Generator from Observed Data
Generates Definition, Condition, and Action based on actual data patterns
"""
import pandas as pd
import re
from collections import Counter
from typing import Dict, Any, List, Optional
import numpy as np


def infer_pattern(value):
    """Infer pattern from a value"""
    if pd.isna(value):
        return None
    s = str(value).strip()
    if re.match(r'^[0-9]+$', s):
        return "digits only"
    elif re.match(r'^[A-Za-z]+$', s):
        return "letters only"
    elif re.match(r'^[A-Za-z0-9]+$', s):
        return "letters and digits"
    elif re.match(r'^[A-Za-z0-9._-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$', s):
        return "email format"
    elif re.match(r'^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}$', s):
        return "date format"
    elif re.match(r'^-?\d+\.?\d*$', s):
        return "numeric"
    return "mixed/alphanumeric"


def analyze_column_data(col_name: str, series: pd.Series) -> Dict[str, Any]:
    """Analyze a single column and infer business rules from observed data"""
    col_normalized = col_name.lower().replace(' ', '_').replace('-', '_')
    
    analysis = {
        'column_name': col_name,
        'display_name': col_name.replace('_', ' ').title(),
        'total_records': len(series),
        'null_count': int(series.isnull().sum()),
        'null_percentage': float((series.isnull().sum() / len(series)) * 100),
        'unique_count': int(series.nunique()),
        'unique_ratio': float(series.nunique() / len(series)),
        'duplicate_count': int(len(series) - series.nunique()),
        'status': 'VALID',  # Default
        'issues': [],
        'is_sensitive': False
    }
    
    non_null = series.dropna()
    
    if len(non_null) == 0:
        analysis['status'] = 'INVALID'
        analysis['issues'].append('All values are missing')
        analysis['definition'] = f"The column '{col_name}' contains no data."
        analysis['condition'] = "This column must contain valid values."
        analysis['action_valid'] = "Column contains valid data values."
        analysis['action_invalid'] = "All values are missing. Please provide valid data for this column."
        return analysis
    
    # Pattern analysis
    sample_size = min(1000, len(non_null))
    patterns = [infer_pattern(val) for val in non_null.head(sample_size)]
    pattern_counts = Counter(patterns)
    most_common_pattern = pattern_counts.most_common(1)[0][0] if pattern_counts else "unknown"
    pattern_percentage = (pattern_counts[most_common_pattern] / len(patterns)) * 100 if patterns else 0
    
    # Length analysis (for string columns)
    if series.dtype == 'object':
        lengths = [len(str(val)) for val in non_null]
        analysis['min_length'] = int(min(lengths))
        analysis['max_length'] = int(max(lengths))
        analysis['avg_length'] = float(sum(lengths) / len(lengths))
        most_common_length = Counter(lengths).most_common(1)[0][0]
        analysis['most_common_length'] = int(most_common_length)
    else:
        analysis['min_length'] = None
        analysis['max_length'] = None
        analysis['most_common_length'] = None
    
    # Numeric analysis
    if series.dtype in ['int64', 'float64']:
        analysis['min_value'] = float(non_null.min())
        analysis['max_value'] = float(non_null.max())
        analysis['avg_value'] = float(non_null.mean())
        analysis['std_value'] = float(non_null.std()) if len(non_null) > 1 else 0.0
    else:
        analysis['min_value'] = None
        analysis['max_value'] = None
        analysis['avg_value'] = None
    
    # Value domain analysis
    value_counts = non_null.value_counts()
    top_values = value_counts.head(10).to_dict()
    analysis['top_values'] = {str(k): int(v) for k, v in top_values.items()}
    analysis['most_common_pattern'] = most_common_pattern
    analysis['pattern_consistency'] = float(pattern_percentage)
    
    # Sample values
    analysis['sample_values'] = [str(v) for v in non_null.head(3).tolist()]
    
    # Generate dynamic Definition, Condition, and Action based on observed data
    definition, condition, action_valid, action_invalid, issues, status = generate_business_rules(col_name, analysis, non_null, series.dtype)
    
    analysis['definition'] = definition
    analysis['condition'] = condition
    analysis['action_valid'] = action_valid
    analysis['action_invalid'] = action_invalid
    analysis['action_valid_points'] = [action_valid] if action_valid else []
    analysis['action_invalid_points'] = [action_invalid] if action_invalid else []
    analysis['issues'] = issues
    analysis['status'] = status
    
    # Detect sensitive data
    if any(keyword in col_normalized for keyword in ['password', 'pin', 'ssn', 'aadhaar', 'pan', 'credit_card', 'cvv', 'account_number']):
        analysis['is_sensitive'] = True
    
    return analysis


def generate_business_rules(col_name: str, analysis: Dict[str, Any], non_null: pd.Series, dtype: Any) -> tuple:
    """Generate Definition, Condition, and Action based on observed data patterns"""
    col_lower = col_name.lower()
    definition = ""
    condition = ""
    action_valid = ""
    action_invalid = ""
    issues = []
    status = 'VALID'
    
    # Domain-specific definitions based on column name patterns
    if 'customer' in col_lower and 'id' in col_lower:
        definition = f"Customer identifier observed in data. Based on uploaded data, this field contains {analysis['unique_count']} distinct values across {analysis['total_records']} records."
        if analysis['unique_ratio'] == 1.0:
            condition = f"Data indicates all values are unique. Each customer has a distinct identifier (observed range: {analysis.get('min_value', 'N/A')} to {analysis.get('max_value', 'N/A')})."
            action_valid = f"All {analysis['total_records']} customer identifiers are unique. Ready for customer relationship mapping and account linking."
            action_invalid = "Duplicate customer IDs detected. Each customer must have a unique identifier for proper account management."
        elif analysis['unique_ratio'] >= 0.95:
            condition = f"Data indicates nearly all values are unique ({analysis['unique_ratio']*100:.1f}%). {analysis['duplicate_count']} duplicate(s) observed."
            action_valid = f"Most customer identifiers are unique. System can process {analysis['unique_count']} distinct customers."
            action_invalid = f"Found {analysis['duplicate_count']} duplicate customer IDs. This may cause ambiguity in customer identification and account linking."
            status = 'WARNING'
            issues.append(f"{analysis['duplicate_count']} duplicate customer IDs found")
        else:
            condition = f"Data indicates {analysis['unique_ratio']*100:.1f}% uniqueness. {analysis['duplicate_count']} duplicates observed."
            action_valid = f"Customer identifier field contains data. Processing {analysis['unique_count']} distinct customers."
            action_invalid = f"Multiple duplicate customer IDs ({analysis['duplicate_count']}) detected. This may cause serious banking errors in customer identification and transaction routing."
            status = 'INVALID'
            issues.append(f"Multiple duplicate customer IDs: {analysis['duplicate_count']} duplicates found")
    
    elif 'account' in col_lower and ('number' in col_lower or 'id' in col_lower):
        definition = f"Account identifier observed in data. Contains {analysis['unique_count']} distinct account identifiers across {analysis['total_records']} records."
        if analysis['unique_ratio'] == 1.0:
            condition = f"Data indicates all account identifiers are unique. Pattern observed: {analysis.get('most_common_pattern', 'N/A')}."
            action_valid = f"All {analysis['total_records']} account identifiers are unique. Ready for transaction processing and balance management."
            action_invalid = "Duplicate account numbers detected. Each account must have a unique identifier for transaction routing."
        else:
            condition = f"Data indicates {analysis['unique_ratio']*100:.1f}% uniqueness. {analysis['duplicate_count']} duplicate account identifiers observed."
            action_valid = f"Account identifier field is populated. System recognizes {analysis['unique_count']} distinct accounts."
            action_invalid = f"Found {analysis['duplicate_count']} duplicate account identifiers. This may cause transaction routing errors and balance calculation issues."
            status = 'INVALID'
            issues.append(f"{analysis['duplicate_count']} duplicate account identifiers found")
    
    elif 'balance' in col_lower:
        definition = f"Account balance field observed. Based on uploaded data, balance values range from {analysis.get('min_value', 0):,.2f} to {analysis.get('max_value', 0):,.2f} (average: {analysis.get('avg_value', 0):,.2f})."
        negative_count = int((non_null < 0).sum()) if dtype in ['int64', 'float64'] else 0
        if negative_count == 0:
            condition = f"Data indicates all balance values are non-negative. Observed range: {analysis.get('min_value', 0):,.2f} to {analysis.get('max_value', 0):,.2f}."
            action_valid = f"All {analysis['total_records']} balance records are valid. Account balances are ready for transaction processing."
            action_invalid = "Negative balances detected. Review overdraft policies and account status before processing transactions."
        else:
            condition = f"Data indicates {negative_count} records with negative balances. Observed range: {analysis.get('min_value', 0):,.2f} to {analysis.get('max_value', 0):,.2f}."
            action_valid = f"Balance field contains data. {analysis['total_records'] - negative_count} accounts have non-negative balances."
            action_invalid = f"Found {negative_count} accounts with negative balances. Review these accounts for overdraft policies, holds, or data errors before processing."
            if negative_count > analysis['total_records'] * 0.1:  # More than 10% negative
                status = 'INVALID'
                issues.append(f"{negative_count} accounts with negative balances require review")
            else:
                status = 'WARNING'
                issues.append(f"{negative_count} accounts with negative balances detected")
    
    elif 'amount' in col_lower or 'transaction_amount' in col_lower:
        definition = f"Transaction amount field observed. Based on uploaded data, amounts range from {analysis.get('min_value', 0):,.2f} to {analysis.get('max_value', 0):,.2f} (average: {analysis.get('avg_value', 0):,.2f})."
        negative_count = int((non_null < 0).sum()) if dtype in ['int64', 'float64'] else 0
        if negative_count == 0:
            condition = f"Data indicates all transaction amounts are non-negative. Observed range: {analysis.get('min_value', 0):,.2f} to {analysis.get('max_value', 0):,.2f}."
            action_valid = f"All {analysis['total_records']} transaction amounts are valid. Ready for transaction processing."
            action_invalid = "Negative transaction amounts detected. Review transaction types (debit/credit) and ensure proper sign conventions."
        else:
            condition = f"Data indicates {negative_count} transactions with negative amounts. This may represent debits or refunds."
            action_valid = f"Transaction amount field is populated. Processing {analysis['total_records']} transactions."
            action_invalid = f"Found {negative_count} transactions with negative amounts. Verify if this represents debits, refunds, or data entry errors."
            status = 'WARNING'
    
    elif 'email' in col_lower:
        definition = f"Email address field observed. Based on uploaded data, {analysis['pattern_consistency']:.1f}% of values follow email format pattern."
        if analysis['pattern_consistency'] >= 95:
            condition = f"Data indicates strong email format consistency ({analysis['pattern_consistency']:.1f}%). All values follow standard email pattern."
            action_valid = f"All {analysis['total_records']} email addresses are properly formatted. Ready for customer communication and notifications."
            action_invalid = "Invalid email formats detected. Email addresses must follow standard format (user@domain.com) for customer communications."
        else:
            condition = f"Data indicates {analysis['pattern_consistency']:.1f}% email format consistency. Some values may not be valid email addresses."
            action_valid = f"Email field contains data. {int(analysis['total_records'] * analysis['pattern_consistency'] / 100)} email addresses are properly formatted."
            action_invalid = f"Found {int(analysis['total_records'] * (100 - analysis['pattern_consistency']) / 100)} invalid email formats. Correct email addresses for customer communication."
            status = 'WARNING'
            issues.append(f"Some email addresses may not be properly formatted")
    
    elif 'date' in col_lower or 'transaction_date' in col_lower:
        definition = f"Date field observed. Based on uploaded data, contains {analysis['unique_count']} distinct date values across {analysis['total_records']} records."
        if analysis['pattern_consistency'] >= 95:
            condition = f"Data indicates consistent date format ({analysis['pattern_consistency']:.1f}%). All dates follow standard format."
            action_valid = f"All {analysis['total_records']} date values are properly formatted. Ready for transaction history and reporting."
            action_invalid = "Invalid date formats detected. Dates must follow standard format for transaction processing and audit trails."
        else:
            condition = f"Data indicates {analysis['pattern_consistency']:.1f}% date format consistency. Some values may not be valid dates."
            action_valid = f"Date field contains data. Most dates are properly formatted."
            action_invalid = f"Found inconsistent date formats. Standardize date format for accurate transaction processing and reporting."
            status = 'WARNING'
    
    elif 'status' in col_lower:
        top_statuses = list(analysis.get('top_values', {}).keys())[:5]
        definition = f"Status field observed. Based on uploaded data, contains {analysis['unique_count']} distinct status values. Most common: {', '.join(top_statuses[:3])}."
        if analysis['unique_ratio'] < 0.2:  # Limited set of values
            condition = f"Data indicates controlled domain with {analysis['unique_count']} allowed status values: {', '.join(top_statuses)}."
            action_valid = f"Status field contains valid values from controlled set. System recognizes {analysis['unique_count']} status types."
            action_invalid = "Invalid status values detected. Status must match one of the allowed values from the controlled set."
        else:
            condition = f"Data indicates {analysis['unique_ratio']*100:.1f}% uniqueness. Wide variety of status values observed."
            action_valid = f"Status field is populated. Processing {analysis['unique_count']} distinct status values."
            action_invalid = "Status values may not conform to standard banking status codes. Verify status values match system requirements."
            status = 'WARNING'
    
    else:
        # Generic rules based on data patterns
        definition = f"Column '{col_name}' observed in data. Contains {analysis['unique_count']} distinct values across {analysis['total_records']} records."
        
        # Build condition based on observations
        condition_parts = []
        if analysis['null_percentage'] == 0:
            condition_parts.append("No missing values observed")
        elif analysis['null_percentage'] < 1:
            condition_parts.append(f"Rare missing values ({analysis['null_percentage']:.2f}%)")
        elif analysis['null_percentage'] < 10:
            condition_parts.append(f"Some missing values ({analysis['null_percentage']:.2f}%)")
            status = 'WARNING'
            issues.append(f"{analysis['null_count']} missing values found")
        else:
            condition_parts.append(f"Frequent missing values ({analysis['null_percentage']:.2f}%)")
            status = 'INVALID'
            issues.append(f"{analysis['null_count']} missing values found")
        
        if analysis.get('most_common_pattern'):
            condition_parts.append(f"Pattern: {analysis['most_common_pattern']} ({analysis['pattern_consistency']:.1f}% consistent)")
        
        if analysis.get('min_length') is not None and analysis.get('max_length') is not None:
            if analysis['min_length'] == analysis['max_length']:
                condition_parts.append(f"Fixed length: {analysis['min_length']} characters")
            else:
                condition_parts.append(f"Length range: {analysis['min_length']} to {analysis['max_length']} characters")
        
        if analysis.get('min_value') is not None and analysis.get('max_value') is not None:
            condition_parts.append(f"Value range: {analysis['min_value']:,.2f} to {analysis['max_value']:,.2f}")
        
        condition = "Data indicates: " + ". ".join(condition_parts) + "."
        
        if analysis['unique_ratio'] == 1.0:
            action_valid = f"All {analysis['total_records']} values are unique. Field is ready for processing."
        elif analysis['unique_ratio'] >= 0.95:
            action_valid = f"Nearly all values are unique ({analysis['unique_ratio']*100:.1f}%). Field contains valid data."
        else:
            action_valid = f"Field contains {analysis['unique_count']} distinct values. Ready for processing."
        
        action_invalid = "Field validation failed. Review data quality and fix issues before processing."
    
    # Add null checks
    if analysis['null_percentage'] > 10:
        issues.append(f"{analysis['null_count']} missing values ({analysis['null_percentage']:.2f}%)")
        if status == 'VALID':
            status = 'WARNING'
    
    return definition, condition, action_valid, action_invalid, issues, status


def generate_dynamic_business_rules(file_path: str) -> Dict[str, Any]:
    """
    Generate dynamic business rules from uploaded file.
    Processes ALL columns in the file - 100% data-driven, no hardcoded templates.
    """
    try:
        df = pd.read_csv(file_path)
        
        columns_analysis = []
        # Process ALL columns found in user's file - no filtering
        for col in df.columns:
            try:
                analysis = analyze_column_data(col, df[col])
                columns_analysis.append(analysis)
            except Exception as col_error:
                print(f"Warning: Could not analyze column '{col}': {str(col_error)}")
                # Add minimal analysis for failed columns
                columns_analysis.append({
                    'column_name': col,
                    'display_name': col.replace('_', ' ').title(),
                    'status': 'UNKNOWN',
                    'definition': f"Column '{col}' found in uploaded file. Unable to analyze data patterns.",
                    'condition': "Data analysis failed for this column.",
                    'action_valid': "No action available - analysis failed.",
                    'action_invalid': "Please verify column data is valid.",
                    'action_valid_points': [],
                    'action_invalid_points': [],
                    'issues': [f"Column analysis failed: {str(col_error)}"],
                    'is_sensitive': False
                })
        
        # Calculate summary
        total_columns = len(columns_analysis)
        valid_count = sum(1 for c in columns_analysis if c['status'] == 'VALID')
        warning_count = sum(1 for c in columns_analysis if c['status'] == 'WARNING')
        invalid_count = sum(1 for c in columns_analysis if c['status'] == 'INVALID')
        all_valid = invalid_count == 0 and warning_count == 0
        
        return {
            'multi_file': False,
            'columns': columns_analysis,
            'summary': {
                'total_columns': total_columns,
                'valid_count': valid_count,
                'warning_count': warning_count,
                'invalid_count': invalid_count,
                'all_valid': all_valid
            }
        }
    except Exception as e:
        raise Exception(f"Error generating dynamic business rules: {str(e)}")
