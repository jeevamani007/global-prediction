import pandas as pd
import re
from typing import Dict, List, Any, Tuple
from datetime import datetime
import numpy as np


class BankingDatasetValidator:
    """
    A comprehensive banking dataset validator that identifies columns, 
    determines domains, applies business rules, and generates reports.
    """
    
    def __init__(self):
        # Define predefined banking column rules
        self.column_rules = {
            "account_number": {
                "description": "Unique per account, Numeric only, 6-18 digits",
                "validation_func": self.validate_account_number,
                "required": False
            },
            "customer_id": {
                "description": "Unique per customer, Alphanumeric (e.g., CUST001)",
                "validation_func": self.validate_customer_id,
                "required": False
            },
            "customer_name": {
                "description": "Letters and spaces only, Minimum 3 characters",
                "validation_func": self.validate_customer_name,
                "required": False
            },
            "account_type": {
                "description": "Only Savings, Current, Salary",
                "validation_func": self.validate_account_type,
                "required": False
            },
            "branch": {
                "description": "Alphanumeric only, Optional spaces",
                "validation_func": self.validate_branch,
                "required": False
            },
            "ifsc_code": {
                "description": "Exactly 11 characters, Letters + numbers",
                "validation_func": self.validate_ifsc_code,
                "required": False
            },
            "transaction_id": {
                "description": "Unique per transaction, Alphanumeric",
                "validation_func": self.validate_transaction_id,
                "required": False
            },
            "txn_date": {
                "description": "Format YYYY-MM-DD",
                "validation_func": self.validate_txn_date,
                "required": False
            },
            "transaction_type": {
                "description": "Only DEBIT or CREDIT",
                "validation_func": self.validate_transaction_type,
                "required": False
            },
            "amount": {
                "description": "Numeric, positive, for transactions",
                "validation_func": self.validate_amount,
                "required": False
            },
            "debit": {
                "description": "Numeric, positive, mutually exclusive with credit",
                "validation_func": self.validate_debit,
                "required": False
            },
            "credit": {
                "description": "Numeric, positive, mutually exclusive with debit",
                "validation_func": self.validate_credit,
                "required": False
            }
        }
        
        # Common variations for column names
        self.column_variations = {
            "account_number": ["account_number", "account_no", "acc_no", "accno", "account"],
            "customer_id": ["customer_id", "cust_id", "customerid", "custid", "client_id", "clientid"],
            "customer_name": ["customer_name", "customer_name", "cust_name", "client_name", "name"],
            "account_type": ["account_type", "acct_type", "type", "accounttype"],
            "branch": ["branch", "branch_name", "branch_code", "branchid"],
            "ifsc_code": ["ifsc_code", "ifsc", "ifsccode", "bank_code"],
            "transaction_id": ["transaction_id", "trans_id", "txnid", "transactionid", "id"],
            "txn_date": ["txn_date", "transaction_date", "date", "trans_date", "tx_date"],
            "transaction_type": ["transaction_type", "trans_type", "type", "transtype"],
            "amount": ["amount", "amt", "transaction_amount", "trans_amount"],
            "debit": ["debit", "dr", "debits", "debit_amount"],
            "credit": ["credit", "cr", "credits", "credit_amount"]
        }
    
    def identify_column_purposes(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Identify the purpose of each column based on name patterns and data characteristics.
        """
        identified_columns = {}
        
        for col in df.columns:
            col_lower = col.lower().strip()
            
            # Try to match column name variations
            matched = False
            for purpose, variations in self.column_variations.items():
                if any(variation.lower() in col_lower for variation in variations):
                    identified_columns[col] = purpose
                    matched = True
                    break
            
            # If no direct match, try to infer from data characteristics
            if not matched:
                sample_data = df[col].dropna().head(10)  # Sample non-null values
                
                if sample_data.dtype in ['int64', 'float64']:
                    # Check if it looks like account number (6-18 digits)
                    if sample_data.astype(str).str.contains(r'^\d{6,18}$').all():
                        identified_columns[col] = "account_number"
                    # Check if it looks like amount
                    elif sample_data.gt(0).all():
                        identified_columns[col] = "amount"
                    else:
                        identified_columns[col] = "unknown_numeric"
                elif sample_data.dtype == 'object':
                    # Check if it looks like dates
                    try:
                        pd.to_datetime(sample_data)
                        identified_columns[col] = "txn_date"
                    except:
                        # Check if it looks like transaction types
                        if sample_data.str.upper().isin(['DEBIT', 'CREDIT']).all():
                            identified_columns[col] = "transaction_type"
                        # Check if it looks like names
                        elif sample_data.str.contains(r'^[A-Za-z\s]+$').all():
                            identified_columns[col] = "customer_name"
                        else:
                            identified_columns[col] = "unknown_text"
                else:
                    identified_columns[col] = "unknown"
        
        return identified_columns
    
    def validate_account_number(self, series: pd.Series) -> Tuple[str, float, List[str], List[str]]:
        """Validate account number: unique, numeric, 6-18 digits"""
        issues = []
        reasons = []
        total_count = len(series)
        non_null_count = series.dropna().count()
        
        if non_null_count == 0:
            return "FAIL", 0.0, ["Column is empty"], ["No data to validate"]
        
        # Convert to string for validation
        series_str = series.dropna().astype(str)
        
        # Check if all values are numeric
        numeric_mask = series_str.str.match(r'^\d+$')
        numeric_ratio = numeric_mask.mean()
        
        if numeric_ratio < 0.95:
            non_numeric_values = series_str[~numeric_mask].head(3).tolist()
            issues.append(f"Non-numeric values found: {(1 - numeric_ratio) * 100:.1f}%")
            reasons.append(f"Found non-numeric values: {non_numeric_values}")
        else:
            reasons.append("All values are numeric")
        
        # Check length (6-18 digits)
        length_ok_mask = series_str.str.len().between(6, 18)
        length_ok_ratio = length_ok_mask.mean()
        
        if length_ok_ratio < 0.95:
            invalid_length_values = series_str[~length_ok_mask].head(3).tolist()
            issues.append(f"Invalid length values: {(1 - length_ok_ratio) * 100:.1f}% (expected 6-18 digits)")
            reasons.append(f"Found values with invalid length: {invalid_length_values}")
        else:
            reasons.append("All values have valid length (6-18 digits)")
        
        # Check uniqueness
        unique_count = series_str.nunique()
        unique_ratio = unique_count / len(series_str) if len(series_str) > 0 else 0
        
        if unique_ratio < 0.95:
            issues.append(f"Low uniqueness: {unique_ratio * 100:.1f}% unique values")
            reasons.append("Values are not sufficiently unique - may indicate duplicate account numbers")
        else:
            reasons.append("Values are unique as expected for account numbers")
        
        # Calculate confidence based on validation results
        confidence = min(numeric_ratio, length_ok_ratio, unique_ratio)
        
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues, reasons
    
    def validate_customer_id(self, series: pd.Series) -> Tuple[str, float, List[str], List[str]]:
        """Validate customer ID: unique, alphanumeric"""
        issues = []
        reasons = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"], ["No data to validate"]
        
        # Check if alphanumeric
        alphanum_mask = non_null.str.match(r'^[A-Za-z0-9]+$')
        alphanum_ratio = alphanum_mask.mean()
        
        if alphanum_ratio < 0.95:
            non_alphanum_values = non_null[~alphanum_mask].head(3).tolist()
            issues.append(f"Non-alphanumeric values found: {(1 - alphanum_ratio) * 100:.1f}%")
            reasons.append(f"Found non-alphanumeric values: {non_alphanum_values}")
        else:
            reasons.append("All values are alphanumeric as expected for customer IDs")
        
        # Check uniqueness
        unique_ratio = non_null.nunique() / len(non_null) if len(non_null) > 0 else 0
        
        if unique_ratio < 0.95:
            issues.append(f"Low uniqueness: {unique_ratio * 100:.1f}% unique values")
            reasons.append("Values are not sufficiently unique - customer IDs should be unique identifiers")
        else:
            reasons.append("Values are unique as expected for customer IDs")
        
        confidence = min(alphanum_ratio, unique_ratio)
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues, reasons
    
    def validate_customer_name(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate customer name: letters and spaces only, minimum 3 characters"""
        issues = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        # Check if contains only letters and spaces
        letters_spaces_mask = non_null.str.match(r'^[A-Za-z\s]+$')
        letters_spaces_ratio = letters_spaces_mask.mean()
        
        if letters_spaces_ratio < 0.95:
            issues.append(f"Non-letter/space characters found: {(1 - letters_spaces_ratio) * 100:.1f}%")
        
        # Check minimum length
        min_length_mask = non_null.str.len() >= 3
        min_length_ratio = min_length_mask.mean()
        
        if min_length_ratio < 0.95:
            issues.append(f"Names shorter than 3 characters: {(1 - min_length_ratio) * 100:.1f}%")
        
        confidence = min(letters_spaces_ratio, min_length_ratio)
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_account_type(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate account type: only Savings, Current, Salary"""
        issues = []
        non_null = series.dropna().str.upper().str.strip()
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        allowed_types = {'SAVINGS', 'CURRENT', 'SALARY'}
        valid_mask = non_null.isin(allowed_types)
        valid_ratio = valid_mask.mean()
        
        if valid_ratio < 0.95:
            invalid_values = non_null[~valid_mask].unique().tolist()[:5]
            issues.append(f"Invalid account types: {invalid_values}")
        
        confidence = valid_ratio
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_branch(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate branch: alphanumeric only with optional spaces"""
        issues = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        # Check if alphanumeric with optional spaces
        alphanum_spaces_mask = non_null.str.match(r'^[A-Za-z0-9\s]+$')
        alphanum_spaces_ratio = alphanum_spaces_mask.mean()
        
        if alphanum_spaces_ratio < 0.95:
            issues.append(f"Non-alphanumeric characters found: {(1 - alphanum_spaces_ratio) * 100:.1f}%")
        
        confidence = alphanum_spaces_ratio
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_ifsc_code(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate IFSC code: alphanumeric, 3-15 characters (flexible format)"""
        issues = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        # Check if length is between 3-15 characters (flexible format)
        length_mask = non_null.str.len().between(3, 15)
        length_ratio = length_mask.mean()
        
        if length_ratio < 0.95:
            issues.append(f"Values not between 3-15 characters: {(1 - length_ratio) * 100:.1f}%")
        
        # Check if alphanumeric
        alphanum_mask = non_null.str.match(r'^[A-Za-z0-9]+$')
        alphanum_ratio = alphanum_mask.mean()
        
        if alphanum_ratio < 0.95:
            issues.append(f"Non-alphanumeric values found: {(1 - alphanum_ratio) * 100:.1f}%")
        
        confidence = min(length_ratio, alphanum_ratio)
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_transaction_id(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate transaction ID: unique, alphanumeric"""
        issues = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        # Check if alphanumeric
        alphanum_mask = non_null.str.match(r'^[A-Za-z0-9]+$')
        alphanum_ratio = alphanum_mask.mean()
        
        if alphanum_ratio < 0.95:
            issues.append(f"Non-alphanumeric values found: {(1 - alphanum_ratio) * 100:.1f}%")
        
        # Check uniqueness
        unique_ratio = non_null.nunique() / len(non_null) if len(non_null) > 0 else 0
        
        if unique_ratio < 0.95:
            issues.append(f"Low uniqueness: {unique_ratio * 100:.1f}% unique values")
        
        confidence = min(alphanum_ratio, unique_ratio)
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_txn_date(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate transaction date: format YYYY-MM-DD"""
        issues = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        # Check date format
        valid_dates = 0
        total_checked = 0
        
        for date_val in non_null:
            try:
                # Try to parse as YYYY-MM-DD
                datetime.strptime(date_val, '%Y-%m-%d')
                valid_dates += 1
            except ValueError:
                try:
                    # Also try other common formats
                    pd.to_datetime(date_val)
                    valid_dates += 1
                except:
                    pass
            total_checked += 1
        
        if total_checked > 0:
            valid_ratio = valid_dates / total_checked
        else:
            valid_ratio = 0
        
        if valid_ratio < 0.95:
            issues.append(f"Invalid date formats: {(1 - valid_ratio) * 100:.1f}%")
        
        confidence = valid_ratio
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_transaction_type(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate transaction type: only DEBIT or CREDIT"""
        issues = []
        non_null = series.dropna().str.upper().str.strip()
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        allowed_types = {'DEBIT', 'CREDIT'}
        valid_mask = non_null.isin(allowed_types)
        valid_ratio = valid_mask.mean()
        
        if valid_ratio < 0.95:
            invalid_values = non_null[~valid_mask].unique().tolist()[:5]
            issues.append(f"Invalid transaction types: {invalid_values}")
        
        confidence = valid_ratio
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_amount(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate amount: numeric, positive"""
        issues = []
        numeric_series = pd.to_numeric(series, errors='coerce')
        non_null = numeric_series.dropna()
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty or non-numeric"]
        
        # Check if all values are numeric
        numeric_ratio = 1.0 - (series.isna().sum() + pd.to_numeric(series, errors='coerce').isna().sum()) / len(series)
        
        # Check if all values are positive
        positive_mask = non_null > 0
        positive_ratio = positive_mask.mean()
        
        if positive_ratio < 0.95:
            issues.append(f"Non-positive values found: {(1 - positive_ratio) * 100:.1f}%")
        
        confidence = min(numeric_ratio, positive_ratio)
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_debit(self, series: pd.Series, credit_series=None) -> Tuple[str, float, List[str]]:
        """Validate debit: numeric, positive, mutually exclusive with credit"""
        issues = []
        numeric_series = pd.to_numeric(series, errors='coerce')
        non_null = numeric_series.dropna()
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty or non-numeric"]
        
        # Check if all values are numeric
        numeric_ratio = 1.0 - (series.isna().sum() + pd.to_numeric(series, errors='coerce').isna().sum()) / len(series)
        
        # Check if all values are positive
        positive_mask = non_null >= 0  # Allow zero for debit
        positive_ratio = positive_mask.mean()
        
        if positive_ratio < 0.95:
            issues.append(f"Negative values found: {(1 - positive_ratio) * 100:.1f}%")
        
        # Check mutual exclusivity with credit if credit column exists
        if credit_series is not None:
            debit_non_null = pd.to_numeric(series, errors='coerce').fillna(0)
            credit_non_null = pd.to_numeric(credit_series, errors='coerce').fillna(0)
            
            # Find rows where both debit and credit are > 0
            both_positive = ((debit_non_null > 0) & (credit_non_null > 0)).sum()
            both_positive_ratio = both_positive / len(series)
            
            if both_positive_ratio > 0.05:  # More than 5% have both > 0
                issues.append(f"Both debit and credit > 0 in {both_positive_ratio * 100:.1f}% of rows")
        
        confidence = min(numeric_ratio, positive_ratio)
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_credit(self, series: pd.Series, debit_series=None) -> Tuple[str, float, List[str]]:
        """Validate credit: numeric, positive, mutually exclusive with debit"""
        issues = []
        numeric_series = pd.to_numeric(series, errors='coerce')
        non_null = numeric_series.dropna()
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty or non-numeric"]
        
        # Check if all values are numeric
        numeric_ratio = 1.0 - (series.isna().sum() + pd.to_numeric(series, errors='coerce').isna().sum()) / len(series)
        
        # Check if all values are positive
        positive_mask = non_null >= 0  # Allow zero for credit
        positive_ratio = positive_mask.mean()
        
        if positive_ratio < 0.95:
            issues.append(f"Negative values found: {(1 - positive_ratio) * 100:.1f}%")
        
        # Check mutual exclusivity with debit if debit column exists
        if debit_series is not None:
            credit_non_null = pd.to_numeric(series, errors='coerce').fillna(0)
            debit_non_null = pd.to_numeric(debit_series, errors='coerce').fillna(0)
            
            # Find rows where both credit and debit are > 0
            both_positive = ((credit_non_null > 0) & (debit_non_null > 0)).sum()
            both_positive_ratio = both_positive / len(series)
            
            if both_positive_ratio > 0.05:  # More than 5% have both > 0
                issues.append(f"Both credit and debit > 0 in {both_positive_ratio * 100:.1f}% of rows")
        
        confidence = min(numeric_ratio, positive_ratio)
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def apply_cross_column_validations(self, df: pd.DataFrame, identified_columns: Dict[str, str]) -> List[Dict[str, Any]]:
        """Apply validations that involve multiple columns."""
        cross_validations = []
        
        # Look for debit/credit mutual exclusivity
        debit_cols = [col for col, purpose in identified_columns.items() if purpose in ["debit", "amount"]]
        credit_cols = [col for col, purpose in identified_columns.items() if purpose in ["credit", "amount"]]
        
        if debit_cols and credit_cols:
            debit_series = pd.to_numeric(df[debit_cols[0]], errors="coerce").fillna(0)
            credit_series = pd.to_numeric(df[credit_cols[0]], errors="coerce").fillna(0)
            
            both_positive_mask = (debit_series > 0) & (credit_series > 0)
            both_positive_count = both_positive_mask.sum()
            
            if both_positive_count > len(df) * 0.05:  # More than 5% of rows
                cross_validations.append({
                    "validation_type": "cross_column",
                    "rule_description": "Debit and credit should be mutually exclusive",
                    "issue": f"Found {both_positive_count} rows where both debit and credit are > 0",
                    "status": "FAIL",
                    "confidence": 1.0 - (both_positive_count / len(df))
                })
        
        return cross_validations
    
    def validate_dataset(self, csv_path: str) -> Dict[str, Any]:
        """
        Main validation function that validates the entire banking dataset.
        
        Args:
            csv_path: Path to the CSV file to validate
            
        Returns:
            Dictionary containing comprehensive validation results
        """
        try:
            # Load the dataset
            df = pd.read_csv(csv_path)
            
            # Step 1: Identify column purposes
            identified_columns = self.identify_column_purposes(df)
            
            # Step 2: Apply validation rules to each column
            validation_results = []
            
            for col_name, purpose in identified_columns.items():
                if purpose in self.column_rules:
                    validation_func = self.column_rules[purpose]["validation_func"]
                    
                    # Handle special cases for mutual exclusivity (debit/credit)
                    if purpose == "debit":
                        credit_cols = [c for c, p in identified_columns.items() if p in ["credit"]]
                        if credit_cols:
                            result = validation_func(df[col_name], df[credit_cols[0]])
                            if len(result) == 4:  # New format with reasons
                                status, confidence, issues, reasons = result
                            else:  # Old format
                                status, confidence, issues = result
                                reasons = ["Validation performed"]
                        else:
                            result = validation_func(df[col_name])
                            if len(result) == 4:  # New format with reasons
                                status, confidence, issues, reasons = result
                            else:  # Old format
                                status, confidence, issues = result
                                reasons = ["Validation performed"]
                    elif purpose == "credit":
                        debit_cols = [c for c, p in identified_columns.items() if p in ["debit"]]
                        if debit_cols:
                            result = validation_func(df[col_name], df[debit_cols[0]])
                            if len(result) == 4:  # New format with reasons
                                status, confidence, issues, reasons = result
                            else:  # Old format
                                status, confidence, issues = result
                                reasons = ["Validation performed"]
                        else:
                            result = validation_func(df[col_name])
                            if len(result) == 4:  # New format with reasons
                                status, confidence, issues, reasons = result
                            else:  # Old format
                                status, confidence, issues = result
                                reasons = ["Validation performed"]
                    else:
                        result = validation_func(df[col_name])
                        if len(result) == 4:  # New format with reasons
                            status, confidence, issues, reasons = result
                        else:  # Old format
                            status, confidence, issues = result
                            reasons = ["Validation performed"]
                    
                    validation_results.append({
                        "column_name": col_name,
                        "column_purpose": purpose,
                        "domain": "Banking",
                        "business_rules_applied": self.column_rules[purpose]["description"],
                        "confidence": round(confidence, 2),
                        "status": status,
                        "issues": issues,
                        "reasons": reasons
                    })
            
            # Step 3: Apply cross-column validations
            cross_validations = self.apply_cross_column_validations(df, identified_columns)
            
            # Step 4: Generate summary
            total_columns = len(validation_results)
            matched_columns = len([r for r in validation_results if r["status"] == "MATCH"])
            failed_columns = len([r for r in validation_results if r["status"] == "FAIL"])
            total_issues = sum(len(r["issues"]) for r in validation_results)
            
            overall_compliance = (matched_columns / total_columns * 100) if total_columns > 0 else 100
            
            if overall_compliance >= 95:
                final_decision = "PASS"
            elif overall_compliance >= 80:
                final_decision = "PASS WITH WARNINGS"
            else:
                final_decision = "FAIL"
            
            # Step 5: Compile results
            report = {
                "validation_report": {
                    "columns": validation_results,
                    "cross_validations": cross_validations,
                    "summary": {
                        "total_columns": total_columns,
                        "matched_columns": matched_columns,
                        "failed_columns": failed_columns,
                        "total_issues": total_issues,
                        "overall_compliance_percentage": round(overall_compliance, 2),
                        "final_decision": final_decision,
                        "dataset_size": len(df),
                        "total_records": len(df)
                    }
                },
                "detailed_report": self.format_detailed_report(validation_results, cross_validations)
            }
            
            return report
            
        except Exception as e:
            return {
                "error": str(e),
                "validation_report": {},
                "detailed_report": "Error occurred during validation."
            }
    
    def format_detailed_report(self, validation_results: List[Dict], cross_validations: List[Dict]) -> str:
        """Format a detailed textual report."""
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("BANKING DATASET VALIDATION REPORT")
        report_lines.append("=" * 80)
        
        report_lines.append("\nCOLUMN-LEVEL VALIDATION RESULTS:")
        report_lines.append("-" * 50)
        report_lines.append(f"{'Column Name':<20} {'Purpose':<15} {'Domain':<10} {'Confidence':<10} {'Status':<8} {'Issues'}")
        report_lines.append("-" * 50)
        
        for result in validation_results:
            issues_str = "; ".join(result["issues"]) if result["issues"] else "None"
            report_lines.append(
                f"{result['column_name']:<20} "
                f"{result['column_purpose']:<15} "
                f"{result['domain']:<10} "
                f"{result['confidence']:<10} "
                f"{result['status']:<8} "
                f"{issues_str}"
            )
        
        if cross_validations:
            report_lines.append("\nCROSS-COLUMN VALIDATION RESULTS:")
            report_lines.append("-" * 50)
            for cv in cross_validations:
                report_lines.append(f"- {cv['rule_description']}: {cv['issue']} (Status: {cv['status']})")
        
        report_lines.append("\nSUMMARY:")
        report_lines.append("-" * 20)
        summary = {
            "total_columns": len(validation_results),
            "matched_columns": len([r for r in validation_results if r["status"] == "MATCH"]),
            "failed_columns": len([r for r in validation_results if r["status"] == "FAIL"]),
            "total_issues": sum(len(r["issues"]) for r in validation_results)
        }
        
        for key, value in summary.items():
            report_lines.append(f"  {key.replace('_', ' ').title()}: {value}")
        
        report_lines.append("=" * 80)
        
        return "\n".join(report_lines)


# Example usage and testing
if __name__ == "__main__":
    validator = BankingDatasetValidator()
    
    # Example of how to use the validator
    # results = validator.validate_dataset("your_bank_file.csv")
    # print(results["detailed_report"])
    print("Banking Dataset Validator initialized. Ready to validate CSV files.")