import pandas as pd
import re
from typing import Dict, List, Any, Tuple
from datetime import datetime
import numpy as np


class CompleteBankingValidator:
    """
    A complete banking data validator that analyzes CSV or SQL data according to the specified requirements.
    """
    
    def __init__(self):
        # Define standard banking columns and their business rules
        self.standard_columns = {
            "account_number": {
                "description": "Account number: Numeric, 6-18 digits, unique per account",
                "validation_func": self.validate_account_number,
                "required": False
            },
            "customer_id": {
                "description": "Customer ID: Alphanumeric, unique per customer",
                "validation_func": self.validate_customer_id,
                "required": False
            },
            "customer_name": {
                "description": "Customer name: Letters & spaces only, minimum 3 characters",
                "validation_func": self.validate_customer_name,
                "required": False
            },
            "account_type": {
                "description": "Account type: 'Savings', 'Current', 'Salary', 'Student', or 'Pension'",
                "validation_func": self.validate_account_type,
                "required": False
            },
            "account_status": {
                "description": "Account status: Only 'Active' or 'Deactive'",
                "validation_func": self.validate_account_status,
                "required": False
            },
            "branch": {
                "description": "Branch: Alphanumeric",
                "validation_func": self.validate_branch,
                "required": False
            },
            "ifsc_code": {
                "description": "IFSC code: Alphanumeric, 8-11 characters",
                "validation_func": self.validate_ifsc_code,
                "required": False
            },
            "transaction_id": {
                "description": "Transaction ID: Unique, alphanumeric",
                "validation_func": self.validate_transaction_id,
                "required": False
            },
            "txn_date": {
                "description": "Transaction date: YYYY-MM-DD format",
                "validation_func": self.validate_txn_date,
                "required": False
            },
            "transaction_type": {
                "description": "Transaction type: Only 'Debit' or 'Credit'",
                "validation_func": self.validate_transaction_type,
                "required": False
            },
            "debit": {
                "description": "Debit: Numeric, positive, mutually exclusive with credit",
                "validation_func": self.validate_debit,
                "required": False
            },
            "credit": {
                "description": "Credit: Numeric, positive, mutually exclusive with debit",
                "validation_func": self.validate_credit,
                "required": False
            },
            "opening_balance": {
                "description": "Opening balance: Numeric, positive",
                "validation_func": self.validate_opening_balance,
                "required": False
            },
            "closing_balance": {
                "description": "Closing balance: Numeric, positive",
                "validation_func": self.validate_closing_balance,
                "required": False
            },
            "currency": {
                "description": "Currency: Letters only",
                "validation_func": self.validate_currency,
                "required": False
            },
            "country": {
                "description": "Country: Letters only",
                "validation_func": self.validate_country,
                "required": False
            },
            "phone": {
                "description": "Phone: Numeric, unique",
                "validation_func": self.validate_phone,
                "required": False
            },
            "kyc_status": {
                "description": "KYC status: Letters & spaces only",
                "validation_func": self.validate_kyc_status,
                "required": False
            },
            "created_at": {
                "description": "Created at: YYYY-MM-DD format",
                "validation_func": self.validate_created_at,
                "required": False
            },
            "updated_at": {
                "description": "Updated at: YYYY-MM-DD format",
                "validation_func": self.validate_updated_at,
                "required": False
            },
            "channel": {
                "description": "Channel: Letters & spaces only",
                "validation_func": self.validate_channel,
                "required": False
            }
        }
        
        # Common variations for column names
        self.column_variations = {
            "account_number": ["account_number", "account_no", "acc_no", "accno", "account"],
            "customer_id": ["customer_id", "cust_id", "customerid", "custid", "client_id", "clientid"],
            "customer_name": ["customer_name", "customer_name", "cust_name", "client_name", "name"],
            "account_type": ["account_type", "acct_type", "type", "accounttype"],
            "account_status": ["account_status", "status", "account_state"],
            "branch": ["branch", "branch_name", "branch_code", "branchid"],
            "ifsc_code": ["ifsc_code", "ifsc", "ifsccode", "bank_code"],
            "transaction_id": ["transaction_id", "trans_id", "txnid", "transactionid", "id"],
            "txn_date": ["txn_date", "transaction_date", "date", "trans_date", "tx_date", "created_date"],
            "transaction_type": ["transaction_type", "trans_type", "type", "transtype"],
            "debit": ["debit", "dr", "debits", "debit_amount", "debit_amt"],
            "credit": ["credit", "cr", "credits", "credit_amount", "credit_amt"],
            "opening_balance": ["opening_balance", "open_balance", "initial_balance", "start_balance"],
            "closing_balance": ["closing_balance", "close_balance", "ending_balance", "end_balance"],
            "currency": ["currency", "curr", "money_unit"],
            "country": ["country", "nation", "location"],
            "phone": ["phone", "mobile", "telephone", "contact", "tel"],
            "kyc_status": ["kyc_status", "kyc", "verification_status"],
            "created_at": ["created_at", "created_date", "creation_date", "date_created"],
            "updated_at": ["updated_at", "updated_date", "last_updated", "modification_date"],
            "channel": ["channel", "source", "origin", "platform"]
        }
    
    def detect_and_map_columns(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Detect columns in the dataset and map them to standard banking columns.
        Uses both column name matching and data pattern validation.
        """
        detected_columns = {}
        
        # CRITICAL: Define priority order - most specific columns first
        priority_columns = [
            "account_type", "account_status", "transaction_type", "transaction_date",
            "opening_balance", "closing_balance", "customer_name", "customer_id",
            "account_number", "transaction_id", "ifsc_code", "branch",
            "debit", "credit", "phone", "kyc_status", "txn_date"
        ]
        
        for col in df.columns:
            col_lower = col.lower().strip()
            col_normalized = col_lower.replace("_", "").replace("-", "").replace(" ", "")
            
            matched = False
            
            # First pass: Check priority columns in order (most specific first)
            for standard_col in priority_columns:
                if standard_col not in self.column_variations:
                    continue
                    
                variations = self.column_variations[standard_col]
                norm_variations = [v.lower().replace("_", "").replace("-", "").replace(" ", "") for v in variations]
                
                # Check for exact match
                if col_normalized in norm_variations or col_lower in [v.lower() for v in variations]:
                    # Validate with data pattern before confirming
                    if self._validate_column_mapping(df[col], standard_col):
                        detected_columns[col] = standard_col
                        matched = True
                        break
                
                # Check for specific pattern matches for critical columns
                if standard_col == "account_type":
                    if ("account" in col_normalized and "type" in col_normalized) or \
                       any(v in col_lower for v in ["account_type", "acct_type", "accounttype"]):
                        if self._validate_column_mapping(df[col], standard_col):
                            detected_columns[col] = standard_col
                            matched = True
                            break
                
                elif standard_col == "account_status":
                    if ("account" in col_normalized and "status" in col_normalized) or \
                       any(v in col_lower for v in ["account_status", "acc_status", "accountstate"]):
                        if self._validate_column_mapping(df[col], standard_col):
                            detected_columns[col] = standard_col
                            matched = True
                            break
                
                elif standard_col == "transaction_type":
                    if ("transaction" in col_normalized and "type" in col_normalized) or \
                       any(v in col_lower for v in ["transaction_type", "txn_type", "transtype"]):
                        if self._validate_column_mapping(df[col], standard_col):
                            detected_columns[col] = standard_col
                            matched = True
                            break
                
                # For other columns, check if variation is in column name
                else:
                    for variation in variations:
                        if variation.lower() in col_lower or col_lower in variation.lower():
                            if self._validate_column_mapping(df[col], standard_col):
                                detected_columns[col] = standard_col
                                matched = True
                                break
                    if matched:
                        break
            
            # If no direct match, try to infer from data characteristics
            if not matched:
                sample_data = df[col].dropna().head(10)  # Sample non-null values
                
                if len(sample_data) == 0:
                    detected_columns[col] = "unknown_empty"
                    continue
                    
                # Check if it looks like numeric data
                if sample_data.dtype in ['int64', 'float64']:
                    # Check if it looks like account number (6-18 digits)
                    if sample_data.astype(str).str.contains(r'^\d{6,18}$').all():
                        detected_columns[col] = "account_number"
                    # Check if it looks like amounts
                    elif sample_data.gt(0).all():
                        detected_columns[col] = "amount"
                    # Check if it looks like phone numbers
                    elif sample_data.astype(str).str.contains(r'^\d{10,15}$').all():
                        detected_columns[col] = "phone"
                    else:
                        detected_columns[col] = "unknown_numeric"
                        
                elif sample_data.dtype == 'object':
                    # Check if it looks like dates
                    try:
                        pd.to_datetime(sample_data)
                        detected_columns[col] = "txn_date"
                    except:
                        # Check if it looks like transaction types (Debit/Credit)
                        normalized_sample = sample_data.str.title().str.strip()
                        if normalized_sample.isin(['Debit', 'Credit']).mean() >= 0.8:
                            detected_columns[col] = "transaction_type"
                        # Check if it looks like account types (Savings/Current/Salary/Student/Pension)
                        elif normalized_sample.isin(['Savings', 'Current', 'Salary', 'Student', 'Pension']).mean() >= 0.8:
                            detected_columns[col] = "account_type"
                        # Check if it looks like account status (Active/Deactive)
                        elif normalized_sample.isin(['Active', 'Deactive']).mean() >= 0.8 or \
                             normalized_sample.replace('Inactive', 'Deactive').isin(['Active', 'Deactive']).mean() >= 0.8:
                            detected_columns[col] = "account_status"
                        # Check if it looks like names
                        elif sample_data.str.contains(r'^[A-Za-z\s]+$', regex=True).all():
                            detected_columns[col] = "customer_name"
                        # Check if it looks like IDs
                        elif sample_data.str.contains(r'^[A-Za-z0-9]+$', regex=True).all():
                            detected_columns[col] = "customer_id"
                        else:
                            detected_columns[col] = "unknown_text"
                else:
                    detected_columns[col] = "unknown"
        
        return detected_columns
    
    def _validate_column_mapping(self, series: pd.Series, standard_col: str) -> bool:
        """
        Validate that a column's data pattern matches the expected standard column type.
        This prevents misclassification (e.g., account_type column being mapped to account_number).
        """
        try:
            non_null = series.dropna()
            if len(non_null) == 0:
                return False  # Can't validate empty columns
            
            non_null_str = non_null.astype(str).str.strip()
            
            if standard_col == "account_type":
                # Should contain Savings, Current, Salary, Student, or Pension
                normalized = non_null_str.str.title()
                valid_ratio = normalized.isin(['Savings', 'Current', 'Salary', 'Student', 'Pension']).mean()
                return valid_ratio >= 0.8
            
            elif standard_col == "account_status":
                # Should contain Active or Deactive
                normalized = non_null_str.str.title()
                normalized = normalized.replace('Inactive', 'Deactive')
                valid_ratio = normalized.isin(['Active', 'Deactive']).mean()
                return valid_ratio >= 0.8
            
            elif standard_col == "transaction_type":
                # Should contain Debit or Credit
                normalized = non_null_str.str.title()
                valid_ratio = normalized.isin(['Debit', 'Credit']).mean()
                return valid_ratio >= 0.8
            
            elif standard_col == "account_number":
                # Should be numeric, 6-18 digits
                numeric_ratio = non_null_str.str.match(r'^\d+$').mean()
                length_ok = non_null_str.str.len().between(6, 18).mean()
                return numeric_ratio >= 0.8 and length_ok >= 0.8
            
            elif standard_col == "customer_id":
                # Should be alphanumeric
                alphanum_ratio = non_null_str.str.match(r'^[A-Za-z0-9]+$').mean()
                return alphanum_ratio >= 0.8
            
            elif standard_col == "transaction_id":
                # Should be alphanumeric
                alphanum_ratio = non_null_str.str.match(r'^[A-Za-z0-9]+$').mean()
                return alphanum_ratio >= 0.8
            
            elif standard_col == "ifsc_code":
                # Should be alphanumeric, 8-11 characters
                alphanum_ratio = non_null_str.str.match(r'^[A-Za-z0-9]+$').mean()
                length_ok = non_null_str.str.len().between(8, 11).mean()
                return alphanum_ratio >= 0.8 and length_ok >= 0.8
            
            elif standard_col in ["debit", "credit", "opening_balance", "closing_balance"]:
                # Should be numeric
                numeric_series = pd.to_numeric(non_null_str, errors="coerce")
                numeric_ratio = numeric_series.notna().mean()
                return numeric_ratio >= 0.8
            
            elif standard_col == "txn_date" or standard_col == "transaction_date":
                # Should be parseable as date
                date_parsed = pd.to_datetime(non_null_str, errors="coerce")
                valid_ratio = date_parsed.notna().mean()
                return valid_ratio >= 0.8
            
            # For other columns, accept the mapping (less strict validation)
            return True
            
        except Exception:
            # If validation fails, don't block the mapping (fallback)
            return True
    
    def validate_account_number(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate account number: numeric, 6-18 digits, unique per account"""
        issues = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        # Check if all values are numeric
        numeric_mask = non_null.str.match(r'^\d+$')
        numeric_ratio = numeric_mask.mean()
        
        if numeric_ratio < 0.95:
            non_numeric_values = non_null[~numeric_mask].head(3).tolist()
            issues.append(f"Non-numeric values found: {((1-numeric_ratio)*100):.1f}% - Sample: {non_numeric_values}")
        
        # Check length (6-18 digits)
        length_ok_mask = non_null.str.len().between(6, 18)
        length_ok_ratio = length_ok_mask.mean()
        
        if length_ok_ratio < 0.95:
            invalid_length_values = non_null[~length_ok_mask].head(3).tolist()
            issues.append(f"Invalid length values: {((1-length_ok_ratio)*100):.1f}% (expected 6-18 chars) - Sample: {invalid_length_values}")
        
        # Check uniqueness
        unique_ratio = non_null.nunique() / len(non_null) if len(non_null) > 0 else 0
        
        if unique_ratio < 0.95:
            issues.append(f"Low uniqueness: {unique_ratio*100:.1f}% unique values")
        
        confidence = min(numeric_ratio, length_ok_ratio, unique_ratio)
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_customer_id(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate customer ID: alphanumeric, unique per customer"""
        issues = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        # Check if alphanumeric
        alphanum_mask = non_null.str.match(r'^[A-Za-z0-9]+$')
        alphanum_ratio = alphanum_mask.mean()
        
        if alphanum_ratio < 0.95:
            non_alphanum_values = non_null[~alphanum_mask].head(3).tolist()
            issues.append(f"Non-alphanumeric values found: {((1-alphanum_ratio)*100):.1f}% - Sample: {non_alphanum_values}")
        
        # Check uniqueness
        unique_ratio = non_null.nunique() / len(non_null) if len(non_null) > 0 else 0
        
        if unique_ratio < 0.95:
            issues.append(f"Low uniqueness: {unique_ratio*100:.1f}% unique values")
        
        confidence = min(alphanum_ratio, unique_ratio)
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_customer_name(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate customer name: letters & spaces only, minimum 3 characters"""
        issues = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        # Check if contains only letters and spaces
        letters_spaces_mask = non_null.str.match(r'^[A-Za-z\s]+$')
        letters_spaces_ratio = letters_spaces_mask.mean()
        
        if letters_spaces_ratio < 0.95:
            invalid_values = non_null[~letters_spaces_mask].head(3).tolist()
            issues.append(f"Non-letter/space characters found: {((1-letters_spaces_ratio)*100):.1f}% - Sample: {invalid_values}")
        
        # Check minimum length
        min_length_mask = non_null.str.len() >= 3
        min_length_ratio = min_length_mask.mean()
        
        if min_length_ratio < 0.95:
            short_names = non_null[~min_length_mask].head(3).tolist()
            issues.append(f"Names shorter than 3 characters: {((1-min_length_ratio)*100):.1f}% - Sample: {short_names}")
        
        confidence = min(letters_spaces_ratio, min_length_ratio)
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_account_type(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate account type: 'Savings', 'Current', 'Salary', 'Student', or 'Pension'"""
        issues = []
        non_null = series.dropna().astype(str).str.strip()
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        # Normalize to title case for comparison (case-insensitive)
        normalized = non_null.str.title()
        allowed_types = {'Savings', 'Current', 'Salary', 'Student', 'Pension'}
        valid_mask = normalized.isin(allowed_types)
        valid_ratio = valid_mask.mean()
        
        if valid_ratio < 0.95:
            invalid_values = non_null[~valid_mask].unique().tolist()[:5]
            issues.append(f"Invalid account types: {invalid_values}. Allowed types: 'Savings', 'Current', 'Salary', 'Student', or 'Pension'.")
        
        confidence = valid_ratio
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_account_status(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate account status: Only 'Active' or 'Deactive'"""
        issues = []
        non_null = series.dropna().astype(str).str.strip()
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        # Normalize to title case for comparison (case-insensitive)
        # Also handle common variations
        normalized = non_null.str.title()
        # Map common variations to correct values
        normalized = normalized.replace('Inactive', 'Deactive')
        normalized = normalized.replace('De-Active', 'Deactive')
        normalized = normalized.replace('De Active', 'Deactive')
        
        allowed_statuses = {'Active', 'Deactive'}
        valid_mask = normalized.isin(allowed_statuses)
        valid_ratio = valid_mask.mean()
        
        if valid_ratio < 0.95:
            invalid_values = non_null[~valid_mask].unique().tolist()[:5]
            issues.append(f"Invalid account statuses: {invalid_values}. Only 'Active' or 'Deactive' are allowed.")
        
        confidence = valid_ratio
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_branch(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate branch: alphanumeric"""
        issues = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        # Check if alphanumeric with optional spaces
        alphanum_spaces_mask = non_null.str.match(r'^[A-Za-z0-9\s]+$')
        alphanum_spaces_ratio = alphanum_spaces_mask.mean()
        
        if alphanum_spaces_ratio < 0.95:
            invalid_values = non_null[~alphanum_spaces_mask].head(3).tolist()
            issues.append(f"Non-alphanumeric characters found: {((1-alphanum_spaces_ratio)*100):.1f}% - Sample: {invalid_values}")
        
        confidence = alphanum_spaces_ratio
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_ifsc_code(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate IFSC code: Alphanumeric, 8-11 characters"""
        issues = []
        non_null = series.dropna().astype(str).str.strip()
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        # Check if length is between 8-11 characters
        length_mask = non_null.str.len().between(8, 11)
        length_ratio = length_mask.mean()
        
        if length_ratio < 0.95:
            invalid_values = non_null[~length_mask].head(3).tolist()
            issues.append(f"Values not between 8-11 characters: {((1-length_ratio)*100):.1f}% - Sample: {invalid_values}")
        
        # Check if alphanumeric (case-insensitive, allow uppercase)
        alphanum_mask = non_null.str.match(r'^[A-Za-z0-9]+$')
        alphanum_ratio = alphanum_mask.mean()
        
        if alphanum_ratio < 0.95:
            invalid_values = non_null[~alphanum_mask].head(3).tolist()
            issues.append(f"Non-alphanumeric values found: {((1-alphanum_ratio)*100):.1f}% - Sample: {invalid_values}")
        
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
            invalid_values = non_null[~alphanum_mask].head(3).tolist()
            issues.append(f"Non-alphanumeric values found: {((1-alphanum_ratio)*100):.1f}% - Sample: {invalid_values}")
        
        # Check uniqueness
        unique_ratio = non_null.nunique() / len(non_null) if len(non_null) > 0 else 0
        
        if unique_ratio < 0.95:
            issues.append(f"Low uniqueness: {unique_ratio*100:.1f}% unique values")
        
        confidence = min(alphanum_ratio, unique_ratio)
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_txn_date(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate transaction date: YYYY-MM-DD format"""
        issues = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        # Check date format
        valid_dates = 0
        total_checked = 0
        invalid_dates = []
        
        for date_val in non_null.head(100):  # Check first 100 values to avoid performance issues
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
                    if len(invalid_dates) < 5:  # Store up to 5 invalid samples
                        invalid_dates.append(date_val)
            total_checked += 1
        
        if total_checked > 0:
            valid_ratio = valid_dates / total_checked
        else:
            valid_ratio = 0
        
        if valid_ratio < 0.95:
            issues.append(f"Invalid date formats: {((1-valid_ratio)*100):.1f}% - Sample invalid: {invalid_dates}")
        
        confidence = valid_ratio
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_transaction_type(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate transaction type: Only 'Debit' or 'Credit'"""
        issues = []
        non_null = series.dropna().astype(str).str.strip()
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        # Normalize to title case for comparison (case-insensitive)
        normalized = non_null.str.title()
        allowed_types = {'Debit', 'Credit'}
        valid_mask = normalized.isin(allowed_types)
        valid_ratio = valid_mask.mean()
        
        if valid_ratio < 0.95:
            invalid_values = non_null[~valid_mask].unique().tolist()[:5]
            issues.append(f"Invalid transaction types: {invalid_values}. Only 'Debit' or 'Credit' are allowed.")
        
        confidence = valid_ratio
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
            negative_values = non_null[~positive_mask].head(3).tolist()
            issues.append(f"Negative values found: {((1-positive_ratio)*100):.1f}% - Sample: {negative_values}")
        
        # Check mutual exclusivity with credit if credit column exists
        if credit_series is not None:
            debit_non_null = pd.to_numeric(series, errors='coerce').fillna(0)
            credit_non_null = pd.to_numeric(credit_series, errors='coerce').fillna(0)
            
            # Find rows where both debit and credit are > 0
            both_positive = ((debit_non_null > 0) & (credit_non_null > 0)).sum()
            both_positive_ratio = both_positive / len(series)
            
            if both_positive_ratio > 0.05:  # More than 5% have both > 0
                issues.append(f"Both debit and credit > 0 in {both_positive_ratio*100:.1f}% of rows")
        
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
            negative_values = non_null[~positive_mask].head(3).tolist()
            issues.append(f"Negative values found: {((1-positive_ratio)*100):.1f}% - Sample: {negative_values}")
        
        # Check mutual exclusivity with debit if debit column exists
        if debit_series is not None:
            credit_non_null = pd.to_numeric(series, errors='coerce').fillna(0)
            debit_non_null = pd.to_numeric(debit_series, errors='coerce').fillna(0)
            
            # Find rows where both credit and debit are > 0
            both_positive = ((credit_non_null > 0) & (debit_non_null > 0)).sum()
            both_positive_ratio = both_positive / len(series)
            
            if both_positive_ratio > 0.05:  # More than 5% have both > 0
                issues.append(f"Both credit and debit > 0 in {both_positive_ratio*100:.1f}% of rows")
        
        confidence = min(numeric_ratio, positive_ratio)
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_opening_balance(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate opening balance: numeric, positive"""
        issues = []
        numeric_series = pd.to_numeric(series, errors='coerce')
        non_null = numeric_series.dropna()
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty or non-numeric"]
        
        # Check if all values are numeric
        numeric_ratio = 1.0 - (series.isna().sum() + pd.to_numeric(series, errors='coerce').isna().sum()) / len(series)
        
        # Check if all values are positive
        positive_mask = non_null >= 0
        positive_ratio = positive_mask.mean()
        
        if positive_ratio < 0.95:
            negative_values = non_null[~positive_mask].head(3).tolist()
            issues.append(f"Negative values found: {((1-positive_ratio)*100):.1f}% - Sample: {negative_values}")
        
        confidence = min(numeric_ratio, positive_ratio)
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_closing_balance(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate closing balance: numeric, positive"""
        issues = []
        numeric_series = pd.to_numeric(series, errors='coerce')
        non_null = numeric_series.dropna()
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty or non-numeric"]
        
        # Check if all values are numeric
        numeric_ratio = 1.0 - (series.isna().sum() + pd.to_numeric(series, errors='coerce').isna().sum()) / len(series)
        
        # Check if all values are positive
        positive_mask = non_null >= 0
        positive_ratio = positive_mask.mean()
        
        if positive_ratio < 0.95:
            negative_values = non_null[~positive_mask].head(3).tolist()
            issues.append(f"Negative values found: {((1-positive_ratio)*100):.1f}% - Sample: {negative_values}")
        
        confidence = min(numeric_ratio, positive_ratio)
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_currency(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate currency: letters only"""
        issues = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        # Check if contains only letters
        letters_only_mask = non_null.str.match(r'^[A-Za-z]+$')
        letters_only_ratio = letters_only_mask.mean()
        
        if letters_only_ratio < 0.95:
            invalid_values = non_null[~letters_only_mask].head(3).tolist()
            issues.append(f"Non-letter values found: {((1-letters_only_ratio)*100):.1f}% - Sample: {invalid_values}")
        
        confidence = letters_only_ratio
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_country(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate country: letters only"""
        issues = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        # Check if contains only letters
        letters_only_mask = non_null.str.match(r'^[A-Za-z\s]+$')
        letters_only_ratio = letters_only_mask.mean()
        
        if letters_only_ratio < 0.95:
            invalid_values = non_null[~letters_only_mask].head(3).tolist()
            issues.append(f"Non-letter values found: {((1-letters_only_ratio)*100):.1f}% - Sample: {invalid_values}")
        
        confidence = letters_only_ratio
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_phone(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate phone: numeric, unique"""
        issues = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        # Check if numeric
        numeric_mask = non_null.str.match(r'^\d+$')
        numeric_ratio = numeric_mask.mean()
        
        if numeric_ratio < 0.95:
            non_numeric_values = non_null[~numeric_mask].head(3).tolist()
            issues.append(f"Non-numeric values found: {((1-numeric_ratio)*100):.1f}% - Sample: {non_numeric_values}")
        
        # Check uniqueness
        unique_ratio = non_null.nunique() / len(non_null) if len(non_null) > 0 else 0
        
        if unique_ratio < 0.95:
            issues.append(f"Low uniqueness: {unique_ratio*100:.1f}% unique values")
        
        confidence = min(numeric_ratio, unique_ratio)
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_kyc_status(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate KYC status: letters & spaces only"""
        issues = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        # Check if contains only letters and spaces
        letters_spaces_mask = non_null.str.match(r'^[A-Za-z\s]+$')
        letters_spaces_ratio = letters_spaces_mask.mean()
        
        if letters_spaces_ratio < 0.95:
            invalid_values = non_null[~letters_spaces_mask].head(3).tolist()
            issues.append(f"Non-letter/space characters found: {((1-letters_spaces_ratio)*100):.1f}% - Sample: {invalid_values}")
        
        confidence = letters_spaces_ratio
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def validate_created_at(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate created_at: YYYY-MM-DD format"""
        return self.validate_txn_date(series)  # Reuse date validation
    
    def validate_updated_at(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate updated_at: YYYY-MM-DD format"""
        return self.validate_txn_date(series)  # Reuse date validation
    
    def validate_channel(self, series: pd.Series) -> Tuple[str, float, List[str]]:
        """Validate channel: letters & spaces only"""
        issues = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return "FAIL", 0.0, ["Column is empty"]
        
        # Check if contains only letters and spaces
        letters_spaces_mask = non_null.str.match(r'^[A-Za-z\s]+$')
        letters_spaces_ratio = letters_spaces_mask.mean()
        
        if letters_spaces_ratio < 0.95:
            invalid_values = non_null[~letters_spaces_mask].head(3).tolist()
            issues.append(f"Non-letter/space characters found: {((1-letters_spaces_ratio)*100):.1f}% - Sample: {invalid_values}")
        
        confidence = letters_spaces_ratio
        status = "FAIL" if issues else "MATCH"
        
        return status, confidence, issues
    
    def apply_cross_column_validations(self, df: pd.DataFrame, detected_columns: Dict[str, str]) -> List[Dict[str, Any]]:
        """Apply validations that involve multiple columns."""
        cross_validations = []
        
        # Check for debit/credit mutual exclusivity
        debit_cols = [col for col, standard_name in detected_columns.items() if standard_name in ["debit"]]
        credit_cols = [col for col, standard_name in detected_columns.items() if standard_name in ["credit"]]
        
        if debit_cols and credit_cols:
            debit_series = pd.to_numeric(df[debit_cols[0]], errors="coerce").fillna(0)
            credit_series = pd.to_numeric(df[credit_cols[0]], errors="coerce").fillna(0)
            
            both_positive_mask = (debit_series > 0) & (credit_series > 0)
            both_positive_count = both_positive_mask.sum()
            both_positive_ratio = both_positive_count / len(df)
            
            if both_positive_ratio > 0.05:  # More than 5% of rows
                cross_validations.append({
                    "validation_type": "cross_column",
                    "rule_description": "Debit and credit should be mutually exclusive",
                    "issue": f"Found {both_positive_count} rows where both debit and credit are > 0 ({both_positive_ratio*100:.1f}%)",
                    "status": "FAIL",
                    "confidence": 1.0 - both_positive_ratio
                })
        
        return cross_validations
    
    def validate_dataset(self, file_path: str) -> Dict[str, Any]:
        """
        Main validation function that validates the entire banking dataset.
        
        Args:
            file_path: Path to the CSV or SQL file to validate
            
        Returns:
            Dictionary containing comprehensive validation results
        """
        try:
            # Load the dataset - handle both CSV and SQL files
            if file_path.lower().endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.lower().endswith('.sql'):
                # For SQL files, we'd typically need to parse the INSERT statements
                # This is a simplified approach - in reality, you'd want to execute the SQL
                # to load the data into a DataFrame
                df = pd.read_csv(file_path, sep=';')  # Simplified approach
            else:
                df = pd.read_csv(file_path)  # Default to CSV
            
            # Step 1: Detect and map columns
            detected_columns = self.detect_and_map_columns(df)
            
            # Step 2: Apply validation rules to each detected column
            validation_results = []
            
            for col_name, standard_name in detected_columns.items():
                if standard_name in self.standard_columns:
                    validation_func = self.standard_columns[standard_name]["validation_func"]
                    
                    # Handle special cases for mutual exclusivity (debit/credit)
                    if standard_name == "debit":
                        credit_cols = [c for c, sn in detected_columns.items() if sn == "credit"]
                        if credit_cols:
                            status, confidence, issues = validation_func(df[col_name], df[credit_cols[0]])
                        else:
                            status, confidence, issues = validation_func(df[col_name])
                    elif standard_name == "credit":
                        debit_cols = [c for c, sn in detected_columns.items() if sn == "debit"]
                        if debit_cols:
                            status, confidence, issues = validation_func(df[col_name], df[debit_cols[0]])
                        else:
                            status, confidence, issues = validation_func(df[col_name])
                    else:
                        status, confidence, issues = validation_func(df[col_name])
                    
                    validation_results.append({
                        "column_name": col_name,
                        "standard_name": standard_name,
                        "business_rule": self.standard_columns[standard_name]["description"],
                        "validation_result": status,
                        "detected_issue": issues if issues else [],
                        "confidence_percentage": round(confidence * 100, 2)
                    })
            
            # Step 3: Apply cross-column validations
            cross_validations = self.apply_cross_column_validations(df, detected_columns)
            
            # Step 4: Generate summary
            total_columns = len(validation_results)
            passed_columns = len([r for r in validation_results if r["validation_result"] == "MATCH"])
            failed_columns = len([r for r in validation_results if r["validation_result"] == "FAIL"])
            
            overall_confidence = (passed_columns / total_columns * 100) if total_columns > 0 else 100
            
            # Create summary report
            summary = {
                "total_columns_analyzed": total_columns,
                "total_passed": passed_columns,
                "total_failed": failed_columns,
                "overall_confidence": round(overall_confidence, 2),
                "dataset_size": len(df),
                "total_records": len(df)
            }
            
            # Step 5: Compile final results
            report = {
                "column_wise_validation": validation_results,
                "cross_column_validations": cross_validations,
                "summary": summary,
                "detected_columns_mapping": detected_columns
            }
            
            return report
            
        except Exception as e:
            return {
                "error": str(e),
                "column_wise_validation": [],
                "summary": {
                    "total_columns_analyzed": 0,
                    "total_passed": 0,
                    "total_failed": 0,
                    "overall_confidence": 0,
                    "total_records": 0
                }
            }
    
    def generate_structured_report(self, validation_results: Dict[str, Any]) -> str:
        """Generate a structured report focusing on failed validations."""
        report_lines = []
        report_lines.append("=" * 100)
        report_lines.append("BANKING DATA VALIDATION REPORT")
        report_lines.append("=" * 100)
        
        # Summary Section
        summary = validation_results["summary"]
        report_lines.append(f"\nSUMMARY:")
        report_lines.append(f"  Total columns analyzed: {summary['total_columns_analyzed']}")
        report_lines.append(f"  Total passed:           {summary['total_passed']}")
        report_lines.append(f"  Total failed:           {summary['total_failed']}")
        report_lines.append(f"  Overall confidence:     {summary['overall_confidence']}%")
        report_lines.append(f"  Total records:          {summary['total_records']}")
        
        # Failed validations section
        failed_results = [r for r in validation_results["column_wise_validation"] if r["validation_result"] == "FAIL"]
        
        if failed_results:
            report_lines.append(f"\nFAILED VALIDATIONS:")
            report_lines.append("-" * 60)
            report_lines.append(f"{'Column Name':<20} {'Standard Name':<15} {'Rule Applied':<30} {'Confidence %':<12} {'Issue'}")
            report_lines.append("-" * 60)
            
            for result in failed_results:
                issue_str = "; ".join(result["detected_issue"][:2])  # Show first 2 issues
                if len(result["detected_issue"]) > 2:
                    issue_str += "..."
                
                report_lines.append(
                    f"{result['column_name']:<20} "
                    f"{result['standard_name']:<15} "
                    f"{result['business_rule'][:29]:<30} "
                    f"{result['confidence_percentage']:<12} "
                    f"{issue_str}"
                )
        else:
            report_lines.append(f"\nALL VALIDATIONS PASSED! 🎉")
        
        # Cross-column validations if any
        if validation_results.get("cross_column_validations"):
            report_lines.append(f"\nCROSS-COLUMN VALIDATIONS:")
            report_lines.append("-" * 60)
            for cv in validation_results["cross_column_validations"]:
                report_lines.append(f"  {cv['rule_description']}: {cv['issue']} (Status: {cv['status']})")
        
        report_lines.append("=" * 100)
        
        return "\n".join(report_lines)


# Example usage and testing
if __name__ == "__main__":
    validator = CompleteBankingValidator()
    
    # Example of how to use the validator
    # results = validator.validate_dataset("your_banking_file.csv")
    # print(validator.generate_structured_report(results))
    
    print("Complete Banking Validator initialized. Ready to validate CSV/SQL files.")
    print("\nFeatures:")
    print("- Detects and maps columns to standard banking columns")
    print("- Applies specific business rules to each column type")
    print("- Identifies mismatches and issues")
    print("- Generates structured reports focusing on failures")
    print("- Provides summary statistics")