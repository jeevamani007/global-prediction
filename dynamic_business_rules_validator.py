"""
Dynamic Business Rules Validator for Banking Data

This validator applies business rules dynamically based on what columns are present
in the user-uploaded data. Missing columns are skipped gracefully - only business
rules for existing columns are applied.

Business Rules Applied (only if column exists):
- account_number
- customer_id
- customer_name
- account_type
- account_status
- branch_code
- ifsc_code
- transaction_id
- transaction_date
- transaction_type
- debit
- credit
- opening_balance
- closing_balance
- phone
- pan
"""

import pandas as pd
import numpy as np
from datetime import datetime
import re
from typing import Dict, List, Optional, Any


class DynamicBusinessRulesValidator:
    """
    Dynamic Business Rules Validator
    
    Applies business rules only to columns that exist in the uploaded data.
    Missing columns are skipped - no failures for missing columns.
    """
    
    # Column name variations mapping
    COLUMN_VARIATIONS = {
        "account_number": ["account_number", "account_no", "acc_no", "accno", "account"],
        "customer_id": ["customer_id", "cust_id", "customerid", "custid", "client_id"],
        "customer_name": ["customer_name", "cust_name", "customername", "name"],
        "account_type": ["account_type", "acct_type", "accounttype", "type"],
        "account_status": ["account_status", "acc_status", "status", "account_state", "state"],
        "branch_code": ["branch_code", "branch", "branch_id", "branchcode"],
        "ifsc_code": ["ifsc_code", "ifsc", "ifsc_code", "ifscnumber"],
        "pan": ["pan", "pan_number", "pan_no", "pannumber", "panno", "permanent_account_number"],
        "transaction_id": ["transaction_id", "txn_id", "trans_id", "transactionid", "txnid"],
        "transaction_date": ["transaction_date", "txn_date", "trans_date", "transactiondate", "date"],
        "transaction_type": ["transaction_type", "txn_type", "trans_type", "transactiontype", "type"],
        "debit": ["debit", "dr_amount", "debit_amount", "withdraw", "withdrawal"],
        "credit": ["credit", "cr_amount", "credit_amount", "deposit"],
        "opening_balance": ["opening_balance", "open_balance", "balance_before", "initial_balance", "op_bal"],
        "closing_balance": ["closing_balance", "closing", "balance_after", "final_balance", "current_balance", "cl_bal"],
        "phone": ["phone", "mobile", "contact", "telephone", "phone_number", "mobile_number"]
    }
    
    # Allowed values for enumerated columns
    ALLOWED_ACCOUNT_TYPES = ["SAVINGS", "CURRENT", "LOAN", "FD", "RD", "SAVING", "CURRENT ACCOUNT", "SAVINGS ACCOUNT"]
    ALLOWED_ACCOUNT_STATUSES = ["ACTIVE", "INACTIVE", "CLOSED", "SUSPENDED"]
    ALLOWED_TRANSACTION_TYPES = ["DEBIT", "CREDIT", "DEPOSIT", "WITHDRAW", "WITHDRAWAL", "TRANSFER"]
    
    def __init__(self):
        """Initialize the validator."""
        pass
    
    def normalize_column_name(self, col_name: str) -> str:
        """Normalize column name for matching."""
        return str(col_name).lower().strip().replace(" ", "_").replace("-", "_")
    
    def identify_column_role(self, col_name: str) -> Optional[str]:
        """
        Identify the role of a column based on name matching.
        Returns the matched column role or None.
        """
        normalized = self.normalize_column_name(col_name)
        
        # Priority order: Check most specific matches first
        # 1. Transaction type (before account type to avoid confusion)
        if "transaction" in normalized and "type" in normalized:
            for variation in self.COLUMN_VARIATIONS["transaction_type"]:
                if variation in normalized or normalized == variation:
                    return "transaction_type"
        
        # 2. Transaction date (after transaction_type check)
        # Check for "txn_date" or "transaction" + "date"
        if ("txn" in normalized and "date" in normalized) or ("transaction" in normalized and "date" in normalized):
            for variation in self.COLUMN_VARIATIONS["transaction_date"]:
                if variation in normalized or normalized == variation:
                    return "transaction_date"
        
        # 3. Account type (check before generic "type" matching)
        if "account" in normalized and "type" in normalized:
            for variation in self.COLUMN_VARIATIONS["account_type"]:
                if variation in normalized or normalized == variation:
                    return "account_type"
        
        # 4. Check all other columns (exact matches first, then contains)
        # Sort by specificity (longer variations first)
        sorted_roles = sorted(
            self.COLUMN_VARIATIONS.items(),
            key=lambda x: max(len(v) for v in x[1]),
            reverse=True
        )
        
        for role, variations in sorted_roles:
            # Skip if already handled above
            if role in ["transaction_type", "transaction_date", "account_type"]:
                continue
            
            # Exact match first
            if normalized in variations:
                return role
            
            # Contains match
            for variation in variations:
                if variation in normalized or normalized in variation:
                    # Additional checks to avoid false matches
                    if role == "transaction_date" and "type" in normalized:
                        continue
                    if role == "transaction_type" and "date" in normalized:
                        continue
                    if role == "account_type" and "transaction" in normalized:
                        continue
                    # PAN should only match when 'pan' is present in the name
                    if role == "pan" and "pan" not in normalized:
                        continue
                    return role
        
        return None
    
    def check_data_pattern_match(self, series: pd.Series, role: str) -> bool:
        """
        Check if column data matches the expected pattern for the role.
        Returns True only if data pattern matches.
        """
        try:
            non_null = series.dropna()
            if len(non_null) == 0:
                return False  # Empty columns don't match patterns
            
            non_null_str = non_null.astype(str)
            
            if role == "account_number":
                # Digits only, length 6-18
                digit_ratio = non_null_str.str.fullmatch(r"\d+").mean()
                length_ratio = non_null_str.str.len().between(6, 18).mean()
                return digit_ratio >= 0.8 and length_ratio >= 0.8
            
            elif role == "customer_id":
                # Alphanumeric with at least one letter (e.g., CUST001, not just 001)
                alphanumeric_ratio = non_null_str.str.fullmatch(r"[A-Za-z0-9]+").mean()
                has_letter_ratio = non_null_str.str.contains(r"[A-Za-z]").mean()
                return alphanumeric_ratio >= 0.8 and has_letter_ratio >= 0.8
            
            elif role == "customer_name":
                # Letters and spaces, min 3 chars
                letter_space_ratio = non_null_str.str.fullmatch(r"[A-Za-z\s]+").mean()
                min_length_ratio = (non_null_str.str.len() >= 3).mean()
                return letter_space_ratio >= 0.8 and min_length_ratio >= 0.8
            
            elif role == "account_type":
                # Must be from allowed account types
                valid_ratio = non_null_str.str.upper().str.strip().isin([v.upper() for v in self.ALLOWED_ACCOUNT_TYPES]).mean()
                return valid_ratio >= 0.8
            
            elif role == "account_status":
                # Must be from allowed statuses
                valid_ratio = non_null_str.str.upper().str.strip().isin([v.upper() for v in self.ALLOWED_ACCOUNT_STATUSES]).mean()
                return valid_ratio >= 0.8
            
            elif role == "branch_code":
                # Alphanumeric with spaces
                alphanumeric_space_ratio = non_null_str.str.fullmatch(r"[A-Za-z0-9\s]+").mean()
                return alphanumeric_space_ratio >= 0.8
            
            elif role == "ifsc_code":
                # Alphanumeric, length 3-15
                alphanumeric_ratio = non_null_str.str.fullmatch(r"[A-Z0-9]+").mean()
                length_ratio = non_null_str.str.len().between(3, 15).mean()
                return alphanumeric_ratio >= 0.8 and length_ratio >= 0.8
            
            elif role == "pan":
                # PAN pattern: 5 letters + 4 digits + 1 letter (case-insensitive)
                uppercase = non_null_str.str.upper()
                pan_ratio = uppercase.str.fullmatch(r"[A-Z]{5}[0-9]{4}[A-Z]").mean()
                return pan_ratio >= 0.8
            
            elif role == "transaction_id":
                # Alphanumeric
                alphanumeric_ratio = non_null_str.str.fullmatch(r"[A-Za-z0-9]+").mean()
                return alphanumeric_ratio >= 0.8
            
            elif role == "transaction_date":
                # Must be parseable as date
                date_parsed = pd.to_datetime(non_null_str, errors="coerce")
                valid_date_ratio = date_parsed.notna().mean()
                return valid_date_ratio >= 0.8
            
            elif role == "transaction_type":
                # Must be from allowed transaction types
                valid_ratio = non_null_str.str.upper().str.strip().isin([v.upper() for v in self.ALLOWED_TRANSACTION_TYPES]).mean()
                return valid_ratio >= 0.8
            
            elif role == "debit":
                # Numeric >= 0
                numeric_series = pd.to_numeric(non_null_str, errors="coerce")
                numeric_ratio = numeric_series.notna().mean()
                if numeric_ratio < 0.8:
                    return False
                non_negative_ratio = (numeric_series.dropna() >= 0).mean()
                return non_negative_ratio >= 0.8
            
            elif role == "credit":
                # Numeric >= 0
                numeric_series = pd.to_numeric(non_null_str, errors="coerce")
                numeric_ratio = numeric_series.notna().mean()
                if numeric_ratio < 0.8:
                    return False
                non_negative_ratio = (numeric_series.dropna() >= 0).mean()
                return non_negative_ratio >= 0.8
            
            elif role == "opening_balance":
                # Numeric >= 0
                numeric_series = pd.to_numeric(non_null_str, errors="coerce")
                numeric_ratio = numeric_series.notna().mean()
                if numeric_ratio < 0.8:
                    return False
                non_negative_ratio = (numeric_series.dropna() >= 0).mean()
                return non_negative_ratio >= 0.8
            
            elif role == "closing_balance":
                # Numeric >= 0
                numeric_series = pd.to_numeric(non_null_str, errors="coerce")
                numeric_ratio = numeric_series.notna().mean()
                if numeric_ratio < 0.8:
                    return False
                non_negative_ratio = (numeric_series.dropna() >= 0).mean()
                return non_negative_ratio >= 0.8
            
            elif role == "phone":
                # Numeric, 10 digits (after removing separators)
                cleaned = non_null_str.str.replace(r'[\s\-\(\)]', '', regex=True)
                numeric_ratio = cleaned.str.fullmatch(r"\d+").mean()
                length_ratio = (cleaned.str.len() == 10).mean()
                return numeric_ratio >= 0.8 and length_ratio >= 0.8
            
            return False  # Unknown role
        except Exception:
            return False
    
    def detect_columns(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Detect which columns exist and map them to their roles.
        Only maps if BOTH column name matches AND data pattern matches.
        Returns dict mapping column_name -> role
        """
        column_roles = {}
        for col in df.columns:
            # First check if column name matches
            role = self.identify_column_role(col)
            if role:
                # Then verify data pattern matches
                if self.check_data_pattern_match(df[col], role):
                    column_roles[col] = role
        return column_roles
    
    # ==================== BUSINESS RULES VALIDATION ====================
    
    def validate_account_number(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Account number must be digits, 6-18 chars, can repeat."""
        violations = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule 1: Digits only
        digit_only_ratio = non_null.str.fullmatch(r"\d+").mean()
        if digit_only_ratio < 0.95:
            violations.append(f"Non-digit values found: {(1-digit_only_ratio)*100:.1f}%")
        
        # Rule 2: Length 6-18
        length_ok_ratio = non_null.str.len().between(6, 18).mean()
        if length_ok_ratio < 0.95:
            violations.append(f"Invalid length values: {(1-length_ok_ratio)*100:.1f}%")
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Account Number Business Rule"
        }
    
    def validate_customer_id(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Customer ID must be alphanumeric."""
        violations = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule: Alphanumeric
        alphanumeric_ratio = non_null.str.fullmatch(r"[A-Za-z0-9]+").mean()
        if alphanumeric_ratio < 0.95:
            violations.append(f"Non-alphanumeric values: {(1-alphanumeric_ratio)*100:.1f}%")
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Customer ID Business Rule"
        }
    
    def validate_customer_name(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Customer name must be text (letters/spaces), min 3 chars."""
        violations = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule 1: Letters and spaces
        letter_space_ratio = non_null.str.fullmatch(r"[A-Za-z\s]+").mean()
        if letter_space_ratio < 0.95:
            violations.append(f"Invalid characters: {(1-letter_space_ratio)*100:.1f}%")
        
        # Rule 2: Min 3 characters
        min_length_ratio = (non_null.str.len() >= 3).mean()
        if min_length_ratio < 0.95:
            violations.append(f"Short names (<3 chars): {(1-min_length_ratio)*100:.1f}%")
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Customer Name Business Rule"
        }
    
    def validate_account_type(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Account type must be from allowed values."""
        violations = []
        non_null = series.dropna().astype(str).str.upper().str.strip()
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule: Must be from allowed types
        valid_ratio = non_null.isin([v.upper() for v in self.ALLOWED_ACCOUNT_TYPES]).mean()
        if valid_ratio < 0.95:
            invalid_values = non_null[~non_null.isin([v.upper() for v in self.ALLOWED_ACCOUNT_TYPES])].unique().tolist()[:5]
            violations.append(f"Invalid account types: {invalid_values}")
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Account Type Business Rule"
        }
    
    def validate_account_status(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Account status must be from allowed values."""
        violations = []
        non_null = series.dropna().astype(str).str.upper().str.strip()
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule: Must be from allowed statuses
        valid_ratio = non_null.isin([v.upper() for v in self.ALLOWED_ACCOUNT_STATUSES]).mean()
        if valid_ratio < 0.95:
            invalid_values = non_null[~non_null.isin([v.upper() for v in self.ALLOWED_ACCOUNT_STATUSES])].unique().tolist()[:5]
            violations.append(f"Invalid account statuses: {invalid_values}")
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Account Status Business Rule"
        }
    
    def validate_branch_code(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Branch code must be alphanumeric (spaces allowed)."""
        violations = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule: Alphanumeric with spaces allowed (for branch names like "Main Branch")
        alphanumeric_space_ratio = non_null.str.fullmatch(r"[A-Za-z0-9\s]+").mean()
        if alphanumeric_space_ratio < 0.95:
            violations.append(f"Invalid characters: {(1-alphanumeric_space_ratio)*100:.1f}%")
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Branch Code Business Rule"
        }
    
    def validate_ifsc_code(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: IFSC code must be alphanumeric (flexible format)."""
        violations = []
        non_null = series.dropna().astype(str).str.upper().str.strip()
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule 1: Alphanumeric only
        alphanumeric_ratio = non_null.str.fullmatch(r"[A-Z0-9]+").mean()
        if alphanumeric_ratio < 0.95:
            violations.append(f"Non-alphanumeric values: {(1-alphanumeric_ratio)*100:.1f}%")
        
        # Rule 2: Reasonable length (3-15 characters)
        length_ok_ratio = non_null.str.len().between(3, 15).mean()
        if length_ok_ratio < 0.95:
            violations.append(f"Invalid length: {(1-length_ok_ratio)*100:.1f}% (Expected: 3-15 characters)")
        
        # Note: We don't enforce strict IFSC format (4 letters + 0 + 6 digits) 
        # because users may have their own format codes
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "IFSC Code Business Rule"
        }
    
    def validate_transaction_id(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Transaction ID must be alphanumeric and unique."""
        violations = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule 1: Alphanumeric
        alphanumeric_ratio = non_null.str.fullmatch(r"[A-Za-z0-9]+").mean()
        if alphanumeric_ratio < 0.95:
            violations.append(f"Non-alphanumeric values: {(1-alphanumeric_ratio)*100:.1f}%")
        
        # Rule 2: Should be unique (warning if not, but not a failure)
        unique_ratio = non_null.nunique() / len(non_null) if len(non_null) > 0 else 0
        if unique_ratio < 0.9:
            violations.append(f"Warning: Low uniqueness {unique_ratio*100:.1f}% (should be unique per transaction)")
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Transaction ID Business Rule"
        }
    
    def validate_transaction_date(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Transaction date must be valid date format."""
        violations = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule: Must be parseable as date
        date_parsed = pd.to_datetime(non_null, errors="coerce")
        valid_date_ratio = date_parsed.notna().mean()
        if valid_date_ratio < 0.95:
            violations.append(f"Invalid date format: {(1-valid_date_ratio)*100:.1f}%")
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Transaction Date Business Rule"
        }
    
    def validate_transaction_type(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Transaction type must be from allowed values."""
        violations = []
        non_null = series.dropna().astype(str).str.upper().str.strip()
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule: Must be from allowed types
        valid_ratio = non_null.isin([v.upper() for v in self.ALLOWED_TRANSACTION_TYPES]).mean()
        if valid_ratio < 0.95:
            invalid_values = non_null[~non_null.isin([v.upper() for v in self.ALLOWED_TRANSACTION_TYPES])].unique().tolist()[:5]
            violations.append(f"Invalid transaction types: {invalid_values}")
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Transaction Type Business Rule"
        }
    
    def validate_debit(self, series: pd.Series, credit_series: Optional[pd.Series] = None) -> Dict[str, Any]:
        """Business Rule: Debit must be numeric, >= 0, mutually exclusive with credit."""
        violations = []
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_null = numeric_series.dropna()
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule 1: Numeric and >= 0
        non_negative_ratio = (non_null >= 0).mean()
        if non_negative_ratio < 0.95:
            violations.append(f"Negative values found: {(1-non_negative_ratio)*100:.1f}%")
        
        # Rule 2: Mutually exclusive with credit (if credit column exists)
        if credit_series is not None:
            credit_numeric = pd.to_numeric(credit_series, errors="coerce").fillna(0)
            both_positive = ((non_null > 0) & (credit_numeric > 0)).sum()
            if both_positive > len(series) * 0.1:  # More than 10% have both > 0
                violations.append(f"Warning: {both_positive} rows have both debit and credit > 0 (should be mutually exclusive)")
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Debit Business Rule"
        }
    
    def validate_credit(self, series: pd.Series, debit_series: Optional[pd.Series] = None) -> Dict[str, Any]:
        """Business Rule: Credit must be numeric, >= 0, mutually exclusive with debit."""
        violations = []
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_null = numeric_series.dropna()
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule 1: Numeric and >= 0
        non_negative_ratio = (non_null >= 0).mean()
        if non_negative_ratio < 0.95:
            violations.append(f"Negative values found: {(1-non_negative_ratio)*100:.1f}%")
        
        # Rule 2: Mutually exclusive with debit (if debit column exists)
        if debit_series is not None:
            debit_numeric = pd.to_numeric(debit_series, errors="coerce").fillna(0)
            both_positive = ((non_null > 0) & (debit_numeric > 0)).sum()
            if both_positive > len(series) * 0.1:  # More than 10% have both > 0
                violations.append(f"Warning: {both_positive} rows have both debit and credit > 0 (should be mutually exclusive)")
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Credit Business Rule"
        }
    
    def validate_opening_balance(self, series: pd.Series, closing_series: Optional[pd.Series] = None,
                                debit_series: Optional[pd.Series] = None,
                                credit_series: Optional[pd.Series] = None) -> Dict[str, Any]:
        """Business Rule: Opening balance must be numeric, >= 0, and match balance formula."""
        violations = []
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_null = numeric_series.dropna()
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule 1: Numeric and >= 0
        non_negative_ratio = (non_null >= 0).mean()
        if non_negative_ratio < 0.95:
            violations.append(f"Negative values found: {(1-non_negative_ratio)*100:.1f}%")
        
        # Rule 2: Balance formula validation (if all related columns exist)
        if closing_series is not None and debit_series is not None and credit_series is not None:
            try:
                opening_vals = pd.to_numeric(series, errors="coerce").fillna(0)
                closing_vals = pd.to_numeric(closing_series, errors="coerce").fillna(0)
                debit_vals = pd.to_numeric(debit_series, errors="coerce").fillna(0)
                credit_vals = pd.to_numeric(credit_series, errors="coerce").fillna(0)
                
                # Formula: Closing = Opening + Credit - Debit
                calculated_closing = opening_vals + credit_vals - debit_vals
                diff = abs(closing_vals - calculated_closing)
                tolerance = closing_vals.abs() * 0.01 + 0.01  # 1% tolerance
                matches = (diff <= tolerance).sum()
                match_ratio = matches / len(series) if len(series) > 0 else 0
                
                if match_ratio < 0.9:
                    violations.append(f"Balance formula mismatch: {(1-match_ratio)*100:.1f}% rows don't match (Closing = Opening + Credit - Debit)")
            except Exception as e:
                violations.append(f"Error validating balance formula: {str(e)}")
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Opening Balance Business Rule"
        }
    
    def validate_closing_balance(self, series: pd.Series, opening_series: Optional[pd.Series] = None,
                                debit_series: Optional[pd.Series] = None,
                                credit_series: Optional[pd.Series] = None) -> Dict[str, Any]:
        """Business Rule: Closing balance must be numeric, >= 0, and match balance formula."""
        violations = []
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_null = numeric_series.dropna()
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule 1: Numeric and >= 0
        non_negative_ratio = (non_null >= 0).mean()
        if non_negative_ratio < 0.95:
            violations.append(f"Negative values found: {(1-non_negative_ratio)*100:.1f}%")
        
        # Rule 2: Balance formula validation (if all related columns exist)
        if opening_series is not None and debit_series is not None and credit_series is not None:
            try:
                opening_vals = pd.to_numeric(opening_series, errors="coerce").fillna(0)
                closing_vals = pd.to_numeric(series, errors="coerce").fillna(0)
                debit_vals = pd.to_numeric(debit_series, errors="coerce").fillna(0)
                credit_vals = pd.to_numeric(credit_series, errors="coerce").fillna(0)
                
                # Formula: Closing = Opening + Credit - Debit
                calculated_closing = opening_vals + credit_vals - debit_vals
                diff = abs(closing_vals - calculated_closing)
                tolerance = closing_vals.abs() * 0.01 + 0.01  # 1% tolerance
                matches = (diff <= tolerance).sum()
                match_ratio = matches / len(series) if len(series) > 0 else 0
                
                if match_ratio < 0.9:
                    violations.append(f"Balance formula mismatch: {(1-match_ratio)*100:.1f}% rows don't match (Closing = Opening + Credit - Debit)")
            except Exception as e:
                violations.append(f"Error validating balance formula: {str(e)}")
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Closing Balance Business Rule"
        }
    
    def validate_phone(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Phone number must be numeric, 10 digits."""
        violations = []
        non_null = series.dropna().astype(str).str.replace(r'[\s\-\(\)]', '', regex=True)
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule 1: Numeric only (after removing common separators)
        numeric_ratio = non_null.str.fullmatch(r"\d+").mean()
        if numeric_ratio < 0.95:
            violations.append(f"Non-numeric values: {(1-numeric_ratio)*100:.1f}%")
        
        # Rule 2: Length 10 digits (Indian phone numbers)
        length_ok_ratio = (non_null.str.len() == 10).mean()
        if length_ok_ratio < 0.95:
            violations.append(f"Invalid length (not 10 digits): {(1-length_ok_ratio)*100:.1f}%")
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Phone Business Rule"
        }
    
    def validate_pan(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: PAN must follow pattern 5 letters + 4 digits + 1 letter."""
        violations = []
        non_null = series.dropna().astype(str).str.upper().str.strip()
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule: PAN pattern
        pan_pattern = r"[A-Z]{5}[0-9]{4}[A-Z]"
        match_ratio = non_null.str.fullmatch(pan_pattern).mean()
        if match_ratio < 0.95:
            violations.append(f"Invalid PAN format: {(1-match_ratio)*100:.1f}% (Expected: 5 letters + 4 digits + 1 letter)")
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "PAN Business Rule"
        }
    
    # ==================== MAIN VALIDATION METHOD ====================
    
    def validate(self, csv_path: str) -> Dict[str, Any]:
        """
        Main validation function.
        
        Dynamically detects columns and applies business rules only to existing columns.
        Missing columns are skipped gracefully.
        
        Returns:
            Dict with validation results including:
            - detected_columns: dict of column_name -> role
            - business_rules: list of rule validation results
            - summary: overall summary
        """
        try:
            # Load dataset
            df = pd.read_csv(csv_path)
            
            if df.empty:
                return {
                    "error": "Dataset is empty",
                    "detected_columns": {},
                    "business_rules": [],
                    "summary": {
                        "total_columns": 0,
                        "detected_columns": 0,
                        "rules_applied": 0,
                        "rules_passed": 0,
                        "rules_failed": 0,
                        "rules_skipped": 0
                    }
                }
            
            # STEP 1: Detect columns dynamically
            column_roles = self.detect_columns(df)
            
            # STEP 2: Apply business rules for each detected column
            business_rules = []
            
            # Get column references for cross-column validations
            debit_col = None
            credit_col = None
            opening_col = None
            closing_col = None
            
            for col_name, role in column_roles.items():
                if role == "debit":
                    debit_col = col_name
                elif role == "credit":
                    credit_col = col_name
                elif role == "opening_balance":
                    opening_col = col_name
                elif role == "closing_balance":
                    closing_col = col_name
            
            # Apply rules for each column
            for col_name, role in column_roles.items():
                series = df[col_name]
                rule_result = None
                
                if role == "account_number":
                    rule_result = self.validate_account_number(series)
                elif role == "customer_id":
                    rule_result = self.validate_customer_id(series)
                elif role == "customer_name":
                    rule_result = self.validate_customer_name(series)
                elif role == "account_type":
                    rule_result = self.validate_account_type(series)
                elif role == "account_status":
                    rule_result = self.validate_account_status(series)
                elif role == "branch_code":
                    rule_result = self.validate_branch_code(series)
                elif role == "ifsc_code":
                    rule_result = self.validate_ifsc_code(series)
                elif role == "pan":
                    rule_result = self.validate_pan(series)
                elif role == "transaction_id":
                    rule_result = self.validate_transaction_id(series)
                elif role == "transaction_date":
                    rule_result = self.validate_transaction_date(series)
                elif role == "transaction_type":
                    rule_result = self.validate_transaction_type(series)
                elif role == "debit":
                    credit_series = df[credit_col] if credit_col else None
                    rule_result = self.validate_debit(series, credit_series)
                elif role == "credit":
                    debit_series = df[debit_col] if debit_col else None
                    rule_result = self.validate_credit(series, debit_series)
                elif role == "opening_balance":
                    closing_series = df[closing_col] if closing_col else None
                    debit_series = df[debit_col] if debit_col else None
                    credit_series = df[credit_col] if credit_col else None
                    rule_result = self.validate_opening_balance(series, closing_series, debit_series, credit_series)
                elif role == "closing_balance":
                    opening_series = df[opening_col] if opening_col else None
                    debit_series = df[debit_col] if debit_col else None
                    credit_series = df[credit_col] if credit_col else None
                    rule_result = self.validate_closing_balance(series, opening_series, debit_series, credit_series)
                elif role == "phone":
                    rule_result = self.validate_phone(series)
                
                if rule_result:
                    rule_result["column_name"] = col_name
                    rule_result["column_role"] = role
                    business_rules.append(rule_result)
            
            # STEP 3: Show ALL business rules (including missing columns)
            all_expected_roles = list(self.COLUMN_VARIATIONS.keys())
            all_business_rules = []
            
            # Add rules for detected columns
            for rule in business_rules:
                all_business_rules.append(rule)
            
            # Add rules for missing columns (show as NOT_APPLICABLE)
            detected_roles = set(column_roles.values())
            for role in all_expected_roles:
                if role not in detected_roles:
                    # Check if any column name matches but data pattern doesn't
                    matched_col = None
                    for col in df.columns:
                        if self.identify_column_role(col) == role:
                            matched_col = col
                            break
                    
                    if matched_col:
                        # Column name matches but data pattern doesn't - show as pattern mismatch
                        all_business_rules.append({
                            "column_name": matched_col,
                            "column_role": role,
                            "status": "PATTERN_MISMATCH",
                            "rule_name": f"{role.replace('_', ' ').title()} Business Rule",
                            "violations": [f"Column name matches but data pattern does not match expected format"],
                            "reason": "Column name matches but data pattern validation failed"
                        })
                    else:
                        # Column doesn't exist at all
                        all_business_rules.append({
                            "column_name": None,
                            "column_role": role,
                            "status": "NOT_APPLICABLE",
                            "rule_name": f"{role.replace('_', ' ').title()} Business Rule",
                            "violations": [],
                            "reason": "Column not present in dataset"
                        })
            
            # STEP 4: Calculate summary
            total_rules = len(all_business_rules)
            applied_rules = len(business_rules)
            passed_rules = len([r for r in business_rules if r["status"] == "PASS"])
            failed_rules = len([r for r in business_rules if r["status"] == "FAIL"])
            skipped_rules = len([r for r in business_rules if r["status"] == "SKIPPED"])
            pattern_mismatch_rules = len([r for r in all_business_rules if r["status"] == "PATTERN_MISMATCH"])
            not_applicable_rules = len([r for r in all_business_rules if r["status"] == "NOT_APPLICABLE"])
            
            summary = {
                "total_columns": len(df.columns),
                "detected_columns": len(column_roles),
                "total_business_rules": total_rules,
                "rules_applied": applied_rules,
                "rules_passed": passed_rules,
                "rules_failed": failed_rules,
                "rules_skipped": skipped_rules,
                "rules_pattern_mismatch": pattern_mismatch_rules,
                "rules_not_applicable": not_applicable_rules
            }
            
            return {
                "detected_columns": column_roles,
                "business_rules": all_business_rules,
                "summary": summary
            }
        
        except Exception as e:
            return {
                "error": str(e),
                "detected_columns": {},
                "business_rules": [],
                "summary": {
                    "total_columns": 0,
                    "detected_columns": 0,
                    "rules_applied": 0,
                    "rules_passed": 0,
                    "rules_failed": 0,
                    "rules_skipped": 0
                }
            }
