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
        "customer_id": ["customer_id", "cust_id", "customerid", "custid", "client_id","C"],
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
        "phone": ["phone", "mobile", "contact", "telephone", "phone_number", "mobile_number"],
        # Additional columns - NOT unique identifiers (can repeat, NOT keys)
        "dob": ["dob", "date_of_birth", "birth_date", "birthdate"],  # NOT UNIQUE - many people share same DOB
        "address": ["address", "street_address", "residential_address", "address_line1", "address_line2"],
        "city": ["city", "city_name"],
        "rate_percentage": ["rate_percentage", "interest_rate", "rate", "roi", "annual_rate", "percentage"],
        "kyc_status": ["kyc_status", "kyc", "verification_status"],
        "employee_status": ["employee_status", "emp_status", "staff_status"],
        "loan_account_no": ["loan_account_no", "loan_account", "loan_account_number"],
        # Generic transaction amount (NOT EMI-specific)
        "amount": ["amount", "transaction_amount", "txn_amount", "amt", "value"],
        # New Banking Columns
        "card_present_flag": ["card_present_flag", "cp_flag", "card_present", "is_card_present"],
        "bpay_biller_code": ["bpay_biller_code", "biller_code", "bpay_code"],
        "txn_description": ["txn_description", "transaction_description", "narration", "description", "details"],
        "merchant_id": ["merchant_id", "merch_id", "mid", "merchant_identifier"],
        "merchant_code": ["merchant_code", "mcc", "merchant_category_code", "category_code"],
        "merchant_suburb": ["merchant_suburb", "suburb", "merchant_city"],
        "merchant_state": ["merchant_state", "state_code", "merchant_province"],
        "currency": ["currency", "curr", "iso_code", "txn_currency"],
        "long_lat": ["long_lat", "lat_long", "coordinates", "geo_location", "customer_location"],
        "merchant_long_lat": ["merchant_long_lat", "merchant_coordinates", "store_location"],
        "extraction": ["extraction", "extraction_id", "batch_id", "run_id", "load_id", "process_id"]
    }
    
    # Allowed values for enumerated columns
    ALLOWED_ACCOUNT_TYPES = ["Savings", "Current", "Salary", "Student", "Pension"]
    ALLOWED_ACCOUNT_STATUSES = ["Active", "Frozen", "Closed"]  # Account-specific statuses (NOT KYC or Employee)
    ALLOWED_TRANSACTION_TYPES = ["Debit", "Credit"]
    # Separate status values for different entities
    ALLOWED_KYC_STATUSES = ["Pending", "Verified", "Rejected"]  # KYC-specific statuses (NOT account status)
    ALLOWED_EMPLOYEE_STATUSES = ["Active", "Resigned", "Suspended"]  # Employee-specific statuses (NOT account status)
    
    def __init__(self):
        """Initialize the validator."""
        pass
    
    def normalize_column_name(self, col_name: str) -> str:
        """Normalize column name for matching."""
        # Handle CamelCase to snake_case conversion (e.g. CustomerId -> customer_id)
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', str(col_name))
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
        return s2.lower().strip().replace(" ", "_").replace("-", "_")
    
    def identify_column_role(self, col_name: str) -> Optional[str]:
        """
        Identify the role of a column based on name matching.
        Returns the matched column role or None.
        
        CRITICAL: account_number in transactions.csv is ALWAYS a Foreign Key, NEVER an amount field.
        """
        normalized = self.normalize_column_name(col_name)
        
        # CRITICAL PRIORITY: account_number should NEVER be treated as amount
        # Check account_number patterns BEFORE generic amount patterns
        if "account" in normalized and ("number" in normalized or "no" in normalized or "num" in normalized):
            for variation in self.COLUMN_VARIATIONS["account_number"]:
                if variation in normalized or normalized == variation:
                    return "account_number"  # Always FK, never amount
        
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
        
        # 4. Status fields - must be specific to avoid confusion
        if "kyc" in normalized and "status" in normalized:
            for variation in self.COLUMN_VARIATIONS["kyc_status"]:
                if variation in normalized or normalized == variation:
                    return "kyc_status"
        
        if ("employee" in normalized or "emp" in normalized or "staff" in normalized) and "status" in normalized:
            for variation in self.COLUMN_VARIATIONS["employee_status"]:
                if variation in normalized or normalized == variation:
                    return "employee_status"
        
        # 5. Loan account number (specific pattern)
        if "loan" in normalized and "account" in normalized:
            for variation in self.COLUMN_VARIATIONS["loan_account_no"]:
                if variation in normalized or normalized == variation:
                    return "loan_account_no"
        
        # 6. Check all other columns (exact matches first, then contains)
        # Sort by specificity (longer variations first)
        sorted_roles = sorted(
            self.COLUMN_VARIATIONS.items(),
            key=lambda x: max(len(v) for v in x[1]),
            reverse=True
        )
        
        for role, variations in sorted_roles:
            # Skip if already handled above
            if role in ["transaction_type", "transaction_date", "account_type", "account_number", "kyc_status", "employee_status", "loan_account_no"]:
                continue
            
            # Exact match first
            if normalized in variations:
                # CRITICAL: Ensure account_number is never treated as amount
                if role == "amount" and "account" in normalized:
                    continue
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
                    # CRITICAL: account_number should NEVER be treated as amount
                    if role == "amount" and "account" in normalized:
                        continue
                    # CRITICAL: Ensure status fields don't match incorrectly
                    if role == "account_status" and ("kyc" in normalized or "employee" in normalized or "emp" in normalized):
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
                normalized = non_null_str.str.title().str.strip()
                valid_ratio = normalized.isin(self.ALLOWED_ACCOUNT_TYPES).mean()
                return valid_ratio >= 0.8
            
            elif role == "account_status":
                # Must be from allowed statuses
                normalized = non_null_str.str.title().str.strip()
                normalized = normalized.replace('Inactive', 'Deactive')
                valid_ratio = normalized.isin(self.ALLOWED_ACCOUNT_STATUSES).mean()
                return valid_ratio >= 0.8
            
            elif role == "branch_code":
                # Alphanumeric with spaces
                alphanumeric_space_ratio = non_null_str.str.fullmatch(r"[A-Za-z0-9\s]+").mean()
                return alphanumeric_space_ratio >= 0.8
            
            elif role == "ifsc_code":
                # Alphanumeric, length 8-11
                alphanumeric_ratio = non_null_str.str.fullmatch(r"[A-Za-z0-9]+").mean()
                length_ratio = non_null_str.str.len().between(8, 11).mean()
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
                normalized = non_null_str.str.title().str.strip()
                valid_ratio = normalized.isin(self.ALLOWED_TRANSACTION_TYPES).mean()
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
            
            elif role == "dob":
                # Must be parseable as date
                date_parsed = pd.to_datetime(non_null_str, errors="coerce")
                valid_date_ratio = date_parsed.notna().mean()
                return valid_date_ratio >= 0.8
            
            elif role == "address":
                # Free text, alphanumeric with spaces and special chars, min 10 chars
                min_length_ratio = (non_null_str.str.len() >= 10).mean()
                return min_length_ratio >= 0.8  # Address can repeat, NOT unique
            
            elif role == "city":
                # Text, letters and spaces, min 2 chars
                letter_space_ratio = non_null_str.str.fullmatch(r"[A-Za-z\s]+").mean()
                min_length_ratio = (non_null_str.str.len() >= 2).mean()
                return letter_space_ratio >= 0.8 and min_length_ratio >= 0.8  # City can repeat, NOT unique
            
            elif role == "rate_percentage":
                # Numeric, typically 0-100 (percentage)
                numeric_series = pd.to_numeric(non_null_str, errors="coerce")
                numeric_ratio = numeric_series.notna().mean()
                if numeric_ratio < 0.8:
                    return False
                return True  # Rate can repeat, NOT unique (many products share same rate)
            
            elif role == "kyc_status":
                # Must be from allowed KYC statuses
                normalized = non_null_str.str.title().str.strip()
                valid_ratio = normalized.isin(self.ALLOWED_KYC_STATUSES).mean()
                return valid_ratio >= 0.8
            
            elif role == "employee_status":
                # Must be from allowed employee statuses
                normalized = non_null_str.str.title().str.strip()
                valid_ratio = normalized.isin(self.ALLOWED_EMPLOYEE_STATUSES).mean()
                return valid_ratio >= 0.8
            
            elif role == "loan_account_no":
                # Alphanumeric or numeric, typically 6-20 chars
                alphanumeric_ratio = non_null_str.str.fullmatch(r"[A-Za-z0-9]+").mean()
                length_ratio = non_null_str.str.len().between(6, 20).mean()
                return alphanumeric_ratio >= 0.8 and length_ratio >= 0.8
            
            elif role == "amount":
                # Generic transaction amount - numeric >= 0
                # CRITICAL: This is NOT EMI-specific, applies to all transaction amounts
                numeric_series = pd.to_numeric(non_null_str, errors="coerce")
                numeric_ratio = numeric_series.notna().mean()
                if numeric_ratio < 0.8:
                    return False
                non_negative_ratio = (numeric_series.dropna() >= 0).mean()
                return non_negative_ratio >= 0.8

            # --- NEW PATTERN CHECKS ---
            elif role == "card_present_flag":
                # Boolean-like (Y/N/0/1)
                valid_vals = ["Y", "N", "YES", "NO", "T", "F", "TRUE", "FALSE", "0", "1"]
                normalized = non_null_str.str.upper().str.strip()
                return normalized.isin(valid_vals).mean() >= 0.8

            elif role == "bpay_biller_code":
                # Numeric/Alphanumeric, 3-10 chars
                length_ok = non_null_str.str.len().between(3, 10).mean() >= 0.8
                return length_ok

            elif role == "txn_description":
                # Text, usually has spaces, length >= 5
                has_space = non_null_str.str.contains(" ").mean() >= 0.5
                min_len = (non_null_str.str.len() >= 5).mean() >= 0.8
                return has_space or min_len

            elif role == "merchant_id":
                # Alphanumeric
                return non_null_str.str.fullmatch(r"[A-Za-z0-9\-\s]+").mean() >= 0.8

            elif role == "merchant_code":
                # 4 digits
                return non_null_str.str.fullmatch(r"\d{4}").mean() >= 0.9

            elif role == "merchant_suburb":
                # Text
                return non_null_str.str.fullmatch(r"[A-Za-z\s]+").mean() >= 0.8

            elif role == "merchant_state":
                # 2-3 uppercase letters
                return non_null_str.str.match(r"[A-Z]{2,3}").mean() >= 0.8

            elif role == "currency":
                # 3 letters
                return non_null_str.str.match(r"[A-Z]{3}").mean() >= 0.9

            elif role == "long_lat" or role == "merchant_long_lat":
                # Numeric, decimals
                is_numeric = pd.to_numeric(non_null_str, errors='coerce').notna().mean() >= 0.9
                return is_numeric

            elif role == "extraction":
                # Alphanumeric
                return non_null_str.str.isalnum().mean() >= 0.8
            
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
        """
        Business Rule: Account number must be digits, 6-18 chars.
        
        CRITICAL: In transactions.csv, account_number is ALWAYS a Foreign Key (FK).
        It links transaction → account. NEVER used in calculations.
        It can repeat (same account can have multiple transactions).
        """
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
        
        # NOTE: account_number can repeat (duplicates allowed) - same account can have multiple transactions
        # This is NOT a uniqueness violation - it's a Foreign Key, not a Primary Key
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Account Number Business Rule (Foreign Key in Transactions)",
            "note": "In transactions table, account_number is a Foreign Key linking to accounts. Duplicates are expected and valid."
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
        """Business Rule: Account type must be from allowed values (Savings, Current, Salary, Student, or Pension)."""
        violations = []
        non_null = series.dropna().astype(str).str.strip()
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Normalize to title case for comparison (case-insensitive)
        normalized = non_null.str.title()
        # Rule: Must be from allowed types
        valid_ratio = normalized.isin(self.ALLOWED_ACCOUNT_TYPES).mean()
        if valid_ratio < 0.95:
            invalid_values = non_null[~normalized.isin(self.ALLOWED_ACCOUNT_TYPES)].unique().tolist()[:5]
            violations.append(f"Invalid account types: {invalid_values}. Allowed types: 'Savings', 'Current', 'Salary', 'Student', or 'Pension'.")
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Account Type Business Rule"
        }
    
    def validate_account_status(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Account status must be from allowed values (Active, Frozen, Closed only)."""
        violations = []
        non_null = series.dropna().astype(str).str.strip()
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Normalize to title case for comparison (case-insensitive)
        normalized = non_null.str.title()
        # Map common variations to correct values
        normalized = normalized.replace('Inactive', 'Frozen')  # Map Inactive to Frozen
        normalized = normalized.replace('Deactive', 'Frozen')  # Map Deactive to Frozen
        normalized = normalized.replace('De-Active', 'Frozen')
        normalized = normalized.replace('De Active', 'Frozen')
        
        # Rule: Must be from allowed account statuses (Active, Frozen, Closed)
        valid_ratio = normalized.isin(self.ALLOWED_ACCOUNT_STATUSES).mean()
        if valid_ratio < 0.95:
            invalid_values = non_null[~normalized.isin(self.ALLOWED_ACCOUNT_STATUSES)].unique().tolist()[:5]
            violations.append(f"Invalid account statuses: {invalid_values}. Allowed: {', '.join(self.ALLOWED_ACCOUNT_STATUSES)} (NOT KYC or Employee statuses)")
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Account Status Business Rule (Separate from KYC/Employee Status)",
            "note": "Account status values: Active, Frozen, Closed. This is different from KYC status (Pending/Verified/Rejected) and Employee status (Active/Resigned/Suspended)."
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
        """Business Rule: IFSC code must be alphanumeric, 8-11 characters."""
        violations = []
        non_null = series.dropna().astype(str).str.strip()
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule 1: Alphanumeric only (case-insensitive)
        alphanumeric_ratio = non_null.str.fullmatch(r"[A-Za-z0-9]+").mean()
        if alphanumeric_ratio < 0.95:
            violations.append(f"Non-alphanumeric values: {(1-alphanumeric_ratio)*100:.1f}%")
        
        # Rule 2: Length between 8-11 characters
        length_ok_ratio = non_null.str.len().between(8, 11).mean()
        if length_ok_ratio < 0.95:
            violations.append(f"Invalid length: {(1-length_ok_ratio)*100:.1f}% (Expected: 8-11 characters)")
        
        # Note: Accepts IFSC codes between 8-11 characters (e.g., IFSC0001, IFSC0000001, etc.)
        
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
        """Business Rule: Transaction type must be from allowed values (Debit or Credit only)."""
        violations = []
        non_null = series.dropna().astype(str).str.strip()
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Normalize to title case for comparison (case-insensitive)
        normalized = non_null.str.title()
        # Rule: Must be from allowed types
        valid_ratio = normalized.isin(self.ALLOWED_TRANSACTION_TYPES).mean()
        if valid_ratio < 0.95:
            invalid_values = non_null[~normalized.isin(self.ALLOWED_TRANSACTION_TYPES)].unique().tolist()[:5]
            violations.append(f"Invalid transaction types: {invalid_values}. Only 'Debit' or 'Credit' are allowed.")
        
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
    
    def validate_dob(self, series: pd.Series) -> Dict[str, Any]:
        """
        Business Rule: Date of birth must be valid date format.
        
        CRITICAL: DOB is NOT unique - many people share the same date of birth.
        DOB should NEVER be used as a unique identifier or primary key.
        DOB is descriptive data, NOT an identifier.
        """
        violations = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule: Must be parseable as date
        date_parsed = pd.to_datetime(non_null, errors="coerce")
        valid_date_ratio = date_parsed.notna().mean()
        if valid_date_ratio < 0.95:
            violations.append(f"Invalid date format: {(1-valid_date_ratio)*100:.1f}%")
        
        # CRITICAL: DOB can repeat (NOT unique) - many people share same date of birth
        # DOB is NOT an identifier, NOT a key, NOT unique
        # Multiple customers can have the same DOB - this is expected and valid
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Date of Birth Business Rule (NOT Unique, NOT Identifier)",
            "note": "DOB can repeat - many people share same date of birth. This is descriptive data, NOT a unique identifier, NOT a key. DOB should NEVER be used for uniqueness checks."
        }
    
    def validate_address(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Address is free text, can repeat, NOT a key, NOT referenceable."""
        violations = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule 1: Minimum length 10 characters
        min_length_ratio = (non_null.str.len() >= 10).mean()
        if min_length_ratio < 0.95:
            violations.append(f"Short addresses (<10 chars): {(1-min_length_ratio)*100:.1f}%")
        
        # NOTE: Address can repeat (NOT unique) - multiple customers can have same address
        # Address is free text, NOT a key, NOT an identifier, NOT referenceable
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Address Business Rule (NOT Unique, NOT Identifier)",
            "note": "Address is free text, can repeat, NOT a key, NOT an identifier. Multiple customers can share same address."
        }
    
    def validate_city(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: City is text, can repeat, NOT unique, NOT an identifier."""
        violations = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule 1: Letters and spaces only
        letter_space_ratio = non_null.str.fullmatch(r"[A-Za-z\s]+").mean()
        if letter_space_ratio < 0.95:
            violations.append(f"Invalid characters: {(1-letter_space_ratio)*100:.1f}%")
        
        # Rule 2: Minimum 2 characters
        min_length_ratio = (non_null.str.len() >= 2).mean()
        if min_length_ratio < 0.95:
            violations.append(f"Short city names (<2 chars): {(1-min_length_ratio)*100:.1f}%")
        
        # NOTE: City can repeat (NOT unique) - many customers can be in same city
        # City is descriptive, NOT an identifier, NOT a key
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "City Business Rule (NOT Unique, NOT Identifier)",
            "note": "City can repeat - many customers can be in same city. This is descriptive, NOT an identifier."
        }
    
    def validate_rate_percentage(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Rate percentage is numeric, can repeat, NOT unique (many products share same rate)."""
        violations = []
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_null = numeric_series.dropna()
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule 1: Must be numeric
        numeric_ratio = numeric_series.notna().mean()
        if numeric_ratio < 0.95:
            violations.append(f"Non-numeric values: {(1-numeric_ratio)*100:.1f}%")
        
        # Rule 2: Typically 0-100 (percentage)
        # Allow wider range for flexibility (0-1000)
        valid_range_ratio = ((non_null >= 0) & (non_null <= 1000)).mean()
        if valid_range_ratio < 0.95:
            violations.append(f"Out of range values (expected 0-1000): {(1-valid_range_ratio)*100:.1f}%")
        
        # NOTE: Rate percentage can repeat (NOT unique)
        # Many products can share same interest rate
        # Rates change over time
        # Logical uniqueness is (product_code + effective_date), NOT rate_percentage alone
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Rate Percentage Business Rule (NOT Unique)",
            "note": "Rate percentage can repeat - many products can share same rate. Rates change over time. Logical uniqueness is (product_code + effective_date), NOT rate_percentage alone."
        }
    
    def validate_kyc_status(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: KYC status must be from allowed values (Pending/Verified/Rejected), NOT account status."""
        violations = []
        non_null = series.dropna().astype(str).str.strip()
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Normalize to title case for comparison (case-insensitive)
        normalized = non_null.str.title()
        
        # Rule: Must be from allowed KYC statuses (NOT account statuses)
        valid_ratio = normalized.isin(self.ALLOWED_KYC_STATUSES).mean()
        if valid_ratio < 0.95:
            invalid_values = non_null[~normalized.isin(self.ALLOWED_KYC_STATUSES)].unique().tolist()[:5]
            violations.append(f"Invalid KYC status values: {invalid_values}. Allowed: {', '.join(self.ALLOWED_KYC_STATUSES)} (NOT account statuses)")
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "KYC Status Business Rule (Separate from Account Status)",
            "note": "KYC status has different values than account status. KYC: Pending/Verified/Rejected. Account: Active/Frozen/Closed."
        }
    
    def validate_employee_status(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Employee status must be from allowed values (Active/Resigned/Suspended), NOT account status."""
        violations = []
        non_null = series.dropna().astype(str).str.strip()
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Normalize to title case for comparison (case-insensitive)
        normalized = non_null.str.title()
        
        # Rule: Must be from allowed employee statuses (NOT account statuses)
        valid_ratio = normalized.isin(self.ALLOWED_EMPLOYEE_STATUSES).mean()
        if valid_ratio < 0.95:
            invalid_values = non_null[~normalized.isin(self.ALLOWED_EMPLOYEE_STATUSES)].unique().tolist()[:5]
            violations.append(f"Invalid employee status values: {invalid_values}. Allowed: {', '.join(self.ALLOWED_EMPLOYEE_STATUSES)} (NOT account statuses)")
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Employee Status Business Rule (Separate from Account Status)",
            "note": "Employee status has different values than account status. Employee: Active/Resigned/Suspended. Account: Active/Frozen/Closed."
        }
    
    def validate_loan_account_no(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Loan account number must be alphanumeric, 6-20 chars. Can be marked unique in Loan table only."""
        violations = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule 1: Alphanumeric
        alphanumeric_ratio = non_null.str.fullmatch(r"[A-Za-z0-9]+").mean()
        if alphanumeric_ratio < 0.95:
            violations.append(f"Non-alphanumeric values: {(1-alphanumeric_ratio)*100:.1f}%")
        
        # Rule 2: Length 6-20
        length_ok_ratio = non_null.str.len().between(6, 20).mean()
        if length_ok_ratio < 0.95:
            violations.append(f"Invalid length values: {(1-length_ok_ratio)*100:.1f}% (Expected: 6-20 characters)")
        
        # NOTE: loan_account_no should be unique ONLY in Loan Master table
        # If it appears in multiple tables (e.g., collaterals, transactions), it's a FK, not unique there
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Loan Account Number Business Rule",
            "note": "loan_account_no should be unique only in Loan Master table. In other tables, it's a Foreign Key (duplicates allowed)."
        }
    
    def validate_amount(self, series: pd.Series) -> Dict[str, Any]:
        """
        Business Rule: Generic transaction amount - numeric, >= 0.
        
        CRITICAL: This is NOT EMI-specific. Transactions table contains:
        - deposits
        - withdrawals
        - transfers
        - EMI (only if it's a loan-specific transaction)
        
        EMI rules only apply to loan-specific EMI fields, NOT generic transaction amounts.
        """
        violations = []
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_null = numeric_series.dropna()
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Rule 1: Must be numeric
        numeric_ratio = numeric_series.notna().mean()
        if numeric_ratio < 0.95:
            violations.append(f"Non-numeric values: {(1-numeric_ratio)*100:.1f}%")
        
        # Rule 2: Must be >= 0
        non_negative_ratio = (non_null >= 0).mean()
        if non_negative_ratio < 0.95:
            violations.append(f"Negative values found: {(1-non_negative_ratio)*100:.1f}%")
        
        # NOTE: This is a generic transaction amount field
        # EMI rules (fixed monthly payment, calculated from loan amount/rate/tenure) do NOT apply here
        # EMI rules only apply to loan-specific EMI fields (emi_amount, emi, monthly_payment in loan tables)
        
        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Transaction Amount Business Rule (NOT EMI-Specific)",
            "note": "This is a generic transaction amount field for deposits, withdrawals, transfers, etc. EMI rules do NOT apply to generic transaction amounts - EMI is loan-specific."
        }

    # ==================== NEW VALIDATION METHODS ====================

    def validate_card_present_flag(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Card Present Flag must be boolean-like (Y/N, 1/0)."""
        violations = []
        non_null = series.dropna().astype(str).str.upper().str.strip()
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
            
        valid_vals = ["Y", "N", "YES", "NO", "T", "F", "TRUE", "FALSE", "0", "1"]
        valid_ratio = non_null.isin(valid_vals).mean()
        
        if valid_ratio < 0.95:
             violations.append(f"Invalid boolean values: {(1-valid_ratio)*100:.1f}%")

        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Card Present Flag Business Rule"
        }

    def validate_bpay_biller_code(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: BPAY Biller Code 3-10 chars."""
        violations = []
        non_null = series.dropna().astype(str).str.strip()
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}

        length_ratio = non_null.str.len().between(3, 10).mean()
        if length_ratio < 0.95:
             violations.append(f"Invalid length (expected 3-10): {(1-length_ratio)*100:.1f}%")

        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "BPAY Biller Code Business Rule"
        }

    def validate_txn_description(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Transaction Description should be descriptive text."""
        violations = []
        non_null = series.dropna().astype(str).str.strip()
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}

        min_len_ratio = (non_null.str.len() >= 5).mean()
        if min_len_ratio < 0.9: # Allow some short ones
             violations.append(f"Description too short (<5 chars): {(1-min_len_ratio)*100:.1f}%")

        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Transaction Description Business Rule"
        }

    def validate_merchant_id(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Merchant ID alphanumeric."""
        violations = []
        non_null = series.dropna().astype(str).str.strip()
        if len(non_null) == 0:
             return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}

        # Just check not empty mostly, and alphanumeric check
        alphanumeric = non_null.str.match(r"^[A-Za-z0-9\-\s]+$").mean()
        if alphanumeric < 0.95:
             violations.append(f"Invalid format (expected alphanumeric): {(1-alphanumeric)*100:.1f}%")

        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Merchant ID Business Rule"
        }

    def validate_merchant_code(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: MCC is 4 digits."""
        violations = []
        non_null = series.dropna().astype(str).str.strip()
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
            
        format_match = non_null.str.fullmatch(r"\d{4}").mean()
        if format_match < 0.95:
             violations.append(f"Invalid MCC format (expected 4 digits): {(1-format_match)*100:.1f}%")

        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Merchant Code (MCC) Business Rule"
        }

    def validate_merchant_suburb(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Merchant Suburb text."""
        violations = []
        non_null = series.dropna().astype(str).str.strip()
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}

        # Loose validation for text
        text_match = non_null.str.match(r"^[A-Za-z\s\.]+$").mean()
        if text_match < 0.9:
             violations.append(f"Invalid suburb format: {(1-text_match)*100:.1f}%")

        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Merchant Suburb Business Rule"
        }
    
    def validate_merchant_state(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Merchant State 2-3 char code."""
        violations = []
        non_null = series.dropna().astype(str).str.upper().str.strip()
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}

        state_match = non_null.str.match(r"^[A-Z]{2,3}$").mean()
        if state_match < 0.9:
             violations.append(f"Invalid state code format: {(1-state_match)*100:.1f}%")

        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Merchant State Business Rule"
        }

    def validate_currency(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Currency ISO code (3 chars)."""
        violations = []
        non_null = series.dropna().astype(str).str.upper().str.strip()
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}

        iso_match = non_null.str.match(r"^[A-Z]{3}$").mean()
        if iso_match < 0.95:
             violations.append(f"Invalid Currency ISO format: {(1-iso_match)*100:.1f}%")

        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": "Currency Business Rule"
        }
    
    def validate_geo_coordinates(self, series: pd.Series, name="Geo Coordinates") -> Dict[str, Any]:
        """Business Rule: Valid Lat/Long."""
        violations = []
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_null = numeric_series.dropna()
        
        if len(non_null) == 0:
            return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
            
        # Range check [-180, 180] covers both lat and long loosely
        range_match = non_null.between(-180, 180).mean()
        if range_match < 0.95:
             violations.append(f"Values out of geographic range: {(1-range_match)*100:.1f}%")

        return {
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "violations": violations,
            "rule_name": f"{name} Business Rule"
        }

    def validate_extraction(self, series: pd.Series) -> Dict[str, Any]:
        """Business Rule: Extraction/Batch ID."""
        violations = []
        non_null = series.dropna().astype(str)
        if len(non_null) == 0:
             return {"status": "SKIPPED", "reason": "Column is empty", "violations": []}
        
        # Just existence is enough usually, maybe check for strange characters
        return {
            "status": "PASS",
            "violations": [],
            "rule_name": "Extraction ID Business Rule"
        }
    
    # ==================== APPLICATION TYPE PREDICTION ====================
    
    def predict_application_type(self, column_roles: Dict[str, str], df: pd.DataFrame) -> Dict[str, Any]:
        """
        Predict banking application type based on detected columns.
        
        Uses pattern matching from application_types.md to determine the most likely
        application type based on column presence and data patterns.
        
        Returns:
            Dict with application_type, confidence, reasoning, and matched_columns
        """
        # Application type patterns (from application_types.md)
        application_patterns = {
            "Core Banking System (CBS)": {
                "required": ["account_number", "customer_id", "balance"],
                "optional": ["customer_name", "account_type", "account_status", "branch_code", 
                           "ifsc_code", "transaction_id", "transaction_date", "transaction_type",
                           "debit", "credit", "opening_balance", "closing_balance"],
                "description": "Central application managing all customer accounts, transactions, and core banking operations."
            },
            "Customer Relationship Management (CRM)": {
                "required": ["customer_id", "customer_name"],
                "optional": ["phone", "email", "address", "city", "dob", "kyc_status"],
                "description": "Manages customer data, interactions, and relationship history."
            },
            "Internet Banking / Online Banking Portal": {
                "required": ["account_number", "transaction_id", "transaction_date"],
                "optional": ["customer_id", "balance", "transaction_type", "amount", "debit", 
                           "credit", "ifsc_code"],
                "description": "Web-based banking platform for online transactions and account management."
            },
            "Mobile Banking Application": {
                "required": ["account_number", "transaction_id", "transaction_date"],
                "optional": ["customer_id", "phone", "balance", "transaction_type", "amount"],
                "description": "Smartphone app for banking services and account monitoring."
            },
            "ATM Transaction System": {
                "required": ["account_number", "transaction_id", "transaction_type"],
                "optional": ["transaction_date", "amount", "atm_location", "card_number", "balance"],
                "description": "Automated Teller Machine network for cash operations."
            },
            "Loan Management System (LMS)": {
                "required": ["loan_account_no", "customer_id"],
                "optional": ["loan_amount", "emi", "interest_rate", "tenure", "disbursement_date",
                           "outstanding_balance", "loan_status", "loan_type"],
                "description": "Handles loan origination, disbursement, and repayment."
            },
            "Credit Card Management System": {
                "required": ["card_number", "customer_id", "transaction_id"],
                "optional": ["card_type", "credit_limit", "available_credit", "billing_date",
                           "due_date", "minimum_payment", "transaction_date", "merchant_name"],
                "description": "Manages credit card lifecycle, transactions, and billing."
            },
            "Debit Card Management System": {
                "required": ["card_number", "account_number", "transaction_id"],
                "optional": ["transaction_date", "transaction_type", "amount", "merchant_name",
                           "atm_location", "card_status"],
                "description": "Administers debit card operations and ATM transactions."
            },
            "Payment Gateway System": {
                "required": ["transaction_id", "amount"],
                "optional": ["merchant_id", "payment_status", "payment_method", "transaction_date",
                           "customer_id", "order_id", "settlement_date"],
                "description": "Processes online payments and merchant transactions."
            },
            "Fund Transfer System (NEFT/RTGS/IMPS)": {
                "required": ["account_number", "transaction_id", "amount"],
                "optional": ["beneficiary_account", "ifsc_code", "transaction_date", "transfer_type",
                           "transaction_status", "reference_number"],
                "description": "Facilitates inter-bank and intra-bank transfers."
            },
            "KYC Compliance System": {
                "required": ["customer_id", "kyc_status", "pan"],
                "optional": ["customer_name", "dob", "address", "kyc_document_type",
                           "kyc_verification_date", "kyc_expiry_date"],
                "description": "Manages customer verification and regulatory compliance."
            },
            "Account Opening System": {
                "required": ["customer_id", "account_number", "account_type"],
                "optional": ["customer_name", "kyc_status", "branch_code", "account_opening_date",
                           "account_status", "initial_deposit"],
                "description": "Digital/branch account creation platform."
            },
            "Transaction Monitoring System": {
                "required": ["transaction_id", "transaction_date", "amount"],
                "optional": ["account_number", "transaction_type", "fraud_flag", "risk_score",
                           "alert_status", "suspicious_reason"],
                "description": "Real-time fraud detection and suspicious activity tracking."
            },
            "Anti-Money Laundering (AML) System": {
                "required": ["transaction_id", "customer_id", "amount"],
                "optional": ["transaction_date", "aml_flag", "risk_level", "suspicious_pattern",
                           "reporting_status", "compliance_status"],
                "description": "Detects and reports suspicious transactions for regulatory compliance."
            },
            "Statement Generation System": {
                "required": ["account_number", "transaction_id", "transaction_date"],
                "optional": ["balance", "debit", "credit", "transaction_type", "statement_period",
                           "opening_balance", "closing_balance"],
                "description": "Creates account statements and transaction reports."
            },
            "Fixed Deposit Management": {
                "required": ["account_number", "customer_id"],
                "optional": ["deposit_amount", "interest_rate", "maturity_date", "tenure",
                           "fd_type", "renewal_status", "maturity_amount"],
                "description": "Handles term deposits, renewals, and maturity processing."
            },
            "Recurring Deposit System": {
                "required": ["account_number", "customer_id"],
                "optional": ["installment_amount", "installment_date", "maturity_date", "tenure",
                           "interest_rate", "total_amount", "rd_status"],
                "description": "Manages periodic savings deposits and maturity calculations."
            },
            "Cheque Processing System": {
                "required": ["account_number", "transaction_id"],
                "optional": ["cheque_number", "cheque_amount", "cheque_date", "clearing_date",
                           "cheque_status", "micr_code", "bank_name"],
                "description": "Handles cheque deposits, clearances, and bounces."
            },
            "Standing Instruction System": {
                "required": ["account_number", "amount"],
                "optional": ["beneficiary_account", "standing_instruction_id", "frequency",
                           "next_execution_date", "instruction_status", "end_date", "transaction_id"],
                "description": "Automates recurring payments and transfers."
            },
            "Bill Payment System": {
                "required": ["account_number", "amount"],
                "optional": ["bill_type", "bill_number", "due_date", "payment_date", "bill_status",
                           "merchant_id", "transaction_id"],
                "description": "Processes utility bills, loan EMIs, and merchant payments."
            }
        }
        
        # Get all detected role names (normalized)
        detected_roles = set(column_roles.values())
        detected_role_names = {self.normalize_column_name(role) for role in detected_roles}
        
        # Calculate match scores for each application type
        app_scores = {}
        for app_type, pattern in application_patterns.items():
            score = 0
            max_score = 0
            matched_required = []
            matched_optional = []
            missing_required = []
            
            # Check required columns (50 points each)
            for req_col in pattern["required"]:
                max_score += 50
                norm_req = self.normalize_column_name(req_col)
                # Check if any detected role matches
                if norm_req in detected_role_names:
                    score += 50
                    matched_required.append(req_col)
                else:
                    # Check for variations
                    found = False
                    for detected_role in detected_roles:
                        norm_detected = self.normalize_column_name(detected_role)
                        if norm_req in norm_detected or norm_detected in norm_req:
                            score += 50
                            matched_required.append(req_col)
                            found = True
                            break
                    if not found:
                        missing_required.append(req_col)
            
            # Check optional columns (10 points each)
            for opt_col in pattern["optional"]:
                max_score += 10
                norm_opt = self.normalize_column_name(opt_col)
                if norm_opt in detected_role_names:
                    score += 10
                    matched_optional.append(opt_col)
                else:
                    # Check for variations
                    for detected_role in detected_roles:
                        norm_detected = self.normalize_column_name(detected_role)
                        if norm_opt in norm_detected or norm_detected in norm_opt:
                            score += 10
                            matched_optional.append(opt_col)
                            break
            
            # Calculate confidence percentage
            if max_score > 0:
                confidence = (score / max_score) * 100
            else:
                confidence = 0
            
            # Only consider if at least one required column is present
            if len(matched_required) > 0:
                app_scores[app_type] = {
                    "score": score,
                    "max_score": max_score,
                    "confidence": round(confidence, 2),
                    "matched_required": matched_required,
                    "matched_optional": matched_optional,
                    "missing_required": missing_required,
                    "description": pattern["description"]
                }
        
        # Select best match (highest confidence)
        if not app_scores:
            return {
                "application_type": "Unknown",
                "confidence": 0,
                "reasoning": "No matching application type patterns found in detected columns.",
                "matched_columns": {},
                "all_matches": []
            }
        
        # Sort by confidence
        sorted_apps = sorted(app_scores.items(), key=lambda x: x[1]["confidence"], reverse=True)
        best_match = sorted_apps[0]
        best_app_type = best_match[0]
        best_info = best_match[1]
        
        # Build reasoning
        reasoning_parts = []
        if best_info["matched_required"]:
            reasoning_parts.append(f"✅ Required columns matched: {', '.join(best_info['matched_required'])}")
        if best_info["matched_optional"]:
            reasoning_parts.append(f"✅ Optional columns matched: {', '.join(best_info['matched_optional'][:5])}")
        if best_info["missing_required"]:
            reasoning_parts.append(f"⚠️ Missing required columns: {', '.join(best_info['missing_required'])}")
        
        reasoning = " | ".join(reasoning_parts) if reasoning_parts else "Pattern matching based on detected columns."
        
        # Prepare all matches (top 3)
        all_matches = []
        for app_type, info in sorted_apps[:3]:
            all_matches.append({
                "application_type": app_type,
                "confidence": info["confidence"],
                "matched_required": info["matched_required"],
                "matched_optional": len(info["matched_optional"])
            })
        
        return {
            "application_type": best_app_type,
            "confidence": best_info["confidence"],
            "reasoning": reasoning,
            "description": best_info["description"],
            "matched_required": best_info["matched_required"],
            "matched_optional": best_info["matched_optional"],
            "missing_required": best_info["missing_required"],
            "all_matches": all_matches
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
            
            # STEP 1.5: Predict application type based on detected columns
            application_type_prediction = self.predict_application_type(column_roles, df)
            
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
                elif role == "dob":
                    rule_result = self.validate_dob(series)
                elif role == "address":
                    rule_result = self.validate_address(series)
                elif role == "city":
                    rule_result = self.validate_city(series)
                elif role == "rate_percentage":
                    rule_result = self.validate_rate_percentage(series)
                elif role == "kyc_status":
                    rule_result = self.validate_kyc_status(series)
                elif role == "employee_status":
                    rule_result = self.validate_employee_status(series)
                elif role == "loan_account_no":
                    rule_result = self.validate_loan_account_no(series)
                elif role == "amount":
                    rule_result = self.validate_amount(series)
                
                # --- NEW VALIDATIONS ---
                elif role == "card_present_flag":
                    rule_result = self.validate_card_present_flag(series)
                elif role == "bpay_biller_code":
                    rule_result = self.validate_bpay_biller_code(series)
                elif role == "txn_description":
                    rule_result = self.validate_txn_description(series)
                elif role == "merchant_id":
                    rule_result = self.validate_merchant_id(series)
                elif role == "merchant_code":
                    rule_result = self.validate_merchant_code(series)
                elif role == "merchant_suburb":
                    rule_result = self.validate_merchant_suburb(series)
                elif role == "merchant_state":
                    rule_result = self.validate_merchant_state(series)
                elif role == "currency":
                    rule_result = self.validate_currency(series)
                elif role == "long_lat":
                    rule_result = self.validate_geo_coordinates(series, "Customer Location")
                elif role == "merchant_long_lat":
                    rule_result = self.validate_geo_coordinates(series, "Merchant Location")
                elif role == "extraction":
                    rule_result = self.validate_extraction(series)
                
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
                "summary": summary,
                "application_type_prediction": application_type_prediction
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
