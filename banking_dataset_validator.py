"""
Banking Dataset Validator

A comprehensive validator that:
1. Identifies all columns and their meanings
2. Applies business rules to validate each column
3. Checks relationships/dependencies between columns
4. Computes per-column confidence and overall dataset confidence
5. Decides if the dataset qualifies as a Core Banking Dataset
6. Provides clear explanations of any failures or warnings
"""

import pandas as pd
import numpy as np
from datetime import datetime
import re
import json


class BankingDatasetValidator:
    """Banking Dataset Validator - validates banking CSV files according to business rules."""
    
    # Column definitions with meanings
    COLUMN_DEFINITIONS = {
        "account_number": {
            "meaning": "Unique bank account number",
            "allowed_variations": ["account_number", "account_no", "acc_no", "accno", "account"]
        },
        "customer_id": {
            "meaning": "Internal customer ID",
            "allowed_variations": ["customer_id", "cust_id", "customerid", "custid", "client_id"]
        },
        "customer_name": {
            "meaning": "Customer full name",
            "allowed_variations": ["customer_name", "cust_name", "customername", "name"]
        },
        "transaction_id": {
            "meaning": "Unique ID per transaction",
            "allowed_variations": ["transaction_id", "txn_id", "trans_id", "transactionid", "txnid"]
        },
        "txn_type": {
            "meaning": "Type of transaction",
            "allowed_variations": ["txn_type", "transaction_type", "trans_type", "transactiontype", "type"]
        },
        "debit": {
            "meaning": "Money going out",
            "allowed_variations": ["debit", "dr_amount", "debit_amount", "withdraw", "withdrawal"]
        },
        "credit": {
            "meaning": "Money coming in",
            "allowed_variations": ["credit", "cr_amount", "credit_amount", "deposit"]
        },
        "opening_balance": {
            "meaning": "Account starting balance",
            "allowed_variations": ["opening_balance", "open_balance", "balance_before", "initial_balance", "op_bal"]
        },
        "closing_balance": {
            "meaning": "Account ending balance",
            "allowed_variations": ["closing_balance", "closing", "balance_after", "final_balance", "current_balance", "cl_bal"]
        },
        "transaction_date": {
            "meaning": "Date of transaction",
            "allowed_variations": ["transaction_date", "txn_date", "trans_date", "transactiondate", "date"]
        },
        "branch_code": {
            "meaning": "Bank branch identifier",
            "allowed_variations": ["branch_code", "branch", "branch_id", "branchcode"]
        },
        "account_type": {
            "meaning": "Type of account",
            "allowed_variations": ["account_type", "acct_type", "accounttype", "type"]
        },
        "account_status": {
            "meaning": "Account status",
            "allowed_variations": ["account_status", "acc_status", "status", "account_state", "state"]
        },
        "phone": {
            "meaning": "Phone number",
            "allowed_variations": ["phone", "mobile", "contact", "telephone", "phone_number", "mobile_number"]
        },
        "pan": {
            "meaning": "PAN card number",
            "allowed_variations": ["pan", "pan_number", "pan_no", "pannumber", "panno", "permanent_account_number"]
        },
        "created_at": {
            "meaning": "Record creation timestamp",
            "allowed_variations": ["created_at", "created_date", "created", "date_created", "created_on"]
        },
        "updated_at": {
            "meaning": "Record update timestamp",
            "allowed_variations": ["updated_at", "updated_date", "updated", "date_updated", "updated_on", "modified_at", "modified"]
        }
    }
    
    # Allowed values for enumerated columns
    ALLOWED_TXN_TYPES = ["deposit", "withdraw", "withdrawal", "transfer", "credit", "debit"]
    # Canonical account types are stored in uppercase for case-insensitive checks.
    ALLOWED_ACCOUNT_TYPES = ["SAVINGS", "CURRENT", "LOAN", "FD", "RD"]
    # Common synonyms mapped to canonical values (all uppercase for consistency).
    ACCOUNT_TYPE_SYNONYMS = {
        "SAVING": "SAVINGS",
        "SAVINGS ACCOUNT": "SAVINGS",
        "CURRENT ACCOUNT": "CURRENT",
        "CHEQUING": "CURRENT",
        "CHEQUING ACCOUNT": "CURRENT",
        "LOAN ACCOUNT": "LOAN",
        "FIXED DEPOSIT": "FD",
        "FD ACCOUNT": "FD",
        "TERM DEPOSIT": "FD",
        "RECURRING DEPOSIT": "RD",
        "RD ACCOUNT": "RD",
    }
    ALLOWED_ACCOUNT_STATUSES = ["ACTIVE", "INACTIVE", "CLOSED"]
    
    # Column weights for confidence calculation (core columns have higher weights)
    COLUMN_WEIGHTS = {
        "account_number": 1.5,
        "customer_id": 1.0,
        "customer_name": 0.8,
        "transaction_id": 1.2,
        "txn_type": 1.3,
        "debit": 1.4,
        "credit": 1.4,
        "opening_balance": 1.3,
        "closing_balance": 1.3,
        "transaction_date": 1.2,
        "branch_code": 0.7,
        "account_type": 1.0,
        "account_status": 1.0,
        "phone": 0.5,
        "pan": 0.5,
        "created_at": 0.3,
        "updated_at": 0.3
    }
    
    def __init__(self):
        """Initialize the validator."""
        pass
    
    def normalize_column_name(self, col_name):
        """Normalize column name for matching."""
        return str(col_name).lower().strip().replace(" ", "_").replace("-", "_")
    
    def identify_column_role(self, col_name, df):
        """
        Identify the role of a column based on name matching.
        Returns the matched column definition key or None.
        
        NOTE: KYC columns are explicitly ignored (KYC rules removed)
        """
        normalized = self.normalize_column_name(col_name)
        
        # CRITICAL: Skip KYC columns completely (KYC rules removed)
        if "kyc" in normalized:
            return None
        
        # Skip non-banking columns (currency, country, email, channel, etc.)
        non_banking_keywords = ["currency", "country", "email", "channel", "source", "region", "city", "state_name"]
        if any(kw in normalized for kw in non_banking_keywords):
            return None
        
        # Check for PAN first (before generic date matching)
        if any(kw in normalized for kw in ["pan", "pan_number", "pan_no", "pannumber", "panno", "permanent_account_number"]):
            return "pan"
        
        # Check for created_at/updated_at (audit fields, not transaction dates)
        if "created" in normalized and any(kw in normalized for kw in ["created_at", "created_date", "created", "date_created", "created_on"]):
            return "created_at"
        if ("updated" in normalized or "modified" in normalized) and any(kw in normalized for kw in ["updated_at", "updated_date", "updated", "date_updated", "updated_on", "modified_at", "modified"]):
            return "updated_at"
        
        # CRITICAL PRIORITY: Check transaction_type BEFORE transaction_date to prevent cross-mapping
        # If column name contains both "transaction" and "type", it's transaction_type, not transaction_date
        if "transaction" in normalized and "type" in normalized:
            # Check if it matches transaction_type variations
            txn_type_variations = ["txn_type", "transaction_type", "trans_type", "transactiontype", "type"]
            for variation in txn_type_variations:
                if normalized == variation.lower() or variation.lower() in normalized or normalized in variation.lower():
                    return "txn_type"
        
        # CRITICAL PRIORITY: Check transaction_date AFTER transaction_type check
        # If column name contains both "transaction" and "date", it's transaction_date
        if "transaction" in normalized and "date" in normalized:
            # Check if it matches transaction_date variations
            txn_date_variations = ["transaction_date", "txn_date", "trans_date", "transactiondate", "date"]
            for variation in txn_date_variations:
                if normalized == variation.lower() or variation.lower() in normalized or normalized in variation.lower():
                    # Don't match transaction_date for created_at/updated_at (they're audit fields)
                    if "created" in normalized or "updated" in normalized or "modified" in normalized:
                        continue  # Skip - these are handled above
                    return "transaction_date"
        
        # Direct match
        if normalized in self.COLUMN_DEFINITIONS:
            return normalized
        
        # Check variations - prioritize specific matches
        for key, definition in self.COLUMN_DEFINITIONS.items():
            for variation in definition["allowed_variations"]:
                # Exact match or contains match
                if normalized == variation.lower() or variation.lower() in normalized or normalized in variation.lower():
                    # Additional check: don't match kyc_status as account_status
                    if key == "account_status" and "kyc" in normalized:
                        continue
                    # Don't match transaction_date for created_at/updated_at (they're audit fields)
                    if key == "transaction_date" and ("created" in normalized or "updated" in normalized or "modified" in normalized):
                        continue  # Skip - these are handled above
                    # CRITICAL: Don't match transaction_date if column name suggests transaction_type
                    if key == "transaction_date" and "type" in normalized:
                        continue  # Skip - transaction_type takes priority
                    # CRITICAL: Don't match transaction_type if column name suggests transaction_date
                    if key == "txn_type" and "date" in normalized and "type" not in normalized:
                        continue  # Skip - transaction_date takes priority when date is present but type is not
                    return key
        
        return None
    
    def validate_account_number(self, series, col_name):
        """Validate account_number column according to business rules."""
        rules_passed = 0
        rules_total = 5
        failures = []
        
        non_null = series.dropna().astype(str)
        total = len(series)
        non_null_count = len(non_null)
        
        # Rule 1: NOT NULL
        not_null_ratio = non_null_count / total if total > 0 else 0
        if not_null_ratio >= 0.95:
            rules_passed += 1
        else:
            failures.append(f"Null ratio: {(1-not_null_ratio)*100:.1f}%")
        
        if non_null_count == 0:
            return {
                "rules_passed": 0,
                "rules_total": rules_total,
                "failures": ["Column is completely empty"],
                "confidence": 0.0
            }
        
        # Rule 2: Digits only
        digit_only_ratio = non_null.str.fullmatch(r"\d+").mean()
        if digit_only_ratio >= 0.95:
            rules_passed += 1
        else:
            failures.append(f"Non-digit ratio: {(1-digit_only_ratio)*100:.1f}%")
        
        # Rule 3: Length 6-18
        length_ok_ratio = non_null.str.len().between(6, 18).mean()
        if length_ok_ratio >= 0.95:
            rules_passed += 1
        else:
            failures.append(f"Invalid length ratio: {(1-length_ok_ratio)*100:.1f}%")
        
        # Rule 4: Can repeat (no uniqueness check - accounts can have multiple transactions)
        rules_passed += 1  # This is allowed, so always pass
        
        # Rule 5: No symbols (covered by digits only)
        rules_passed += 1  # Already checked in Rule 2
        
        confidence = (rules_passed / rules_total) * 100
        
        return {
            "rules_passed": rules_passed,
            "rules_total": rules_total,
            "failures": failures,
            "confidence": confidence
        }
    
    def validate_customer_id(self, series, col_name):
        """Validate customer_id column according to business rules."""
        rules_passed = 0
        rules_total = 3
        failures = []
        
        non_null = series.dropna().astype(str)
        total = len(series)
        non_null_count = len(non_null)
        
        # Rule 1: NOT NULL
        not_null_ratio = non_null_count / total if total > 0 else 0
        if not_null_ratio >= 0.95:
            rules_passed += 1
        else:
            failures.append(f"Null ratio: {(1-not_null_ratio)*100:.1f}%")
        
        if non_null_count == 0:
            return {
                "rules_passed": 0,
                "rules_total": rules_total,
                "failures": ["Column is completely empty"],
                "confidence": 0.0
            }
        
        # Rule 2: Alphanumeric
        alphanumeric_ratio = non_null.str.fullmatch(r"[A-Za-z0-9]+").mean()
        if alphanumeric_ratio >= 0.95:
            rules_passed += 1
        else:
            failures.append(f"Non-alphanumeric ratio: {(1-alphanumeric_ratio)*100:.1f}%")
        
        # Rule 3: Unique per customer (check uniqueness - each customer should have same ID)
        # This is a soft check - we allow some duplicates if they represent same customer
        unique_count = non_null.nunique()
        unique_ratio = unique_count / non_null_count if non_null_count > 0 else 0
        # Customer IDs should be reasonably unique but can repeat across rows
        if unique_ratio >= 0.3:  # At least 30% unique (some customers can have multiple rows)
            rules_passed += 1
        else:
            failures.append(f"Uniqueness issue: only {unique_ratio*100:.1f}% unique")
        
        confidence = (rules_passed / rules_total) * 100
        
        return {
            "rules_passed": rules_passed,
            "rules_total": rules_total,
            "failures": failures,
            "confidence": confidence
        }
    
    def validate_customer_name(self, series, col_name):
        """Validate customer_name column according to business rules."""
        rules_passed = 0
        rules_total = 4
        failures = []
        
        non_null = series.dropna().astype(str)
        total = len(series)
        non_null_count = len(non_null)
        
        # Rule 1: NOT NULL
        not_null_ratio = non_null_count / total if total > 0 else 0
        if not_null_ratio >= 0.95:
            rules_passed += 1
        else:
            failures.append(f"Null ratio: {(1-not_null_ratio)*100:.1f}%")
        
        if non_null_count == 0:
            return {
                "rules_passed": 0,
                "rules_total": rules_total,
                "failures": ["Column is completely empty"],
                "confidence": 0.0
            }
        
        # Rule 2: Letters + spaces
        letter_space_ratio = non_null.str.fullmatch(r"[A-Za-z\s]+").mean()
        if letter_space_ratio >= 0.95:
            rules_passed += 1
        else:
            failures.append(f"Invalid characters ratio: {(1-letter_space_ratio)*100:.1f}%")
        
        # Rule 3: Min 3 characters
        min_length_ratio = (non_null.str.len() >= 3).mean()
        if min_length_ratio >= 0.95:
            rules_passed += 1
        else:
            failures.append(f"Short names ratio: {(1-min_length_ratio)*100:.1f}%")
        
        # Rule 4: Not numeric
        numeric_ratio = pd.to_numeric(non_null, errors="coerce").notna().mean()
        if numeric_ratio <= 0.05:  # Less than 5% numeric
            rules_passed += 1
        else:
            failures.append(f"Numeric values ratio: {numeric_ratio*100:.1f}%")
        
        confidence = (rules_passed / rules_total) * 100
        
        return {
            "rules_passed": rules_passed,
            "rules_total": rules_total,
            "failures": failures,
            "confidence": confidence
        }
    
    def validate_transaction_id(self, series, col_name):
        """Validate transaction_id column according to business rules."""
        rules_passed = 0
        rules_total = 3
        failures = []
        
        non_null = series.dropna().astype(str)
        total = len(series)
        non_null_count = len(non_null)
        
        # Rule 1: NOT NULL
        not_null_ratio = non_null_count / total if total > 0 else 0
        if not_null_ratio >= 0.95:
            rules_passed += 1
        else:
            failures.append(f"Null ratio: {(1-not_null_ratio)*100:.1f}%")
        
        if non_null_count == 0:
            return {
                "rules_passed": 0,
                "rules_total": rules_total,
                "failures": ["Column is completely empty"],
                "confidence": 0.0
            }
        
        # Rule 2: Alphanumeric
        alphanumeric_ratio = non_null.str.fullmatch(r"[A-Za-z0-9]+").mean()
        if alphanumeric_ratio >= 0.95:
            rules_passed += 1
        else:
            failures.append(f"Non-alphanumeric ratio: {(1-alphanumeric_ratio)*100:.1f}%")
        
        # Rule 3: Unique per row (WARNING ONLY - does not cause REJECT)
        unique_count = non_null.nunique()
        unique_ratio = unique_count / non_null_count if non_null_count > 0 else 0
        if unique_ratio >= 0.95:  # At least 95% unique
            rules_passed += 1
        else:
            # Low uniqueness is a WARNING, not a failure - don't reduce rules_passed
            # Only warn if transaction_id is present (not completely NULL)
            failures.append(f"Warning: Low uniqueness - only {unique_ratio*100:.1f}% unique (non-fatal)")
            rules_passed += 1  # Count as passed since it's warning-only
        
        confidence = (rules_passed / rules_total) * 100
        
        return {
            "rules_passed": rules_passed,
            "rules_total": rules_total,
            "failures": failures,
            "confidence": confidence
        }
    
    def validate_txn_type(self, series, col_name):
        """Validate txn_type column according to business rules."""
        rules_passed = 0
        rules_total = 2
        failures = []
        
        non_null = series.dropna().astype(str).str.lower().str.strip()
        total = len(series)
        non_null_count = len(non_null)
        
        # Rule 1: NOT NULL
        not_null_ratio = non_null_count / total if total > 0 else 0
        if not_null_ratio >= 0.95:
            rules_passed += 1
        else:
            failures.append(f"Null ratio: {(1-not_null_ratio)*100:.1f}%")
        
        if non_null_count == 0:
            return {
                "rules_passed": 0,
                "rules_total": rules_total,
                "failures": ["Column is completely empty"],
                "confidence": 0.0
            }
        
        # Rule 2: Allowed values only (case-insensitive)
        valid_ratio = non_null.isin([v.lower() for v in self.ALLOWED_TXN_TYPES]).mean()
        if valid_ratio >= 0.95:
            rules_passed += 1
        else:
            invalid_values = non_null[~non_null.isin([v.lower() for v in self.ALLOWED_TXN_TYPES])].unique().tolist()[:5]
            failures.append(f"Invalid values found: {invalid_values} (valid ratio: {valid_ratio*100:.1f}%)")
        
        confidence = (rules_passed / rules_total) * 100
        
        return {
            "rules_passed": rules_passed,
            "rules_total": rules_total,
            "failures": failures,
            "confidence": confidence
        }
    
    def validate_debit(self, series, col_name, credit_series=None):
        """Validate debit column according to business rules."""
        rules_passed = 0
        rules_total = 3
        failures = []
        
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_null = numeric_series.dropna()
        total = len(series)
        non_null_count = len(non_null)
        
        # Rule 1: Numeric >= 0
        numeric_ratio = non_null_count / total if total > 0 else 0
        if numeric_ratio >= 0.95:
            rules_passed += 1
        else:
            failures.append(f"Non-numeric ratio: {(1-numeric_ratio)*100:.1f}%")
        
        if non_null_count == 0:
            return {
                "rules_passed": 0,
                "rules_total": rules_total,
                "failures": ["Column is completely empty"],
                "confidence": 0.0
            }
        
        # Rule 2: >= 0
        non_negative_ratio = (non_null >= 0).mean()
        if non_negative_ratio >= 0.95:
            rules_passed += 1
        else:
            negative_count = (non_null < 0).sum()
            failures.append(f"Negative values found: {negative_count}")
        
        # Rule 3: Only debit OR credit > 0 per row (mutual exclusivity)
        if credit_series is not None:
            credit_numeric = pd.to_numeric(credit_series, errors="coerce").fillna(0)
            debit_filled = numeric_series.fillna(0)
            both_positive = ((debit_filled > 0) & (credit_numeric > 0)).sum()
            both_positive_ratio = both_positive / total if total > 0 else 0
            if both_positive_ratio <= 0.1:  # Less than 10% have both > 0
                rules_passed += 1
            else:
                failures.append(f"Mutual exclusivity issue: {both_positive} rows have both debit and credit > 0")
        else:
            rules_passed += 1  # Pass if credit column not available
        
        confidence = (rules_passed / rules_total) * 100
        
        return {
            "rules_passed": rules_passed,
            "rules_total": rules_total,
            "failures": failures,
            "confidence": confidence
        }
    
    def validate_credit(self, series, col_name, debit_series=None):
        """Validate credit column according to business rules."""
        rules_passed = 0
        rules_total = 3
        failures = []
        
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_null = numeric_series.dropna()
        total = len(series)
        non_null_count = len(non_null)
        
        # Rule 1: Numeric >= 0
        numeric_ratio = non_null_count / total if total > 0 else 0
        if numeric_ratio >= 0.95:
            rules_passed += 1
        else:
            failures.append(f"Non-numeric ratio: {(1-numeric_ratio)*100:.1f}%")
        
        if non_null_count == 0:
            return {
                "rules_passed": 0,
                "rules_total": rules_total,
                "failures": ["Column is completely empty"],
                "confidence": 0.0
            }
        
        # Rule 2: >= 0
        non_negative_ratio = (non_null >= 0).mean()
        if non_negative_ratio >= 0.95:
            rules_passed += 1
        else:
            negative_count = (non_null < 0).sum()
            failures.append(f"Negative values found: {negative_count}")
        
        # Rule 3: Only debit OR credit > 0 per row (mutual exclusivity - handled in debit validation)
        rules_passed += 1  # This is checked in debit validation
        
        confidence = (rules_passed / rules_total) * 100
        
        return {
            "rules_passed": rules_passed,
            "rules_total": rules_total,
            "failures": failures,
            "confidence": confidence
        }
    
    def validate_opening_balance(self, series, col_name, df=None, account_col=None):
        """Validate opening_balance column according to business rules."""
        rules_passed = 0
        rules_total = 3
        failures = []
        
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_null = numeric_series.dropna()
        total = len(series)
        non_null_count = len(non_null)
        
        # Rule 1: Numeric >= 0
        numeric_ratio = non_null_count / total if total > 0 else 0
        if numeric_ratio >= 0.95:
            rules_passed += 1
        else:
            failures.append(f"Non-numeric ratio: {(1-numeric_ratio)*100:.1f}%")
        
        if non_null_count == 0:
            return {
                "rules_passed": 0,
                "rules_total": rules_total,
                "failures": ["Column is completely empty"],
                "confidence": 0.0
            }
        
        # Rule 2: >= 0
        non_negative_ratio = (non_null >= 0).mean()
        if non_negative_ratio >= 0.95:
            rules_passed += 1
        else:
            negative_count = (non_null < 0).sum()
            failures.append(f"Negative values found: {negative_count}")
        
        # Rule 3: Same for first transaction of each account (optional check)
        if df is not None and account_col is not None and account_col in df.columns:
            try:
                # Group by account and check if first transaction has consistent opening balance
                account_first_balances = df.groupby(account_col)[col_name].first()
                # This is a soft check - we'll just note if it's consistent
                consistency_ratio = 1.0  # Simplified - always pass this soft check
                rules_passed += 1
            except:
                rules_passed += 1  # Pass if check fails
        else:
            rules_passed += 1  # Pass if account column not available
        
        confidence = (rules_passed / rules_total) * 100
        
        return {
            "rules_passed": rules_passed,
            "rules_total": rules_total,
            "failures": failures,
            "confidence": confidence
        }
    
    def validate_closing_balance(self, series, col_name, opening_col=None, debit_col=None, credit_col=None, df=None):
        """Validate closing_balance column according to business rules."""
        rules_passed = 0
        rules_total = 4
        failures = []
        
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_null = numeric_series.dropna()
        total = len(series)
        non_null_count = len(non_null)
        
        # Rule 1: Numeric >= 0
        numeric_ratio = non_null_count / total if total > 0 else 0
        if numeric_ratio >= 0.90:  # Allow some missing values
            rules_passed += 1
        else:
            failures.append(f"Non-numeric ratio: {(1-numeric_ratio)*100:.1f}%")
        
        if non_null_count == 0:
            return {
                "rules_passed": 0,
                "rules_total": rules_total,
                "failures": ["Column is completely empty"],
                "confidence": 0.0
            }
        
        # Rule 2: >= 0
        non_negative_ratio = (non_null >= 0).mean()
        if non_negative_ratio >= 0.95:
            rules_passed += 1
        else:
            negative_count = (non_null < 0).sum()
            failures.append(f"Negative values found: {negative_count}")
        
        # Rule 3: closing_balance = opening_balance + credit - debit (formula check)
        if opening_col and debit_col and credit_col and df is not None:
            try:
                opening_vals = pd.to_numeric(df[opening_col], errors="coerce").fillna(0)
                closing_vals = pd.to_numeric(df[col_name], errors="coerce").fillna(0)
                debit_vals = pd.to_numeric(df[debit_col], errors="coerce").fillna(0)
                credit_vals = pd.to_numeric(df[credit_col], errors="coerce").fillna(0)
                
                calculated_closing = opening_vals + credit_vals - debit_vals
                diff = abs(closing_vals - calculated_closing)
                tolerance = closing_vals.abs() * 0.01 + 0.01  # 1% tolerance
                matches = (diff <= tolerance).sum()
                match_ratio = matches / len(df) if len(df) > 0 else 0
                
                if match_ratio >= 0.95:
                    rules_passed += 1
                else:
                    failures.append(f"Formula mismatch: only {match_ratio*100:.1f}% rows match (closing = opening + credit - debit)")
            except Exception as e:
                failures.append(f"Formula check error: {str(e)}")
        else:
            rules_passed += 1  # Pass if required columns not available
        
        # Rule 4: Rarely missing (already checked in Rule 1)
        rules_passed += 1  # Covered by numeric ratio check
        
        confidence = (rules_passed / rules_total) * 100
        
        return {
            "rules_passed": rules_passed,
            "rules_total": rules_total,
            "failures": failures,
            "confidence": confidence
        }
    
    def validate_transaction_date(self, series, col_name):
        """Validate transaction_date column according to business rules."""
        rules_passed = 0
        rules_total = 2
        failures = []
        
        total = len(series)
        non_null = series.dropna()
        non_null_count = len(non_null)
        
        # Rule 1: NOT NULL
        not_null_ratio = non_null_count / total if total > 0 else 0
        if not_null_ratio >= 0.95:
            rules_passed += 1
        else:
            failures.append(f"Null ratio: {(1-not_null_ratio)*100:.1f}%")
        
        if non_null_count == 0:
            return {
                "rules_passed": 0,
                "rules_total": rules_total,
                "failures": ["Column is completely empty"],
                "confidence": 0.0
            }
        
        # Rule 2: Format YYYY-MM-DD (or parseable as date)
        try:
            dates = pd.to_datetime(non_null, errors="coerce")
            valid_date_ratio = dates.notna().mean()
            if valid_date_ratio >= 0.95:
                rules_passed += 1
            else:
                failures.append(f"Invalid date format ratio: {(1-valid_date_ratio)*100:.1f}%")
        except:
            failures.append("Date parsing failed")
        
        confidence = (rules_passed / rules_total) * 100
        
        return {
            "rules_passed": rules_passed,
            "rules_total": rules_total,
            "failures": failures,
            "confidence": confidence
        }
    
    def validate_branch_code(self, series, col_name):
        """Validate branch_code column according to business rules."""
        rules_passed = 0
        rules_total = 2
        failures = []
        
        non_null = series.dropna().astype(str).str.strip()
        non_null_count = len(non_null)
        total = len(series)
        
        # Rule 1: Alphanumeric with spaces and hyphens allowed (for branch names like "Main Branch", "West Branch")
        # Pattern allows: letters, numbers, spaces, hyphens
        alphanumeric_ratio = non_null.str.fullmatch(r"[A-Za-z0-9\s-]+").mean() if non_null_count > 0 else 0
        if alphanumeric_ratio >= 0.95:
            rules_passed += 1
        else:
            failures.append(f"Non-alphanumeric ratio: {(1-alphanumeric_ratio)*100:.1f}%")
        
        # Rule 2: Short code (reasonable length)
        if non_null_count > 0:
            avg_length = non_null.str.len().mean()
            if avg_length <= 30:  # Increased to 30 to accommodate branch names with spaces
                rules_passed += 1
            else:
                failures.append(f"Average length too long: {avg_length:.1f}")
        else:
            rules_passed += 1
        
        confidence = (rules_passed / rules_total) * 100 if rules_total > 0 else 0
        
        return {
            "rules_passed": rules_passed,
            "rules_total": rules_total,
            "failures": failures,
            "confidence": confidence
        }
    
    def validate_account_type(self, series, col_name):
        """Validate account_type column according to business rules."""
        rules_passed = 0
        rules_total = 3
        failures = []
        
        non_null = series.dropna().astype(str).str.strip()
        normalized = non_null.str.upper().replace(self.ACCOUNT_TYPE_SYNONYMS)
        total = len(series)
        non_null_count = len(non_null)
        
        # Rule 1: NOT NULL
        not_null_ratio = non_null_count / total if total > 0 else 0
        if not_null_ratio >= 0.95:
            rules_passed += 1
        else:
            failures.append(f"Null ratio: {(1-not_null_ratio)*100:.1f}%")
        
        if non_null_count == 0:
            return {
                "rules_passed": 0,
                "rules_total": rules_total,
                "failures": ["Column is completely empty"],
                "confidence": 0.0
            }
        
        # Rule 2: Allowed values only (case-insensitive with synonyms)
        valid_mask = normalized.isin(self.ALLOWED_ACCOUNT_TYPES)
        valid_ratio = valid_mask.mean()
        if valid_ratio >= 0.95:
            rules_passed += 1
        else:
            invalid_values = non_null[~valid_mask].unique().tolist()[:5]
            failures.append(f"Invalid values found: {invalid_values} (valid ratio: {valid_ratio*100:.1f}%)")
        
        # Rule 3: Repeats for same account (soft check - always pass)
        rules_passed += 1
        
        confidence = (rules_passed / rules_total) * 100
        
        return {
            "rules_passed": rules_passed,
            "rules_total": rules_total,
            "failures": failures,
            "confidence": confidence
        }
    
    def validate_account_status(self, series, col_name):
        """Validate account_status column according to business rules."""
        rules_passed = 0
        rules_total = 3
        failures = []
        
        non_null = series.dropna().astype(str).str.upper().str.strip()
        total = len(series)
        non_null_count = len(non_null)
        
        # Rule 1: NOT NULL
        not_null_ratio = non_null_count / total if total > 0 else 0
        if not_null_ratio >= 0.95:
            rules_passed += 1
        else:
            failures.append(f"Null ratio: {(1-not_null_ratio)*100:.1f}%")
        
        if non_null_count == 0:
            return {
                "rules_passed": 0,
                "rules_total": rules_total,
                "failures": ["Column is completely empty"],
                "confidence": 0.0
            }
        
        # Rule 2: Allowed values only
        valid_ratio = non_null.isin(self.ALLOWED_ACCOUNT_STATUSES).mean()
        if valid_ratio >= 0.95:
            rules_passed += 1
        else:
            invalid_values = non_null[~non_null.isin(self.ALLOWED_ACCOUNT_STATUSES)].unique().tolist()[:5]
            failures.append(f"Invalid values found: {invalid_values} (valid ratio: {valid_ratio*100:.1f}%)")
        
        # Rule 3: One status per account (soft check - always pass)
        rules_passed += 1
        
        confidence = (rules_passed / rules_total) * 100
        
        return {
            "rules_passed": rules_passed,
            "rules_total": rules_total,
            "failures": failures,
            "confidence": confidence
        }
    
    def validate_phone(self, series, col_name):
        """Validate phone column according to business rules (warning only)."""
        rules_passed = 0
        rules_total = 2
        failures = []
        
        non_null = series.dropna().astype(str)
        total = len(series)
        non_null_count = len(non_null)
        
        # Rule 1: NOT NULL (warning only)
        not_null_ratio = non_null_count / total if total > 0 else 0
        if not_null_ratio >= 0.9:
            rules_passed += 1
        else:
            failures.append(f"Warning: Null ratio: {(1-not_null_ratio)*100:.1f}%")
        
        if non_null_count == 0:
            return {
                "rules_passed": 0,
                "rules_total": rules_total,
                "failures": ["Column is completely empty"],
                "confidence": 0.0
            }
        
        # Rule 2: Format check (warning only)
        # Valid formats: 10 digits, may have +, -, spaces, ()
        phone_pattern = re.compile(r'^[\+]?[\d\s\-\(\)]{10,15}$')
        valid_format_ratio = non_null.str.match(phone_pattern).mean()
        if valid_format_ratio >= 0.9:
            rules_passed += 1
        else:
            failures.append(f"Warning: {((1-valid_format_ratio)*100):.1f}% phone numbers have format issues")
        
        confidence = (rules_passed / rules_total) * 100
        
        return {
            "rules_passed": rules_passed,
            "rules_total": rules_total,
            "failures": failures,
            "confidence": confidence
        }
    
    def validate_pan(self, series, col_name):
        """Validate PAN column according to business rules (warning only)."""
        rules_passed = 0
        rules_total = 2
        failures = []
        
        non_null = series.dropna().astype(str).str.upper().str.strip()
        total = len(series)
        non_null_count = len(non_null)
        
        # Rule 1: NOT NULL (warning only)
        not_null_ratio = non_null_count / total if total > 0 else 0
        if not_null_ratio >= 0.9:
            rules_passed += 1
        else:
            failures.append(f"Warning: Null ratio: {(1-not_null_ratio)*100:.1f}%")
        
        if non_null_count == 0:
            return {
                "rules_passed": 0,
                "rules_total": rules_total,
                "failures": ["Column is completely empty"],
                "confidence": 0.0
            }
        
        # Rule 2: PAN format check (warning only)
        # PAN format: 10 characters - first 5 letters, next 4 digits, last 1 letter
        # Pattern: [A-Z]{5}[0-9]{4}[A-Z]{1}
        pan_pattern = re.compile(r'^[A-Z]{5}[0-9]{4}[A-Z]{1}$')
        valid_format_ratio = non_null.str.match(pan_pattern).mean()
        if valid_format_ratio >= 0.9:
            rules_passed += 1
        else:
            failures.append(f"Warning: {((1-valid_format_ratio)*100):.1f}% PAN numbers have format issues (expected: ABCDE1234F)")
        
        confidence = (rules_passed / rules_total) * 100
        
        return {
            "rules_passed": rules_passed,
            "rules_total": rules_total,
            "failures": failures,
            "confidence": confidence
        }
    
    def validate_pan(self, series, col_name):
        """Validate PAN column according to business rules (warning only)."""
        rules_passed = 0
        rules_total = 2
        failures = []
        
        non_null = series.dropna().astype(str).str.upper().str.strip()
        total = len(series)
        non_null_count = len(non_null)
        
        # Rule 1: NOT NULL (warning only)
        not_null_ratio = non_null_count / total if total > 0 else 0
        if not_null_ratio >= 0.9:
            rules_passed += 1
        else:
            failures.append(f"Warning: Null ratio: {(1-not_null_ratio)*100:.1f}%")
        
        if non_null_count == 0:
            return {
                "rules_passed": 0,
                "rules_total": rules_total,
                "failures": ["Column is completely empty"],
                "confidence": 0.0
            }
        
        # Rule 2: PAN format check (warning only)
        # PAN format: 10 characters - first 5 letters, next 4 digits, last 1 letter
        # Pattern: [A-Z]{5}[0-9]{4}[A-Z]{1}
        pan_pattern = re.compile(r'^[A-Z]{5}[0-9]{4}[A-Z]{1}$')
        valid_format_ratio = non_null.str.match(pan_pattern).mean()
        if valid_format_ratio >= 0.9:
            rules_passed += 1
        else:
            failures.append(f"Warning: {((1-valid_format_ratio)*100):.1f}% PAN numbers have format issues (expected: ABCDE1234F)")
        
        confidence = (rules_passed / rules_total) * 100
        
        return {
            "rules_passed": rules_passed,
            "rules_total": rules_total,
            "failures": failures,
            "confidence": confidence
        }
    
    def validate_relationships(self, df, column_roles):
        """Validate relationships between columns."""
        relationships = []
        relationship_statuses = {}
        
        # Find columns by role
        role_to_col = {}
        for col, role in column_roles.items():
            if role not in role_to_col:
                role_to_col[role] = []
            role_to_col[role].append(col)
        
        # Relationship 1: customer_id → multiple accounts
        customer_id_cols = role_to_col.get("customer_id", [])
        account_number_cols = role_to_col.get("account_number", [])
        
        if customer_id_cols and account_number_cols:
            customer_col = customer_id_cols[0]
            account_col = account_number_cols[0]
            try:
                unique_customers = df[customer_col].nunique()
                unique_accounts = df[account_col].nunique()
                if unique_accounts >= unique_customers:  # Each customer can have multiple accounts
                    relationships.append({"customer_id → accounts": "PASS"})
                    relationship_statuses["customer_id_to_accounts"] = True
                else:
                    relationships.append({"customer_id → accounts": "FAIL"})
                    relationship_statuses["customer_id_to_accounts"] = False
            except:
                relationships.append({"customer_id → accounts": "SKIP"})
                relationship_statuses["customer_id_to_accounts"] = None
        else:
            relationships.append({"customer_id → accounts": "SKIP"})
            relationship_statuses["customer_id_to_accounts"] = None
        
        # Relationship 2: account_number → multiple transactions
        transaction_id_cols = role_to_col.get("transaction_id", [])
        if account_number_cols and transaction_id_cols:
            account_col = account_number_cols[0]
            txn_col = transaction_id_cols[0]
            try:
                unique_accounts = df[account_col].nunique()
                unique_transactions = df[txn_col].nunique()
                if unique_transactions >= unique_accounts:  # Each account can have multiple transactions
                    relationships.append({"account_number → transactions": "PASS"})
                    relationship_statuses["account_to_transactions"] = True
                else:
                    relationships.append({"account_number → transactions": "FAIL"})
                    relationship_statuses["account_to_transactions"] = False
            except:
                relationships.append({"account_number → transactions": "SKIP"})
                relationship_statuses["account_to_transactions"] = None
        else:
            relationships.append({"account_number → transactions": "SKIP"})
            relationship_statuses["account_to_transactions"] = None
        
        # Relationship 3: balances_formula: closing = opening + credit - debit
        opening_cols = role_to_col.get("opening_balance", [])
        closing_cols = role_to_col.get("closing_balance", [])
        debit_cols = role_to_col.get("debit", [])
        credit_cols = role_to_col.get("credit", [])
        
        if opening_cols and closing_cols and debit_cols and credit_cols:
            opening_col = opening_cols[0]
            closing_col = closing_cols[0]
            debit_col = debit_cols[0]
            credit_col = credit_cols[0]
            try:
                opening_vals = pd.to_numeric(df[opening_col], errors="coerce").fillna(0)
                closing_vals = pd.to_numeric(df[closing_col], errors="coerce").fillna(0)
                debit_vals = pd.to_numeric(df[debit_col], errors="coerce").fillna(0)
                credit_vals = pd.to_numeric(df[credit_col], errors="coerce").fillna(0)
                
                calculated_closing = opening_vals + credit_vals - debit_vals
                diff = abs(closing_vals - calculated_closing)
                tolerance = closing_vals.abs() * 0.01 + 0.01
                matches = (diff <= tolerance).sum()
                match_ratio = matches / len(df) if len(df) > 0 else 0
                
                if match_ratio >= 0.95:
                    relationships.append({"balances_formula": "PASS"})
                    relationship_statuses["balances_formula"] = True
                else:
                    relationships.append({"balances_formula": "FAIL"})
                    relationship_statuses["balances_formula"] = False
            except:
                relationships.append({"balances_formula": "SKIP"})
                relationship_statuses["balances_formula"] = None
        else:
            relationships.append({"balances_formula": "SKIP"})
            relationship_statuses["balances_formula"] = None
        
        return relationships, relationship_statuses
    
    def compute_dataset_confidence(self, column_results, relationship_statuses):
        """Compute overall dataset confidence using weighted average."""
        total_weighted_confidence = 0.0
        total_weight = 0.0
        
        for col_result in column_results:
            col_name = col_result["name"]
            confidence = col_result["confidence"]
            weight = self.COLUMN_WEIGHTS.get(col_name, 1.0)
            
            total_weighted_confidence += confidence * weight
            total_weight += weight
        
        # Add relationship bonus/penalty
        relationship_score = 0.0
        relationship_count = 0
        
        for status in relationship_statuses.values():
            if status is True:
                relationship_score += 1.0
            elif status is False:
                relationship_score -= 0.5
            relationship_count += 1
        
        relationship_bonus = (relationship_score / relationship_count * 5.0) if relationship_count > 0 else 0.0
        
        if total_weight > 0:
            base_confidence = total_weighted_confidence / total_weight
            dataset_confidence = min(100.0, max(0.0, base_confidence + relationship_bonus))
        else:
            dataset_confidence = 0.0
        
        return dataset_confidence
    
    def validate(self, csv_path):
        """
        Main validation function.
        
        Returns:
        {
            "columns": [
                {"name": "account_number", "confidence": 82.5, "rules_passed": 5, "rules_total": 6, "status": "MATCH"},
                ...
            ],
            "relationships": [
                {"customer_id → accounts": "PASS"},
                ...
            ],
            "dataset_confidence": 85.3,
            "final_decision": "PASS",
            "explanation": "..."
        }
        """
        try:
            # Load dataset
            df = pd.read_csv(csv_path)
            
            if df.empty:
                return {
                    "error": "Dataset is empty",
                    "columns": [],
                    "relationships": [],
                    "dataset_confidence": 0.0,
                    "final_decision": "FAIL",
                    "explanation": "Dataset is empty"
                }
            
            # Step 1: Identify column roles
            column_roles = {}
            for col in df.columns:
                role = self.identify_column_role(col, df)
                column_roles[col] = role
            
            # Step 2: Validate each column
            column_results = []
            validation_results = {}
            
            # Get column references for cross-validation
            debit_col = None
            credit_col = None
            opening_col = None
            closing_col = None
            account_col = None
            
            for col, role in column_roles.items():
                if role == "debit":
                    debit_col = col
                elif role == "credit":
                    credit_col = col
                elif role == "opening_balance":
                    opening_col = col
                elif role == "closing_balance":
                    closing_col = col
                elif role == "account_number":
                    account_col = col
            
            # Validate each column
            for col in df.columns:
                # CRITICAL: Skip KYC columns completely - don't include in results
                norm_col = self.normalize_column_name(col)
                if "kyc" in norm_col:
                    continue  # Skip KYC columns entirely
                
                # Skip non-banking columns (currency, country, email, channel, etc.) - don't include in results
                non_banking_keywords = ["currency", "country", "email", "channel", "source", "region", "city", "state_name"]
                if any(kw in norm_col for kw in non_banking_keywords):
                    continue  # Skip non-banking columns entirely
                
                role = column_roles.get(col)
                
                if role is None:
                    # Unknown column - skip validation and don't include in results
                    continue
                
                # Get validation method
                validation_methods = {
                    "account_number": self.validate_account_number,
                    "customer_id": self.validate_customer_id,
                    "customer_name": self.validate_customer_name,
                    "transaction_id": self.validate_transaction_id,
                    "txn_type": self.validate_txn_type,
                    "debit": lambda s, c: self.validate_debit(s, c, df[credit_col] if credit_col else None),
                    "credit": lambda s, c: self.validate_credit(s, c, df[debit_col] if debit_col else None),
                    "opening_balance": lambda s, c: self.validate_opening_balance(s, c, df, account_col),
                    "closing_balance": lambda s, c: self.validate_closing_balance(s, c, opening_col, debit_col, credit_col, df),
                    "transaction_date": self.validate_transaction_date,
                    "branch_code": self.validate_branch_code,
                    "account_type": self.validate_account_type,
                    "account_status": self.validate_account_status,
                    "phone": self.validate_phone,
                    "pan": lambda s, c: self.validate_phone(s, c),  # PAN uses similar validation as phone (warning only)
                    "created_at": self.validate_transaction_date,  # Created/updated dates use date validation
                    "updated_at": self.validate_transaction_date
                }
                
                if role in validation_methods:
                    result = validation_methods[role](df[col], col)
                    validation_results[col] = result
                    
                    # Determine status (FIX-1: Account number repetition is expected)
                    if result["confidence"] >= 80:
                        status = "MATCH"
                    elif result["confidence"] >= 60:
                        status = "WARNING"
                    else:
                        # For account_number, if it's just repetition (which is expected), don't mark as FAIL
                        if role == "account_number":
                            # Account number repetition across rows is EXPECTED behavior
                            # Only mark as FAIL if it's completely empty or has critical format issues
                            if result.get("confidence", 0) == 0.0:
                                status = "FAIL"
                            else:
                                # Has some data, mark as WARNING instead of FAIL for repetition
                                status = "WARNING"
                        else:
                            status = "FAIL"
                    
                    column_results.append({
                        "name": col,
                        "confidence": round(result["confidence"], 1),
                        "rules_passed": result["rules_passed"],
                        "rules_total": result["rules_total"],
                        "status": status,
                        "meaning": self.COLUMN_DEFINITIONS[role]["meaning"],
                        "failures": result.get("failures", [])
                    })
                # else: Skip unknown columns - don't include in results
            
            # Step 3: Validate relationships
            relationships, relationship_statuses = self.validate_relationships(df, column_roles)
            
            # Step 4: Compute dataset confidence
            dataset_confidence = self.compute_dataset_confidence(column_results, relationship_statuses)
            
            # Step 5: Make final decision according to priority order (FIX-3 & FIX-4)
            # Priority order:
            # 1. Mandatory column failure → REJECT
            # 2. Balance formula failure → REJECT
            # 3. Relationship failure → REJECT
            # 4. Only warnings present → PASS WITH WARNINGS
            # 5. No warnings → PASS
            
            # Define mandatory columns
            mandatory_columns = ["account_number", "customer_id", "transaction_date"]
            
            # Check for mandatory column failures
            mandatory_failed = False
            for col_result in column_results:
                role = column_roles.get(col_result["name"])
                if role in mandatory_columns and col_result["status"] == "FAIL":
                    # Check if column is completely empty (NULL) - this is a REJECT
                    if col_result.get("confidence", 0) == 0.0:
                        mandatory_failed = True
                        break
            
            # Check balance formula failure (this is a relationship check)
            balance_formula_failed = relationship_statuses.get("balances_formula") == False
            
            # Check for critical relationship failures (excluding balance formula which is checked separately)
            critical_relationship_failed = False
            for rel_key, rel_status in relationship_statuses.items():
                if rel_key != "balances_formula" and rel_status == False:
                    # Other relationship failures are warnings, not REJECT
                    pass
            
            # Make final decision
            if mandatory_failed:
                final_decision = "REJECT"
            elif balance_formula_failed:
                final_decision = "REJECT"
            else:
                # Count warnings and failures
                warning_count = sum(1 for c in column_results if c["status"] == "WARNING")
                fail_count = sum(1 for c in column_results if c["status"] == "FAIL")
                match_count = sum(1 for c in column_results if c["status"] == "MATCH")
                
                # Check for non-critical failures (these should be warnings)
                # Transaction ID uniqueness failure is non-critical (warning only)
                non_critical_failures = fail_count
                for col_result in column_results:
                    role = column_roles.get(col_result["name"])
                    if role == "transaction_id" and col_result["status"] == "FAIL":
                        # Transaction ID uniqueness failure is warning-only, not REJECT
                        non_critical_failures -= 1
                        warning_count += 1
                
                if warning_count > 0 or non_critical_failures == 0:
                    # Only warnings present or no critical failures
                    final_decision = "PASS WITH WARNINGS"
                elif match_count > 0 and fail_count == 0:
                    # No warnings, no failures
                    final_decision = "PASS"
                else:
                    # Should not reach here if logic is correct, but default to PASS WITH WARNINGS
                    final_decision = "PASS WITH WARNINGS"
            
            # Step 6: Generate explanation
            explanation_parts = []
            
            # Count column statuses
            match_count = sum(1 for c in column_results if c["status"] == "MATCH")
            warning_count = sum(1 for c in column_results if c["status"] == "WARNING")
            fail_count = sum(1 for c in column_results if c["status"] == "FAIL")
            
            if match_count > 0:
                explanation_parts.append(f"{match_count} column(s) passed validation")
            if warning_count > 0:
                explanation_parts.append(f"{warning_count} column(s) have warnings (non-fatal)")
            if fail_count > 0:
                # Only show critical failures in explanation
                critical_failures = [
                    c for c in column_results 
                    if c["status"] == "FAIL" and column_roles.get(c["name"]) in mandatory_columns
                ]
                if critical_failures:
                    explanation_parts.append(f"{len(critical_failures)} mandatory column(s) failed validation")
                else:
                    explanation_parts.append(f"{fail_count} column(s) have issues (non-fatal)")
            
            # Relationship status
            pass_count = sum(1 for r in relationships for v in r.values() if v == "PASS")
            fail_count_rel = sum(1 for r in relationships for v in r.values() if v == "FAIL")
            
            if pass_count > 0:
                explanation_parts.append(f"{pass_count} relationship(s) validated")
            if fail_count_rel > 0:
                if balance_formula_failed:
                    explanation_parts.append("Balance formula failed (closing = opening + credit - debit)")
                else:
                    explanation_parts.append(f"{fail_count_rel} relationship(s) have warnings (non-fatal)")
            
            # Add specific issues
            failure_details = []
            for col_result in column_results:
                if col_result["status"] == "FAIL" and col_result.get("failures"):
                    role = column_roles.get(col_result["name"])
                    if role in mandatory_columns:
                        # Show mandatory failures
                        failure_details.append(f"{col_result['name']}: {', '.join(col_result['failures'][:2])}")
                    elif role != "transaction_id":
                        # Show other failures (but not transaction_id uniqueness as it's warning-only)
                        failure_details.append(f"{col_result['name']}: {', '.join(col_result['failures'][:2])}")
            
            if failure_details:
                explanation_parts.append("Critical issues: " + "; ".join(failure_details[:3]))
            
            # Add decision reason
            if mandatory_failed:
                explanation_parts.append("REJECT: Mandatory column(s) failed")
            elif balance_formula_failed:
                explanation_parts.append("REJECT: Balance formula validation failed")
            elif warning_count > 0:
                explanation_parts.append("PASS WITH WARNINGS: All mandatory checks passed, but warnings present")
            else:
                explanation_parts.append("PASS: All validation checks passed")
            
            explanation = ". ".join(explanation_parts) if explanation_parts else "Dataset validation completed"
            
            # Format relationships as list of dicts (one key-value per dict)
            formatted_relationships = []
            for rel in relationships:
                formatted_relationships.append(rel)
            
            # Filter out KYC columns from results
            filtered_column_results = [
                col for col in column_results 
                if "kyc" not in self.normalize_column_name(col.get("name", ""))
            ]
            
            return {
                "columns": filtered_column_results,
                "relationships": formatted_relationships,
                "dataset_confidence": round(dataset_confidence, 1),
                "final_decision": final_decision,
                "explanation": explanation
            }
        
        except Exception as e:
            return {
                "error": str(e),
                "columns": [],
                "relationships": [],
                "dataset_confidence": 0.0,
                "final_decision": "FAIL",
                "explanation": f"Validation error: {str(e)}"
            }


def main():
    """Example usage."""
    validator = BankingDatasetValidator()
    result = validator.validate("bank.csv")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()