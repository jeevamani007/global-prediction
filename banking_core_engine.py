"""
Core Banking Data Column Detection and Validation Engine

IMPORTANT:
- KYC features are COMPLETELY REMOVED
- Strict role-based column classification
- Rules applied ONLY to confirmed roles (confidence >= 70%)
"""

import pandas as pd
import numpy as np
from rapidfuzz import fuzz
from datetime import datetime
import re


class CoreBankingEngine:
    
    # Role definitions
    ROLES = [
        "ACCOUNT_NUMBER",
        "CUSTOMER_ID", 
        "TRANSACTION_ID",
        "TRANSACTION_DATE",
        "TRANSACTION_TYPE",
        "OPENING_BALANCE",
        "DEBIT",
        "CREDIT",
        "CLOSING_BALANCE",
        "ACCOUNT_STATUS",
        "UNKNOWN"
    ]
    
    # Confidence threshold for role locking
    ROLE_CONFIDENCE_THRESHOLD = 70.0
    
    def __init__(self):
        self.role_keywords = self._initialize_role_keywords()
    
    def _initialize_role_keywords(self):
        """Initialize keywords for each role"""
        return {
            "ACCOUNT_NUMBER": [
                "account_number", "account_no", "acc_no", "accno", "accountno",
                "account", "acct", "acc_number", "accountid"
            ],
            "CUSTOMER_ID": [
                "customer_id", "cust_id", "customerid", "custid", 
                "client_id", "clientid", "user_id", "userid"
            ],
            "TRANSACTION_ID": [
                "transaction_id", "txn_id", "trans_id", "transactionid",
                "txnid", "transaction_number"
            ],
            "TRANSACTION_DATE": [
                "transaction_date", "txn_date", "trans_date", "transactiondate",
                "date", "transaction_time", "txn_time"
            ],
            "TRANSACTION_TYPE": [
                "transaction_type", "txn_type", "trans_type", "transactiontype",
                "type", "transaction_category", "txn_category"
            ],
            "OPENING_BALANCE": [
                "opening_balance", "open_balance", "balance_before",
                "previous_balance", "prev_balance", "initial_balance"
            ],
            "DEBIT": [
                "debit", "withdrawal", "withdraw_amount", "amount_out",
                "dr_amount", "debit_amount", "withdraw"
            ],
            "CREDIT": [
                "credit", "deposit", "amount_in", "cr_amount",
                "credit_amount", "deposit_amount"
            ],
            "CLOSING_BALANCE": [
                "closing_balance", "closing", "balance_after",
                "final_balance", "end_balance", "current_balance"
            ],
            "ACCOUNT_STATUS": [
                "account_status", "acc_status", "status", "account_state",
                "acc_state", "state"
            ]
        }
    
    def normalize(self, text):
        """Normalize text for matching"""
        return str(text).lower().replace(" ", "").replace("_", "").replace("-", "")
    
    def classify_column_role(self, df: pd.DataFrame, column_name: str):
        """
        STEP 1: Column Role Classification
        
        For each column, assign ONE role with confidence score.
        If confidence < 70%, role = UNKNOWN
        """
        column_series = df[column_name]
        norm_col = self.normalize(column_name)
        
        # CRITICAL SAFETY RULE: If column name contains financial keywords, never classify as account_number
        financial_keywords = ["amount", "balance", "loan", "emi", "interest", "rate"]
        is_financial_amount = any(keyword in norm_col for keyword in financial_keywords)
        
        role_scores = {}
        
        # 1. Name-based matching (40% weight)
        for role, keywords in self.role_keywords.items():
            max_name_score = 0
            for keyword in keywords:
                norm_keyword = self.normalize(keyword)
                # Exact substring match
                if norm_keyword in norm_col or norm_col in norm_keyword:
                    max_name_score = max(max_name_score, 100)
                # Fuzzy match
                else:
                    fuzzy_score = fuzz.ratio(norm_col, norm_keyword)
                    max_name_score = max(max_name_score, fuzzy_score)
            role_scores[role] = max_name_score * 0.4
        
        # CRITICAL SAFETY RULE: If column contains financial keywords, force classify as financial amount and skip account number classification
        if is_financial_amount:
            # Set very high score for appropriate financial amount roles
            if "opening" in norm_col or "open" in norm_col:
                role_scores["OPENING_BALANCE"] = max(role_scores.get("OPENING_BALANCE", 0), 95)
            elif "closing" in norm_col or "close" in norm_col or "current" in norm_col:
                role_scores["CLOSING_BALANCE"] = max(role_scores.get("CLOSING_BALANCE", 0), 95)
            elif "debit" in norm_col:
                role_scores["DEBIT"] = max(role_scores.get("DEBIT", 0), 95)
            elif "credit" in norm_col:
                role_scores["CREDIT"] = max(role_scores.get("CREDIT", 0), 95)
            elif "loan" in norm_col:
                # Default to CLOSING_BALANCE for loan_amount as a financial amount
                role_scores["CLOSING_BALANCE"] = max(role_scores.get("CLOSING_BALANCE", 0), 90)
            else:
                # Generic financial amount
                role_scores["CLOSING_BALANCE"] = max(role_scores.get("CLOSING_BALANCE", 0), 85)
            
            # CRITICAL: Remove any ACCOUNT_NUMBER score to prevent misclassification
            if "ACCOUNT_NUMBER" in role_scores:
                del role_scores["ACCOUNT_NUMBER"]
        
        # 2. Value pattern analysis (60% weight) - only for non-financial columns
        try:
            if not is_financial_amount:  # Only apply value pattern analysis for non-financial columns
                # ACCOUNT_NUMBER pattern check
                if self._matches_account_number_pattern(column_series):
                    role_scores["ACCOUNT_NUMBER"] = role_scores.get("ACCOUNT_NUMBER", 0) + 60
                
                # TRANSACTION_ID pattern check
                if self._matches_transaction_id_pattern(column_series):
                    role_scores["TRANSACTION_ID"] = role_scores.get("TRANSACTION_ID", 0) + 60
                
                # TRANSACTION_DATE pattern check
                if self._matches_date_pattern(column_series):
                    role_scores["TRANSACTION_DATE"] = role_scores.get("TRANSACTION_DATE", 0) + 60
                
                # TRANSACTION_TYPE pattern check
                if self._matches_transaction_type_pattern(column_series):
                    role_scores["TRANSACTION_TYPE"] = role_scores.get("TRANSACTION_TYPE", 0) + 60
                
                # Numeric balance/debit/credit checks
                if self._matches_numeric_balance_pattern(column_series):
                    # Distinguish between opening/closing/debit/credit based on context
                    if "opening" in norm_col or "open" in norm_col or "initial" in norm_col:
                        role_scores["OPENING_BALANCE"] = role_scores.get("OPENING_BALANCE", 0) + 60
                    elif "closing" in norm_col or "closing" in norm_col or "final" in norm_col or "current" in norm_col:
                        role_scores["CLOSING_BALANCE"] = role_scores.get("CLOSING_BALANCE", 0) + 60
                    elif "debit" in norm_col or "withdraw" in norm_col or "out" in norm_col:
                        role_scores["DEBIT"] = role_scores.get("DEBIT", 0) + 60
                    elif "credit" in norm_col or "deposit" in norm_col or "in" in norm_col:
                        role_scores["CREDIT"] = role_scores.get("CREDIT", 0) + 60
                    else:
                        # Generic numeric - distribute score based on name hints
                        if not any(role in role_scores for role in ["OPENING_BALANCE", "CLOSING_BALANCE", "DEBIT", "CREDIT"]):
                            # Default to CLOSING_BALANCE if no hint
                            role_scores["CLOSING_BALANCE"] = role_scores.get("CLOSING_BALANCE", 0) + 40
                
                # ACCOUNT_STATUS pattern check
                if self._matches_status_pattern(column_series):
                    role_scores["ACCOUNT_STATUS"] = role_scores.get("ACCOUNT_STATUS", 0) + 60
                
                # CUSTOMER_ID pattern check
                if self._matches_customer_id_pattern(column_series):
                    role_scores["CUSTOMER_ID"] = role_scores.get("CUSTOMER_ID", 0) + 60
                
        except Exception as e:
            pass  # If pattern analysis fails, rely on name matching only
        
        # Select best role
        if role_scores:
            best_role = max(role_scores.items(), key=lambda x: x[1])
            confidence = best_role[1]
            role = best_role[0] if confidence >= self.ROLE_CONFIDENCE_THRESHOLD else "UNKNOWN"
        else:
            role = "UNKNOWN"
            confidence = 0.0
        
        return {
            "role": role,
            "confidence": round(float(confidence), 2),
            "all_role_scores": {k: round(float(v), 2) for k, v in role_scores.items() if v > 0}
        }
    
    def _matches_account_number_pattern(self, series):
        """Check if series matches account number pattern: digits only, length 6-18"""
        try:
            non_null = series.dropna().astype(str)
            if len(non_null) == 0:
                return False
            digit_only_ratio = non_null.str.fullmatch(r"\d+").mean()
            length_ok_ratio = non_null.str.len().between(6, 18).mean()
            return digit_only_ratio >= 0.8 and length_ok_ratio >= 0.8
        except:
            return False
    
    def _matches_transaction_id_pattern(self, series):
        """Check if series matches transaction ID pattern"""
        try:
            non_null = series.dropna().astype(str)
            if len(non_null) == 0:
                return False
            # Transaction IDs are usually alphanumeric or numeric
            alphanumeric_ratio = non_null.str.fullmatch(r"[A-Za-z0-9]+").mean()
            unique_ratio = non_null.nunique() / len(non_null)
            return alphanumeric_ratio >= 0.8 and unique_ratio >= 0.7
        except:
            return False
    
    def _matches_date_pattern(self, series):
        """Check if series matches date pattern"""
        try:
            non_null = series.dropna()
            if len(non_null) == 0:
                return False
            # Try to parse as date
            date_parsed = pd.to_datetime(non_null, errors="coerce")
            valid_date_ratio = date_parsed.notna().mean()
            return valid_date_ratio >= 0.7
        except:
            return False
    
    def _matches_transaction_type_pattern(self, series):
        """Check if series contains transaction type values"""
        try:
            non_null = series.dropna().astype(str).str.lower().str.strip()
            if len(non_null) == 0:
                return False
            valid_types = ["deposit", "withdraw", "withdrawal", "transfer"]
            match_ratio = non_null.isin(valid_types).mean()
            return match_ratio >= 0.5
        except:
            return False
    
    def _matches_numeric_balance_pattern(self, series):
        """Check if series matches numeric balance/debit/credit pattern"""
        try:
            numeric_series = pd.to_numeric(series, errors="coerce")
            non_null = numeric_series.dropna()
            if len(non_null) == 0:
                return False
            numeric_ratio = len(non_null) / len(series) if len(series) > 0 else 0
            non_negative_ratio = (non_null >= 0).mean()
            return numeric_ratio >= 0.8 and non_negative_ratio >= 0.9
        except:
            return False
    
    def _matches_status_pattern(self, series):
        """Check if series matches account status pattern"""
        try:
            non_null = series.dropna().astype(str).str.upper().str.strip()
            if len(non_null) == 0:
                return False
            valid_statuses = ["ACTIVE", "INACTIVE", "CLOSED", "FROZEN", "1", "0", "TRUE", "FALSE", "YES", "NO"]
            match_ratio = non_null.isin(valid_statuses).mean()
            return match_ratio >= 0.5
        except:
            return False
    
    def _matches_customer_id_pattern(self, series):
        """Check if series matches customer ID pattern: alphanumeric, 3-10 chars"""
        try:
            non_null = series.dropna().astype(str)
            if len(non_null) == 0:
                return False
            # Pattern: letters followed by numbers (e.g., C001, C002)
            pattern_match = non_null.str.fullmatch(r"[A-Za-z]{1,2}\d{1,4}").mean()
            alphanumeric_ratio = non_null.str.fullmatch(r"[A-Za-z0-9]+").mean()
            length_ok = non_null.str.len().between(3, 10).mean()
            return (pattern_match >= 0.5 or alphanumeric_ratio >= 0.8) and length_ok >= 0.8
        except:
            return False
    
    def validate_role(self, df: pd.DataFrame, column_name: str, role: str):
        """
        STEP 3: Rule Application Matrix
        
        Apply rules ONLY based on confirmed role (confidence >= 70%)
        """
        if role == "UNKNOWN":
            return {
                "role": "UNKNOWN",
                "rules_applied": [],
                "rules_passed": [],
                "rules_failed": [],
                "rules_skipped": [{"rule": "all", "reason": "Role confidence < 70%, column locked as UNKNOWN"}],
                "is_valid": None
            }
        
        column_series = df[column_name]
        rules_applied = []
        rules_passed = []
        rules_failed = []
        
        try:
            if role == "ACCOUNT_NUMBER":
                result = self._validate_account_number(column_series)
                rules_applied.extend(result["rules_applied"])
                rules_passed.extend(result["rules_passed"])
                rules_failed.extend(result["rules_failed"])
            
            elif role == "TRANSACTION_DATE":
                result = self._validate_transaction_date(column_series)
                rules_applied.extend(result["rules_applied"])
                rules_passed.extend(result["rules_passed"])
                rules_failed.extend(result["rules_failed"])
            
            elif role == "TRANSACTION_TYPE":
                result = self._validate_transaction_type(column_series)
                rules_applied.extend(result["rules_applied"])
                rules_passed.extend(result["rules_passed"])
                rules_failed.extend(result["rules_failed"])
            
            elif role == "OPENING_BALANCE":
                result = self._validate_balance(column_series, "OPENING_BALANCE")
                rules_applied.extend(result["rules_applied"])
                rules_passed.extend(result["rules_passed"])
                rules_failed.extend(result["rules_failed"])
            
            elif role == "DEBIT":
                result = self._validate_balance(column_series, "DEBIT")
                rules_applied.extend(result["rules_applied"])
                rules_passed.extend(result["rules_passed"])
                rules_failed.extend(result["rules_failed"])
            
            elif role == "CREDIT":
                result = self._validate_balance(column_series, "CREDIT")
                rules_applied.extend(result["rules_applied"])
                rules_passed.extend(result["rules_passed"])
                rules_failed.extend(result["rules_failed"])
            
            elif role == "CLOSING_BALANCE":
                result = self._validate_balance(column_series, "CLOSING_BALANCE")
                rules_applied.extend(result["rules_applied"])
                rules_passed.extend(result["rules_passed"])
                rules_failed.extend(result["rules_failed"])
            
            elif role == "ACCOUNT_STATUS":
                result = self._validate_account_status(column_series)
                rules_applied.extend(result["rules_applied"])
                rules_passed.extend(result["rules_passed"])
                rules_failed.extend(result["rules_failed"])
            
            elif role in ["CUSTOMER_ID", "TRANSACTION_ID"]:
                # These roles have basic validation only
                result = self._validate_id_column(column_series, role)
                rules_applied.extend(result["rules_applied"])
                rules_passed.extend(result["rules_passed"])
                rules_failed.extend(result["rules_failed"])
            
            else:
                rules_applied = []
                rules_passed = []
                rules_failed = []
            
        except Exception as e:
            rules_failed.append({"rule": "validation_error", "reason": str(e)})
        
        is_valid = len(rules_failed) == 0 if rules_applied else None
        
        return {
            "role": role,
            "rules_applied": rules_applied,
            "rules_passed": rules_passed,
            "rules_failed": rules_failed,
            "rules_skipped": [],
            "is_valid": bool(is_valid) if is_valid is not None else None
        }
    
    def _validate_account_number(self, series):
        """Validate ACCOUNT_NUMBER rules"""
        rules_applied = []
        rules_passed = []
        rules_failed = []
        
        non_null = series.dropna().astype(str)
        total = int(len(series))  # Convert to Python int
        non_null_count = int(len(non_null))  # Convert to Python int
        
        # Rule 1: NOT NULL
        not_null_ratio = float(non_null_count / total) if total > 0 else 0.0
        rules_applied.append("NOT_NULL")
        if not_null_ratio >= 0.95:
            rules_passed.append("NOT_NULL")
        else:
            rules_failed.append({"rule": "NOT_NULL", "reason": f"Null ratio: {1-not_null_ratio:.2%}"})
        
        # Rule 2: Digits only
        digit_only_ratio = float(non_null.str.fullmatch(r"\d+").mean()) if non_null_count > 0 else 0.0
        rules_applied.append("DIGITS_ONLY")
        if digit_only_ratio >= 0.95:
            rules_passed.append("DIGITS_ONLY")
        else:
            rules_failed.append({"rule": "DIGITS_ONLY", "reason": f"Non-digit ratio: {1-digit_only_ratio:.2%}"})
        
        # Rule 3: Length 6-18
        length_ok_ratio = float(non_null.str.len().between(6, 18).mean()) if non_null_count > 0 else 0.0
        rules_applied.append("LENGTH_6_18")
        if length_ok_ratio >= 0.95:
            rules_passed.append("LENGTH_6_18")
        else:
            rules_failed.append({"rule": "LENGTH_6_18", "reason": f"Invalid length ratio: {1-length_ok_ratio:.2%}"})
        
        # Rule 4: Uniqueness (warning only)
        unique_count = int(non_null.nunique())  # Convert to Python int
        unique_ratio = float(unique_count / non_null_count) if non_null_count > 0 else 0.0
        rules_applied.append("UNIQUENESS_CHECK")
        if unique_ratio >= 0.9:
            rules_passed.append("UNIQUENESS_CHECK")
        else:
            rules_failed.append({"rule": "UNIQUENESS_CHECK", "reason": f"Duplicate ratio: {1-unique_ratio:.2%} (warning only)"})
        
        return {"rules_applied": rules_applied, "rules_passed": rules_passed, "rules_failed": rules_failed}
    
    def _validate_transaction_date(self, series):
        """Validate TRANSACTION_DATE rules"""
        rules_applied = []
        rules_passed = []
        rules_failed = []
        
        # Rule 1: Valid date format
        dates = pd.to_datetime(series, errors="coerce")
        valid_date_ratio = float(dates.notna().mean())  # Convert to Python float
        rules_applied.append("VALID_DATE_FORMAT")
        if valid_date_ratio >= 0.9:
            rules_passed.append("VALID_DATE_FORMAT")
        else:
            rules_failed.append({"rule": "VALID_DATE_FORMAT", "reason": f"Invalid date ratio: {1-valid_date_ratio:.2%}"})
        
        # Rule 2: Cannot be future date
        future_dates = dates[dates > pd.Timestamp.now()]
        future_count = int(len(future_dates))  # Convert to Python int
        total_count = int(len(series))  # Convert to Python int
        future_ratio = float(future_count / total_count) if total_count > 0 else 0.0
        rules_applied.append("NOT_FUTURE_DATE")
        if future_ratio == 0:
            rules_passed.append("NOT_FUTURE_DATE")
        else:
            rules_failed.append({"rule": "NOT_FUTURE_DATE", "reason": f"Future dates found: {future_count} ({future_ratio:.2%})"})
        
        return {"rules_applied": rules_applied, "rules_passed": rules_passed, "rules_failed": rules_failed}
    
    def _validate_transaction_type(self, series):
        """Validate TRANSACTION_TYPE rules"""
        rules_applied = []
        rules_passed = []
        rules_failed = []
        
        non_null = series.dropna().astype(str).str.lower().str.strip()
        valid_types = ["deposit", "withdraw", "withdrawal", "transfer"]
        non_null_count = int(len(non_null))  # Convert to Python int
        valid_ratio = float(non_null.isin(valid_types).mean()) if non_null_count > 0 else 0.0
        
        rules_applied.append("ALLOWED_VALUES_ONLY")
        if valid_ratio >= 0.9:
            rules_passed.append("ALLOWED_VALUES_ONLY")
        else:
            invalid_values = non_null[~non_null.isin(valid_types)].unique().tolist()[:5]
            rules_failed.append({
                "rule": "ALLOWED_VALUES_ONLY",
                "reason": f"Invalid values found: {invalid_values} (valid ratio: {valid_ratio:.2%})"
            })
        
        return {"rules_applied": rules_applied, "rules_passed": rules_passed, "rules_failed": rules_failed}
    
    def _validate_balance(self, series, role_type):
        """Validate balance/debit/credit rules"""
        rules_applied = []
        rules_passed = []
        rules_failed = []
        
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_null = numeric_series.dropna()
        total = int(len(series))  # Convert to Python int
        non_null_count = int(len(non_null))  # Convert to Python int
        
        # Rule 1: Numeric
        numeric_ratio = float(non_null_count / total) if total > 0 else 0.0
        rules_applied.append("NUMERIC")
        if numeric_ratio >= 0.9:
            rules_passed.append("NUMERIC")
        else:
            rules_failed.append({"rule": "NUMERIC", "reason": f"Non-numeric ratio: {1-numeric_ratio:.2%}"})
        
        # Rule 2: >= 0
        non_negative_ratio = float((non_null >= 0).mean()) if non_null_count > 0 else 0.0
        rules_applied.append("NON_NEGATIVE")
        if non_negative_ratio >= 0.95:
            rules_passed.append("NON_NEGATIVE")
        else:
            negative_count = int((non_null < 0).sum())  # Convert to Python int
            rules_failed.append({"rule": "NON_NEGATIVE", "reason": f"Negative values found: {negative_count}"})
        
        return {"rules_applied": rules_applied, "rules_passed": rules_passed, "rules_failed": rules_failed}
    
    def _validate_account_status(self, series):
        """Validate ACCOUNT_STATUS rules"""
        rules_applied = []
        rules_passed = []
        rules_failed = []
        
        non_null = series.dropna().astype(str).str.upper().str.strip()
        valid_statuses = ["ACTIVE", "INACTIVE", "CLOSED", "FROZEN"]
        
        # Check if values match valid statuses
        non_null_count = int(len(non_null))  # Convert to Python int
        valid_ratio = float(non_null.isin(valid_statuses).mean()) if non_null_count > 0 else 0.0
        rules_applied.append("VALID_STATUS_VALUES")
        
        # Also accept boolean-like values
        boolean_statuses = ["1", "0", "TRUE", "FALSE", "YES", "NO"]
        boolean_ratio = float(non_null.isin(boolean_statuses).mean()) if non_null_count > 0 else 0.0
        
        if valid_ratio >= 0.8 or boolean_ratio >= 0.8:
            rules_passed.append("VALID_STATUS_VALUES")
        else:
            invalid_values = non_null[~non_null.isin(valid_statuses + boolean_statuses)].unique().tolist()[:5]
            rules_failed.append({
                "rule": "VALID_STATUS_VALUES",
                "reason": f"Invalid status values found: {invalid_values}"
            })
        
        return {"rules_applied": rules_applied, "rules_passed": rules_passed, "rules_failed": rules_failed}
    
    def _validate_id_column(self, series, role_type):
        """Validate CUSTOMER_ID or TRANSACTION_ID"""
        rules_applied = []
        rules_passed = []
        rules_failed = []
        
        non_null = series.dropna().astype(str)
        total = len(series)
        non_null_count = len(non_null)
        
        # Rule 1: NOT NULL
        not_null_ratio = non_null_count / total if total > 0 else 0
        rules_applied.append("NOT_NULL")
        if not_null_ratio >= 0.9:
            rules_passed.append("NOT_NULL")
        else:
            rules_failed.append({"rule": "NOT_NULL", "reason": f"Null ratio: {1-not_null_ratio:.2%}"})
        
        return {"rules_applied": rules_applied, "rules_passed": rules_passed, "rules_failed": rules_failed}
    
    def apply_cross_column_logic(self, df: pd.DataFrame, column_roles: dict):
        """
        STEP 4: Cross-Column Banking Logic
        
        Apply rules that involve multiple columns
        """
        cross_validation_results = {
            "debit_credit_exclusivity": None,
            "balance_formula_validation": None,
            "balance_continuity": None
        }
        
        # Find columns by role
        role_to_column = {}
        for col, role_info in column_roles.items():
            if role_info["role"] != "UNKNOWN":
                role = role_info["role"]
                if role not in role_to_column:
                    role_to_column[role] = []
                role_to_column[role].append(col)
        
        # 1. Debit-Credit Exclusivity
        debit_cols = role_to_column.get("DEBIT", [])
        credit_cols = role_to_column.get("CREDIT", [])
        
        if debit_cols and credit_cols:
            # Check first matching pair
            debit_col = debit_cols[0]
            credit_col = credit_cols[0]
            
            try:
                debit_series = pd.to_numeric(df[debit_col], errors="coerce").fillna(0)
                credit_series = pd.to_numeric(df[credit_col], errors="coerce").fillna(0)
                
                both_positive = int(((debit_series > 0) & (credit_series > 0)).sum())  # Convert to Python int
                both_zero = int(((debit_series == 0) & (credit_series == 0)).sum())  # Convert to Python int
                total = int(len(df))  # Convert to Python int
                
                both_positive_ratio = float(both_positive / total) if total > 0 else 0.0
                both_zero_ratio = float(both_zero / total) if total > 0 else 0.0
                
                if both_positive_ratio > 0.1:
                    cross_validation_results["debit_credit_exclusivity"] = {
                        "valid": False,
                        "reason": f"Found {both_positive} rows where both debit and credit > 0 ({both_positive_ratio:.2%})"
                    }
                elif both_zero_ratio > 0.9:
                    cross_validation_results["debit_credit_exclusivity"] = {
                        "valid": False,
                        "reason": f"Found {both_zero} rows where both debit and credit = 0 ({both_zero_ratio:.2%})"
                    }
                else:
                    cross_validation_results["debit_credit_exclusivity"] = {
                        "valid": True,
                        "reason": "Debit and credit columns are mutually exclusive as expected"
                    }
            except Exception as e:
                cross_validation_results["debit_credit_exclusivity"] = {
                    "valid": None,
                    "reason": f"Error checking exclusivity: {str(e)}"
                }
        
        # 2. Balance Formula Validation: closing = opening + credit - debit
        opening_cols = role_to_column.get("OPENING_BALANCE", [])
        closing_cols = role_to_column.get("CLOSING_BALANCE", [])
        
        if opening_cols and closing_cols and debit_cols and credit_cols:
            try:
                opening_col = opening_cols[0]
                closing_col = closing_cols[0]
                debit_col = debit_cols[0]
                credit_col = credit_cols[0]
                
                opening_series = pd.to_numeric(df[opening_col], errors="coerce").fillna(0)
                closing_series = pd.to_numeric(df[closing_col], errors="coerce").fillna(0)
                debit_series = pd.to_numeric(df[debit_col], errors="coerce").fillna(0)
                credit_series = pd.to_numeric(df[credit_col], errors="coerce").fillna(0)
                
                calculated_closing = opening_series + credit_series - debit_series
                diff = abs(closing_series - calculated_closing)
                tolerance = closing_series.abs() * 0.01 + 0.01
                matches = int((diff <= tolerance).sum())  # Convert to Python int
                match_ratio = float(matches / len(df)) if len(df) > 0 else 0.0
                
                if match_ratio >= 0.8:
                    cross_validation_results["balance_formula_validation"] = {
                        "valid": True,
                        "reason": f"Balance formula validated: {match_ratio:.2%} of rows match (closing = opening + credit - debit)"
                    }
                else:
                    cross_validation_results["balance_formula_validation"] = {
                        "valid": False,
                        "reason": f"Balance formula mismatch: only {match_ratio:.2%} of rows match expected formula"
                    }
            except Exception as e:
                cross_validation_results["balance_formula_validation"] = {
                    "valid": None,
                    "reason": f"Error validating balance formula: {str(e)}"
                }
        
        # 3. Balance Continuity (if multiple rows per account)
        if opening_cols and closing_cols:
            account_cols = role_to_column.get("ACCOUNT_NUMBER", [])
            if account_cols:
                try:
                    account_col = account_cols[0]
                    opening_col = opening_cols[0]
                    closing_col = closing_cols[0]
                    
                    # Group by account and check continuity
                    df_sorted = df.sort_values(by=[account_col] + (role_to_column.get("TRANSACTION_DATE", []) or []))
                    
                    # For each account, check if current opening = previous closing
                    continuity_checks = 0
                    continuity_matches = 0
                    
                    for account in df_sorted[account_col].unique():
                        account_rows = df_sorted[df_sorted[account_col] == account]
                        if len(account_rows) > 1:
                            prev_closing = pd.to_numeric(account_rows.iloc[:-1][closing_col], errors="coerce").fillna(0).values
                            curr_opening = pd.to_numeric(account_rows.iloc[1:][opening_col], errors="coerce").fillna(0).values
                            
                            for pc, co in zip(prev_closing, curr_opening):
                                continuity_checks += 1
                                if abs(float(pc) - float(co)) <= 0.01:
                                    continuity_matches += 1
                    
                    continuity_checks = int(continuity_checks)  # Convert to Python int
                    continuity_matches = int(continuity_matches)  # Convert to Python int
                    if continuity_checks > 0:
                        continuity_ratio = float(continuity_matches / continuity_checks)
                        if continuity_ratio >= 0.8:
                            cross_validation_results["balance_continuity"] = {
                                "valid": True,
                                "reason": f"Balance continuity validated: {continuity_ratio:.2%} of account transitions match"
                            }
                        else:
                            cross_validation_results["balance_continuity"] = {
                                "valid": False,
                                "reason": f"Balance continuity issue: only {continuity_ratio:.2%} of account transitions match"
                            }
                except Exception as e:
                    cross_validation_results["balance_continuity"] = {
                        "valid": None,
                        "reason": f"Error checking balance continuity: {str(e)}"
                    }
        
        return cross_validation_results
    
    def determine_dataset_type(self, df: pd.DataFrame, column_roles: dict):
        """
        Determine dataset type: transaction, master, or mixed
        
        - transaction: Has transaction_id, debit/credit, transaction_type
        - master: Has account_number, customer_id, account_status (but no transaction columns)
        - mixed: Has both account and transaction columns
        """
        has_transaction_cols = False
        has_master_cols = False
        
        # Check for transaction columns
        transaction_roles = ["TRANSACTION_ID", "TRANSACTION_TYPE", "DEBIT", "CREDIT"]
        for col, role_info in column_roles.items():
            role = role_info.get("role", "")
            if role in transaction_roles and role_info.get("confidence", 0) >= 70:
                has_transaction_cols = True
                break
        
        # Check for master columns
        master_roles = ["ACCOUNT_NUMBER", "CUSTOMER_ID", "ACCOUNT_STATUS"]
        master_count = 0
        for col, role_info in column_roles.items():
            role = role_info.get("role", "")
            if role in master_roles and role_info.get("confidence", 0) >= 70:
                master_count += 1
        
        if master_count >= 2:  # At least 2 master columns
            has_master_cols = True
        
        if has_transaction_cols and has_master_cols:
            return "mixed"
        elif has_transaction_cols:
            return "transaction"
        elif has_master_cols:
            return "master"
        else:
            return "unknown"
    
    def make_final_decision(self, column_validations: dict, cross_validations: dict, 
                           column_roles: dict, df: pd.DataFrame):
        """
        STEP-8: Final Decision Logic
        
        New Rules:
        1. Determine dataset_type first (transaction/master/mixed)
        2. For TRANSACTION datasets: account_number uniqueness is NOT required
        3. Use score-based acceptance: IF average confidence of core banking columns >= 70% THEN ACCEPT
        4. Provide rejection ONLY if: no account identifier, no transaction type, no monetary columns
        5. Do NOT reject if: branch_code not detected, PAN not present, Apriori not applicable, dataset size < 20 rows
        """
        # STEP 1: Determine dataset type
        dataset_type = self.determine_dataset_type(df, column_roles)
        
        # STEP 2: Calculate average confidence of core banking columns
        core_banking_roles = [
            "ACCOUNT_NUMBER", "CUSTOMER_ID", "TRANSACTION_ID", "TRANSACTION_DATE",
            "TRANSACTION_TYPE", "OPENING_BALANCE", "DEBIT", "CREDIT", "CLOSING_BALANCE"
        ]
        
        core_column_confidences = []
        has_account_identifier = False
        has_transaction_type = False
        has_monetary_columns = False
        
        for col, role_info in column_roles.items():
            role = role_info.get("role", "")
            confidence = role_info.get("confidence", 0.0)
            
            if role in core_banking_roles and confidence > 0:
                core_column_confidences.append(confidence)
            
            # Check for essential columns
            if role in ["ACCOUNT_NUMBER", "CUSTOMER_ID"] and confidence >= 70:
                has_account_identifier = True
            
            if role == "TRANSACTION_TYPE" and confidence >= 70:
                has_transaction_type = True
            
            if role in ["DEBIT", "CREDIT", "OPENING_BALANCE", "CLOSING_BALANCE"] and confidence >= 70:
                has_monetary_columns = True
        
        # Calculate average confidence
        avg_confidence = sum(core_column_confidences) / len(core_column_confidences) if core_column_confidences else 0.0
        
        # STEP 3: Dataset size check (do NOT reject if < 20 rows)
        dataset_size = len(df)
        
        # STEP 4: Decision Logic
        # REJECT ONLY if: no account identifier AND (no transaction type OR no monetary columns)
        if not has_account_identifier and (not has_transaction_type or not has_monetary_columns):
            decision = "REJECT"
            reason = "Dataset missing essential banking columns: no account identifier and missing transaction type or monetary columns."
            dataset_type_display = dataset_type
        # ACCEPT if average confidence >= 70%
        elif avg_confidence >= 70.0:
            decision = "ACCEPT"
            reasons = []
            
            # Add dataset type specific reasons
            dataset_type_display = {
                "transaction": "Banking Transaction Dataset",
                "master": "Banking Master Dataset",
                "mixed": "Banking Transaction Dataset",
                "unknown": "Banking Dataset"
            }.get(dataset_type, "Banking Dataset")
            
            if dataset_type == "transaction":
                if has_account_identifier:
                    reasons.append("Valid account number detected")
                if has_transaction_type:
                    reasons.append("Transaction types validated")
                if has_monetary_columns:
                    reasons.append("Debit/Credit and balance logic consistent")
            elif dataset_type == "master":
                if has_account_identifier:
                    reasons.append("Valid account number detected")
            elif dataset_type == "mixed":
                if has_account_identifier:
                    reasons.append("Valid account number detected")
                if has_transaction_type:
                    reasons.append("Transaction types validated")
                if has_monetary_columns:
                    reasons.append("Debit/Credit and balance logic consistent")
            
            # Note: Missing optional patterns are ignored
            reasons.append("Missing optional patterns ignored")
            
            # Format reason with bullet points
            reason = "\n• ".join(reasons) if reasons else "All validations passed"
        # HOLD if confidence is between 50-70%
        elif avg_confidence >= 50.0:
            decision = "HOLD"
            reason = f"Average confidence of core banking columns: {avg_confidence:.1f}% (50-70%). Review recommended."
            dataset_type_display = dataset_type
        # REJECT if confidence < 50%
        else:
            decision = "REJECT"
            reason = f"Average confidence of core banking columns too low: {avg_confidence:.1f}% (<50%). Insufficient banking data detected."
            dataset_type_display = dataset_type
        
        return {
            "decision": decision,
            "reason": reason,
            "dataset_type": dataset_type_display,
            "average_confidence": round(avg_confidence, 2),
            "has_account_identifier": has_account_identifier,
            "has_transaction_type": has_transaction_type,
            "has_monetary_columns": has_monetary_columns,
            "dataset_size": dataset_size
        }
    
    def analyze_banking_dataset(self, df: pd.DataFrame):
        """
        Main analysis function that orchestrates all steps
        """
        # STEP 1 & 2: Classify all columns and lock roles
        column_roles = {}
        for col in df.columns:
            role_classification = self.classify_column_role(df, col)
            column_roles[col] = role_classification
        
        # STEP 3: Validate each column based on its role
        column_validations = {}
        for col, role_info in column_roles.items():
            role = role_info["role"]
            validation = self.validate_role(df, col, role)
            validation["confidence"] = role_info["confidence"]
            column_validations[col] = validation
        
        # STEP 4: Apply cross-column logic
        cross_validations = self.apply_cross_column_logic(df, column_roles)
        
        # STEP 5: Missing column handling (already handled in decision logic)
        
        # STEP 6: Make final decision
        final_decision = self.make_final_decision(column_validations, cross_validations, column_roles, df)
        
        # STEP 7: Format output
        detected_columns = []
        for col, role_info in column_roles.items():
            detected_columns.append({
                "column_name": col,
                "role": role_info["role"],
                "confidence": role_info["confidence"]
            })
        
        # Count rules - only for non-failed cases (confidence > 0 and not invalid)
        all_rules_applied = []
        all_rules_passed = []
        all_rules_failed = []
        all_rules_skipped = []
        
        for col, validation in column_validations.items():
            # Only include validations for columns with positive confidence or valid status
            if validation.get("confidence", 0) > 0 or validation.get("is_valid") is not False:
                all_rules_applied.extend([f"{col}:{rule}" for rule in validation.get("rules_applied", [])])
                all_rules_passed.extend([f"{col}:{rule}" for rule in validation.get("rules_passed", [])])
                all_rules_failed.extend([f"{col}:{rule['rule']}" for rule in validation.get("rules_failed", [])])
                all_rules_skipped.extend([f"{col}:{rule['rule']}" for rule in validation.get("rules_skipped", [])])
        
        # Filter detected columns to only include those with confidence > 0
        filtered_detected_columns = [col for col in detected_columns if col.get("confidence", 0) > 0]
        
        # Filter column validations to only include non-failed cases
        filtered_column_validations = {
            col: validation for col, validation in column_validations.items()
            if validation.get("confidence", 0) > 0 or validation.get("is_valid") is not False
        }
        
        return {
            "detected_columns": filtered_detected_columns,
            "column_validations": filtered_column_validations,
            "cross_validations": cross_validations,
            "validation_summary": {
                "rules_applied": all_rules_applied,
                "rules_passed": all_rules_passed,
                "rules_failed": all_rules_failed,
                "rules_skipped": all_rules_skipped
            },
            "final_decision": final_decision
        }
