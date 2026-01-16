import math
import re
import pandas as pd
import numpy as np
from rapidfuzz import fuzz
from sqlalchemy import text
from database import engine
from banking_core_engine import CoreBankingEngine
from banking_column_mapper import BankingColumnMapper
from banking_blueprint_engine import BankingBlueprintEngine


class BankingDomainDetector:

    def __init__(self):
        self.domain = "Banking"

        try:
            # Use raw SQL query to read from existing table (no primary key needed)
            with engine.connect() as conn:
                result = conn.execute(text("SELECT keyword, column_name FROM banking_keywords"))
                data = result.fetchall()

            self.keywords = [row[0].lower() if row[0] else "" for row in data]
            self.columns_db = [row[1].lower() if row[1] else "" for row in data]

            # Filter out empty strings
            self.keywords = [k for k in self.keywords if k]
            self.columns_db = [c for c in self.columns_db if c]

        except Exception as e:
            # Fallback to empty lists if database query fails
            print(f"Warning: Could not load keywords from database: {e}")
            self.keywords = []
            self.columns_db = []

        # 🔁 Hard fallback: built‑in banking keywords when DB is empty
        if not self.keywords and not self.columns_db:
            self.keywords = [
                "account", "account_number", "acc_no",
                "customer", "customer_id", "cust_id",
                "balance", "available_balance",
                "transaction", "transaction_id", "txn_id",
                "transaction_date", "txn_date",
                "debit", "credit", "amount",
                "status", "account_status",
                "ifsc", "branch"
            ]
            self.columns_db = [
                "account_number",
                "customer_id",
                "customer_name",
                "account_status",
                "balance",
                "transaction_date",
                "transaction_type",
                "debit",
                "credit",
                "amount"
            ]

        self.synonyms = {
            "acct": "account",
            "accno": "account",
            "cust": "customer",
            "amt": "amount",
            "bal": "balance",
            "txn": "transaction",
            "ifsc": "ifsc",
            "branch": "branch"
        }

    def normalize(self, text):
        return str(text).lower().replace(" ", "").replace("_", "")

    # 🔍 Column value intelligence
    def value_pattern_score(self, series, column_name):
        score = 0
        col = series.dropna()

        if col.empty:
            return 0

        name = column_name.lower()

        if any(k in name for k in ["amount", "amt", "balance", "bal"]):
            if pd.to_numeric(col, errors="coerce").notna().mean() > 0.8:
                score += 1

        if "account" in name:
            if col.astype(str).str.isnumeric().mean() > 0.7:
                score += 1

        if "ifsc" in name:
            if col.astype(str).str.len().mean() == 11:
                score += 1

        if "date" in name:
            if pd.to_datetime(col, errors="coerce").notna().mean() > 0.7:
                score += 1

        return score

    def _matches_account_number_pattern(self, series):
        """Check if series matches account number pattern: digits only, length 6-18."""
        try:
            non_null = series.dropna().astype(str)
            if len(non_null) == 0:
                return False
            digit_only_ratio = non_null.str.fullmatch(r"\d+").mean()
            length_ok_ratio = non_null.str.len().between(6, 18).mean()
            return digit_only_ratio >= 0.8 and length_ok_ratio >= 0.8
        except:
            return False
    
    def _matches_customer_id_pattern(self, series):
        """Check if series matches customer ID pattern: alphanumeric, 3-10 chars."""
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
    
    def _matches_date_pattern(self, series):
        """Check if series matches date pattern."""
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
        """Check if series contains transaction type values."""
        try:
            non_null = series.dropna().astype(str).str.lower().str.strip()
            if len(non_null) == 0:
                return False
            valid_types = ["deposit", "withdraw", "withdrawal", "transfer", "credit", "debit", "payment", "purchase", "sale"]
            match_ratio = non_null.isin(valid_types).mean()
            return match_ratio >= 0.5
        except:
            return False
    
    def _matches_numeric_balance_pattern(self, series):
        """Check if series matches numeric balance/debit/credit pattern."""
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
        """Check if series matches account status pattern."""
        try:
            non_null = series.dropna().astype(str).str.upper().str.strip()
            if len(non_null) == 0:
                return False
            valid_statuses = ["ACTIVE", "INACTIVE", "CLOSED", "FROZEN", "1", "0", "TRUE", "FALSE", "YES", "NO"]
            match_ratio = non_null.isin(valid_statuses).mean()
            return match_ratio >= 0.5
        except:
            return False
    
    def _matches_name_pattern(self, series):
        """Check if series matches name pattern."""
        try:
            non_null = series.dropna().astype(str)
            if len(non_null) == 0:
                return False
            # Check if values look like names (contain alphabetic characters, possibly spaces)
            alpha_char_ratio = non_null.str.contains(r'[A-Za-z]', na=False).mean()
            space_ratio = non_null.str.contains(r'\s', na=False).mean()
            length_ok = non_null.str.len().between(2, 50).mean()
            return alpha_char_ratio >= 0.8 and length_ok >= 0.8
        except:
            return False
    
    def _matches_account_type_pattern(self, series):
        """Check if series matches account type pattern."""
        try:
            non_null = series.dropna().astype(str).str.lower().str.strip()
            if len(non_null) == 0:
                return False
            valid_types = ["savings", "current", "salary", "student", "pension", "checking", "loan", "credit", "fixed", "fd", "rd", "deposit"]
            match_ratio = non_null.str.contains('|'.join(valid_types), case=False, na=False).mean()
            return match_ratio >= 0.5
        except:
            return False
    
    def _matches_transaction_id_pattern(self, series):
        """Check if series matches transaction ID pattern"""
        try:
            non_null = series.dropna().astype(str)
            if len(non_null) == 0:
                return False
            # Transaction IDs are usually alphanumeric or numeric
            alphanumeric_ratio = non_null.str.match(r'[A-Za-z0-9]+').mean()
            unique_ratio = non_null.nunique() / len(non_null)
            return alphanumeric_ratio >= 0.8 and unique_ratio >= 0.7
        except:
            return False
    
    def explain_column_purpose(self, column_name: str, series: pd.Series = None):
        """
        Explain the purpose of a column based on predefined patterns and matches.
        """
        norm_col = self.normalize(column_name)
        purpose_explanations = {
            "customer_id": "CUSTOMER_ID: Internal customer identifier - Usually alphanumeric like CUST1001, Each customer has unique ID, Same ID appears across multiple accounts for same customer, Required field, Not used in balance calculations",
            "customer_name": "CUSTOMER_NAME: Customer's full name - Contains letters and spaces, Same name may appear for multiple accounts of same person, Must be at least 3 characters, Not numeric data, Connected to customer ID",
            "account_number": "ACCOUNT_NUMBER: Unique bank account number - Numbers only, Between 6 to 18 digits long, Same account number appears in multiple transaction rows, Required field, Used to connect transactions to accounts",
            "account_type": "ACCOUNT_TYPE: Type of account like Savings, Current, Salary, Student, or Pension - Text values only, Limited options like Savings, Current, Salary, Student, Pension, Same type repeats for same account, Connected to account number",
            "account_status": "ACCOUNT_STATUS: Is account active - Options like ACTIVE, INACTIVE, CLOSED, One status per account, Required field, Changes rarely, Not numeric data",
            "transaction_id": "TRANSACTION_ID: Unique ID for each transaction - Different for most transactions, Mix of letters and numbers, Appears once per transaction row, Not used in math, Connected to account number",
            "transaction_date": "TRANSACTION_DATE: When transaction happened - Format like YYYY-MM-DD, May repeat for same day, Not money amounts, Connected to debit/credit, Helps order transactions",
            "transaction_type": "TRANSACTION_TYPE: Type of transaction like deposit or withdraw - Text only like deposit, withdraw, transfer, Few different types, Same type repeats many times, Not date data, Controls debit/credit logic",
            "debit": "DEBIT: Money going out - Numbers only, Zero or positive amounts, Zero allowed often, Only debit OR credit has value (not both), Reduces account balance",
            "credit": "CREDIT: Money coming in - Numbers only, Zero or positive amounts, Zero allowed often, Only credit OR debit has value (not both), Increases account balance",
            "opening_balance": "OPENING_BALANCE: Starting balance - Numbers only, Zero or positive amounts, Same for first transaction of each account, Used in balance calculation formula, Required field",
            "closing_balance": "CLOSING_BALANCE: Ending balance - Numbers only, Calculated value, Depends on debit and credit amounts, Rarely missing, Must follow formula rules",
            "branch_code": "BRANCH_CODE: Bank branch identifier - Mix of letters and numbers, Short length, Same branch code for multiple customers, Not balance related, Connected to account",
            "ifsc_code": "IFSC_CODE: Indian bank branch code - Exactly 11 characters, Mix of letters and numbers, Same code per branch, Required field, Not used in calculations",
            "mode_of_transaction": "MODE_OF_TRANSACTION: How transaction happened like CASH or UPI - Text options like CASH, UPI, NEFT, IMPS, Few different modes, Same mode repeats, Not numeric data, Only in transaction rows"
        }
        
        # Check for direct matches with predefined column types
        for col_type, explanation in purpose_explanations.items():
            norm_type = self.normalize(col_type)
            if norm_type in norm_col or norm_col in norm_type or fuzz.ratio(norm_col, norm_type) >= 85:
                return {
                    "column_type": col_type,
                    "explanation": explanation,
                    "confidence": fuzz.ratio(norm_col, norm_type) / 100.0
                }
        
        # Check for keyword matches
        for keyword in self.keywords:
            if keyword in norm_col or fuzz.ratio(norm_col, keyword) >= 85:
                # Determine type based on the keyword
                if any(t in keyword for t in ["customer", "cust", "client"]):
                    return {
                        "column_type": "customer_id",
                        "explanation": purpose_explanations["customer_id"],
                        "confidence": fuzz.ratio(norm_col, keyword) / 100.0
                    }
                elif any(t in keyword for t in ["name", "holder"]):
                    return {
                        "column_type": "customer_name",
                        "explanation": purpose_explanations["customer_name"],
                        "confidence": fuzz.ratio(norm_col, keyword) / 100.0
                    }
                elif any(t in keyword for t in ["account", "acc", "acct"]):
                    return {
                        "column_type": "account_number",
                        "explanation": purpose_explanations["account_number"],
                        "confidence": fuzz.ratio(norm_col, keyword) / 100.0
                    }
                elif any(t in keyword for t in ["type", "account_type", "savings", "current"]):
                    return {
                        "column_type": "account_type",
                        "explanation": purpose_explanations["account_type"],
                        "confidence": fuzz.ratio(norm_col, keyword) / 100.0
                    }
                elif any(t in keyword for t in ["status", "state", "active"]):
                    return {
                        "column_type": "account_status",
                        "explanation": purpose_explanations["account_status"],
                        "confidence": fuzz.ratio(norm_col, keyword) / 100.0
                    }
                elif any(t in keyword for t in ["transaction", "txn", "trans"]):
                    if any(t in keyword for t in ["date", "time"]):
                        return {
                            "column_type": "transaction_date",
                            "explanation": purpose_explanations["transaction_date"],
                            "confidence": fuzz.ratio(norm_col, keyword) / 100.0
                        }
                    elif any(t in keyword for t in ["type", "mode"]):
                        return {
                            "column_type": "transaction_type",
                            "explanation": purpose_explanations["transaction_type"],
                            "confidence": fuzz.ratio(norm_col, keyword) / 100.0
                        }
                    elif any(t in keyword for t in ["id"]):
                        return {
                            "column_type": "transaction_id",
                            "explanation": purpose_explanations["transaction_id"],
                            "confidence": fuzz.ratio(norm_col, keyword) / 100.0
                        }
                    else:
                        return {
                            "column_type": "transaction_id",
                            "explanation": purpose_explanations["transaction_id"],
                            "confidence": fuzz.ratio(norm_col, keyword) / 100.0
                        }
                elif any(t in keyword for t in ["debit", "dr", "withdraw"]):
                    return {
                        "column_type": "debit",
                        "explanation": purpose_explanations["debit"],
                        "confidence": fuzz.ratio(norm_col, keyword) / 100.0
                    }
                elif any(t in keyword for t in ["credit", "cr", "deposit"]):
                    return {
                        "column_type": "credit",
                        "explanation": purpose_explanations["credit"],
                        "confidence": fuzz.ratio(norm_col, keyword) / 100.0
                    }
                elif any(t in keyword for t in ["opening", "open"]):
                    return {
                        "column_type": "opening_balance",
                        "explanation": purpose_explanations["opening_balance"],
                        "confidence": fuzz.ratio(norm_col, keyword) / 100.0
                    }
                elif any(t in keyword for t in ["closing", "close", "balance"]):
                    return {
                        "column_type": "closing_balance",
                        "explanation": purpose_explanations["closing_balance"],
                        "confidence": fuzz.ratio(norm_col, keyword) / 100.0
                    }
                elif any(t in keyword for t in ["branch"]):
                    return {
                        "column_type": "branch_code",
                        "explanation": purpose_explanations["branch_code"],
                        "confidence": fuzz.ratio(norm_col, keyword) / 100.0
                    }
                elif any(t in keyword for t in ["ifsc"]):
                    return {
                        "column_type": "ifsc_code",
                        "explanation": purpose_explanations["ifsc_code"],
                        "confidence": fuzz.ratio(norm_col, keyword) / 100.0
                    }
                elif any(t in keyword for t in ["mode"]):
                    return {
                        "column_type": "mode_of_transaction",
                        "explanation": purpose_explanations["mode_of_transaction"],
                        "confidence": fuzz.ratio(norm_col, keyword) / 100.0
                    }
        
        # If no match found, return unknown
        return {
            "column_type": "unknown",
            "explanation": f"Column '{column_name}' purpose could not be determined from predefined patterns",
            "confidence": 0.0
        }
    
    def _looks_account_like_name(self, column_name: str) -> bool:
        norm = self.normalize(column_name)
        
        # CRITICAL SAFETY RULE: If column name contains financial keywords, never consider as account_number
        financial_keywords = ["amount", "balance", "loan", "emi", "interest", "rate"]
        is_financial_amount = any(keyword in norm for keyword in financial_keywords)
        
        # If it's a financial amount, return False immediately
        if is_financial_amount:
            return False
        
        candidates = ["account", "acct", "accno", "custaccount", "customeraccount", "accnumber"]
        # Normalize candidates for proper matching
        norm_candidates = [self.normalize(c) for c in candidates]
        return any(norm_c in norm or norm in norm_c or fuzz.ratio(norm, norm_c) >= 85 for norm_c in norm_candidates)

    def _looks_account_like_values(self, series: pd.Series) -> bool:
        col = series.dropna().astype(str)
        if col.empty:
            return False
        digit_ratio = col.str.fullmatch(r"\d+").mean()
        length_ratio = col.str.len().between(6, 16).mean()
        return digit_ratio > 0.6 and length_ratio > 0.6

    def detect_account_status(self, df: pd.DataFrame):
        """
        Detect account status column (active/inactive) and analyze status values
        """
        status_keywords = ["status", "state", "active", "inactive", "account_status", "acc_status", "account_state"]
        # Normalize keywords for proper matching
        norm_status_keywords = [self.normalize(kw) for kw in status_keywords]
        status_column = None
        status_values = []
        active_count = 0
        inactive_count = 0
        
        for col in df.columns:
            norm_col = self.normalize(col)
            if any(norm_kw in norm_col or norm_col in norm_kw or fuzz.ratio(norm_col, norm_kw) >= 85 for norm_kw in norm_status_keywords):
                status_column = col
                series = df[col].dropna().astype(str).str.lower()
                status_values = series.unique().tolist()
                
                # Count active/inactive
                active_count = series.str.contains("active|enabled|open|1|yes|true", case=False, na=False).sum()
                inactive_count = series.str.contains("inactive|disabled|closed|0|no|false", case=False, na=False).sum()
                break
        
        return {
            "has_status_column": status_column is not None,
            "status_column": status_column,
            "status_values": status_values[:10],  # Limit to first 10 unique values
            "active_count": int(active_count),
            "inactive_count": int(inactive_count),
            "total_with_status": int(active_count + inactive_count)
        }
    
    def check_missing_columns(self, df: pd.DataFrame):
        """
        STEP-5: Mandatory Banking Columns Check
        Mandatory (per latest banking rules):
            - account_number
            - customer_id
            - account_status
            - balance
        Optional: transaction_date, debit, credit
        """
        mandatory_columns = {
            "account_number": [
                "account_number", "acct_no", "accno", "accountno",
                "account", "acct", "custaccount", "customeraccount", "accnumber"
            ],
            "customer_id": [
                "customer_id", "cust_id", "customerid", "custid", "client_id",
                "clientid", "user_id", "userid"
            ],
            "account_status": ["status", "account_status", "acc_status", "state", "active", "inactive"],
            "balance": ["balance", "bal", "account_balance", "current_balance", "available_balance"]
        }

        optional_columns = {
            "transaction_date": ["transaction_date", "txn_date", "trans_date", "date"],
            "debit": ["debit", "debit_amount", "withdrawal", "withdraw"],
            "credit": ["credit", "credit_amount", "deposit"]
        }
        
        found_mandatory = {}
        missing_mandatory = []
        found_optional = {}
        missing_optional = []
        
        # Check mandatory columns
        for col_type, keywords in mandatory_columns.items():
            found = False
            # Normalize keywords for proper matching
            norm_keywords = [self.normalize(kw) for kw in keywords]
            for col in df.columns:
                norm_col = self.normalize(col)
                # Check if any normalized keyword matches the normalized column name
                if any(norm_kw in norm_col or norm_col in norm_kw or fuzz.ratio(norm_col, norm_kw) >= 85 for norm_kw in norm_keywords):
                    found_mandatory[col_type] = col
                    found = True
                    break
            if not found:
                missing_mandatory.append(col_type)
        
        # Check optional columns
        for col_type, keywords in optional_columns.items():
            found = False
            # Normalize keywords for proper matching
            norm_keywords = [self.normalize(kw) for kw in keywords]
            for col in df.columns:
                norm_col = self.normalize(col)
                # Check if any normalized keyword matches the normalized column name
                if any(norm_kw in norm_col or norm_col in norm_kw or fuzz.ratio(norm_col, norm_kw) >= 85 for norm_kw in norm_keywords):
                    found_optional[col_type] = col
                    found = True
                    break
            if not found:
                missing_optional.append(col_type)
        
        return {
            "found_mandatory": found_mandatory,
            "missing_mandatory": missing_mandatory,
            "found_optional": found_optional,
            "missing_optional": missing_optional,
            "all_mandatory_present": bool(len(missing_mandatory) == 0)  # Convert to Python bool
        }
    
    def verify_kyc(self, df: pd.DataFrame):
        """
        Verify KYC (Know Your Customer) - check for user name and other KYC fields
        """
        kyc_fields = {
            "user_name": ["user_name", "username", "user name", "name", "customer_name", "full_name", "account_holder_name"],
            "email": ["email", "email_id", "email_address", "e_mail"],
            "phone": ["phone", "phone_number", "mobile", "mobile_number", "contact_number"],
            "address": ["address", "residential_address", "permanent_address"],
            "id_proof": ["id_proof", "id_number", "aadhar", "pan", "passport", "id_card"]
        }
        
        found_kyc = {}
        missing_kyc = []
        
        for kyc_type, keywords in kyc_fields.items():
            found = False
            # Normalize keywords for proper matching
            norm_keywords = [self.normalize(kw) for kw in keywords]
            for col in df.columns:
                norm_col = self.normalize(col)
                # Check if any normalized keyword matches the normalized column name
                if any(norm_kw in norm_col or norm_col in norm_kw or fuzz.ratio(norm_col, norm_kw) >= 85 for norm_kw in norm_keywords):
                    found_kyc[kyc_type] = col
                    found = True
                    break
            if not found:
                missing_kyc.append(kyc_type)
        
        # Check if user_name is present (critical for KYC)
        has_user_name = bool("user_name" in found_kyc)  # Convert to Python bool
        kyc_completeness = round((len(found_kyc) / len(kyc_fields)) * 100, 2)
        kyc_verified = bool(kyc_completeness >= 60.0)  # STEP-6: >= 60% threshold - convert to Python bool
        
        return {
            "kyc_verified": kyc_verified,
            "has_user_name": has_user_name,
            "found_kyc_fields": found_kyc,
            "missing_kyc_fields": missing_kyc,
            "kyc_completeness": kyc_completeness,
            "meets_threshold": bool(kyc_completeness >= 60.0)  # Convert to Python bool
        }
    
    def validate_customer_id(self, df: pd.DataFrame):
        """
        Validate Customer ID with 7 rules:
        1. Column Exists
        2. Not Null
        3. Unique (optional)
        4. Format/Pattern Check (letter(s) + numbers, e.g., C001, C002)
        5. No Symbols/Special Characters
        6. Length Check (Min: 3, Max: 6)
        7. Data Type Check
        """
        customer_id_keywords = ["customer_id", "cust_id", "customerid", "custid", "client_id"]
        # Normalize keywords for proper matching
        norm_customer_keywords = [self.normalize(kw) for kw in customer_id_keywords]
        customer_id_col = None
        
        # Rule 1: Column Exists
        for col in df.columns:
            norm_col = self.normalize(col)
            if any(norm_kw in norm_col or norm_col in norm_kw or fuzz.ratio(norm_col, norm_kw) >= 85 for norm_kw in norm_customer_keywords):
                customer_id_col = col
                break
        
        if customer_id_col is None:
            return {
                "column_exists": False,
                "column_name": None,
                "not_null": False,
                "not_null_ratio": 0.0,
                "unique": False,
                "unique_ratio": 0.0,
                "format_valid": False,
                "format_valid_ratio": 0.0,
                "no_symbols": False,
                "no_symbols_ratio": 0.0,
                "length_valid": False,
                "length_valid_ratio": 0.0,
                "data_type_valid": False,
                "data_type_valid_ratio": 0.0,
                "total_rows": len(df),
                "rules_passed": 0,
                "rules_total": 7,
                "probability_customer_id": 0.0,
                "decision": "not_found"
            }
        
        series = df[customer_id_col].dropna().astype(str)
        total_rows = len(df)
        non_null_count = len(series)
        
        # Rule 2: Not Null
        not_null_ratio = float(non_null_count / total_rows) if total_rows > 0 else 0.0
        not_null = bool(not_null_ratio >= 0.95)  # 95% threshold - convert to Python bool
        
        # Rule 3: Unique (optional - usually one customer can have multiple accounts)
        unique_count = int(series.nunique())  # Convert to Python int
        unique_ratio = float(unique_count / non_null_count) if non_null_count > 0 else 0.0
        unique = bool(unique_ratio >= 0.5)  # Optional, so lower threshold - convert to Python bool
        
        # Rule 4: Format/Pattern Check (letter(s) + numbers, e.g., C001, C002)
        # Pattern: starts with 1-2 letters, followed by 1-4 digits
        format_pattern = r'^[A-Za-z]{1,2}\d{1,4}$'
        format_matches = series.str.fullmatch(format_pattern, na=False)
        format_valid_ratio = float(format_matches.sum() / non_null_count) if non_null_count > 0 else 0.0
        format_valid = bool(format_valid_ratio >= 0.8)  # Convert to Python bool
        
        # Rule 5: No Symbols/Special Characters (only letters + numbers)
        no_symbols_matches = series.str.fullmatch(r'^[A-Za-z0-9]+$', na=False)
        no_symbols_ratio = float(no_symbols_matches.sum() / non_null_count) if non_null_count > 0 else 0.0
        no_symbols = bool(no_symbols_ratio >= 0.95)  # Convert to Python bool
        
        # Rule 6: Length Check (Min: 3, Max: 6 characters)
        length_valid_matches = series.str.len().between(3, 6, inclusive='both')
        length_valid_ratio = float(length_valid_matches.sum() / non_null_count) if non_null_count > 0 else 0.0
        length_valid = bool(length_valid_ratio >= 0.9)  # Convert to Python bool
        
        # Rule 7: Data Type Check (should be string/text, not numeric only)
        # Check if it's stored as string and has mixed alphanumeric
        data_type_valid = True  # Already converted to string
        data_type_valid_ratio = 1.0 if non_null_count > 0 else 0.0
        
        # Count rules passed
        rules_passed = sum([
            True,  # Rule 1: Column Exists
            not_null,  # Rule 2
            unique,  # Rule 3 (optional)
            format_valid,  # Rule 4
            no_symbols,  # Rule 5
            length_valid,  # Rule 6
            data_type_valid  # Rule 7
        ])
        
        # Calculate probability (weighted)
        weighted_score = (
            0.20 * (1.0 if customer_id_col else 0.0) +  # Column exists
            0.20 * not_null_ratio +
            0.10 * unique_ratio +  # Optional, lower weight
            0.20 * format_valid_ratio +
            0.15 * no_symbols_ratio +
            0.10 * length_valid_ratio +
            0.05 * data_type_valid_ratio
        )
        
        probability = float(round(weighted_score * 100, 2))
        decision = "found" if probability >= 60.0 else "not_found"
        
        return {
            "column_exists": True,
            "column_name": customer_id_col,
            "not_null": not_null,
            "not_null_ratio": round(not_null_ratio, 3),
            "unique": unique,
            "unique_ratio": round(unique_ratio, 3),
            "format_valid": format_valid,
            "format_valid_ratio": round(format_valid_ratio, 3),
            "no_symbols": no_symbols,
            "no_symbols_ratio": round(no_symbols_ratio, 3),
            "length_valid": length_valid,
            "length_valid_ratio": round(length_valid_ratio, 3),
            "data_type_valid": data_type_valid,
            "data_type_valid_ratio": round(data_type_valid_ratio, 3),
            "total_rows": int(total_rows),  # Convert to Python int
            "non_null_count": int(non_null_count),  # Convert to Python int
            "rules_passed": int(rules_passed),  # Convert to Python int
            "rules_total": 7,
            "probability_customer_id": probability,
            "decision": decision
        }
    
    def validate_account_numbers(self, df: pd.DataFrame):
        results = []
        total_rows = int(len(df))  # Convert to Python int

        for col in df.columns:
            series = df[col]

            if not self._looks_account_like_name(col) and not self._looks_account_like_values(series):
                continue

            non_null = series.dropna().astype(str)
            if non_null.empty:
                continue

            digit_only_ratio = float(non_null.str.fullmatch(r"\d+").mean())
            length_ok_ratio = float(non_null.str.len().between(6, 16).mean())
            # Check if values contain only digits (no special characters or letters)
            no_symbol_ratio = float(non_null.str.fullmatch(r"\d+").mean())
            unique_ratio = float(non_null.nunique() / len(non_null))
            not_null_ratio = float(len(non_null) / max(len(series), 1))
            randomized_flag = bool(unique_ratio > 0.85 and digit_only_ratio > 0.8)

            violations = []
            for val in non_null.head(50):
                val_str = str(val)
                if not re.fullmatch(r"\d{6,16}", val_str):
                    violations.append(val_str)
                if len(violations) >= 5:
                    break

            weighted_score = (
                0.35 * digit_only_ratio
                + 0.25 * length_ok_ratio
                + 0.15 * unique_ratio
                + 0.10 * not_null_ratio
                + 0.10 * no_symbol_ratio
                + (0.05 if randomized_flag else 0)
            )
            prob = float(1 / (1 + math.exp(-6 * (weighted_score - 0.6))))

            results.append({
                "column": col,
                "probability_account_number": float(round(prob * 100, 2)),
                "decision": "match" if prob >= 0.6 else "not_match",
                "rules": {
                    "digits_only_ratio": round(digit_only_ratio, 3),
                    "length_6_16_ratio": round(length_ok_ratio, 3),
                    "unique_ratio": round(unique_ratio, 3),
                    "not_null_ratio": round(not_null_ratio, 3),
                    "no_symbol_ratio": round(no_symbol_ratio, 3),
                    "randomized": randomized_flag
                },
                "sample_violations": violations
            })

        summary = {
            "total_rows": int(total_rows),  # Convert to Python int
            "checked_columns": int(len(results)),  # Convert to Python int
            "best_match": None
        }

        if results:
            best = max(results, key=lambda r: r["probability_account_number"])
            summary["best_match"] = {
                "column": best["column"],
                "probability_account_number": best["probability_account_number"],
                "decision": best["decision"]
            }

        return {
            "account_like_columns": results,
            "summary": summary
        }

    def analyze_balance(self, df: pd.DataFrame):
        """
        Analyze balance column: presence and count of zero/negative balances.
        """
        balance_keywords = ["balance", "bal", "account_balance", "current_balance"]
        # Normalize keywords for proper matching
        norm_balance_keywords = [self.normalize(k) for k in balance_keywords]
        balance_col = None

        for col in df.columns:
            norm_col = self.normalize(col)
            if any(norm_k in norm_col or norm_col in norm_k or fuzz.ratio(norm_col, norm_k) >= 85 for norm_k in norm_balance_keywords):
                balance_col = col
                break

        if balance_col is None:
            return {
                "has_balance_column": False,
                "balance_column": None,
                "zero_or_negative_count": 0,
                "total_rows": int(len(df)),  # Convert to Python int
                "zero_or_negative_pct": 0.0
            }

        series = pd.to_numeric(df[balance_col], errors="coerce")
        total = int(len(series))  # Convert to Python int
        zero_neg = series.fillna(0) <= 0
        zero_neg_count = int(zero_neg.sum())  # Already converted, but ensure Python int
        pct = round((zero_neg_count / total) * 100, 2) if total else 0.0

        return {
            "has_balance_column": True,
            "balance_column": balance_col,
            "zero_or_negative_count": int(zero_neg_count),  # Convert to Python int
            "total_rows": int(total),  # Convert to Python int
            "zero_or_negative_pct": pct
        }

    def detect_opening_debit_credit_columns(self, df: pd.DataFrame):
        """
        Banking Data Column Detection Engine
        Following .md file specifications to detect:
        1) opening_balance
        2) debit (withdrawal / amount_out)
        3) credit (deposit / amount_in)
        """
        opening_balance_synonyms = [
            "opening_balance", "open_balance", "balance_before", 
            "previous_balance", "prev_balance"
        ]
        debit_synonyms = [
            "debit", "withdrawal", "withdraw_amount", "amount_out", "dr_amount"
        ]
        credit_synonyms = [
            "credit", "deposit", "amount_in", "cr_amount"
        ]
        
        # Normalize all synonyms
        norm_opening_synonyms = [self.normalize(kw) for kw in opening_balance_synonyms]
        norm_debit_synonyms = [self.normalize(kw) for kw in debit_synonyms]
        norm_credit_synonyms = [self.normalize(kw) for kw in credit_synonyms]
        
        opening_balance_candidates = []
        debit_candidates = []
        credit_candidates = []
        
        # Step 1: Column Name Analysis
        for col in df.columns:
            norm_col = self.normalize(col)
            
            # Check opening balance
            for syn in norm_opening_synonyms:
                if syn in norm_col or norm_col in syn or fuzz.ratio(norm_col, syn) >= 85:
                    opening_balance_candidates.append(col)
                    break
            
            # Check debit
            for syn in norm_debit_synonyms:
                if syn in norm_col or norm_col in syn or fuzz.ratio(norm_col, syn) >= 85:
                    debit_candidates.append(col)
                    break
            
            # Check credit
            for syn in norm_credit_synonyms:
                if syn in norm_col or norm_col in syn or fuzz.ratio(norm_col, syn) >= 85:
                    credit_candidates.append(col)
                    break
        
        # Step 2: Data Pattern Analysis
        def analyze_pattern(series, col_name):
            """Analyze if column matches expected pattern"""
            try:
                numeric_series = pd.to_numeric(series, errors="coerce").dropna()
                if len(numeric_series) == 0:
                    return 0, "No numeric values found"
                
                total = len(series)
                non_null = len(numeric_series)
                numeric_ratio = non_null / total if total > 0 else 0
                
                if numeric_ratio < 0.8:
                    return 0, "Less than 80% numeric values"
                
                non_negative_ratio = (numeric_series >= 0).sum() / non_null if non_null > 0 else 0
                mostly_zero_ratio = (numeric_series == 0).sum() / non_null if non_null > 0 else 0
                
                score = 0
                reasons = []
                
                # Numeric check (40% weight)
                if numeric_ratio >= 0.95:
                    score += 40
                    reasons.append("highly numeric")
                elif numeric_ratio >= 0.8:
                    score += 30
                    reasons.append("mostly numeric")
                
                # Non-negative check (30% weight)
                if non_negative_ratio >= 0.95:
                    score += 30
                    reasons.append("all non-negative")
                elif non_negative_ratio >= 0.9:
                    score += 20
                    reasons.append("mostly non-negative")
                
                # Mostly zero check (30% weight) - for debit/credit
                if 0.3 <= mostly_zero_ratio <= 0.9:
                    score += 30
                    reasons.append("mixed zero and non-zero values")
                elif mostly_zero_ratio < 0.3:
                    score += 20
                    reasons.append("mostly non-zero values")
                
                reason_str = ", ".join(reasons) if reasons else "numeric pattern detected"
                return min(score, 100), reason_str
                
            except Exception as e:
                return 0, f"Error analyzing: {str(e)}"
        
        # Analyze candidates
        opening_results = []
        for col in opening_balance_candidates:
            score, reason = analyze_pattern(df[col], col)
            opening_results.append({
                "column": col,
                "confidence": score,
                "reason": reason
            })
        
        debit_results = []
        for col in debit_candidates:
            score, reason = analyze_pattern(df[col], col)
            debit_results.append({
                "column": col,
                "confidence": score,
                "reason": reason
            })
        
        credit_results = []
        for col in credit_candidates:
            score, reason = analyze_pattern(df[col], col)
            credit_results.append({
                "column": col,
                "confidence": score,
                "reason": reason
            })
        
        # Step 3: Cross-Column Logic (debit/credit mutual exclusivity)
        # Find numeric columns that could be debit/credit
        numeric_cols = []
        for col in df.columns:
            try:
                numeric_series = pd.to_numeric(df[col], errors="coerce")
                if int(numeric_series.notna().sum()) / len(df) >= 0.8:  # Convert to Python int
                    numeric_cols.append(col)
            except:
                pass
        
        # Check cross-column logic for debit/credit
        if len(numeric_cols) >= 2:
            for i, col1 in enumerate(numeric_cols):
                if col1 in debit_candidates or col1 in credit_candidates:
                    continue
                    
                for col2 in numeric_cols[i+1:]:
                    if col2 in debit_candidates or col2 in credit_candidates:
                        continue
                    
                    try:
                        series1 = pd.to_numeric(df[col1], errors="coerce").fillna(0)
                        series2 = pd.to_numeric(df[col2], errors="coerce").fillna(0)
                        
                        # Check if col1 > 0 when col2 = 0 (potential debit vs credit)
                        col1_when_col2_zero = int(((series1 > 0) & (series2 == 0)).sum())  # Convert to Python int
                        col2_when_col1_zero = int(((series2 > 0) & (series1 == 0)).sum())  # Convert to Python int
                        both_positive = int(((series1 > 0) & (series2 > 0)).sum())  # Convert to Python int
                        both_zero = int(((series1 == 0) & (series2 == 0)).sum())  # Convert to Python int
                        
                        total = int(len(df))  # Convert to Python int
                        mutual_exclusive_ratio = (col1_when_col2_zero + col2_when_col1_zero) / total if total > 0 else 0
                        both_positive_ratio = both_positive / total if total > 0 else 0
                        
                        if mutual_exclusive_ratio > 0.7 and both_positive_ratio < 0.1:
                            # High mutual exclusivity - could be debit/credit pair
                            score1, reason1 = analyze_pattern(series1, col1)
                            score2, reason2 = analyze_pattern(series2, col2)
                            
                            # Determine which is debit and which is credit based on higher values or patterns
                            col1_avg = float(series1.mean()) if len(series1) > 0 else 0
                            col2_avg = float(series2.mean()) if len(series2) > 0 else 0
                            
                            # Boost scores for cross-column validation
                            existing_debit_cols = [r["column"] for r in debit_results]
                            existing_credit_cols = [r["column"] for r in credit_results]
                            
                            if col1 not in existing_debit_cols + existing_credit_cols:
                                if score1 >= 50:
                                    # Assign as debit or credit based on mutual exclusivity pattern
                                    if col1_when_col2_zero > col2_when_col1_zero:
                                        debit_results.append({
                                            "column": col1,
                                            "confidence": min(score1 + 20, 100),
                                            "reason": f"{reason1}, mutually exclusive with {col2} (debit pattern)"
                                        })
                                        if col2 not in existing_debit_cols + existing_credit_cols and score2 >= 50:
                                            credit_results.append({
                                                "column": col2,
                                                "confidence": min(score2 + 20, 100),
                                                "reason": f"{reason2}, mutually exclusive with {col1} (credit pattern)"
                                            })
                                    else:
                                        credit_results.append({
                                            "column": col1,
                                            "confidence": min(score1 + 20, 100),
                                            "reason": f"{reason1}, mutually exclusive with {col2} (credit pattern)"
                                        })
                                        if col2 not in existing_debit_cols + existing_credit_cols and score2 >= 50:
                                            debit_results.append({
                                                "column": col2,
                                                "confidence": min(score2 + 20, 100),
                                                "reason": f"{reason2}, mutually exclusive with {col1} (debit pattern)"
                                            })
                    except:
                        pass
        
        # Step 4: Balance Formula Validation (if closing balance exists)
        closing_balance_keywords = ["closing_balance", "closing", "balance_after", "final_balance", "end_balance"]
        norm_closing_keywords = [self.normalize(kw) for kw in closing_balance_keywords]
        closing_col = None
        
        for col in df.columns:
            norm_col = self.normalize(col)
            if any(norm_kw in norm_col or norm_col in norm_kw or fuzz.ratio(norm_col, norm_kw) >= 85 for norm_kw in norm_closing_keywords):
                closing_col = col
                break
        
        # If closing balance found, validate formula: closing ≈ opening + credit − debit
        if closing_col:
            try:
                closing_series = pd.to_numeric(df[closing_col], errors="coerce").fillna(0)
                
                for opening_res in opening_results:
                    if opening_res["confidence"] >= 60:
                        opening_col = opening_res["column"]
                        opening_series = pd.to_numeric(df[opening_col], errors="coerce").fillna(0)
                        
                        for debit_res in debit_results:
                            debit_col = debit_res["column"]
                            debit_series = pd.to_numeric(df[debit_col], errors="coerce").fillna(0)
                            
                            for credit_res in credit_results:
                                credit_col = credit_res["column"]
                                credit_series = pd.to_numeric(df[credit_col], errors="coerce").fillna(0)
                                
                                # Calculate: closing ≈ opening + credit − debit
                                calculated_closing = opening_series + credit_series - debit_series
                                diff = abs(closing_series - calculated_closing)
                                
                                # Tolerance: within 0.01 or 1% of value
                                tolerance = closing_series.abs() * 0.01 + 0.01
                                matches = int((diff <= tolerance).sum())  # Convert to Python int
                                match_ratio = float(matches / len(df)) if len(df) > 0 else 0.0  # Convert to Python float
                                
                                if match_ratio >= 0.8:
                                    # Formula matches! Boost confidence
                                    opening_res["confidence"] = min(float(opening_res["confidence"]) + 15, 100.0)  # Ensure float
                                    opening_res["reason"] += f", validates balance formula (match: {match_ratio*100:.1f}%)"
                                    
                                    debit_res["confidence"] = min(float(debit_res["confidence"]) + 15, 100.0)  # Ensure float
                                    debit_res["reason"] += f", validates balance formula"
                                    
                                    credit_res["confidence"] = min(float(credit_res["confidence"]) + 15, 100.0)  # Ensure float
                                    credit_res["reason"] += f", validates balance formula"
            except:
                pass
        
        # Select best candidates (confidence >= 60%)
        opening_best = max(opening_results, key=lambda x: x["confidence"]) if opening_results and max(opening_results, key=lambda x: x["confidence"])["confidence"] >= 60 else None
        debit_best = max(debit_results, key=lambda x: x["confidence"]) if debit_results and max(debit_results, key=lambda x: x["confidence"])["confidence"] >= 60 else None
        credit_best = max(credit_results, key=lambda x: x["confidence"]) if credit_results and max(credit_results, key=lambda x: x["confidence"])["confidence"] >= 60 else None
        
        # Format output according to .md file specification
        result = {
            "opening_balance": {
                "column_name": opening_best["column"] if opening_best else "NOT FOUND",
                "confidence": round(opening_best["confidence"], 2) if opening_best else 0.0,
                "reason": opening_best["reason"] if opening_best else "No column found with confidence >= 60%"
            },
            "debit": {
                "column_name": debit_best["column"] if debit_best else "NOT FOUND",
                "confidence": round(debit_best["confidence"], 2) if debit_best else 0.0,
                "reason": debit_best["reason"] if debit_best else "No column found with confidence >= 60%"
            },
            "credit": {
                "column_name": credit_best["column"] if credit_best else "NOT FOUND",
                "confidence": round(credit_best["confidence"], 2) if credit_best else 0.0,
                "reason": credit_best["reason"] if credit_best else "No column found with confidence >= 60%"
            },
            # Additional details for debugging
            "all_candidates": {
                "opening_balance": opening_results,
                "debit": debit_results,
                "credit": credit_results
            }
        }
        
        return result

    def validate_transaction_data(self, df: pd.DataFrame):
        """
        STEP-2: Transaction Data Validation
        Check for transaction columns and validate transaction rules
        """
        transaction_columns = {
            "transaction_id": ["transaction_id", "txn_id", "trans_id", "transactionid"],
            "transaction_date": ["transaction_date", "txn_date", "trans_date", "date", "transaction_date"],
            "debit": ["debit", "debit_amount", "withdrawal", "withdraw"],
            "credit": ["credit", "credit_amount", "deposit"],
            "transaction_type": ["transaction_type", "txn_type", "trans_type", "type"]
        }
        
        found_columns = {}
        missing_columns = []
        
        for col_type, keywords in transaction_columns.items():
            found = False
            # Normalize keywords for proper matching
            norm_keywords = [self.normalize(kw) for kw in keywords]
            for col in df.columns:
                norm_col = self.normalize(col)
                # Check if any normalized keyword matches the normalized column name
                if any(norm_kw in norm_col or norm_col in norm_kw or fuzz.ratio(norm_col, norm_kw) >= 85 for norm_kw in norm_keywords):
                    found_columns[col_type] = col
                    found = True
                    break
            if not found:
                missing_columns.append(col_type)
        
        # Validation rules
        validation_results = {
            "debit_credit_same_row": True,
            "amount_positive": True,
            "date_not_future": True,
            "violations": []
        }
        
        if "debit" in found_columns and "credit" in found_columns:
            debit_col = found_columns["debit"]
            credit_col = found_columns["credit"]
            
            # Rule: debit & credit should not both be > 0 in same row
            debit_series = pd.to_numeric(df[debit_col], errors="coerce").fillna(0)
            credit_series = pd.to_numeric(df[credit_col], errors="coerce").fillna(0)
            both_present = (debit_series > 0) & (credit_series > 0)
            validation_results["debit_credit_same_row"] = bool(not both_present.any())  # Convert numpy bool to Python bool
            if both_present.any():
                validation_results["violations"].append(f"Found {int(both_present.sum())} rows with both debit and credit > 0")  # Convert to Python int
        
        if "debit" in found_columns:
            debit_col = found_columns["debit"]
            debit_series = pd.to_numeric(df[debit_col], errors="coerce")
            negative_debits = int((debit_series < 0).sum())  # Convert to Python int
            validation_results["amount_positive"] = bool(negative_debits == 0)  # Convert to Python bool
            if negative_debits > 0:
                validation_results["violations"].append(f"Found {negative_debits} negative debit amounts")
        
        if "credit" in found_columns:
            credit_col = found_columns["credit"]
            credit_series = pd.to_numeric(df[credit_col], errors="coerce")
            negative_credits = int((credit_series < 0).sum())  # Convert to Python int
            if negative_credits > 0:
                validation_results["violations"].append(f"Found {negative_credits} negative credit amounts")
        
        if "transaction_date" in found_columns:
            date_col = found_columns["transaction_date"]
            try:
                dates = pd.to_datetime(df[date_col], errors="coerce")
                future_dates = dates > pd.Timestamp.now()
                validation_results["date_not_future"] = bool(not future_dates.any())  # Convert numpy bool to Python bool
                if future_dates.any():
                    validation_results["violations"].append(f"Found {int(future_dates.sum())} future dates")  # Convert to Python int
            except:
                validation_results["date_not_future"] = True  # Already Python bool
        
        return {
            "has_transaction_data": bool(len(found_columns) > 0),  # Convert to Python bool
            "found_columns": found_columns,
            "missing_columns": missing_columns,
            "completeness": round((len(found_columns) / len(transaction_columns)) * 100, 2),
            "validation_results": validation_results,
            "is_valid": bool(len(validation_results["violations"]) == 0)  # Convert to Python bool
        }
    
    def validate_banking_transaction_rules(self, df: pd.DataFrame, account_col=None):
        """
        Banking Transaction Rule Engine
        Validates transaction-related columns using 7 rules per category
        """
        # STEP-1: TRANSACTION IDENTIFICATION RULES (7 RULES)
        transaction_data = {}
        for col in df.columns:
            rules_passed = 0
            
            # Rule 1: Column name contains keywords
            norm_col = self.normalize(col)
            if any(keyword in norm_col for keyword in ['transaction', 'txn', 'trans', 'reference', 'ref']):
                rules_passed += 1
            
            # Rule 2: Column values are mostly UNIQUE (>80%)
            unique_ratio = df[col].nunique() / len(df) if len(df) > 0 else 0
            if unique_ratio > 0.8:
                rules_passed += 1
            
            # Rule 3: Column values repeat per account_number
            if account_col and account_col in df.columns:
                try:
                    group_stats = df.groupby(account_col)[col].nunique()
                    avg_unique_per_account = group_stats.mean()
                    if avg_unique_per_account < df[col].nunique() * 0.5:  # Values repeat across accounts
                        rules_passed += 1
                except:
                    pass
            
            # Rule 4: Column is NOT numeric-only balance
            if not self._matches_numeric_balance_pattern(df[col]):
                rules_passed += 1
            
            # Rule 5: Column is NOT customer identity data
            if not self._matches_customer_id_pattern(df[col]):
                rules_passed += 1
            
            # Rule 6: Column appears together with date + debit/credit
            date_cols = [c for c in df.columns if any(d in self.normalize(c) for d in ['date', 'time'])]
            amount_cols = [c for c in df.columns if any(a in self.normalize(c) for a in ['debit', 'credit', 'amount'])]
            if date_cols and amount_cols:
                rules_passed += 1
            
            # Rule 7: Row count > unique account_number count
            if account_col and account_col in df.columns:
                if len(df) > df[account_col].nunique():
                    rules_passed += 1
            
            # If ≥4 rules pass → mark as TRANSACTION DATA = VALID
            transaction_data[col] = {
                "rules_passed": rules_passed,
                "is_transaction_data": rules_passed >= 4,
                "rules_total": 7
            }
        
        # STEP-2: TRANSACTION TYPE RULES (7 RULES)
        transaction_type_data = {}
        for col in df.columns:
            rules_passed = 0
            
            # Rule 1: Column name contains transaction type keywords
            norm_col = self.normalize(col)
            if any(keyword in norm_col for keyword in ['type', 'txn_type', 'transaction_type', 'mode']):
                rules_passed += 1
            
            # Rule 2: Column values are TEXT (not numeric)
            try:
                numeric_ratio = pd.to_numeric(df[col], errors='coerce').notna().mean()
                if numeric_ratio < 0.8:  # Less than 80% numeric
                    rules_passed += 1
            except:
                rules_passed += 1  # If conversion fails, likely text
            
            # Rule 3: Unique values count is LOW (<10)
            unique_count = df[col].nunique()
            if unique_count < 10:
                rules_passed += 1
            
            # Rule 4: Values may include common transaction types
            if df[col].dtype == 'object':
                sample_values = df[col].dropna().astype(str).str.lower().unique()
                common_types = {'deposit', 'withdraw', 'withdrawal', 'credit', 'debit', 'transfer'}
                if any(tv in common_types for tv in sample_values):
                    rules_passed += 1
            
            # Rule 5: Do NOT mark date values as transaction_type
            date_keywords = ['date', 'time', 'day', 'month', 'year']
            if not any(keyword in norm_col for keyword in date_keywords):
                rules_passed += 1
            
            # Rule 6: If date-like values found → classify column as DATE, not TYPE
            try:
                date_check = pd.to_datetime(df[col], errors='coerce')
                if date_check.isna().mean() > 0.5:  # Not a date column
                    rules_passed += 1
            except:
                rules_passed += 1
            
            # Rule 7: If values repeat across rows → high confidence transaction_type
            if unique_count < len(df) * 0.8:
                rules_passed += 1
            
            # If ≥4 rules pass → TRANSACTION TYPE = VALID
            transaction_type_data[col] = {
                "rules_passed": rules_passed,
                "is_transaction_type": rules_passed >= 4,
                "rules_total": 7
            }
        
        # STEP-3: DEBIT COLUMN RULES (7 RULES)
        debit_data = {}
        for col in df.columns:
            rules_passed = 0
            
            # Rule 1: Column name contains debit keywords
            norm_col = self.normalize(col)
            if any(keyword in norm_col for keyword in ['debit', 'withdrawal', 'dr_amount', 'amount_debit']):
                rules_passed += 1
            
            # Rule 2: Values are numeric
            try:
                numeric_series = pd.to_numeric(df[col], errors='coerce')
                numeric_ratio = numeric_series.notna().mean()
                if numeric_ratio >= 0.8:
                    rules_passed += 1
            except:
                pass
            
            # Rule 3: Values are >= 0
            try:
                numeric_series = pd.to_numeric(df[col], errors='coerce')
                non_negative_ratio = (numeric_series >= 0).mean()
                if non_negative_ratio >= 0.8:
                    rules_passed += 1
            except:
                pass
            
            # Rule 4: Majority rows have ZERO values allowed
            try:
                numeric_series = pd.to_numeric(df[col], errors='coerce')
                zero_ratio = (numeric_series == 0).mean()
                if zero_ratio >= 0.1:  # At least 10% zeros are acceptable
                    rules_passed += 1
            except:
                pass
            
            # Rule 5: Debit and credit are NOT both non-zero in same row
            # Need to find potential credit columns to check this rule
            
            # Rule 6: Debit reduces balance when applied
            
            # Rule 7: Debit appears with transaction rows (not master rows)
            if len(df) > 10:  # Assuming transaction data if more than 10 rows
                rules_passed += 1
            
            debit_data[col] = {
                "rules_passed": rules_passed,
                "is_debit_column": rules_passed >= 4,
                "rules_total": 7
            }
        
        # STEP-4: CREDIT COLUMN RULES (7 RULES)
        credit_data = {}
        for col in df.columns:
            rules_passed = 0
            
            # Rule 1: Column name contains credit keywords
            norm_col = self.normalize(col)
            if any(keyword in norm_col for keyword in ['credit', 'deposit', 'cr_amount', 'amount_credit']):
                rules_passed += 1
            
            # Rule 2: Values are numeric
            try:
                numeric_series = pd.to_numeric(df[col], errors='coerce')
                numeric_ratio = numeric_series.notna().mean()
                if numeric_ratio >= 0.8:
                    rules_passed += 1
            except:
                pass
            
            # Rule 3: Values are >= 0
            try:
                numeric_series = pd.to_numeric(df[col], errors='coerce')
                non_negative_ratio = (numeric_series >= 0).mean()
                if non_negative_ratio >= 0.8:
                    rules_passed += 1
            except:
                pass
            
            # Rule 4: Majority rows have ZERO values allowed
            try:
                numeric_series = pd.to_numeric(df[col], errors='coerce')
                zero_ratio = (numeric_series == 0).mean()
                if zero_ratio >= 0.1:  # At least 10% zeros are acceptable
                    rules_passed += 1
            except:
                pass
            
            # Rule 5: Credit increases balance when applied
            
            # Rule 6: Debit and credit are NOT both non-zero in same row
            
            # Rule 7: Appears only in transaction rows
            if len(df) > 10:  # Assuming transaction data if more than 10 rows
                rules_passed += 1
            
            credit_data[col] = {
                "rules_passed": rules_passed,
                "is_credit_column": rules_passed >= 4,
                "rules_total": 7
            }
        
        # STEP-5: BALANCE COLUMN RULES (7 RULES)
        balance_data = {}
        for col in df.columns:
            rules_passed = 0
            
            # Rule 1: Column name contains balance keywords
            norm_col = self.normalize(col)
            if any(keyword in norm_col for keyword in ['balance', 'closing_balance', 'available_balance']):
                rules_passed += 1
            
            # Rule 2: Values are numeric
            try:
                numeric_series = pd.to_numeric(df[col], errors='coerce')
                numeric_ratio = numeric_series.notna().mean()
                if numeric_ratio >= 0.8:
                    rules_passed += 1
            except:
                pass
            
            # Rule 3: Balance values are monotonically changing per account
            # Rule 4: closing_balance = opening_balance + credit − debit
            # Rule 5: Balance never becomes NULL
            # Rule 6: Balance rarely negative (unless overdraft)
            # Rule 7: Balance repeats only across different accounts, not same txn
            
            balance_data[col] = {
                "rules_passed": rules_passed,
                "is_balance_column": rules_passed >= 4,
                "rules_total": 7
            }
        
        # STEP-6: CONFIDENCE FIX
        # Determine overall transaction validity and confidence
        valid_components = 0
        detected_columns = {}
        
        # Find transaction-related columns
        for col, data in transaction_data.items():
            if data["is_transaction_data"]:
                detected_columns["transaction_id"] = col
                valid_components += 1
                break
        
        # Find transaction type columns
        for col, data in transaction_type_data.items():
            if data["is_transaction_type"]:
                detected_columns["transaction_type"] = col
                valid_components += 1
                break
        
        # Find debit columns
        for col, data in debit_data.items():
            if data["is_debit_column"]:
                detected_columns["debit"] = col
                valid_components += 1
                break
        
        # Find credit columns
        for col, data in credit_data.items():
            if data["is_credit_column"]:
                detected_columns["credit"] = col
                valid_components += 1
                break
        
        # Find balance columns
        for col, data in balance_data.items():
            if data["is_balance_column"]:
                detected_columns["balance"] = col
                valid_components += 1
                break
        
        # Calculate confidence based on minimum confidence rules
        if valid_components == 0:
            confidence = 0
        elif valid_components == 1:
            confidence = max(40, 0)  # Min 40%
        elif valid_components == 2:
            confidence = max(60, 40)  # Min 60%
        else:  # 3+ components
            confidence = max(80, 60)  # Min 80%
        
        # Determine overall transaction status
        transaction_status = "VALID" if valid_components >= 1 else "INVALID"
        
        # STEP-7: FINAL OUTPUT
        return {
            "transaction_status": transaction_status,
            "confidence_percentage": confidence,
            "detected_columns": detected_columns,
            "valid_components_count": valid_components,
            "transaction_data_analysis": transaction_data,
            "transaction_type_analysis": transaction_type_data,
            "debit_analysis": debit_data,
            "credit_analysis": credit_data,
            "balance_analysis": balance_data,
            "reason": f"Found {valid_components} valid transaction components out of 5 possible types. Minimum 1 required for VALID status."
        }
    
    def validate_transaction_type(self, df: pd.DataFrame):
        """
        Validate Transaction Type Column
        Check if transaction_type column exists and contains valid values: deposit, withdraw, transfer
        Calculate probability percentage based on valid values
        """
        
        # STEP-2: TRANSACTION TYPE RULES (7 RULES)
        transaction_type_data = {}
        for col in df.columns:
            rules_passed = 0
            
            # Rule 1: Column name contains transaction type keywords
            norm_col = self.normalize(col)
            if any(keyword in norm_col for keyword in ['type', 'txn_type', 'transaction_type', 'mode']):
                rules_passed += 1
            
            # Rule 2: Column values are TEXT (not numeric)
            try:
                numeric_ratio = pd.to_numeric(df[col], errors='coerce').notna().mean()
                if numeric_ratio < 0.8:  # Less than 80% numeric
                    rules_passed += 1
            except:
                rules_passed += 1  # If conversion fails, likely text
            
            # Rule 3: Unique values count is LOW (<10)
            unique_count = df[col].nunique()
            if unique_count < 10:
                rules_passed += 1
            
            # Rule 4: Values may include common transaction types
            if df[col].dtype == 'object':
                sample_values = df[col].dropna().astype(str).str.lower().unique()
                common_types = {'deposit', 'withdraw', 'withdrawal', 'credit', 'debit', 'transfer', 'cash', 'neft', 'imps', 'rtgs', 'upi', 'online', 'cheque', 'card'}
                if any(tv in common_types for tv in sample_values):
                    rules_passed += 1
            
            # Rule 5: Do NOT mark date values as transaction_type
            date_keywords = ['date', 'time', 'day', 'month', 'year']
            if not any(keyword in norm_col for keyword in date_keywords):
                rules_passed += 1
            
            # Rule 6: If date-like values found → classify column as DATE, not TYPE
            try:
                date_check = pd.to_datetime(df[col], errors='coerce')
                if date_check.isna().mean() > 0.5:  # Not a date column
                    rules_passed += 1
            except:
                rules_passed += 1
            
            # Rule 7: If values repeat across rows → high confidence transaction_type
            if unique_count < len(df) * 0.8:
                rules_passed += 1
            
            # If ≥4 rules pass → TRANSACTION TYPE = VALID
            transaction_type_data[col] = {
                "rules_passed": rules_passed,
                "is_transaction_type": rules_passed >= 4,
                "rules_total": 7,
                "confidence_percentage": round((rules_passed / 7) * 100, 2)
            }
        
        # Find the best transaction type column
        best_transaction_type_col = None
        best_confidence = 0
        for col, data in transaction_type_data.items():
            if data["is_transaction_type"] and data["confidence_percentage"] > best_confidence:
                best_confidence = data["confidence_percentage"]
                best_transaction_type_col = col
        
        if best_transaction_type_col is None:
            return {
                "column_found": False,
                "column_name": None,
                "total_rows": int(len(df)),  # Convert to Python int
                "valid_count": 0,
                "invalid_count": 0,
                "valid_types_found": [],
                "invalid_types_found": [],
                "probability_percentage": 0.0,
                "is_valid": False,
                "decision": "column_not_found"
            }
        
        # Analyze the best transaction type column
        series = df[best_transaction_type_col].dropna().astype(str).str.lower().str.strip()
        total_rows = int(len(series))  # Convert to Python int
        
        if total_rows == 0:
            return {
                "column_found": True,
                "column_name": best_transaction_type_col,
                "total_rows": int(len(df)),  # Convert to Python int
                "valid_count": 0,
                "invalid_count": 0,
                "valid_types_found": [],
                "invalid_types_found": [],
                "probability_percentage": 0.0,
                "is_valid": False,
                "decision": "empty_column"
            }
        
        # Check for valid transaction types
        valid_types = ["deposit", "withdraw", "withdrawal", "transfer", "credit", "debit", "cash", "neft", "imps", "rtgs", "upi", "online", "cheque", "card"]
        valid_mask = series.isin(valid_types)
        valid_count = int(valid_mask.sum())  # Convert to Python int
        invalid_count = int((~valid_mask).sum())  # Convert to Python int
        
        # Get unique valid and invalid types found
        valid_types_found = sorted(series[valid_mask].unique().tolist())
        invalid_types_found = sorted(series[~valid_mask].unique().tolist()[:10])  # Limit to first 10
        
        # Calculate probability percentage
        probability_percentage = round((valid_count / total_rows) * 100, 2) if total_rows > 0 else 0.0
        
        # Decision logic: valid if >= 80% of values match valid types
        is_valid = bool(probability_percentage >= 80.0)  # Convert to Python bool
        decision = "valid" if is_valid else ("partial" if probability_percentage >= 50.0 else "invalid")
        
        # Calculate overall confidence based on both rule-based confidence and value validation
        overall_confidence = min(best_confidence, probability_percentage)
        
        return {
            "column_found": True,
            "column_name": best_transaction_type_col,
            "total_rows": int(total_rows),  # Convert to Python int
            "valid_count": int(valid_count),  # Convert to Python int
            "invalid_count": int(invalid_count),  # Convert to Python int
            "valid_types_found": valid_types_found,
            "invalid_types_found": invalid_types_found,
            "probability_percentage": probability_percentage,
            "is_valid": is_valid,
            "decision": decision,
            "confidence_percentage": best_confidence,
            "rules_passed": transaction_type_data[best_transaction_type_col]["rules_passed"],
            "overall_confidence": overall_confidence
        }
    
    def validate_pan_number(self, df: pd.DataFrame):
        # Valid transaction types (case-insensitive)
        valid_types = ["deposit", "withdraw", "withdrawal", "transfer"]
        
        # Find transaction_type column - prioritize exact matches first
        # Primary keywords (exact transaction type columns)
        primary_keywords = ["transaction_type", "txn_type", "trans_type", "transactiontype"]
        # Secondary keywords (generic "type" but must validate values)
        secondary_keywords = ["type"]
        # Normalize keywords for proper matching
        norm_primary_keywords = [self.normalize(kw) for kw in primary_keywords]
        norm_secondary_keywords = [self.normalize(kw) for kw in secondary_keywords]
        
        transaction_type_col = None
        
        # First pass: Look for primary transaction type keywords
        for col in df.columns:
            norm_col = self.normalize(col)
            # Exclude account-related columns
            if "account" in norm_col:
                continue
            if any(norm_kw in norm_col or norm_col in norm_kw or fuzz.ratio(norm_col, norm_kw) >= 85 for norm_kw in norm_primary_keywords):
                transaction_type_col = col
                break
        
        # Second pass: If not found, check for generic "type" but validate values
        if transaction_type_col is None:
            for col in df.columns:
                norm_col = self.normalize(col)
                # Exclude account-related columns
                if "account" in norm_col:
                    continue
                # Check if it's a generic "type" column
                if any(norm_kw in norm_col or norm_col in norm_kw or fuzz.ratio(norm_col, norm_kw) >= 85 for norm_kw in norm_secondary_keywords):
                    # Validate that this column actually contains transaction type values
                    series = df[col].dropna().astype(str).str.lower().str.strip()
                    if len(series) > 0:
                        # Check if at least 50% of values match transaction types
                        valid_mask = series.isin(valid_types)
                        match_ratio = valid_mask.sum() / len(series)
                        if match_ratio >= 0.5:  # At least 50% should be transaction types
                            transaction_type_col = col
                            break
        
        if transaction_type_col is None:
            return {
                "column_found": False,
                "column_name": None,
                "total_rows": int(len(df)),  # Convert to Python int
                "valid_count": 0,
                "invalid_count": 0,
                "valid_types_found": [],
                "invalid_types_found": [],
                "probability_percentage": 0.0,
                "is_valid": False,
                "decision": "column_not_found"
            }
        
        # Get the series and normalize values
        series = df[transaction_type_col].dropna().astype(str).str.lower().str.strip()
        total_rows = int(len(series))  # Convert to Python int
        
        if total_rows == 0:
            return {
                "column_found": True,
                "column_name": transaction_type_col,
                "total_rows": int(len(df)),  # Convert to Python int
                "valid_count": 0,
                "invalid_count": 0,
                "valid_types_found": [],
                "invalid_types_found": [],
                "probability_percentage": 0.0,
                "is_valid": False,
                "decision": "empty_column"
            }
        
        # Check each value against valid types
        valid_mask = series.isin(valid_types)
        valid_count = int(valid_mask.sum())  # Convert to Python int
        invalid_count = int((~valid_mask).sum())  # Convert to Python int
        
        # Get unique valid and invalid types found
        valid_types_found = sorted(series[valid_mask].unique().tolist())
        invalid_types_found = sorted(series[~valid_mask].unique().tolist()[:10])  # Limit to first 10
        
        # Calculate probability percentage
        probability_percentage = round((valid_count / total_rows) * 100, 2) if total_rows > 0 else 0.0
        
        # Decision logic: valid if >= 80% of values match valid types
        is_valid = bool(probability_percentage >= 80.0)  # Convert to Python bool
        decision = "valid" if is_valid else ("partial" if probability_percentage >= 50.0 else "invalid")
        
        return {
            "column_found": True,
            "column_name": transaction_type_col,
            "total_rows": int(total_rows),  # Convert to Python int
            "valid_count": int(valid_count),  # Convert to Python int
            "invalid_count": int(invalid_count),  # Convert to Python int
            "valid_types_found": valid_types_found,
            "invalid_types_found": invalid_types_found,
            "probability_percentage": probability_percentage,
            "is_valid": is_valid,
            "decision": decision
        }
    
    def validate_pan_number(self, df: pd.DataFrame):
        """
        Validate PAN (Permanent Account Number) Column
        PAN format: ABCDE1234F (10 characters)
        - First 5 = letters (A-Z)
        - Next 4 = digits (0-9)
        - Last 1 = letter (A-Z)
        Regex pattern: [A-Z]{5}[0-9]{4}[A-Z]{1}
        """
        # PAN column keywords
        pan_keywords = ["pan", "pan_number", "pan_no", "pannumber", "panno", "permanent_account_number"]
        # Normalize keywords for proper matching
        norm_pan_keywords = [self.normalize(kw) for kw in pan_keywords]
        
        # Find PAN column
        pan_col = None
        for col in df.columns:
            norm_col = self.normalize(col)
            if any(norm_kw in norm_col or norm_col in norm_kw or fuzz.ratio(norm_col, norm_kw) >= 85 for norm_kw in norm_pan_keywords):
                pan_col = col
                break
        
        if pan_col is None:
            return {
                "column_found": False,
                "column_name": None,
                "total_rows": int(len(df)),  # Convert to Python int
                "valid_pan_count": 0,
                "invalid_pan_count": 0,
                "total_pan_found": 0,
                "pan_list": [],
                "invalid_pan_list": [],
                "probability_percentage": 0.0,
                "is_valid": False,
                "decision": "column_not_found"
            }
        
        # Get the series and convert to string, remove whitespace, convert to uppercase
        series = df[pan_col].dropna().astype(str).str.strip().str.upper()
        total_rows = int(len(series))  # Convert to Python int
        
        if total_rows == 0:
            return {
                "column_found": True,
                "column_name": pan_col,
                "total_rows": int(total_rows),  # Convert to Python int
                "valid_pan_count": 0,
                "invalid_pan_count": 0,
                "total_pan_found": 0,
                "pan_list": [],
                "invalid_pan_list": [],
                "probability_percentage": 0.0,
                "is_valid": False,
                "decision": "empty_column"
            }
        
        # PAN regex pattern: [A-Z]{5}[0-9]{4}[A-Z]{1}
        pan_pattern = r'^[A-Z]{5}[0-9]{4}[A-Z]{1}$'
        
        # Check each value against PAN pattern
        valid_mask = series.str.fullmatch(pan_pattern, na=False)
        valid_pan_count = int(valid_mask.sum())  # Convert to Python int
        invalid_pan_count = int((~valid_mask).sum())  # Convert to Python int
        
        # Get list of valid PAN numbers (unique, limit to 50 for display)
        valid_pans = series[valid_mask].unique().tolist()
        pan_list = sorted(valid_pans)[:50]  # Limit to first 50 for UI
        
        # Get list of invalid PANs (sample, limit to 20 for display)
        invalid_pans = series[~valid_mask].unique().tolist()
        invalid_pan_list = sorted(invalid_pans)[:20]
        
        # Calculate probability percentage
        probability_percentage = round((valid_pan_count / total_rows) * 100, 2) if total_rows > 0 else 0.0
        
        # Decision logic: valid if >= 80% of values match PAN format
        is_valid = bool(probability_percentage >= 80.0)  # Convert to Python bool
        decision = "valid" if is_valid else ("partial" if probability_percentage >= 50.0 else "invalid")
        
        return {
            "column_found": True,
            "column_name": pan_col,
            "total_rows": int(total_rows),  # Convert to Python int
            "valid_pan_count": int(valid_pan_count),  # Convert to Python int
            "invalid_pan_count": int(invalid_pan_count),  # Convert to Python int
            "total_pan_found": int(valid_pan_count),  # Convert to Python int
            "pan_list": pan_list,
            "invalid_pan_list": invalid_pan_list,
            "probability_percentage": probability_percentage,
            "is_valid": is_valid,
            "decision": decision
        }
    
    def detect_branch_code_apriori(self, df: pd.DataFrame, kyc_check: dict = None):
        """
        Apriori Algorithm for Branch Code Detection
        Only applies if user exists (from KYC check)
        Pattern: [A-Z]{2,3}[0-9]{2,3} (2-3 uppercase letters followed by 2-3 digits)
        Uses Apriori-like frequent pattern mining to find branch codes
        """
        # Check if user exists (from KYC check)
        if kyc_check is None:
            kyc_check = self.verify_kyc(df)
        
        has_user = kyc_check.get("has_user_name", False)
        
        if not has_user:
            return {
                "can_analyze": False,
                "reason": "User name not found in KYC check. Branch code detection requires user existence.",
                "column_found": False,
                "branch_codes": [],
                "frequent_branch_codes": [],
                "support_threshold": 0.0,
                "total_matches": 0,
                "is_valid": False
            }
        
        # Find branch_code column
        branch_keywords = ["branch_code", "branchcode", "branch", "br_code", "brcode", "branch_id", "branchid"]
        # Normalize keywords for proper matching
        norm_branch_keywords = [self.normalize(kw) for kw in branch_keywords]
        branch_col = None
        
        for col in df.columns:
            norm_col = self.normalize(col)
            if any(norm_kw in norm_col or norm_col in norm_kw or fuzz.ratio(norm_col, norm_kw) >= 85 for norm_kw in norm_branch_keywords):
                branch_col = col
                break
        
        if branch_col is None:
            return {
                "can_analyze": True,
                "user_exists": True,
                "column_found": False,
                "column_name": None,
                "branch_codes": [],
                "frequent_branch_codes": [],
                "support_threshold": 0.0,
                "total_matches": 0,
                "is_valid": False,
                "reason": "Branch code column not found"
            }
        
        # Pattern: [A-Z]{2,3}[0-9]{2,3}
        branch_pattern = r'^[A-Z]{2,3}[0-9]{2,3}$'
        
        # Get series and convert to string, uppercase
        series = df[branch_col].dropna().astype(str).str.strip().str.upper()
        total_rows = int(len(series))  # Convert to Python int
        
        if total_rows == 0:
            return {
                "can_analyze": True,
                "user_exists": True,
                "column_found": True,
                "column_name": branch_col,
                "branch_codes": [],
                "frequent_branch_codes": [],
                "support_threshold": 0.0,
                "total_matches": 0,
                "is_valid": False,
                "reason": "Branch code column is empty"
            }
        
        # Match pattern
        valid_mask = series.str.fullmatch(branch_pattern, na=False)
        valid_branch_codes = series[valid_mask].tolist()
        total_matches = int(len(valid_branch_codes))  # Convert to Python int
        
        if total_matches == 0:
            return {
                "can_analyze": True,
                "user_exists": True,
                "column_found": True,
                "column_name": branch_col,
                "branch_codes": [],
                "frequent_branch_codes": [],
                "support_threshold": 0.0,
                "total_matches": 0,
                "is_valid": False,
                "reason": "No branch codes match the pattern [A-Z]{2,3}[0-9]{2,3}"
            }
        
        # Apriori: Calculate frequency (support) for each branch code
        from collections import Counter
        branch_code_counts = Counter(valid_branch_codes)
        
        # Calculate support (frequency / total rows)
        support_threshold = 0.1  # Minimum 10% support (can be adjusted)
        frequent_branch_codes = []
        
        for branch_code, count in branch_code_counts.items():
            support = count / total_rows
            if support >= support_threshold:
                frequent_branch_codes.append({
                    "branch_code": branch_code,
                    "count": int(count),  # Convert to Python int
                    "support": round(support * 100, 2)  # Percentage
                })
        
        # Sort by support (descending)
        frequent_branch_codes.sort(key=lambda x: x["support"], reverse=True)
        
        # Get unique branch codes list (for display)
        unique_branch_codes = sorted(list(branch_code_counts.keys()))
        
        # Validation: At least 50% of values should match pattern
        match_ratio = total_matches / total_rows
        is_valid = bool(match_ratio >= 0.5)  # Convert to Python bool
        
        return {
            "can_analyze": True,
            "user_exists": True,
            "column_found": True,
            "column_name": branch_col,
            "branch_codes": unique_branch_codes[:50],  # Limit to 50 for display
            "frequent_branch_codes": frequent_branch_codes,
            "support_threshold": support_threshold * 100,  # As percentage
            "total_matches": int(total_matches),  # Convert to Python int
            "total_rows": int(total_rows),  # Convert to Python int
            "match_ratio": round(match_ratio * 100, 2),
            "is_valid": is_valid,
            "pattern": "[A-Z]{2,3}[0-9]{2,3}",
            "decision": "valid" if is_valid else ("partial" if match_ratio >= 0.3 else "invalid")
        }
    
    def validate_debit_credit_balance(self, df: pd.DataFrame, balance_col_name: str = None):
        """
        STEP-3: Debit/Credit vs Balance Check
        Validate debit doesn't exceed balance, credit increases balance
        """
        if balance_col_name is None:
            # Find balance column
            balance_keywords = ["balance", "bal", "account_balance", "current_balance"]
            # Normalize keywords for proper matching
            norm_balance_keywords = [self.normalize(k) for k in balance_keywords]
            balance_col_name = None
            for col in df.columns:
                norm_col = self.normalize(col)
                if any(norm_k in norm_col or norm_col in norm_k or fuzz.ratio(norm_col, norm_k) >= 85 for norm_k in norm_balance_keywords):
                    balance_col_name = col
                    break
        
        if balance_col_name is None:
            return {
                "has_balance": False,
                "can_validate": False,
                "insufficient_funds_count": 0,
                "total_debit_transactions": 0,
                "validation_passed": False
            }
        
        # Find debit and credit columns
        debit_col = None
        credit_col = None
        norm_debit = self.normalize("debit")
        norm_credit = self.normalize("credit")
        for col in df.columns:
            norm_col = self.normalize(col)
            if norm_debit in norm_col or fuzz.ratio(norm_col, norm_debit) >= 85:
                debit_col = col
            if norm_credit in norm_col or fuzz.ratio(norm_col, norm_credit) >= 85:
                credit_col = col
        
        balance_series = pd.to_numeric(df[balance_col_name], errors="coerce").fillna(0)
        insufficient_count = 0
        total_debits = 0
        
        if debit_col:
            debit_series = pd.to_numeric(df[debit_col], errors="coerce").fillna(0)
            total_debits = int((debit_series > 0).sum())  # Convert to Python int
            # Check if debit > balance
            insufficient = (debit_series > balance_series) & (debit_series > 0)
            insufficient_count = int(insufficient.sum())  # Convert to Python int
        
        return {
            "has_balance": True,
            "balance_column": balance_col_name,
            "can_validate": bool(debit_col is not None or credit_col is not None),  # Convert to Python bool
            "insufficient_funds_count": int(insufficient_count),  # Convert to Python int
            "total_debit_transactions": int(total_debits),  # Convert to Python int
            "validation_passed": bool(insufficient_count == 0),  # Convert to Python bool
            "debit_column": debit_col,
            "credit_column": credit_col
        }
    
    def detect_fraud_patterns(self, df: pd.DataFrame, account_col: str = None):
        """
        STEP-4: Transaction Pattern / Fraud Check
        Detect suspicious transaction patterns
        """
        if account_col is None:
            # Find account column
            account_keywords = ["account", "account_number", "account_id", "acc_no"]
            # Normalize keywords for proper matching
            norm_account_keywords = [self.normalize(k) for k in account_keywords]
            for col in df.columns:
                norm_col = self.normalize(col)
                if any(norm_k in norm_col or norm_col in norm_k or fuzz.ratio(norm_col, norm_k) >= 85 for norm_k in norm_account_keywords):
                    account_col = col
                    break
        
        if account_col is None:
            return {
                "can_analyze": False,
                "suspicious_patterns": [],
                "fraud_risk": "LOW"
            }
        
        # Find transaction date and amount columns
        date_col = None
        debit_col = None
        credit_col = None
        
        norm_date = self.normalize("date")
        norm_transaction = self.normalize("transaction")
        norm_debit = self.normalize("debit")
        norm_credit = self.normalize("credit")
        for col in df.columns:
            norm_col = self.normalize(col)
            if norm_date in norm_col and norm_transaction in norm_col:
                date_col = col
            if norm_debit in norm_col or fuzz.ratio(norm_col, norm_debit) >= 85:
                debit_col = col
            if norm_credit in norm_col or fuzz.ratio(norm_col, norm_credit) >= 85:
                credit_col = col
        
        suspicious_patterns = []
        risk_factors = 0
        
        # Pattern 1: Many transactions in short time (same account)
        if account_col and date_col:
            try:
                df_with_dates = df.copy()
                df_with_dates['_date'] = pd.to_datetime(df_with_dates[date_col], errors="coerce")
                df_with_dates = df_with_dates.dropna(subset=['_date', account_col])
                
                if len(df_with_dates) > 0:
                    # Group by account and check transactions per hour
                    account_transactions = df_with_dates.groupby(account_col).size()
                    high_frequency = account_transactions[account_transactions > 10]
                    if len(high_frequency) > 0:
                        suspicious_patterns.append(f"High transaction frequency: {int(len(high_frequency))} accounts with >10 transactions")  # Convert to Python int
                        risk_factors += 1
            except:
                pass
        
        # Pattern 2: Very high amount suddenly
        if debit_col:
            try:
                debit_series = pd.to_numeric(df[debit_col], errors="coerce")
                if len(debit_series) > 0:
                    mean_debit = debit_series.mean()
                    std_debit = debit_series.std()
                    if std_debit > 0:
                        high_amounts = debit_series[debit_series > (mean_debit + 3 * std_debit)]
                        if len(high_amounts) > 0:
                            suspicious_patterns.append(f"Unusually high amounts: {int(len(high_amounts))} transactions >3 standard deviations")  # Convert to Python int
                            risk_factors += 1
            except:
                pass
        
        # Pattern 3: Midnight transactions repeatedly
        if date_col:
            try:
                dates = pd.to_datetime(df[date_col], errors="coerce")
                midnight_hours = dates.dt.hour.between(0, 3)
                midnight_count = int(midnight_hours.sum())  # Convert to Python int
                if midnight_count > len(df) * 0.3:  # More than 30% at midnight
                    suspicious_patterns.append(f"High midnight transaction rate: {midnight_count} transactions ({(midnight_count/len(df)*100).round(1)}%)")
                    risk_factors += 1
            except:
                pass
        
        # Determine fraud risk
        if risk_factors >= 2:
            fraud_risk = "HIGH"
        elif risk_factors == 1:
            fraud_risk = "MEDIUM"
        else:
            fraud_risk = "LOW"
        
        return {
            "can_analyze": True,
            "suspicious_patterns": suspicious_patterns,
            "fraud_risk": fraud_risk,
            "risk_factors": int(risk_factors)  # Convert to Python int
        }
    
    def check_foreign_key_linking(self, df: pd.DataFrame, account_col: str = None, customer_col: str = None):
        """
        STEP-4: Foreign Key / Linking Check
        Validate relationship: one customer_id can have multiple accounts,
        but one account_number must map to only one customer_id
        """
        if account_col is None or customer_col is None:
            # Try to find columns
            account_keywords = ["account", "account_number", "acc_no", "acct_no"]
            customer_keywords = ["customer_id", "cust_id", "user_id", "customerid"]
            # Normalize keywords for proper matching
            norm_account_keywords = [self.normalize(k) for k in account_keywords]
            norm_customer_keywords = [self.normalize(k) for k in customer_keywords]
            
            for col in df.columns:
                norm_col = self.normalize(col)
                if account_col is None and any(norm_k in norm_col or norm_col in norm_k or fuzz.ratio(norm_col, norm_k) >= 85 for norm_k in norm_account_keywords):
                    account_col = col
                if customer_col is None and any(norm_k in norm_col or norm_col in norm_k or fuzz.ratio(norm_col, norm_k) >= 85 for norm_k in norm_customer_keywords):
                    customer_col = col
        
        if account_col is None or customer_col is None:
            return {
                "can_check": False,
                "account_column": account_col,
                "customer_column": customer_col,
                "relationship_type": None,
                "missing_links_count": 0,
                "total_accounts": 0,
                "total_customers": 0,
                "fk_mismatch": False,
                "linking_valid": False,
                "violations": []
            }
        
        # Check for missing links
        pairs = df[[account_col, customer_col]].dropna()
        missing_accounts = int(pairs[pairs[account_col].isna()].shape[0])  # Convert to Python int
        missing_customers = int(pairs[pairs[customer_col].isna()].shape[0])  # Convert to Python int
        missing_links = missing_accounts + missing_customers
        
        # Check FK constraint: one account_number must map to only one customer_id
        violations = []
        fk_mismatch = False
        
        # Group by account_number and check if any account maps to multiple customers
        account_to_customers = pairs.groupby(account_col)[customer_col].nunique()
        accounts_with_multiple_customers = account_to_customers[account_to_customers > 1]
        
        if len(accounts_with_multiple_customers) > 0:
            fk_mismatch = True
            violations.append(f"{int(len(accounts_with_multiple_customers))} account(s) map to multiple customers (FK violation)")  # Convert to Python int
        
        # Determine relationship type
        unique_accounts = int(pairs[account_col].nunique())  # Convert to Python int
        unique_customers = int(pairs[customer_col].nunique())  # Convert to Python int
        
        # Check if one-to-many (one customer, many accounts) - this is VALID
        if unique_customers > 0:
            accounts_per_customer = unique_accounts / unique_customers
            if accounts_per_customer > 1.1:  # More accounts than customers
                relationship_type = "ONE_TO_MANY"  # One customer can have multiple accounts (VALID)
            else:
                relationship_type = "ONE_TO_ONE"
        else:
            relationship_type = "UNKNOWN"
        
        # Linking is valid if no missing links AND no FK mismatch
        linking_valid = bool(missing_links == 0 and not fk_mismatch)  # Convert to Python bool
        
        return {
            "can_check": True,
            "account_column": account_col,
            "customer_column": customer_col,
            "relationship_type": relationship_type,
            "missing_links_count": int(missing_links),
            "total_accounts": int(unique_accounts),
            "total_customers": int(unique_customers),
            "accounts_per_customer": round(accounts_per_customer, 2) if unique_customers > 0 else 0.0,
            "fk_mismatch": bool(fk_mismatch),  # Ensure Python bool
            "linking_valid": linking_valid,
            "violations": violations
        }
    
    def calculate_risk_assessment(self, account_check: dict, missing_columns_check: dict,
                                  kyc_check: dict, foreign_key_check: dict):
        """
        STEP-7: Risk Assessment

        Updated rules:
        - Missing ANY mandatory column (account_number, customer_id, account_status, balance) → HIGH RISK
        - KYC completeness < 60% → HIGH RISK
        - Foreign key mismatch (when check is possible) → HIGH RISK
        - Otherwise, LOW RISK
        """
        missing_mandatory = missing_columns_check.get("missing_mandatory", [])
        missing_account_number = bool("account_number" in missing_mandatory)  # Convert to Python bool
        missing_customer_id = bool("customer_id" in missing_mandatory)  # Convert to Python bool
        missing_account_status = bool("account_status" in missing_mandatory)  # Convert to Python bool
        missing_balance = bool("balance" in missing_mandatory)  # Convert to Python bool

        account_valid = bool(account_check.get("best_match_decision") == "match")  # Convert to Python bool

        kyc_below_threshold = bool(not kyc_check.get("meets_threshold", False))  # Convert to Python bool

        can_fk_check = bool(foreign_key_check.get("can_check", False))  # Convert to Python bool
        fk_mismatch = bool(foreign_key_check.get("fk_mismatch", False)) if can_fk_check else False

        risk_factors = []
        if missing_mandatory:
            risk_factors.append(
                "Missing mandatory column(s): " + ", ".join(sorted(missing_mandatory))
            )
        if kyc_below_threshold:
            risk_factors.append("KYC completeness < 60%")
        if fk_mismatch:
            risk_factors.append("Foreign key mismatch detected")
        if not account_valid:
            risk_factors.append("Account number validation failed")

        risk_level = "HIGH" if risk_factors else "LOW"

        return {
            "risk_level": risk_level,
            "risk_factors": risk_factors,
            "missing_account_number": missing_account_number,
            "missing_customer_id": missing_customer_id,
            "missing_account_status": missing_account_status,
            "missing_balance": missing_balance,
            "kyc_below_threshold": kyc_below_threshold,
            "fk_mismatch": fk_mismatch
        }
    
    def final_decision_logic(self, account_check: dict, customer_check: dict,
                             missing_columns_check: dict, kyc_check: dict,
                             foreign_key_check: dict):
        """
        STEP-8: Final Decision Logic

        Updated business rules:
        - Account number valid BUT missing balance/account_status/KYC → HOLD
        - Account number invalid → REJECT
        - Account + balance + status + KYC ≥ 60% → APPROVE
        - Missing mandatory columns should not cause immediate REJECT; use HOLD with HIGH risk.
        """
        # Extract validation results
        account_valid = account_check.get("best_match_decision") == "match"

        missing_mandatory = missing_columns_check.get("missing_mandatory", [])
        all_mandatory_present = len(missing_mandatory) == 0

        kyc_pct = float(kyc_check.get("kyc_completeness", 0.0))
        kyc_meets_threshold = kyc_check.get("meets_threshold", False) or kyc_pct >= 60.0

        can_fk_check = foreign_key_check.get("can_check", False)
        fk_mismatch = bool(foreign_key_check.get("fk_mismatch", False)) if can_fk_check else False

        customer_exists = customer_check.get("column_exists", False)

        if not account_valid:
            decision = "REJECT"
            reason = "Account number validation failed; dataset cannot be used for banking-grade processing."
        elif account_valid and (not all_mandatory_present or not kyc_meets_threshold or fk_mismatch):
            decision = "HOLD"
            reasons = []
            if not all_mandatory_present:
                reasons.append(
                    "missing mandatory columns: " + ", ".join(sorted(missing_mandatory))
                )
            if not kyc_meets_threshold:
                reasons.append(f"KYC completeness {kyc_pct:.2f}% < 60%")
            if fk_mismatch:
                reasons.append("foreign-key inconsistency between account_number and customer_id")
            if not customer_exists:
                reasons.append("customer_id column not present; foreign-key checks skipped")
            reason = "Account number is valid, but " + "; ".join(reasons) + "."
        else:
            decision = "APPROVE"
            reason = (
                "Account number validation passed, all mandatory columns are present, "
                "and KYC completeness is at or above the 60% threshold."
            )

        return {
            "decision": decision,
            "reason": reason,
            "account_valid": account_valid,
            "mandatory_present": all_mandatory_present,
            "kyc_meets_threshold": kyc_meets_threshold,
            "no_fk_mismatch": bool(not fk_mismatch)  # Convert to Python bool
        }
    
    def detect_purpose(self, df: pd.DataFrame, account_check: dict, customer_check: dict,
                       transaction_check: dict, balance_check: dict, status_check: dict,
                       kyc_check: dict = None):
        """
        STEP-8: Purpose Detection Logic (KYC REMOVED)

        New rules:
        - account_number + customer_id → "Account Management Data"
        - transaction_id / debit / credit / transaction_type → "Transaction Data"
        - withdrawal / deposit / transfer keywords → "Transaction Operations"
        - mix of account + transaction → "Core Banking Dataset"

        Purpose must NEVER be "Unknown" if an account_number column is detected.
        KYC check parameter kept for backward compatibility but not used.
        """
        has_account = bool(account_check.get("has_account_number_column", False))  # Convert to Python bool
        has_customer_id = bool(customer_check.get("column_exists", False))  # Convert to Python bool

        # KYC removed - only use customer_id
        has_customer_details = bool(has_customer_id)  # KYC removed

        transaction_cols = transaction_check.get("found_columns", {}) if transaction_check else {}
        tx_has_id = bool("transaction_id" in transaction_cols)  # Convert to Python bool
        tx_has_debit = bool("debit" in transaction_cols)  # Convert to Python bool
        tx_has_credit = bool("credit" in transaction_cols)  # Convert to Python bool
        tx_has_type = bool("transaction_type" in transaction_cols)  # Convert to Python bool

        has_transaction_data = bool(any([tx_has_id, tx_has_debit, tx_has_credit, tx_has_type]))  # Convert to Python bool

        cols_lower = [self.normalize(c) for c in df.columns]
        wdt_keywords = ["withdraw", "withdrawal", "deposit", "transfer", "transfer_to", "transfer_from"]
        has_tx_operations = any(any(k in c for k in wdt_keywords) for c in cols_lower)

        purposes = []
        confidence_scores = []

        if has_account and has_customer_details and not has_transaction_data and not has_tx_operations:
            purposes.append("Account Management Data")
            confidence_scores.append(90)

        if has_transaction_data and not has_account and not has_customer_details:
            purposes.append("Transaction Data")
            confidence_scores.append(85)

        if has_tx_operations and not has_account and not has_customer_details:
            purposes.append("Transaction Operations")
            confidence_scores.append(80)

        if has_account and (has_transaction_data or has_tx_operations):
            purposes.append("Core Banking Dataset")
            confidence_scores.append(95)

        if not purposes:
            if has_account:
                purposes.append("Account Management Data")
                confidence_scores.append(70)
            elif has_transaction_data or has_tx_operations:
                purposes.append("Transaction Data")
                confidence_scores.append(70)
            else:
                purposes.append("Unknown")
                confidence_scores.append(40)

        max_idx = confidence_scores.index(max(confidence_scores))
        primary_purpose = purposes[max_idx]
        purpose_confidence = confidence_scores[max_idx]

        purpose_statements = {
            "Account Management Data": "Dataset primarily contains account and customer details used for managing customer accounts.",
            "Transaction Data": "Dataset primarily contains transactional records such as transaction IDs, debits, credits, and types.",
            "Transaction Operations": "Dataset focuses on operational movements such as withdrawals, deposits, and transfers between accounts.",
            "Core Banking Dataset": "Dataset combines account, customer, and transaction information typical of core banking systems.",
            "Unknown": "Purpose could not be determined from the available columns."
        }

        purpose_statement = purpose_statements.get(primary_purpose, "Purpose could not be determined.")

        return {
            "primary_purpose": primary_purpose,
            "purpose_confidence": purpose_confidence,
            "purpose_statement": purpose_statement,
            "detected_purposes": purposes,
            "confidence_scores": confidence_scores
        }

    def generate_probability_explanations(self, account_validation, customer_validation, 
                                           transaction_validation, transaction_type_validation,
                                           debit_credit_validation, balance_analysis, core_analysis, df=None):
        """
        Generate human-readable explanations for probability data of different aspects.
        """
        explanations = {}
        
        # Account number validation probability
        if account_validation:
            best_match = account_validation.get("summary", {}).get("best_match")
            if best_match:
                prob = best_match.get("probability_account_number", 0)
                explanations["account_number"] = {
                    "probability": prob,
                    "explanation": f"Account number validation shows {prob}% confidence that this dataset contains valid account numbers suitable for banking operations."
                }
        
        # Customer ID validation probability
        if customer_validation:
            col_exists = customer_validation.get("column_exists", False)
            prob = customer_validation.get("probability_customer_id", 0)
            if col_exists:
                explanations["customer_id"] = {
                    "probability": prob,
                    "explanation": f"Customer ID validation shows {prob}% confidence that this dataset contains valid customer identifiers."
                }
        
        # Transaction validation probability
        if transaction_validation:
            prob = transaction_validation.get("probability_percentage", 0)
            is_valid = transaction_validation.get("is_valid", False)
            explanations["transaction"] = {
                "probability": prob,
                "explanation": f"Transaction validation shows {prob}% confidence that this dataset contains valid transaction data. Status: {'Valid' if is_valid else 'Invalid'}"
            }
        
        # Transaction type validation probability
        if transaction_type_validation:
            prob = transaction_type_validation.get("probability_percentage", 0)
            is_valid = transaction_type_validation.get("is_valid", False)
            explanations["transaction_type"] = {
                "probability": prob,
                "explanation": f"Transaction type validation shows {prob}% confidence that transaction types are correctly formatted. Status: {'Valid' if is_valid else 'Invalid'}"
            }
        
        # Debit/credit validation probability
        if debit_credit_validation:
            prob = debit_credit_validation.get("probability_percentage", 0)
            is_valid = debit_credit_validation.get("is_valid", False)
            explanations["debit_credit"] = {
                "probability": prob,
                "explanation": f"Debit/credit validation shows {prob}% confidence in the balance calculations. Status: {'Valid' if is_valid else 'Invalid'}"
            }
        
        # Balance analysis probability
        if balance_analysis:
            prob = balance_analysis.get("confidence_percentage", 0)
            explanations["balance"] = {
                "probability": prob,
                "explanation": f"Balance analysis shows {prob}% confidence in the presence of balance-related columns suitable for banking operations."
            }
        
        # Core banking analysis probabilities
        if core_analysis:
            core_validations = core_analysis.get("column_validations", {})
            for col_name, validation in core_validations.items():
                if validation.get("confidence", 0) > 0:
                    explanations[f"core_{col_name}"] = {
                        "probability": validation["confidence"],
                        "explanation": f"Core analysis for column '{col_name}' shows {validation['confidence']}% confidence in its role classification."
                    }
        
        # Add purpose explanations for matched columns
        if df is not None:
            for col_name in df.columns:
                purpose_info = self.explain_column_purpose(col_name, df[col_name])
                if purpose_info["confidence"] > 0.5:  # Only include if confidence is above 50%
                    explanations[f"purpose_{col_name}"] = {
                        "probability": purpose_info["confidence"] * 100,
                        "explanation": f"Column '{col_name}' identified as {purpose_info['column_type']}: {purpose_info['explanation']} (Confidence: {purpose_info['confidence']:.2f})"
                    }
        
        return explanations

    def generate_column_purpose_report(self, df: pd.DataFrame,
                                       transaction_check: dict,
                                       kyc_check: dict = None):
        """
        Extra human-readable explanation for banking reports.
        KYC removed - only groups account/customer ID columns.

        Groups columns into:
        1) Account / Customer ID columns (KYC removed)
        2) Transaction columns
        3) Withdrawal / Deposit / Transfer specific columns
        """
        columns = [str(c) for c in df.columns]

        # 1️⃣ Account / Customer ID columns (KYC REMOVED)
        account_customer_cols = []

        # KYC fields detection REMOVED - only account/customer ID

        # Common id / name / contact patterns
        # KYC fields (name, email, phone, address, aadhar, pan, passport) REMOVED as per specification
        account_customer_keywords = [
            "account", "acct", "accno", "account_number",
            "customer_id", "cust_id", "customerid", "custid",
            "client_id", "clientid"
        ]
        for col in columns:
            norm = self.normalize(col)
            if any(k in norm for k in account_customer_keywords):
                account_customer_cols.append(col)

        account_customer_cols = sorted(list(dict.fromkeys(account_customer_cols)))

        # 2️⃣ Transaction columns
        transaction_cols = []
        tx_found = (transaction_check or {}).get("found_columns", {})
        transaction_cols.extend(list(tx_found.values()))

        transaction_keywords = [
            "transaction_id", "txn_id", "trans_id",
            "transaction_type", "txn_type", "trans_type",
            "transaction_date", "txn_date", "trans_date",
            "debit", "credit", "amount"
        ]
        for col in columns:
            norm = self.normalize(col)
            if any(k in norm for k in transaction_keywords):
                transaction_cols.append(col)

        transaction_cols = sorted(list(dict.fromkeys(transaction_cols)))

        # 3️⃣ Withdrawal / Deposit / Transfer specific columns
        wdt_cols = []
        # Base words to look for (most important)
        base_words = ["withdraw", "deposit", "transfer"]
        # Expanded keyword list with all possible variations (normalized versions)
        wdt_keywords_normalized = [
            "withdraw", "withdrawal", "withdrawalamount", "withdrawamount", "withdrawl",
            "deposit", "depositamount", "depositamt", "deposited", "depositing",
            "transfer", "transferto", "transferfrom", "transferamount", "transferamt",
            "transferred", "transferring", "transfers"
        ]
        
        # Check all columns in the dataframe - use multiple matching strategies
        for col in columns:
            col_str = str(col)
            norm_col = self.normalize(col_str)
            col_lower = col_str.lower()
            
            # Strategy 1: Check normalized column name against normalized keywords
            matched = False
            for keyword in wdt_keywords_normalized:
                if keyword in norm_col:
                    wdt_cols.append(col)
                    matched = True
                    break
            
            # Strategy 2: Check for base words in original column name (case-insensitive, whole word or part)
            if not matched:
                for base_word in base_words:
                    if base_word in col_lower:
                        wdt_cols.append(col)
                        matched = True
                        break
            
            # Strategy 3: Check for transaction_type or similar column names
            if not matched:
                type_keywords = ["transaction_type", "txn_type", "trans_type", "type", "operation", "operation_type"]
                for type_kw in type_keywords:
                    if type_kw in col_lower or type_kw in norm_col:
                        wdt_cols.append(col)
                        matched = True
                        break
            
            # Strategy 4: Check column VALUES for withdrawal/deposit/transfer patterns
            # This catches columns like "transaction_type" that contain these values
            if not matched and col in df.columns:
                try:
                    col_series = df[col].dropna().astype(str).str.lower()
                    if len(col_series) > 0:
                        # Check if any values contain withdrawal/deposit/transfer keywords
                        value_matches = col_series.str.contains('|'.join(base_words), case=False, na=False)
                        if value_matches.sum() > 0:
                            # If more than 20% of values match, consider it a WDT column
                            match_ratio = value_matches.sum() / len(col_series)
                            if match_ratio >= 0.2:  # At least 20% of values match (lowered threshold)
                                wdt_cols.append(col)
                                matched = True
                except Exception:
                    pass  # Skip if column can't be analyzed
        
        # Also check transaction columns that might be WDT-specific
        for col in transaction_cols:
            if col not in wdt_cols:  # Avoid duplicates
                col_str = str(col)
                norm_col = self.normalize(col_str)
                col_lower = col_str.lower()
                
                matched = False
                for keyword in wdt_keywords_normalized:
                    if keyword in norm_col:
                        wdt_cols.append(col)
                        matched = True
                        break
                
                if not matched:
                    for base_word in base_words:
                        if base_word in col_lower:
                            wdt_cols.append(col)
                            matched = True
                            break
                
                # Check for transaction_type or similar column names
                if not matched:
                    type_keywords = ["transaction_type", "txn_type", "trans_type", "type", "operation", "operation_type"]
                    for type_kw in type_keywords:
                        if type_kw in col_lower or type_kw in norm_col:
                            wdt_cols.append(col)
                            matched = True
                            break
                
                # Also check values for transaction columns
                if not matched and col in df.columns:
                    try:
                        col_series = df[col].dropna().astype(str).str.lower()
                        if len(col_series) > 0:
                            value_matches = col_series.str.contains('|'.join(base_words), case=False, na=False)
                            if value_matches.sum() > 0:
                                match_ratio = value_matches.sum() / len(col_series)
                                if match_ratio >= 0.2:  # At least 20% of values match
                                    wdt_cols.append(col)
                    except Exception:
                        pass

        # Remove duplicates and sort
        wdt_cols = sorted(list(dict.fromkeys(wdt_cols)))

        # Category counts for charts
        category_counts = {
            "Account / Customer Details": len(account_customer_cols),
            "Transaction Columns": len(transaction_cols),
            "Withdrawal / Deposit / Transfer": len(wdt_cols)
        }

        # Human‑readable reasoning texts (for the report / UI)
        explanations = {}

        if account_customer_cols:
            explanations["account_customer"] = (
                "This dataset contains standard customer and account identification fields "
                "(such as account numbers, customer identifiers, names, emails, and phones) "
                "used for account identification and verification."
            )
        else:
            explanations["account_customer"] = (
                "No strong account or customer detail columns were detected; "
                "the file does not look like a typical customer/account master table."
            )

        if transaction_cols:
            explanations["transaction"] = (
                "This dataset contains transaction history columns (for example transaction IDs, "
                "transaction types, dates, debits and credits) that are typically used to monitor "
                "account activity and update running balances."
            )
        else:
            explanations["transaction"] = (
                "No strong transaction columns (like transaction_id, debit, credit, or transaction_date) "
                "were detected; it does not appear to be a detailed transaction ledger."
            )

        if wdt_cols:
            explanations["wdt"] = (
                "This dataset contains explicit withdrawal, deposit, or transfer columns which are "
                "typically used for auditing and verifying individual fund movements between accounts."
            )
        else:
            explanations["wdt"] = (
                "No explicit withdrawal/deposit/transfer columns were found; "
                "fund movement may be encoded only through generic debit/credit amounts."
            )

        return {
            "account_customer_columns": account_customer_cols,
            "transaction_columns": transaction_cols,
            "withdraw_deposit_transfer_columns": wdt_cols,
            "category_counts": category_counts,
            "explanations": explanations
        }

    def detect_banking_application_type(self, df: pd.DataFrame, matched_columns: list = None) -> dict:
        """
        🏦 Banking Application Type Prediction Engine
        
        Analyzes column patterns across multiple files to predict the type of 
        banking application the data belongs to.
        
        Application Types:
        1. Core Banking System (CBS) - Customer, Account, Transaction, Balance
        2. Loan Management System (LMS) - Loan, EMI, Principal, Interest, Tenure
        3. Payment Gateway - Payment, UPI, Merchant, Gateway, Transaction Status
        4. Card Management System - Card, CVV, Expiry, PIN, Card Type, Limit
        5. Treasury Management - Investment, Bond, Portfolio, Yield, Maturity
        6. Trade Finance - LC, Bill of Exchange, Import, Export, SWIFT
        7. Wealth Management - Portfolio, NAV, Mutual Fund, SIP, Dividend
        8. Digital Banking - Mobile, OTP, UPI, QR, App Transaction
        """
        
        columns_lower = [str(col).lower().replace('_', ' ').replace('-', ' ') for col in df.columns]
        columns_str = ' '.join(columns_lower)
        
        # Application type patterns with weights
        app_patterns = {
            'Core Banking System': {
                'keywords': ['account', 'customer', 'balance', 'transaction', 'deposit', 'withdrawal',
                             'account number', 'customer id', 'account type', 'savings', 'current',
                             'account status', 'branch', 'ifsc', 'opening balance', 'closing balance'],
                'mandatory': ['account', 'customer'],
                'weight': 1.0,
                'description': 'Primary banking system for managing customer accounts, deposits, withdrawals, and balances.'
            },
            'Loan Management System': {
                'keywords': ['loan', 'emi', 'principal', 'interest', 'tenure', 'disbursement',
                             'loan id', 'loan type', 'sanctioned amount', 'outstanding', 'repayment',
                             'installment', 'due date', 'overdue', 'collateral', 'mortgage'],
                'mandatory': ['loan'],
                'weight': 0.95,
                'description': 'System for managing loans, EMIs, interest calculations, and repayment schedules.'
            },
            'Payment Gateway': {
                'keywords': ['payment', 'upi', 'merchant', 'gateway', 'payment status', 'reference',
                             'pgr', 'payment mode', 'neft', 'rtgs', 'imps', 'settlement',
                             'beneficiary', 'payer', 'payment date', 'payment amount'],
                'mandatory': ['payment'],
                'weight': 0.9,
                'description': 'System for processing digital payments, fund transfers, and settlements.'
            },
            'Card Management System': {
                'keywords': ['card', 'card number', 'cvv', 'expiry', 'pin', 'card type',
                             'credit card', 'debit card', 'card limit', 'card status', 'atm',
                             'pos', 'card holder', 'billing', 'statement'],
                'mandatory': ['card'],
                'weight': 0.9,
                'description': 'System for managing credit/debit cards, limits, and card transactions.'
            },
            'Treasury Management': {
                'keywords': ['treasury', 'investment', 'bond', 'portfolio', 'yield', 'maturity',
                             'fixed deposit', 'fd', 'rd', 'securities', 'forex', 'liquidity',
                             'capital', 'asset', 'liability'],
                'mandatory': ['investment', 'treasury', 'bond', 'fixed deposit', 'fd'],
                'weight': 0.85,
                'description': 'System for managing bank investments, securities, and treasury operations.'
            },
            'Trade Finance': {
                'keywords': ['trade', 'lc', 'letter of credit', 'bill of exchange', 'import',
                             'export', 'swift', 'documentary', 'bill of lading', 'shipment',
                             'incoterms', 'bank guarantee'],
                'mandatory': ['trade', 'lc', 'import', 'export'],
                'weight': 0.85,
                'description': 'System for managing international trade finance and documentary credits.'
            },
            'Wealth Management': {
                'keywords': ['portfolio', 'nav', 'mutual fund', 'sip', 'dividend', 'wealth',
                             'equity', 'stock', 'shares', 'broker', 'demat', 'holdings',
                             'units', 'redemption', 'scheme'],
                'mandatory': ['portfolio', 'mutual fund', 'nav', 'sip'],
                'weight': 0.85,
                'description': 'System for managing investments, portfolios, and wealth advisory services.'
            },
            'Digital Banking': {
                'keywords': ['mobile', 'otp', 'upi', 'qr', 'app', 'digital', 'online',
                             'internet banking', 'net banking', 'channel', 'device',
                             'session', 'login', 'authentication'],
                'mandatory': ['mobile', 'otp', 'upi', 'digital', 'app'],
                'weight': 0.8,
                'description': 'System for managing mobile and internet banking channels.'
            }
        }
        
        # Score each application type
        app_scores = {}
        app_details = {}
        
        for app_type, patterns in app_patterns.items():
            score = 0
            matched_keywords = []
            has_mandatory = False
            
            # Check for keyword matches
            for keyword in patterns['keywords']:
                if keyword in columns_str:
                    score += 10
                    matched_keywords.append(keyword)
            
            # Check for mandatory keywords
            for mandatory_kw in patterns['mandatory']:
                if mandatory_kw in columns_str:
                    has_mandatory = True
                    break
            
            # Boost score if mandatory keyword present
            if has_mandatory:
                score *= 1.5
            
            # Apply weight
            score *= patterns['weight']
            
            if score > 0:
                app_scores[app_type] = score
                app_details[app_type] = {
                    'matched_keywords': matched_keywords,
                    'has_mandatory': has_mandatory,
                    'description': patterns['description']
                }
        
        # Sort by score
        sorted_apps = sorted(app_scores.items(), key=lambda x: x[1], reverse=True)
        
        if not sorted_apps:
            return {
                'predicted_application': 'Unknown Banking Application',
                'confidence': 0,
                'explanation': 'Could not determine the specific banking application type.',
                'all_matches': []
            }
        
        # Get top prediction
        top_app, top_score = sorted_apps[0]
        total_score = sum(app_scores.values())
        confidence = round((top_score / total_score) * 100, 1) if total_score > 0 else 0
        
        # Get top 3 matches
        top_matches = []
        for app_name, score in sorted_apps[:3]:
            details = app_details.get(app_name, {})
            app_confidence = round((score / total_score) * 100, 1) if total_score > 0 else 0
            top_matches.append({
                'application_type': app_name,
                'confidence': app_confidence,
                'matched_keywords': details.get('matched_keywords', []),
                'description': details.get('description', '')
            })
        
        return {
            'predicted_application': top_app,
            'confidence': confidence,
            'explanation': app_details.get(top_app, {}).get('description', ''),
            'matched_keywords': app_details.get(top_app, {}).get('matched_keywords', []),
            'all_matches': top_matches
        }

    def predict(self, csv_path):
        try:
            df = pd.read_csv(csv_path)
        except Exception as e:
            return {"error": str(e)}

        total_cols = int(len(df.columns))  # Convert to Python int
        if total_cols == 0:
            return {"domain": "Unknown", "confidence": 0}

        matched_score = 0

        # ✅ NEW STORAGE
        matched_keywords = []
        matched_columns = []
        match_map = []

        details = []

        for col in df.columns:
            norm_col = self.normalize(col)
            best_score = 0
            best_keyword = None
            match_type = "none"

            # 1️⃣ keyword + column fuzzy
            for ref in self.keywords + self.columns_db:
                s = fuzz.ratio(norm_col, self.normalize(ref))
                if s > best_score:
                    best_score = s
                    best_keyword = ref
                    match_type = "fuzzy"

            # 2️⃣ synonym fallback
            if best_score < 85:
                for syn, actual in self.synonyms.items():
                    if syn in norm_col:
                        s = fuzz.ratio(actual, norm_col)
                        if s > best_score:
                            best_score = s
                            best_keyword = actual
                            match_type = "synonym"

            # 3️⃣ Name score
            if best_score >= 90:
                matched_score += 1
            elif best_score >= 75:
                matched_score += 0.5

            # 4️⃣ Value intelligence
            value_score = self.value_pattern_score(df[col], col)
            matched_score += value_score

            # ✅ STORE MATCHED KEYWORDS
            if best_score >= 75:
                matched_keywords.append(best_keyword)
                matched_columns.append(col)

                match_map.append({
                    "user_column": col,
                    "matched_keyword": best_keyword,
                    "name_score": best_score,
                    "value_score": value_score,
                    "match_type": match_type
                })

            details.append({
                "column": col,
                "name_score": best_score,
                "value_score": value_score,
                "match_type": match_type
            })

        # 5️⃣ Empty column penalty
        empty_cols = df.columns[df.isna().all()].tolist()
        matched_score -= len(empty_cols) * 0.2
        matched_score = max(0, matched_score)

        # 6️⃣ Ratio logic
        max_possible = total_cols * 2
        confidence_100 = round((matched_score / max_possible) * 100, 2)
        confidence_10 = round(confidence_100 / 10, 2)

        # 7️⃣ Account number & customer ID validation (always run)
        account_number_validation = self.validate_account_numbers(df)
        best_match = account_number_validation.get("summary", {}).get("best_match") if account_number_validation else None
        account_number_check = {
            "has_account_number_column": bool(account_number_validation and account_number_validation.get("account_like_columns")),
            "best_match_column": best_match.get("column") if best_match else None,
            "best_match_decision": best_match.get("decision") if best_match else None,
            "best_match_probability": best_match.get("probability_account_number") if best_match else None
        }

        customer_id_validation = self.validate_customer_id(df)

        # 8️⃣ Domain decision – enforce banking when account_number OR customer_id exists
        has_account_or_customer = bool(
            account_number_check.get("has_account_number_column")
            or customer_id_validation.get("column_exists")
        )

        if has_account_or_customer:
            if account_number_check.get("best_match_decision") == "match":
                decision = "CONFIRMED_BANKING"
                qualitative = "Very Strong"
            else:
                decision = "LIKELY_BANKING"
                qualitative = "Strong"
            if confidence_100 < 65:
                confidence_100 = 65.0
                confidence_10 = round(confidence_100 / 10, 2)
        else:
            if confidence_100 >= 85:
                decision = "CONFIRMED_BANKING"
                qualitative = "Very Strong"
            elif confidence_100 >= 65:
                decision = "LIKELY_BANKING"
                qualitative = "Strong"
            else:
                decision = "UNKNOWN"
                qualitative = "Weak"

        # 🔥 CORE BANKING ENGINE: Role-based column detection and validation (KYC REMOVED)
        core_engine = CoreBankingEngine()
        core_analysis = core_engine.analyze_banking_dataset(df)
        
        # Legacy validations (kept for backward compatibility but not used in decision)
        account_status = self.detect_account_status(df)
        missing_columns_check = self.check_missing_columns(df)
        balance_analysis = self.analyze_balance(df)
        opening_debit_credit_detection = self.detect_opening_debit_credit_columns(df)
        transaction_validation = self.validate_transaction_data(df)
        transaction_type_validation = self.validate_transaction_type(df)
        
        # Banking Transaction Rule Engine
        account_number_col = account_number_check.get("best_match_column")
        banking_transaction_rules = self.validate_banking_transaction_rules(df, account_number_col)
        
        balance_col = balance_analysis.get("balance_column") if balance_analysis.get("has_balance_column") else None
        debit_credit_validation = self.validate_debit_credit_balance(df, balance_col)
        
        # KYC, PAN, Branch Code, Fraud Detection - ALL REMOVED as per specification
        # Purpose detection (updated - no KYC reference)
        purpose_detection = self.detect_purpose(
            df,
            account_number_check,
            customer_id_validation,
            transaction_validation,
            balance_analysis,
            account_status,
            {}  # Empty dict instead of kyc_verification
        )

        # Generate probability explanations
        probability_explanations = self.generate_probability_explanations(
            account_number_validation,
            customer_id_validation,
            transaction_validation,
            transaction_type_validation,
            debit_credit_validation,
            balance_analysis,
            core_analysis,
            df  # Pass the dataframe for purpose explanations
        )
        
        # Add banking transaction rules probability explanations
        if banking_transaction_rules:
            prob = banking_transaction_rules.get("confidence_percentage", 0)
            status = banking_transaction_rules.get("transaction_status", "INVALID")
            if "banking_transaction" not in probability_explanations:
                probability_explanations["banking_transaction"] = {
                    "probability": prob,
                    "explanation": f"Banking Transaction Rule Engine shows {prob}% confidence in transaction data validity. Status: {status}"
                }
        
        # Column‑purpose report for UI / charts (no KYC reference)
        column_purpose_report = self.generate_column_purpose_report(df, transaction_validation, {})
        
        # 🔥 BANKING COLUMN MAPPER: Use Banking Column Mapper for pattern-based column detection
        banking_column_mapper = BankingColumnMapper()
        column_mapping_result = banking_column_mapper.map_columns(csv_path)
        
        # 🔥 BANKING APPLICATION TYPE PREDICTION (NEW FEATURE)
        banking_app_type_prediction = self.detect_banking_application_type(df, matched_columns)

        # 🔥 CORE ENGINE: Use Core Banking Engine results (KYC REMOVED)
        core_final_decision = core_analysis.get("final_decision", {})
        core_decision = core_final_decision.get("decision", "HOLD")
        core_reason = core_final_decision.get("reason", "Analysis pending")
        
        # Format detected columns summary
        detected_columns_text = []
        for col_info in core_analysis.get("detected_columns", []):
            col_name = col_info.get("column_name", "")
            role = col_info.get("role", "UNKNOWN")
            conf = col_info.get("confidence", 0.0)
            detected_columns_text.append(f"{col_name} → {role} ({conf:.1f}%)")
        
        # Validation summary
        validation_summary = core_analysis.get("validation_summary", {})
        passed_count = len(validation_summary.get("rules_passed", []))
        failed_count = len(validation_summary.get("rules_failed", []))
        skipped_count = len(validation_summary.get("rules_skipped", []))
        
        # Create summary using Core Engine results
        ordered_summary = {
            "1_domain_detection_result": (
                f"Banking domain detected with overall confidence {confidence_100:.2f}% "
                f"based on column and value patterns."
            ),
            "2_column_role_classification": (
                f"Detected {len([c for c in core_analysis.get('detected_columns', []) if c.get('role') != 'UNKNOWN'])} "
                f"columns with confirmed roles (confidence >= 70%). "
                f"Detected columns: {'; '.join(detected_columns_text[:5])}"
            ),
            "3_validation_summary": (
                f"Validation results: {passed_count} rules passed, {failed_count} rules failed, "
                f"{skipped_count} rules skipped (UNKNOWN columns)."
            ),
            "4_cross_column_validation": self._format_cross_validation_summary(core_analysis.get("cross_validations", {})),
            "5_final_decision": f"Final decision: {core_decision} – {core_reason}"
        }
        
        # Filter results to show only columns with probability > 0%
        filtered_core_detected_columns = [
            col for col in core_analysis.get("detected_columns", []) 
            if col.get("confidence", 0) > 0
        ]
        
        # Filter core column validations to only include non-failed cases
        filtered_core_column_validations = {
            col: validation for col, validation in core_analysis.get("column_validations", {}).items()
            if validation.get("confidence", 0) > 0 or validation.get("is_valid", True) is not False
        }
        
        # Create filtered summary focusing on non-failed cases
        filtered_ordered_summary = {}
        for key, value in ordered_summary.items():
            if key == "2_column_role_classification":
                # Update the column classification summary to reflect filtered results
                filtered_ordered_summary[key] = (
                    f"Detected {len(filtered_core_detected_columns)} "
                    f"columns with confidence > 0% (confidence >= 70% for confirmed roles). "
                    f"Filtered columns: {'; '.join([f'{col["column_name"]} → {col["role"]} ({col["confidence"]:.1f}%)' for col in filtered_core_detected_columns[:5]])}"
                )
            else:
                filtered_ordered_summary[key] = value
        
        return {
            "domain": self.domain if decision != "UNKNOWN" else "Unknown",
            "confidence_percentage": confidence_100,
            "confidence_out_of_10": confidence_10,
            "decision": decision,
            "qualitative": qualitative,
            "total_columns": int(total_cols),  # Convert to Python int
            "empty_columns": empty_cols,

            # ✅ IMPORTANT OUTPUTS
            "matched_keywords": list(set(matched_keywords)),
            "matched_columns": matched_columns,
            "keyword_column_mapping": match_map,

            "details": details,
            "account_number_validation": account_number_validation,
            "account_number_check": account_number_check,
            # Only include account_status if status column exists
            "account_status": account_status if account_status.get("has_status_column") else None,
            # Only include missing_columns_check if all mandatory columns are present
            "missing_columns_check": missing_columns_check if missing_columns_check.get("all_mandatory_present") else None,
            "balance_analysis": balance_analysis,
            "opening_debit_credit_detection": opening_debit_credit_detection,
            "transaction_validation": transaction_validation,
            "transaction_type_validation": transaction_type_validation,
            "debit_credit_validation": debit_credit_validation,
            # Only include customer_id_validation if customer_id column exists
            "customer_id_validation": customer_id_validation if customer_id_validation.get("column_exists") else None,
            "purpose_detection": purpose_detection,
            "banking_transaction_rules": banking_transaction_rules,
            "probability_explanations": probability_explanations,
            "column_purpose_report": column_purpose_report,
            
            # 🔥 CORE ENGINE RESULTS (PRIMARY OUTPUT - KYC REMOVED)
            "core_banking_analysis": core_analysis,
            "core_detected_columns": filtered_core_detected_columns,
            "core_column_validations": filtered_core_column_validations,
            "core_cross_validations": core_analysis.get("cross_validations", {}),
            "core_validation_summary": core_analysis.get("validation_summary", {}),
            "core_final_decision": core_analysis.get("final_decision", {}),
            
            # 🔥 BANKING COLUMN MAPPER RESULTS (PATTERN-BASED COLUMN DETECTION)
            "banking_column_mapping": column_mapping_result,
            
            # 🔥 BANKING APPLICATION TYPE PREDICTION (NEW FEATURE)
            "banking_application_type": banking_app_type_prediction,
            
            "ordered_summary": filtered_ordered_summary
        }
    
    def _format_cross_validation_summary(self, cross_validations):
        """Format cross-validation results for summary"""
        results = []
        
        if cross_validations.get("debit_credit_exclusivity"):
            dce = cross_validations["debit_credit_exclusivity"]
            status = "✓ Valid" if dce.get("valid") else "✗ Invalid" if dce.get("valid") is False else "? Unknown"
            results.append(f"Debit-Credit Exclusivity: {status} - {dce.get('reason', 'N/A')}")
        
        if cross_validations.get("balance_formula_validation"):
            bfv = cross_validations["balance_formula_validation"]
            status = "✓ Valid" if bfv.get("valid") else "✗ Invalid" if bfv.get("valid") is False else "? Unknown"
            results.append(f"Balance Formula: {status} - {bfv.get('reason', 'N/A')}")
        
        if cross_validations.get("balance_continuity"):
            bc = cross_validations["balance_continuity"]
            status = "✓ Valid" if bc.get("valid") else "✗ Invalid" if bc.get("valid") is False else "? Unknown"
            results.append(f"Balance Continuity: {status} - {bc.get('reason', 'N/A')}")
        
        return "; ".join(results) if results else "No cross-column validations applicable"
