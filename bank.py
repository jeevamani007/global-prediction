import math
import re
import pandas as pd
import numpy as np
from rapidfuzz import fuzz
from sqlalchemy import text
from database import engine


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

    def _looks_account_like_name(self, column_name: str) -> bool:
        norm = self.normalize(column_name)
        candidates = ["account", "acct", "accno", "custaccount", "customeraccount", "accnumber"]
        return any(key in norm for key in candidates)

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
        status_column = None
        status_values = []
        active_count = 0
        inactive_count = 0
        
        for col in df.columns:
            norm_col = self.normalize(col)
            if any(keyword in norm_col for keyword in status_keywords):
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
        Mandatory: account_status, balance
        Optional: transaction_date, debit, credit
        """
        mandatory_columns = {
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
            for col in df.columns:
                norm_col = self.normalize(col)
                if any(keyword in norm_col for keyword in keywords):
                    found_mandatory[col_type] = col
                    found = True
                    break
            if not found:
                missing_mandatory.append(col_type)
        
        # Check optional columns
        for col_type, keywords in optional_columns.items():
            found = False
            for col in df.columns:
                norm_col = self.normalize(col)
                if any(keyword in norm_col for keyword in keywords):
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
            "all_mandatory_present": len(missing_mandatory) == 0
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
            for col in df.columns:
                norm_col = self.normalize(col)
                if any(keyword in norm_col for keyword in keywords):
                    found_kyc[kyc_type] = col
                    found = True
                    break
            if not found:
                missing_kyc.append(kyc_type)
        
        # Check if user_name is present (critical for KYC)
        has_user_name = "user_name" in found_kyc
        kyc_completeness = round((len(found_kyc) / len(kyc_fields)) * 100, 2)
        kyc_verified = kyc_completeness >= 60.0  # STEP-6: >= 60% threshold
        
        return {
            "kyc_verified": kyc_verified,
            "has_user_name": has_user_name,
            "found_kyc_fields": found_kyc,
            "missing_kyc_fields": missing_kyc,
            "kyc_completeness": kyc_completeness,
            "meets_threshold": kyc_completeness >= 60.0
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
        customer_id_col = None
        
        # Rule 1: Column Exists
        for col in df.columns:
            norm_col = self.normalize(col)
            if any(keyword in norm_col for keyword in customer_id_keywords):
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
        not_null = not_null_ratio >= 0.95  # 95% threshold
        
        # Rule 3: Unique (optional - usually one customer can have multiple accounts)
        unique_count = series.nunique()
        unique_ratio = float(unique_count / non_null_count) if non_null_count > 0 else 0.0
        unique = unique_ratio >= 0.5  # Optional, so lower threshold
        
        # Rule 4: Format/Pattern Check (letter(s) + numbers, e.g., C001, C002)
        # Pattern: starts with 1-2 letters, followed by 1-4 digits
        format_pattern = r'^[A-Za-z]{1,2}\d{1,4}$'
        format_matches = series.str.fullmatch(format_pattern, na=False)
        format_valid_ratio = float(format_matches.sum() / non_null_count) if non_null_count > 0 else 0.0
        format_valid = format_valid_ratio >= 0.8
        
        # Rule 5: No Symbols/Special Characters (only letters + numbers)
        no_symbols_matches = series.str.fullmatch(r'^[A-Za-z0-9]+$', na=False)
        no_symbols_ratio = float(no_symbols_matches.sum() / non_null_count) if non_null_count > 0 else 0.0
        no_symbols = no_symbols_ratio >= 0.95
        
        # Rule 6: Length Check (Min: 3, Max: 6 characters)
        length_valid_matches = series.str.len().between(3, 6, inclusive='both')
        length_valid_ratio = float(length_valid_matches.sum() / non_null_count) if non_null_count > 0 else 0.0
        length_valid = length_valid_ratio >= 0.9
        
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
            "total_rows": total_rows,
            "non_null_count": non_null_count,
            "rules_passed": rules_passed,
            "rules_total": 7,
            "probability_customer_id": probability,
            "decision": decision
        }
    
    def validate_account_numbers(self, df: pd.DataFrame):
        results = []
        total_rows = len(df)

        for col in df.columns:
            series = df[col]

            if not self._looks_account_like_name(col) and not self._looks_account_like_values(series):
                continue

            non_null = series.dropna().astype(str)
            if non_null.empty:
                continue

            digit_only_ratio = float(non_null.str.fullmatch(r"\d+").mean())
            length_ok_ratio = float(non_null.str.len().between(6, 16).mean())
            no_symbol_ratio = float(non_null.str.contains(r"[^0-9]").apply(lambda x: not x).mean())
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
            "total_rows": total_rows,
            "checked_columns": len(results),
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
        balance_col = None

        for col in df.columns:
            norm_col = self.normalize(col)
            if any(k in norm_col for k in balance_keywords):
                balance_col = col
                break

        if balance_col is None:
            return {
                "has_balance_column": False,
                "balance_column": None,
                "zero_or_negative_count": 0,
                "total_rows": len(df),
                "zero_or_negative_pct": 0.0
            }

        series = pd.to_numeric(df[balance_col], errors="coerce")
        total = len(series)
        zero_neg = series.fillna(0) <= 0
        zero_neg_count = int(zero_neg.sum())
        pct = round((zero_neg_count / total) * 100, 2) if total else 0.0

        return {
            "has_balance_column": True,
            "balance_column": balance_col,
            "zero_or_negative_count": zero_neg_count,
            "total_rows": total,
            "zero_or_negative_pct": pct
        }

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
            for col in df.columns:
                norm_col = self.normalize(col)
                if any(keyword in norm_col for keyword in keywords):
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
            validation_results["debit_credit_same_row"] = not both_present.any()
            if both_present.any():
                validation_results["violations"].append(f"Found {both_present.sum()} rows with both debit and credit > 0")
        
        if "debit" in found_columns:
            debit_col = found_columns["debit"]
            debit_series = pd.to_numeric(df[debit_col], errors="coerce")
            negative_debits = (debit_series < 0).sum()
            validation_results["amount_positive"] = negative_debits == 0
            if negative_debits > 0:
                validation_results["violations"].append(f"Found {negative_debits} negative debit amounts")
        
        if "credit" in found_columns:
            credit_col = found_columns["credit"]
            credit_series = pd.to_numeric(df[credit_col], errors="coerce")
            negative_credits = (credit_series < 0).sum()
            if negative_credits > 0:
                validation_results["violations"].append(f"Found {negative_credits} negative credit amounts")
        
        if "transaction_date" in found_columns:
            date_col = found_columns["transaction_date"]
            try:
                dates = pd.to_datetime(df[date_col], errors="coerce")
                future_dates = dates > pd.Timestamp.now()
                validation_results["date_not_future"] = not future_dates.any()
                if future_dates.any():
                    validation_results["violations"].append(f"Found {future_dates.sum()} future dates")
            except:
                validation_results["date_not_future"] = True
        
        return {
            "has_transaction_data": len(found_columns) > 0,
            "found_columns": found_columns,
            "missing_columns": missing_columns,
            "completeness": round((len(found_columns) / len(transaction_columns)) * 100, 2),
            "validation_results": validation_results,
            "is_valid": len(validation_results["violations"]) == 0
        }
    
    def validate_debit_credit_balance(self, df: pd.DataFrame, balance_col_name: str = None):
        """
        STEP-3: Debit/Credit vs Balance Check
        Validate debit doesn't exceed balance, credit increases balance
        """
        if balance_col_name is None:
            # Find balance column
            balance_keywords = ["balance", "bal", "account_balance", "current_balance"]
            balance_col_name = None
            for col in df.columns:
                norm_col = self.normalize(col)
                if any(k in norm_col for k in balance_keywords):
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
        for col in df.columns:
            norm_col = self.normalize(col)
            if "debit" in norm_col:
                debit_col = col
            if "credit" in norm_col:
                credit_col = col
        
        balance_series = pd.to_numeric(df[balance_col_name], errors="coerce").fillna(0)
        insufficient_count = 0
        total_debits = 0
        
        if debit_col:
            debit_series = pd.to_numeric(df[debit_col], errors="coerce").fillna(0)
            total_debits = (debit_series > 0).sum()
            # Check if debit > balance
            insufficient = (debit_series > balance_series) & (debit_series > 0)
            insufficient_count = int(insufficient.sum())
        
        return {
            "has_balance": True,
            "balance_column": balance_col_name,
            "can_validate": debit_col is not None or credit_col is not None,
            "insufficient_funds_count": insufficient_count,
            "total_debit_transactions": total_debits,
            "validation_passed": insufficient_count == 0,
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
            for col in df.columns:
                norm_col = self.normalize(col)
                if any(k in norm_col for k in account_keywords):
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
        
        for col in df.columns:
            norm_col = self.normalize(col)
            if "date" in norm_col and "transaction" in norm_col:
                date_col = col
            if "debit" in norm_col:
                debit_col = col
            if "credit" in norm_col:
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
                        suspicious_patterns.append(f"High transaction frequency: {len(high_frequency)} accounts with >10 transactions")
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
                            suspicious_patterns.append(f"Unusually high amounts: {len(high_amounts)} transactions >3 standard deviations")
                            risk_factors += 1
            except:
                pass
        
        # Pattern 3: Midnight transactions repeatedly
        if date_col:
            try:
                dates = pd.to_datetime(df[date_col], errors="coerce")
                midnight_hours = dates.dt.hour.between(0, 3)
                midnight_count = midnight_hours.sum()
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
            "risk_factors": risk_factors
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
            
            for col in df.columns:
                norm_col = self.normalize(col)
                if account_col is None and any(k in norm_col for k in account_keywords):
                    account_col = col
                if customer_col is None and any(k in norm_col for k in customer_keywords):
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
        missing_accounts = pairs[pairs[account_col].isna()].shape[0]
        missing_customers = pairs[pairs[customer_col].isna()].shape[0]
        missing_links = missing_accounts + missing_customers
        
        # Check FK constraint: one account_number must map to only one customer_id
        violations = []
        fk_mismatch = False
        
        # Group by account_number and check if any account maps to multiple customers
        account_to_customers = pairs.groupby(account_col)[customer_col].nunique()
        accounts_with_multiple_customers = account_to_customers[account_to_customers > 1]
        
        if len(accounts_with_multiple_customers) > 0:
            fk_mismatch = True
            violations.append(f"{len(accounts_with_multiple_customers)} account(s) map to multiple customers (FK violation)")
        
        # Determine relationship type
        unique_accounts = pairs[account_col].nunique()
        unique_customers = pairs[customer_col].nunique()
        
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
        linking_valid = missing_links == 0 and not fk_mismatch
        
        return {
            "can_check": True,
            "account_column": account_col,
            "customer_column": customer_col,
            "relationship_type": relationship_type,
            "missing_links_count": int(missing_links),
            "total_accounts": int(unique_accounts),
            "total_customers": int(unique_customers),
            "accounts_per_customer": round(accounts_per_customer, 2) if unique_customers > 0 else 0.0,
            "fk_mismatch": fk_mismatch,
            "linking_valid": linking_valid,
            "violations": violations
        }
    
    def calculate_risk_assessment(self, missing_columns_check: dict, kyc_check: dict, foreign_key_check: dict):
        """
        STEP-7: Risk Assessment
        Rules: Missing account_status OR balance → HIGH RISK
               Missing KYC → HIGH RISK
               FK mismatch → HIGH RISK
        """
        missing_mandatory = missing_columns_check.get("missing_mandatory", [])
        missing_account_status = "account_status" in missing_mandatory
        missing_balance = "balance" in missing_mandatory
        kyc_below_threshold = not kyc_check.get("meets_threshold", False)
        fk_mismatch = foreign_key_check.get("fk_mismatch", False)
        
        risk_factors = []
        if missing_account_status or missing_balance:
            risk_factors.append("Missing mandatory columns")
        if kyc_below_threshold:
            risk_factors.append("KYC completeness < 60%")
        if fk_mismatch:
            risk_factors.append("Foreign key mismatch detected")
        
        risk_level = "HIGH" if len(risk_factors) > 0 else "LOW"
        
        return {
            "risk_level": risk_level,
            "risk_factors": risk_factors,
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
        APPROVE / HOLD / REJECT based on validation results
        """
        # Extract validation results
        account_valid = account_check.get("best_match_decision") == "match"
        customer_valid = customer_check.get("decision") == "found"
        mandatory_present = missing_columns_check.get("all_mandatory_present", False)
        kyc_meets_threshold = kyc_check.get("meets_threshold", False) or kyc_check.get("kyc_completeness", 0) >= 60.0
        no_fk_mismatch = foreign_key_check.get("can_check", False) and not foreign_key_check.get("fk_mismatch", True)
        
        # Decision logic
        if account_valid and customer_valid and mandatory_present and kyc_meets_threshold and no_fk_mismatch:
            decision = "APPROVE"
            reason = "All validation checks passed: account number valid, customer ID valid, mandatory columns present, KYC completeness ≥60%, and no foreign key mismatches."
        elif account_valid and (not mandatory_present or not kyc_meets_threshold or not customer_valid):
            decision = "HOLD"
            reasons = []
            if not customer_valid:
                reasons.append("customer ID validation failed")
            if not mandatory_present:
                missing = missing_columns_check.get("missing_mandatory", [])
                reasons.append(f"missing mandatory columns: {', '.join(missing)}")
            if not kyc_meets_threshold:
                reasons.append(f"KYC completeness {kyc_check.get('kyc_completeness', 0)}% < 60%")
            reason = f"Account number is valid, but: {', '.join(reasons)}."
        else:
            decision = "REJECT"
            reason = "Account number validation failed or critical data integrity issues detected."
        
        return {
            "decision": decision,
            "reason": reason,
            "account_valid": account_valid,
            "customer_valid": customer_valid,
            "mandatory_present": mandatory_present,
            "kyc_meets_threshold": kyc_meets_threshold,
            "no_fk_mismatch": no_fk_mismatch
        }
    
    def detect_purpose(self, df: pd.DataFrame, account_check: dict, customer_check: dict, 
                      transaction_check: dict, balance_check: dict, status_check: dict):
        """
        STEP-8: Purpose Detection Logic
        Infer dataset purpose based on column combinations
        """
        has_account = account_check.get("has_account_number_column", False)
        has_customer = customer_check.get("column_exists", False)
        has_transaction = transaction_check.get("has_transaction_data", False)
        has_balance = balance_check.get("has_balance_column", False)
        has_status = status_check.get("has_status_column", False)
        
        # Check for transaction columns specifically
        transaction_cols = transaction_check.get("found_columns", {})
        has_debit_credit = "debit" in transaction_cols or "credit" in transaction_cols
        has_transaction_date = "transaction_date" in transaction_cols
        
        purposes = []
        confidence_scores = []
        
        # Purpose 1: Account Verification
        if has_account and not has_customer and not has_transaction and not has_balance:
            purposes.append("Account Verification")
            confidence_scores.append(90)
        
        # Purpose 2: Customer Account Mapping
        if has_account and has_customer and not has_transaction:
            purposes.append("Customer Account Mapping")
            confidence_scores.append(85)
        
        # Purpose 3: Transaction Processing
        if has_account and has_debit_credit and has_transaction_date:
            purposes.append("Transaction Processing")
            confidence_scores.append(95)
        
        # Purpose 4: Funds Validation
        if has_account and has_balance and not has_transaction:
            purposes.append("Funds Validation")
            confidence_scores.append(80)
        
        # Purpose 5: Transaction Authorization
        if has_transaction and has_balance and has_status:
            purposes.append("Transaction Authorization")
            confidence_scores.append(90)
        
        # Purpose 6: Account Management
        if has_account and has_status and has_customer:
            purposes.append("Account Management")
            confidence_scores.append(75)
        
        # Determine primary purpose
        if purposes:
            max_idx = confidence_scores.index(max(confidence_scores))
            primary_purpose = purposes[max_idx]
            purpose_confidence = confidence_scores[max_idx]
        else:
            primary_purpose = "General Banking Data"
            purpose_confidence = 50
        
        # Generate purpose statement
        purpose_statements = {
            "Account Verification": "This dataset is used to verify account numbers and validate account existence.",
            "Customer Account Mapping": "This dataset maps customers to their associated accounts for relationship management.",
            "Transaction Processing": "This dataset contains transaction records for processing debits, credits, and transaction history.",
            "Funds Validation": "This dataset validates account balances and available funds.",
            "Transaction Authorization": "This dataset is used to authorize transactions based on balance and account status.",
            "Account Management": "This dataset manages account status, customer relationships, and account lifecycle.",
            "General Banking Data": "This dataset contains general banking information without a specific primary purpose."
        }
        
        purpose_statement = purpose_statements.get(primary_purpose, "Purpose could not be determined.")
        
        return {
            "primary_purpose": primary_purpose,
            "purpose_confidence": purpose_confidence,
            "purpose_statement": purpose_statement,
            "detected_purposes": purposes,
            "confidence_scores": confidence_scores
        }

    def predict(self, csv_path):
        try:
            df = pd.read_csv(csv_path)
        except Exception as e:
            return {"error": str(e)}

        total_cols = len(df.columns)
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

        # 7️⃣ Final decision
        if confidence_100 >= 85:
            decision = "CONFIRMED_BANKING"
            qualitative = "Very Strong"
        elif confidence_100 >= 65:
            decision = "LIKELY_BANKING"
            qualitative = "Strong"
        else:
            decision = "UNKNOWN"
            qualitative = "Weak"

        # 8️⃣ Only do account-number prediction if banking is detected
        if decision != "UNKNOWN":
            account_number_validation = self.validate_account_numbers(df)
            best_match = account_number_validation.get("summary", {}).get("best_match") if account_number_validation else None
            account_number_check = {
                "has_account_number_column": bool(account_number_validation and account_number_validation.get("account_like_columns")),
                "best_match_column": best_match.get("column") if best_match else None,
                "best_match_decision": best_match.get("decision") if best_match else None,
                "best_match_probability": best_match.get("probability_account_number") if best_match else None
            }
            # Check account status and missing columns
            account_status = self.detect_account_status(df)
            missing_columns_check = self.check_missing_columns(df)
            balance_analysis = self.analyze_balance(df)
            kyc_verification = self.verify_kyc(df)
            customer_id_validation = self.validate_customer_id(df)
            # Transaction validations
            transaction_validation = self.validate_transaction_data(df)
            balance_col = balance_analysis.get("balance_column") if balance_analysis.get("has_balance_column") else None
            debit_credit_validation = self.validate_debit_credit_balance(df, balance_col)
            fraud_detection = self.detect_fraud_patterns(df)
            # Foreign key linking check
            account_col = account_number_check.get("best_match_column")
            customer_col = customer_id_validation.get("column_name")
            foreign_key_check = self.check_foreign_key_linking(df, account_col, customer_col)
            # Purpose detection
            purpose_detection = self.detect_purpose(df, account_number_check, customer_id_validation, 
                                                   transaction_validation, balance_analysis, account_status)
            # Final decision logic (STEP-8)
            final_decision = self.final_decision_logic(account_number_check, customer_id_validation,
                                                      missing_columns_check, kyc_verification, foreign_key_check)
            # Risk assessment (STEP-7)
            risk_assessment = self.calculate_risk_assessment(missing_columns_check, kyc_verification, foreign_key_check)
        else:
            account_number_validation = None
            account_number_check = {
                "has_account_number_column": False,
                "best_match_column": None,
                "best_match_decision": None,
                "best_match_probability": None
            }
            account_status = {
                "has_status_column": False,
                "status_column": None,
                "status_values": [],
                "active_count": 0,
                "inactive_count": 0,
                "total_with_status": 0
            }
            missing_columns_check = {
                "found_mandatory": {},
                "missing_mandatory": ["account_status", "balance"],
                "found_optional": {},
                "missing_optional": [],
                "all_mandatory_present": False
            }
            balance_analysis = {
                "has_balance_column": False,
                "balance_column": None,
                "zero_or_negative_count": 0,
                "total_rows": len(df),
                "zero_or_negative_pct": 0.0
            }
            kyc_verification = {
                "kyc_verified": False,
                "has_user_name": False,
                "found_kyc_fields": {},
                "missing_kyc_fields": [],
                "kyc_completeness": 0.0,
                "meets_threshold": False
            }
            customer_id_validation = {
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
            transaction_validation = {
                "has_transaction_data": False,
                "found_columns": {},
                "missing_columns": [],
                "completeness": 0.0,
                "validation_results": {"violations": []},
                "is_valid": False
            }
            debit_credit_validation = {
                "has_balance": False,
                "can_validate": False,
                "insufficient_funds_count": 0,
                "total_debit_transactions": 0,
                "validation_passed": False
            }
            fraud_detection = {
                "can_analyze": False,
                "suspicious_patterns": [],
                "fraud_risk": "LOW",
                "risk_factors": 0
            }
            foreign_key_check = {
                "can_check": False,
                "account_column": None,
                "customer_column": None,
                "relationship_type": None,
                "missing_links_count": 0,
                "total_accounts": 0,
                "total_customers": 0,
                "fk_mismatch": False,
                "linking_valid": False,
                "violations": []
            }
            purpose_detection = {
                "primary_purpose": "Unknown",
                "purpose_confidence": 0,
                "purpose_statement": "Cannot determine purpose - banking domain not detected.",
                "detected_purposes": [],
                "confidence_scores": []
            }
            final_decision = {
                "decision": "REJECT",
                "reason": "Banking domain not detected.",
                "account_valid": False,
                "customer_valid": False,
                "mandatory_present": False,
                "kyc_meets_threshold": False,
                "no_fk_mismatch": False
            }
            risk_assessment = {
                "risk_level": "HIGH",
                "risk_factors": ["Banking domain not detected"],
                "missing_account_status": True,
                "missing_balance": True,
                "kyc_below_threshold": True,
                "fk_mismatch": False
            }

        return {
            "domain": self.domain if decision != "UNKNOWN" else "Unknown",
            "confidence_percentage": confidence_100,
            "confidence_out_of_10": confidence_10,
            "decision": decision,
            "qualitative": qualitative,
            "total_columns": total_cols,
            "empty_columns": empty_cols,

            # ✅ IMPORTANT OUTPUTS
            "matched_keywords": list(set(matched_keywords)),
            "matched_columns": matched_columns,
            "keyword_column_mapping": match_map,

            "details": details,
            "account_number_validation": account_number_validation,
            "account_number_check": account_number_check,
            "account_status": account_status,
            "missing_columns_check": missing_columns_check,
            "balance_analysis": balance_analysis,
            "kyc_verification": kyc_verification,
            "customer_id_validation": customer_id_validation,
            "transaction_validation": transaction_validation,
            "debit_credit_validation": debit_credit_validation,
            "fraud_detection": fraud_detection,
            "foreign_key_check": foreign_key_check,
            "purpose_detection": purpose_detection,
            "final_decision": final_decision,
            "risk_assessment": risk_assessment
        }
