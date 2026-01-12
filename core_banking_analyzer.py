"""
Core Banking Data Analyzer

Analyzes banking datasets to identify column roles, relationships, and meanings.
Focuses on banking domain: customers, accounts, transactions, balances.
"""

import pandas as pd
import numpy as np
from rapidfuzz import fuzz
import json
from datetime import datetime
import re


class CoreBankingDataAnalyzer:
    """Core Banking Data Analyzer - analyzes banking datasets and identifies column roles and relationships."""
    
    # Define the possible roles
    ROLES = [
        "CUSTOMER_ID", 
        "CUSTOMER_NAME",
        "ACCOUNT_NUMBER",
        "ACCOUNT_TYPE",
        "ACCOUNT_STATUS",
        "TRANSACTION_ID",
        "TRANSACTION_DATE",
        "TRANSACTION_TYPE",
        "OPENING_BALANCE",
        "DEBIT",
        "CREDIT",
        "CLOSING_BALANCE",
        "UNKNOWN"
    ]
    
    def __init__(self):
        """Initialize the analyzer with role keywords and patterns."""
        self.role_keywords = self._initialize_role_keywords()
    
    def _initialize_role_keywords(self):
        """Initialize keywords for each role."""
        return {
            "CUSTOMER_ID": [
                "customer_id", "cust_id", "customerid", "custid", 
                "client_id", "clientid", "user_id", "userid", "customer_no", "cust_no"
            ],
            "CUSTOMER_NAME": [
                "customer_name", "cust_name", "customername", "custname", 
                "name", "full_name", "account_holder_name", "holder_name"
            ],
            "ACCOUNT_NUMBER": [
                "account_number", "account_no", "acc_no", "accno", "accountno",
                "account", "acct", "acc_number", "accountid", "account_num"
            ],
            "ACCOUNT_TYPE": [
                "account_type", "acct_type", "accounttype", "acctype", 
                "type", "account_class", "product_type"
            ],
            "ACCOUNT_STATUS": [
                "account_status", "acc_status", "status", "account_state",
                "acc_state", "state", "account_active", "acc_active"
            ],
            "TRANSACTION_ID": [
                "transaction_id", "txn_id", "trans_id", "transactionid",
                "txnid", "transaction_number", "trans_no", "txn_no"
            ],
            "TRANSACTION_DATE": [
                "transaction_date", "txn_date", "trans_date", "transactiondate",
                "date", "transaction_time", "txn_time", "trans_time", "datetime"
            ],
            "TRANSACTION_TYPE": [
                "transaction_type", "txn_type", "trans_type", "transactiontype",
                "type", "transaction_category", "txn_category", "trans_category"
            ],
            "OPENING_BALANCE": [
                "opening_balance", "open_balance", "balance_before",
                "previous_balance", "prev_balance", "initial_balance", "op_bal"
            ],
            "DEBIT": [
                "debit", "withdrawal", "withdraw_amount", "amount_out",
                "dr_amount", "debit_amount", "withdraw", "outflow"
            ],
            "CREDIT": [
                "credit", "deposit", "amount_in", "cr_amount",
                "credit_amount", "deposit_amount", "inflow"
            ],
            "CLOSING_BALANCE": [
                "closing_balance", "closing", "balance_after",
                "final_balance", "end_balance", "current_balance", "cl_bal"
            ]
        }
    
    def normalize(self, text):
        """Normalize text for matching."""
        return str(text).lower().replace(" ", "").replace("_", "").replace("-", "")
    
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
        """Check if series matches date pattern strictly using YYYY-MM-DD format and calendar validation."""
        try:
            non_null = series.dropna().astype(str).str.strip()
            if len(non_null) == 0:
                return False
            
            # CRITICAL: Require strict YYYY-MM-DD format (or parseable variants with separators)
            # Check for date format with separators (YYYY-MM-DD, YYYY/MM/DD, DD-MM-YYYY, etc.)
            date_format_pattern = r'^\d{4}[-/]\d{1,2}[-/]\d{1,2}|^\d{1,2}[-/]\d{1,2}[-/]\d{4}'
            has_date_format = non_null.str.contains(date_format_pattern, na=False, regex=True).mean()
            
            if has_date_format < 0.7:  # At least 70% must have date format
                return False
            
            # Try to parse as date with calendar validation
            date_parsed = pd.to_datetime(non_null, errors="coerce")
            valid_date_ratio = date_parsed.notna().mean()
            
            # Calendar validation: check if parsed dates are valid calendar dates
            if valid_date_ratio >= 0.7:
                # Additional validation: dates should be reasonable (not all same date, not future dates)
                valid_dates = date_parsed.dropna()
                if len(valid_dates) > 0:
                    # Check if dates are reasonable (not too far in future, not ancient)
                    now = pd.Timestamp.now()
                    future_dates = (valid_dates > now).sum()
                    ancient_dates = (valid_dates < pd.Timestamp('1900-01-01')).sum()
                    total_valid = len(valid_dates)
                    
                    # If too many future or ancient dates, might not be transaction dates
                    if future_dates / total_valid > 0.5 or ancient_dates / total_valid > 0.5:
                        return False
                    
                    return valid_date_ratio >= 0.9  # Require 90% valid dates for transaction_date
            
            return False
        except:
            return False
    
    def _matches_transaction_type_pattern(self, series):
        """Check if series contains transaction type values - ONLY DEBIT/CREDIT categorical values."""
        try:
            non_null = series.dropna().astype(str).str.upper().str.strip()
            if len(non_null) == 0:
                return False
            # CRITICAL: Only match DEBIT and CREDIT (case-insensitive)
            valid_types = ["DEBIT", "CREDIT"]
            match_ratio = non_null.isin(valid_types).mean()
            # Require at least 80% match for transaction_type (strict requirement)
            return match_ratio >= 0.8
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
            valid_types = ["savings", "current", "checking", "loan", "credit", "fixed", "fd", "rd", "deposit"]
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
    
    def classify_column_role(self, df, column_name):
        """
        Classify the role of a column based on name and value patterns.
        """
        column_series = df[column_name]
        norm_col = self.normalize(column_name)
        
        # CRITICAL SAFETY RULE: If column name contains financial keywords, never classify as account_number
        financial_keywords = ["amount", "balance", "loan", "emi", "interest", "rate"]
        is_financial_amount = any(keyword in norm_col for keyword in financial_keywords)
        
        role_scores = {}
        
        # Name-based matching (50% weight)
        for role, keywords in self.role_keywords.items():
            max_name_score = 0
            exact_matches = []
            fuzzy_matches = []
            
            for keyword in keywords:
                norm_keyword = self.normalize(keyword)
                # Exact substring match
                if norm_keyword in norm_col or norm_col in norm_keyword:
                    exact_matches.append(100)
                # Fuzzy match
                else:
                    fuzzy_score = fuzz.ratio(norm_col, norm_keyword)
                    fuzzy_matches.append(fuzzy_score)
            
            # Use highest exact match if available, otherwise highest fuzzy match
            if exact_matches:
                max_name_score = max(exact_matches)
            elif fuzzy_matches:
                max_name_score = max(fuzzy_matches)
            
            role_scores[role] = max_name_score * 0.5
        
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
        
        # Value pattern analysis (50% weight) - only for non-financial columns
        try:
            if not is_financial_amount:  # Only apply value pattern analysis for non-financial columns
                # ACCOUNT_NUMBER pattern check
                if self._matches_account_number_pattern(column_series):
                    role_scores["ACCOUNT_NUMBER"] = role_scores.get("ACCOUNT_NUMBER", 0) + 50
                
                # CUSTOMER_ID pattern check
                if self._matches_customer_id_pattern(column_series):
                    role_scores["CUSTOMER_ID"] = role_scores.get("CUSTOMER_ID", 0) + 50
                
                # TRANSACTION_DATE pattern check (PRIORITY: Check date first)
                if self._matches_date_pattern(column_series):
                    role_scores["TRANSACTION_DATE"] = role_scores.get("TRANSACTION_DATE", 0) + 50
                    # CRITICAL: If column matches date pattern, exclude it from transaction_type
                    # Remove transaction_type score to prevent cross-mapping
                    if "TRANSACTION_TYPE" in role_scores:
                        del role_scores["TRANSACTION_TYPE"]
                
                # TRANSACTION_TYPE pattern check (ONLY if not already identified as date)
                # Only check if date pattern didn't match
                elif self._matches_transaction_type_pattern(column_series):
                    role_scores["TRANSACTION_TYPE"] = role_scores.get("TRANSACTION_TYPE", 0) + 50
                    # CRITICAL: If column matches transaction_type pattern, ensure it's NOT a date
                    # Remove transaction_date score to prevent cross-mapping
                    if "TRANSACTION_DATE" in role_scores:
                        del role_scores["TRANSACTION_DATE"]
                
                # Numeric balance/debit/credit checks
                if self._matches_numeric_balance_pattern(column_series):
                    # Distinguish between opening/closing/debit/credit based on context
                    if "opening" in norm_col or "open" in norm_col or "initial" in norm_col:
                        role_scores["OPENING_BALANCE"] = role_scores.get("OPENING_BALANCE", 0) + 50
                    elif "closing" in norm_col or "closing" in norm_col or "final" in norm_col or "current" in norm_col:
                        role_scores["CLOSING_BALANCE"] = role_scores.get("CLOSING_BALANCE", 0) + 50
                    elif "debit" in norm_col or "withdraw" in norm_col or "out" in norm_col:
                        role_scores["DEBIT"] = role_scores.get("DEBIT", 0) + 50
                    elif "credit" in norm_col or "deposit" in norm_col or "in" in norm_col:
                        role_scores["CREDIT"] = role_scores.get("CREDIT", 0) + 50
                    else:
                        # Generic numeric - distribute score based on name hints
                        if not any(role in role_scores for role in ["OPENING_BALANCE", "CLOSING_BALANCE", "DEBIT", "CREDIT"]):
                            # Default to CLOSING_BALANCE if no hint
                            role_scores["CLOSING_BALANCE"] = role_scores.get("CLOSING_BALANCE", 0) + 30
                
                # ACCOUNT_STATUS pattern check
                if self._matches_status_pattern(column_series):
                    role_scores["ACCOUNT_STATUS"] = role_scores.get("ACCOUNT_STATUS", 0) + 50
                
                # CUSTOMER_NAME pattern check
                if self._matches_name_pattern(column_series):
                    role_scores["CUSTOMER_NAME"] = role_scores.get("CUSTOMER_NAME", 0) + 50
                
                # ACCOUNT_TYPE pattern check
                if self._matches_account_type_pattern(column_series):
                    role_scores["ACCOUNT_TYPE"] = role_scores.get("ACCOUNT_TYPE", 0) + 50
                
        except Exception as e:
            pass  # If pattern analysis fails, rely on name matching only
        
        # Select best role
        if role_scores:
            best_role = max(role_scores.items(), key=lambda x: x[1])
            confidence = best_role[1]
            best_role_name = best_role[0]
            
            # Apply additional validation to avoid misclassification
            validation_passed = True
            if best_role_name == "CUSTOMER_ID":
                # Validate that it's actually a customer ID
                if not self._matches_customer_id_pattern(column_series):
                    validation_passed = False
            elif best_role_name == "TRANSACTION_ID":
                # Validate that it's actually a transaction ID
                if not self._matches_transaction_id_pattern(column_series):
                    validation_passed = False
            elif best_role_name == "TRANSACTION_TYPE":
                # Validate that it's actually a transaction type (DEBIT/CREDIT only)
                if not self._matches_transaction_type_pattern(column_series):
                    validation_passed = False
                # CRITICAL: Also ensure it's NOT a date to prevent cross-mapping
                if self._matches_date_pattern(column_series):
                    validation_passed = False  # Reject if it's actually a date
            elif best_role_name == "TRANSACTION_DATE":
                # Validate that it's actually a date
                if not self._matches_date_pattern(column_series):
                    validation_passed = False
                # CRITICAL: Also ensure it's NOT a transaction type to prevent cross-mapping
                if self._matches_transaction_type_pattern(column_series):
                    validation_passed = False  # Reject if it matches transaction_type pattern
            elif best_role_name == "ACCOUNT_NUMBER":
                # Validate that it's actually an account number
                if not self._matches_account_number_pattern(column_series):
                    validation_passed = False
            elif best_role_name == "ACCOUNT_TYPE":
                # Validate that it's actually an account type
                if not self._matches_account_type_pattern(column_series):
                    validation_passed = False
            
            # If validation failed, set to UNKNOWN
            if not validation_passed:
                best_role_name = "UNKNOWN"
                confidence = 0.0
            
            # If confidence is high enough (> 25), assign the role, otherwise UNKNOWN
            role = best_role_name if confidence >= 25 else "UNKNOWN"
        else:
            role = "UNKNOWN"
            confidence = 0.0
        
        return {
            "role": role,
            "confidence": round(float(confidence), 2),
            "all_role_scores": {k: round(float(v), 2) for k, v in role_scores.items() if v > 0}
        }
    
    def analyze_relationships(self, df, column_roles):
        """
        Analyze relationships between columns in the dataset.
        """
        relationships = []
        
        # Find columns by role
        role_to_columns = {}
        for col, role_info in column_roles.items():
            role = role_info["role"]
            if role != "UNKNOWN":
                if role not in role_to_columns:
                    role_to_columns[role] = []
                role_to_columns[role].append(col)
        
        # Customer → Account relationship
        customer_ids = role_to_columns.get("CUSTOMER_ID", [])
        account_numbers = role_to_columns.get("ACCOUNT_NUMBER", [])
        
        for cust_col in customer_ids:
            for acc_col in account_numbers:
                # Check if there's a relationship (each customer can have multiple accounts)
                unique_cust = df[cust_col].nunique()
                unique_acc = df[acc_col].nunique()
                if unique_acc >= unique_cust:  # Each customer can have multiple accounts
                    relationships.append(f"{cust_col} → {acc_col} (Customer → Account)")
        
        # Account → Transaction relationship
        transaction_ids = role_to_columns.get("TRANSACTION_ID", [])
        
        for acc_col in account_numbers:
            for txn_col in transaction_ids:
                # Check if there's a relationship (each account can have multiple transactions)
                unique_acc = df[acc_col].nunique()
                unique_txn = df[txn_col].nunique()
                if unique_txn >= unique_acc:  # Each account can have multiple transactions
                    relationships.append(f"{acc_col} → {txn_col} (Account → Transaction)")
        
        # Balance formula relationship: closing_balance = opening_balance + credit - debit
        opening_balances = role_to_columns.get("OPENING_BALANCE", [])
        closing_balances = role_to_columns.get("CLOSING_BALANCE", [])
        debits = role_to_columns.get("DEBIT", [])
        credits = role_to_columns.get("CREDIT", [])
        
        for close_col in closing_balances:
            for open_col in opening_balances:
                for debit_col in debits:
                    for credit_col in credits:
                        try:
                            # Check if the balance formula holds
                            opening_vals = pd.to_numeric(df[open_col], errors="coerce").fillna(0)
                            closing_vals = pd.to_numeric(df[close_col], errors="coerce").fillna(0)
                            debit_vals = pd.to_numeric(df[debit_col], errors="coerce").fillna(0)
                            credit_vals = pd.to_numeric(df[credit_col], errors="coerce").fillna(0)
                            
                            calculated_closing = opening_vals + credit_vals - debit_vals
                            diff = abs(closing_vals - calculated_closing)
                            tolerance = closing_vals.abs() * 0.01 + 0.01
                            matches = (diff <= tolerance).sum()
                            match_ratio = matches / len(df) if len(df) > 0 else 0
                            
                            if match_ratio >= 0.8:
                                relationships.append(f"{close_col} = {open_col} + {credit_col} - {debit_col} (Closing = Opening + Credit - Debit)")
                        except:
                            pass
        
        # Debit-Credit exclusivity relationship
        if debits and credits:
            for debit_col in debits:
                for credit_col in credits:
                    try:
                        debit_series = pd.to_numeric(df[debit_col], errors="coerce").fillna(0)
                        credit_series = pd.to_numeric(df[credit_col], errors="coerce").fillna(0)
                        
                        both_positive = ((debit_series > 0) & (credit_series > 0)).sum()
                        total = len(df)
                        both_positive_ratio = both_positive / total if total > 0 else 0
                        
                        if both_positive_ratio < 0.1:  # Usually only one is positive at a time
                            relationships.append(f"{debit_col} ↔ {credit_col} (Debit and Credit are mutually exclusive)")
                    except:
                        pass
        
        return relationships
    
    def explain_column_meaning(self, df, column_name, role_info):
        """
        Generate a human-readable explanation of a column's meaning.
        """
        role = role_info["role"]
        example_value = ""
        
        # Get an example value from the column
        non_null_values = df[column_name].dropna()
        if len(non_null_values) > 0:
            example_value = str(non_null_values.iloc[0])
        
        # Define meanings for each role
        meanings = {
            "CUSTOMER_ID": "Unique identifier for each customer in the banking system",
            "CUSTOMER_NAME": "Name of the customer associated with the account",
            "ACCOUNT_NUMBER": "Unique bank account identifier linking transactions to an account",
            "ACCOUNT_TYPE": "Type of account (savings, current, loan, etc.)",
            "ACCOUNT_STATUS": "Current status of the account (active, inactive, closed, etc.)",
            "TRANSACTION_ID": "Unique identifier for each transaction",
            "TRANSACTION_DATE": "Date and time when the transaction occurred",
            "TRANSACTION_TYPE": "Type of transaction (deposit, withdrawal, transfer, etc.)",
            "OPENING_BALANCE": "Account balance at the beginning of a period",
            "DEBIT": "Amount withdrawn or debited from the account",
            "CREDIT": "Amount deposited or credited to the account",
            "CLOSING_BALANCE": "Account balance at the end of a period",
            "UNKNOWN": "Role could not be determined from the column name or values"
        }
        
        meaning = meanings.get(role, "Purpose could not be determined")
        
        return {
            "name": column_name,
            "role": role,
            "meaning": meaning,
            "example": example_value
        }
    
    def generate_dataset_summary(self, df, column_roles, relationships):
        """
        Generate a summary of the dataset meaning and structure.
        """
        # Count unique values for different roles
        role_to_columns = {}
        for col, role_info in column_roles.items():
            role = role_info["role"]
            if role != "UNKNOWN":
                if role not in role_to_columns:
                    role_to_columns[role] = []
                role_to_columns[role].append(col)
        
        # Count unique customers, accounts, and transactions
        unique_customers = 0
        unique_accounts = 0
        unique_transactions = 0
        
        if "CUSTOMER_ID" in role_to_columns:
            for col in role_to_columns["CUSTOMER_ID"]:
                unique_customers = max(unique_customers, df[col].nunique())
        
        if "ACCOUNT_NUMBER" in role_to_columns:
            for col in role_to_columns["ACCOUNT_NUMBER"]:
                unique_accounts = max(unique_accounts, df[col].nunique())
        
        if "TRANSACTION_ID" in role_to_columns:
            for col in role_to_columns["TRANSACTION_ID"]:
                unique_transactions = max(unique_transactions, df[col].nunique())
        
        # Generate summary based on detected roles
        summary_parts = []
        
        if unique_customers > 0:
            summary_parts.append(f"The dataset contains {unique_customers} unique customers.")
        
        
        if unique_accounts > 0:
            summary_parts.append(f"There are {unique_accounts} unique accounts.")
        
        if unique_transactions > 0:
            summary_parts.append(f"The dataset includes {unique_transactions} transactions.")
        
        # Determine dataset type based on roles
        if "ACCOUNT_NUMBER" in role_to_columns and "TRANSACTION_ID" in role_to_columns:
            dataset_type = "Core banking transaction dataset"
        elif "ACCOUNT_NUMBER" in role_to_columns and "CLOSING_BALANCE" in role_to_columns:
            dataset_type = "Account balance dataset"
        elif "CUSTOMER_ID" in role_to_columns and "ACCOUNT_NUMBER" in role_to_columns:
            dataset_type = "Customer-account master dataset"
        elif "TRANSACTION_ID" in role_to_columns:
            dataset_type = "Transaction-only dataset"
        else:
            dataset_type = "General banking dataset"
        
        summary_parts.insert(0, f"This is a {dataset_type}.")
        
        # Add information about relationships
        if relationships:
            summary_parts.append(f"It includes {len(relationships)} identified column relationships.")
        
        return " ".join(summary_parts)
    
    def analyze(self, csv_path):
        """
        Main analysis function that orchestrates all steps.
        """
        # Load the dataset
        df = pd.read_csv(csv_path)
        
        # Step 1: Identify column roles
        column_roles = {}
        for col in df.columns:
            role_classification = self.classify_column_role(df, col)
            column_roles[col] = role_classification
        
        # Step 2: Analyze relationships
        relationships = self.analyze_relationships(df, column_roles)
        
        # Step 3: Explain column meanings
        columns_explanation = []
        for col, role_info in column_roles.items():
            if role_info["role"] != "UNKNOWN":  # Only include known roles
                explanation = self.explain_column_meaning(df, col, role_info)
                columns_explanation.append(explanation)
        
        # Step 4: Generate dataset summary
        dataset_summary = self.generate_dataset_summary(df, column_roles, relationships)
        
        # Return the complete analysis
        result = {
            "columns": columns_explanation,
            "relationships": relationships,
            "dataset_summary": dataset_summary,
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "analyzed_columns": len([c for c in columns_explanation if c["role"] != "UNKNOWN"])
        }
        
        return result


def main():
    """Example usage of the Core Banking Data Analyzer."""
    # This is just an example - the actual usage would be through the API
    print("Core Banking Data Analyzer initialized.")


if __name__ == "__main__":
    main()