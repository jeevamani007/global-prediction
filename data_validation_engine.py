"""
Data Validation and Business Rules Engine

This module implements a dynamic data validation engine that analyzes CSV datasets,
infers column semantics, and applies adaptive business rules based on the detected
data structure and patterns.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import re
from typing import Dict, List, Optional, Any, Tuple
import json


class DataValidationEngine:
    """
    Dynamic Data Validation and Business Rules Engine
    
    Analyzes CSV datasets to infer column meanings and apply adaptive business rules
    based on detected data patterns and relationships.
    """

    def __init__(self):
        """Initialize the validation engine."""
        self.column_variations = {
            "account_number": ["account_number", "account_no", "acc_no", "accno", "account"],
            "customer_id": ["customer_id", "cust_id", "customerid", "custid", "client_id"],
            "customer_name": ["customer_name", "cust_name", "customername", "name", "full_name"],
            "account_type": ["account_type", "acct_type", "accounttype", "type"],
            "account_status": ["account_status", "acc_status", "status", "account_state", "state"],
            "branch_code": ["branch_code", "branch", "branch_id", "branchcode", "location"],
            "ifsc_code": ["ifsc_code", "ifsc", "ifsc_code", "ifscnumber"],
            "pan": ["pan", "pan_number", "pan_no", "pannumber", "panno", "permanent_account_number"],
            "transaction_id": ["transaction_id", "txn_id", "trans_id", "transactionid", "txnid", "ref_id"],
            "transaction_date": ["transaction_date", "txn_date", "trans_date", "transactiondate", "date", "txn_dt"],
            "transaction_type": ["transaction_type", "txn_type", "trans_type", "transactiontype", "type", "txn_mode"],
            "debit": ["debit", "dr_amount", "debit_amount", "withdraw", "withdrawal", "outflow"],
            "credit": ["credit", "cr_amount", "credit_amount", "deposit", "inflow", "received"],
            "amount": ["amount", "amt", "transaction_amount", "value", "total"],
            "opening_balance": ["opening_balance", "open_balance", "balance_before", "initial_balance", "op_bal"],
            "closing_balance": ["closing_balance", "closing", "balance_after", "final_balance", "current_balance", "cl_bal"],
            "phone": ["phone", "mobile", "contact", "telephone", "phone_number", "mobile_number"],
            "email": ["email", "email_address", "email_addr", "mail"],
            "product": ["product", "product_name", "item", "service", "goods"],
            "quantity": ["quantity", "qty", "amount_qty", "count"],
            "rate": ["rate", "price", "unit_price", "cost", "charges"],
            "tax": ["tax", "tax_amount", "gst", "vat", "sales_tax"],
            "net_amount": ["net_amount", "net_amt", "final_amount", "total_with_tax"]
        }

        self.allowed_account_types = ["SAVINGS", "CURRENT", "LOAN", "FD", "RD", "SAVING", "CURRENT ACCOUNT", "SAVINGS ACCOUNT"]
        self.allowed_account_statuses = ["ACTIVE", "INACTIVE", "CLOSED", "SUSPENDED", "BLOCKED"]
        self.allowed_transaction_types = ["DEBIT", "CREDIT", "DEPOSIT", "WITHDRAW", "WITHDRAWAL", "TRANSFER", "PURCHASE", "PAYMENT"]

    def analyze_column_semantics(self, df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        Analyze each column to infer its semantic meaning based on:
        - Column name patterns
        - Data type (numeric, text, date)
        - Value format and repetition
        """
        column_analysis = {}
        
        for col in df.columns:
            col_data = df[col]
            
            # Analyze data characteristics
            data_type = str(col_data.dtype)
            non_null_count = col_data.count()
            total_count = len(col_data)
            null_percentage = (total_count - non_null_count) / total_count * 100 if total_count > 0 else 0
            
            # Infer semantic meaning
            inferred_meaning = self.infer_column_meaning(col, col_data)
            
            # Analyze value patterns
            unique_values = col_data.nunique()
            unique_percentage = unique_values / total_count * 100 if total_count > 0 else 0
            
            # Get sample values
            sample_values = col_data.dropna().head(3).tolist()
            
            column_analysis[col] = {
                "column_name": col,
                "inferred_meaning": inferred_meaning,
                "data_type": data_type,
                "null_percentage": null_percentage,
                "unique_percentage": unique_percentage,
                "sample_values": sample_values,
                "total_records": total_count,
                "non_null_records": non_null_count
            }
        
        return column_analysis

    def infer_column_meaning(self, col_name: str, col_data: pd.Series) -> str:
        """
        Infer the semantic meaning of a column based on name and data patterns.
        """
        # Normalize column name
        normalized_name = str(col_name).lower().strip().replace(" ", "_").replace("-", "_")
        
        # First try name-based matching
        for meaning, variations in self.column_variations.items():
            for variation in variations:
                if variation in normalized_name or normalized_name == variation:
                    # Verify data pattern matches the inferred meaning
                    if self.verify_data_pattern(col_data, meaning):
                        return meaning
        
        # If name-based matching fails, try data pattern analysis
        return self.analyze_data_pattern(col_data, normalized_name)

    def verify_data_pattern(self, col_data: pd.Series, expected_meaning: str) -> bool:
        """
        Verify if the column data matches the expected pattern for the meaning.
        """
        non_null = col_data.dropna()
        if len(non_null) == 0:
            return False  # Empty columns don't match patterns
        
        non_null_str = non_null.astype(str)
        
        if expected_meaning == "account_number":
            # Digits only, length 6-18
            digit_ratio = non_null_str.str.fullmatch(r"\d+").mean()
            length_ratio = non_null_str.str.len().between(6, 18).mean()
            return digit_ratio >= 0.8 and length_ratio >= 0.8
        
        elif expected_meaning == "customer_id":
            # Alphanumeric with at least one letter
            alphanumeric_ratio = non_null_str.str.fullmatch(r"[A-Za-z0-9]+").mean()
            has_letter_ratio = non_null_str.str.contains(r"[A-Za-z]").mean()
            return alphanumeric_ratio >= 0.8 and has_letter_ratio >= 0.8
        
        elif expected_meaning == "customer_name":
            # Letters and spaces, min 3 chars
            letter_space_ratio = non_null_str.str.fullmatch(r"[A-Za-z\s]+").mean()
            min_length_ratio = (non_null_str.str.len() >= 3).mean()
            return letter_space_ratio >= 0.8 and min_length_ratio >= 0.8
        
        elif expected_meaning == "account_type":
            # Must be from allowed account types
            valid_ratio = non_null_str.str.upper().str.strip().isin(
                [v.upper() for v in self.allowed_account_types]
            ).mean()
            return valid_ratio >= 0.8
        
        elif expected_meaning == "account_status":
            # Must be from allowed statuses
            valid_ratio = non_null_str.str.upper().str.strip().isin(
                [v.upper() for v in self.allowed_account_statuses]
            ).mean()
            return valid_ratio >= 0.8
        
        elif expected_meaning == "transaction_date":
            # Must be parseable as date
            date_parsed = pd.to_datetime(non_null_str, errors="coerce")
            valid_date_ratio = date_parsed.notna().mean()
            return valid_date_ratio >= 0.8
        
        elif expected_meaning == "transaction_type":
            # Must be from allowed transaction types
            valid_ratio = non_null_str.str.upper().str.strip().isin(
                [v.upper() for v in self.allowed_transaction_types]
            ).mean()
            return valid_ratio >= 0.8
        
        elif expected_meaning in ["debit", "credit", "amount", "opening_balance", "closing_balance"]:
            # Numeric values
            numeric_series = pd.to_numeric(non_null_str, errors="coerce")
            numeric_ratio = numeric_series.notna().mean()
            return numeric_ratio >= 0.8
        
        elif expected_meaning == "phone":
            # Numeric, 10 digits (after removing separators)
            cleaned = non_null_str.str.replace(r'[\s\-\(\)]', '', regex=True)
            numeric_ratio = cleaned.str.fullmatch(r"\d+").mean()
            length_ratio = (cleaned.str.len() == 10).mean()
            return numeric_ratio >= 0.8 and length_ratio >= 0.8
        
        elif expected_meaning == "pan":
            # PAN pattern: 5 letters + 4 digits + 1 letter (case-insensitive)
            uppercase = non_null_str.str.upper()
            pan_ratio = uppercase.str.fullmatch(r"[A-Z]{5}[0-9]{4}[A-Z]").mean()
            return pan_ratio >= 0.8
        
        return False

    def analyze_data_pattern(self, col_data: pd.Series, normalized_name: str) -> str:
        """
        Analyze data patterns to infer column meaning when name matching fails.
        """
        non_null = col_data.dropna()
        if len(non_null) == 0:
            return "unknown"
        
        non_null_str = non_null.astype(str)
        sample_value = str(non_null.iloc[0]) if len(non_null) > 0 else ""
        
        # Check for various patterns
        if re.match(r"^\d{6,18}$", sample_value):
            return "account_number"
        elif re.match(r"^[A-Za-z]{5}[0-9]{4}[A-Z]$", sample_value.upper()):
            return "pan"
        elif re.match(r"^\d{10}$", re.sub(r'[\s\-\(\)]', '', sample_value)):
            return "phone"
        elif pd.api.types.is_numeric_dtype(col_data):
            # Numeric column - determine specific type
            numeric_data = pd.to_numeric(col_data, errors='coerce')
            if (numeric_data >= 0).mean() > 0.9:  # Mostly positive
                if normalized_name in ["debit", "dr_amount", "withdraw", "outflow"]:
                    return "debit"
                elif normalized_name in ["credit", "cr_amount", "deposit", "inflow"]:
                    return "credit"
                elif normalized_name in ["amount", "amt", "value", "total"]:
                    return "amount"
                elif normalized_name in ["opening", "open_bal", "initial"]:
                    return "opening_balance"
                elif normalized_name in ["closing", "final", "current"]:
                    return "closing_balance"
                else:
                    return "numeric_field"
            else:
                return "numeric_field"
        elif pd.api.types.is_datetime64_any_dtype(col_data) or pd.to_datetime(col_data, errors='coerce').notna().mean() > 0.8:
            return "transaction_date"
        elif re.match(r"^[A-Za-z0-9]+$", sample_value) and len(sample_value) <= 20:
            # Alphanumeric, likely ID
            if any(x in normalized_name for x in ["id", "customer", "cust", "client"]):
                return "customer_id"
            elif any(x in normalized_name for x in ["transaction", "txn", "trans", "ref"]):
                return "transaction_id"
            else:
                return "id_field"
        else:
            # Text field - likely name or description
            if any(x in normalized_name for x in ["name", "customer", "cust", "person"]):
                return "customer_name"
            elif any(x in normalized_name for x in ["type", "mode", "category"]):
                return "transaction_type"
            else:
                return "text_field"

    def detect_industry_domain(self, df: pd.DataFrame, column_analysis: Dict) -> str:
        """
        Detect the industry/domain based on dataset patterns.
        """
        domain_scores = {
            "Banking": 0,
            "Finance": 0,
            "Insurance": 0,
            "Sales": 0,
            "Healthcare": 0,
            "HR": 0,
            "Retail": 0,
            "Government": 0
        }
        
        # Banking indicators
        banking_indicators = ["account", "debit", "credit", "transaction", "balance", "customer"]
        finance_indicators = ["amount", "rate", "interest", "investment", "portfolio"]
        insurance_indicators = ["policy", "claim", "premium", "coverage"]
        sales_indicators = ["product", "quantity", "price", "sale", "order"]
        healthcare_indicators = ["patient", "medical", "doctor", "treatment"]
        hr_indicators = ["employee", "salary", "department", "designation"]
        retail_indicators = ["product", "inventory", "store", "sales"]
        government_indicators = ["citizen", "beneficiary", "scheme", "allocation"]
        
        col_text = " ".join(df.columns).lower()
        
        # Score domains based on column names
        for domain, indicators in [("Banking", banking_indicators), 
                                  ("Finance", finance_indicators),
                                  ("Insurance", insurance_indicators),
                                  ("Sales", sales_indicators),
                                  ("Healthcare", healthcare_indicators),
                                  ("HR", hr_indicators),
                                  ("Retail", retail_indicators),
                                  ("Government", government_indicators)]:
            for indicator in indicators:
                if indicator in col_text:
                    domain_scores[domain] += 1
        
        # Return the domain with highest score
        return max(domain_scores, key=domain_scores.get) if max(domain_scores.values()) > 0 else "General"

    def apply_business_rules(self, df: pd.DataFrame, column_analysis: Dict) -> Dict[str, Any]:
        """
        Apply dynamic business rules based on detected column meanings and relationships.
        """
        results = []
        
        # Get column mappings
        col_meanings = {info["column_name"]: info["inferred_meaning"] for info in column_analysis.values()}
        
        # Identify related columns for cross-validation
        debit_cols = [col for col, meaning in col_meanings.items() if meaning == "debit"]
        credit_cols = [col for col, meaning in col_meanings.items() if meaning == "credit"]
        amount_cols = [col for col, meaning in col_meanings.items() if meaning == "amount"]
        transaction_type_cols = [col for col, meaning in col_meanings.items() if meaning == "transaction_type"]
        txn_date_cols = [col for col, meaning in col_meanings.items() if meaning == "transaction_date"]
        acc_num_cols = [col for col, meaning in col_meanings.items() if meaning == "account_number"]
        
        for col_name, info in column_analysis.items():
            meaning = info["inferred_meaning"]
            
            # Apply specific rules based on column meaning
            rule_name = f"validate_{meaning.replace(' ', '_').replace('-', '_')}"
            if meaning == "account_number":
                result = self.validate_account_number(df[col_name])
                result["applied_rule_names"] = ["Account Number Validation: digits only, 6-18 chars"]
            elif meaning == "customer_id":
                result = self.validate_customer_id(df[col_name])
                result["applied_rule_names"] = ["Customer ID Validation: alphanumeric, unique"]
            elif meaning == "customer_name":
                result = self.validate_customer_name(df[col_name])
                result["applied_rule_names"] = ["Customer Name Validation: text format"]
            elif meaning == "transaction_date":
                result = self.validate_transaction_date(df[col_name])
                result["applied_rule_names"] = ["Transaction Date Validation: valid date format"]
            elif meaning == "transaction_type":
                result = self.validate_transaction_type(df[col_name])
                result["applied_rule_names"] = ["Transaction Type Validation: allowed values"]
            elif meaning == "debit":
                # If both debit and credit exist, check mutual exclusivity
                credit_col = credit_cols[0] if credit_cols else None
                result = self.validate_debit(df[col_name], df[credit_col] if credit_col else None)
                result["applied_rule_names"] = ["Debit Validation: numeric, non-negative", "Mutual Exclusivity with Credit"]
            elif meaning == "credit":
                # If both debit and credit exist, check mutual exclusivity
                debit_col = debit_cols[0] if debit_cols else None
                result = self.validate_credit(df[col_name], df[debit_col] if debit_col else None)
                result["applied_rule_names"] = ["Credit Validation: numeric, non-negative", "Mutual Exclusivity with Debit"]
            elif meaning == "amount":
                result = self.validate_amount(df[col_name])
                result["applied_rule_names"] = ["Amount Validation: numeric format"]
            elif meaning == "transaction_id":
                result = self.validate_transaction_id(df[col_name])
                result["applied_rule_names"] = ["Transaction ID Validation: unique identifier"]
            else:
                # Generic validation for other columns
                result = self.generic_validation(df[col_name], meaning)
                result["applied_rule_names"] = [f"Generic Validation for {meaning}"]
            
            # Calculate confidence based on data quality
            confidence_score = self.calculate_confidence(info)
            
            result.update({
                "column_name": col_name,
                "inferred_meaning": meaning,
                "rules_applied": result.get("rules_applied", 1),
                "match_status": result.get("match_status", "MATCH"),
                "confidence_score": confidence_score
            })
            
            results.append(result)
        
        # Apply cross-column validations
        cross_validations = self.apply_cross_column_validations(df, col_meanings)
        results.extend(cross_validations)
        
        return results

    def calculate_confidence(self, column_info: Dict) -> float:
        """
        Calculate confidence score for column analysis.
        """
        null_penalty = column_info["null_percentage"] * 0.3
        unique_bonus = min(column_info["unique_percentage"] * 0.1, 20)  # Cap at 20%
        
        base_score = 100 - null_penalty + unique_bonus
        return max(0, min(100, base_score))  # Clamp between 0-100

    def validate_account_number(self, series: pd.Series) -> Dict[str, Any]:
        """Validate account number: digits only, 6-18 chars, unique not required."""
        violations = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return {
                "match_status": "FAIL",
                "violations": ["Column is empty"],
                "rules_applied": 1
            }
        
        # Check if all values are digits
        digit_only_mask = non_null.str.fullmatch(r"\d+")
        digit_only_ratio = digit_only_mask.mean()
        
        # Check length (6-18 chars)
        length_ok_mask = non_null.str.len().between(6, 18)
        length_ok_ratio = length_ok_mask.mean()
        
        if digit_only_ratio < 0.95:
            violations.append(f"Non-digit values found: {((1-digit_only_ratio)*100):.1f}%")
        
        if length_ok_ratio < 0.95:
            violations.append(f"Invalid length values: {((1-length_ok_ratio)*100):.1f}% (expected 6-18 chars)")
        
        match_status = "FAIL" if len(violations) > 0 else "MATCH"
        
        return {
            "match_status": match_status,
            "violations": violations,
            "rules_applied": 2
        }

    def validate_customer_id(self, series: pd.Series) -> Dict[str, Any]:
        """Validate customer ID: alphanumeric."""
        violations = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return {
                "match_status": "FAIL",
                "violations": ["Column is empty"],
                "rules_applied": 1
            }
        
        alphanumeric_mask = non_null.str.fullmatch(r"[A-Za-z0-9]+")
        alphanumeric_ratio = alphanumeric_mask.mean()
        
        if alphanumeric_ratio < 0.95:
            violations.append(f"Non-alphanumeric values: {((1-alphanumeric_ratio)*100):.1f}%")
        
        match_status = "FAIL" if len(violations) > 0 else "MATCH"
        
        return {
            "match_status": match_status,
            "violations": violations,
            "rules_applied": 1
        }

    def validate_customer_name(self, series: pd.Series) -> Dict[str, Any]:
        """Validate customer name: letters and spaces, minimum 3 chars."""
        violations = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return {
                "match_status": "FAIL",
                "violations": ["Column is empty"],
                "rules_applied": 1
            }
        
        # Check if values contain only letters and spaces
        letter_space_mask = non_null.str.fullmatch(r"[A-Za-z\s]+")
        letter_space_ratio = letter_space_mask.mean()
        
        # Check minimum length
        min_length_mask = (non_null.str.len() >= 3)
        min_length_ratio = min_length_mask.mean()
        
        if letter_space_ratio < 0.95:
            violations.append(f"Invalid characters in names: {((1-letter_space_ratio)*100):.1f}%")
        
        if min_length_ratio < 0.95:
            violations.append(f"Names too short (<3 chars): {((1-min_length_ratio)*100):.1f}%")
        
        match_status = "FAIL" if len(violations) > 0 else "MATCH"
        
        return {
            "match_status": match_status,
            "violations": violations,
            "rules_applied": 2
        }

    def validate_transaction_date(self, series: pd.Series) -> Dict[str, Any]:
        """Validate transaction date: must be valid date format."""
        violations = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return {
                "match_status": "FAIL",
                "violations": ["Column is empty"],
                "rules_applied": 1
            }
        
        # Try to parse dates
        parsed_dates = pd.to_datetime(non_null, errors="coerce")
        valid_date_mask = parsed_dates.notna()
        valid_date_ratio = valid_date_mask.mean()
        
        if valid_date_ratio < 0.95:
            violations.append(f"Invalid date format: {((1-valid_date_ratio)*100):.1f}%")
        
        match_status = "FAIL" if len(violations) > 0 else "MATCH"
        
        return {
            "match_status": match_status,
            "violations": violations,
            "rules_applied": 1
        }

    def validate_transaction_type(self, series: pd.Series) -> Dict[str, Any]:
        """Validate transaction type: must be from allowed values."""
        violations = []
        non_null = series.dropna().astype(str).str.upper().str.strip()
        
        if len(non_null) == 0:
            return {
                "match_status": "FAIL",
                "violations": ["Column is empty"],
                "rules_applied": 1
            }
        
        valid_mask = non_null.isin([v.upper() for v in self.allowed_transaction_types])
        valid_ratio = valid_mask.mean()
        
        if valid_ratio < 0.95:
            invalid_values = non_null[~valid_mask].unique().tolist()[:5]  # Show first 5 invalid values
            violations.append(f"Invalid transaction types: {invalid_values}")
        
        match_status = "FAIL" if len(violations) > 0 else "MATCH"
        
        return {
            "match_status": match_status,
            "violations": violations,
            "rules_applied": 1
        }

    def validate_debit(self, series: pd.Series, credit_series: Optional[pd.Series] = None) -> Dict[str, Any]:
        """Validate debit: numeric, >= 0, mutually exclusive with credit if both exist."""
        violations = []
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_null = numeric_series.dropna()
        
        if len(non_null) == 0:
            return {
                "match_status": "FAIL",
                "violations": ["Column is empty"],
                "rules_applied": 1
            }
        
        # Check if all values are numeric and non-negative
        non_negative_mask = (non_null >= 0)
        non_negative_ratio = non_negative_mask.mean()
        
        if non_negative_ratio < 0.95:
            violations.append(f"Negative values found: {((1-non_negative_ratio)*100):.1f}%")
        
        # Check mutual exclusivity with credit if credit column exists
        if credit_series is not None:
            credit_numeric = pd.to_numeric(credit_series, errors="coerce").fillna(0)
            both_positive_mask = (non_null > 0) & (credit_numeric > 0)
            both_positive_count = both_positive_mask.sum()
            
            if both_positive_count > len(series) * 0.1:  # More than 10% have both > 0
                violations.append(f"Warning: {both_positive_count} rows have both debit and credit > 0")
        
        match_status = "FAIL" if len(violations) > 0 else "MATCH"
        
        return {
            "match_status": match_status,
            "violations": violations,
            "rules_applied": 2
        }

    def validate_credit(self, series: pd.Series, debit_series: Optional[pd.Series] = None) -> Dict[str, Any]:
        """Validate credit: numeric, >= 0, mutually exclusive with debit if both exist."""
        violations = []
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_null = numeric_series.dropna()
        
        if len(non_null) == 0:
            return {
                "match_status": "FAIL",
                "violations": ["Column is empty"],
                "rules_applied": 1
            }
        
        # Check if all values are numeric and non-negative
        non_negative_mask = (non_null >= 0)
        non_negative_ratio = non_negative_mask.mean()
        
        if non_negative_ratio < 0.95:
            violations.append(f"Negative values found: {((1-non_negative_ratio)*100):.1f}%")
        
        # Check mutual exclusivity with debit if debit column exists
        if debit_series is not None:
            debit_numeric = pd.to_numeric(debit_series, errors="coerce").fillna(0)
            both_positive_mask = (non_null > 0) & (debit_numeric > 0)
            both_positive_count = both_positive_mask.sum()
            
            if both_positive_count > len(series) * 0.1:  # More than 10% have both > 0
                violations.append(f"Warning: {both_positive_count} rows have both credit and debit > 0")
        
        match_status = "FAIL" if len(violations) > 0 else "MATCH"
        
        return {
            "match_status": match_status,
            "violations": violations,
            "rules_applied": 2
        }

    def validate_amount(self, series: pd.Series) -> Dict[str, Any]:
        """Validate amount: numeric."""
        violations = []
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_null = numeric_series.dropna()
        
        if len(non_null) == 0:
            return {
                "match_status": "FAIL",
                "violations": ["Column is empty"],
                "rules_applied": 1
            }
        
        # Check if all values are numeric
        numeric_ratio = (numeric_series.notna()).mean()
        
        if numeric_ratio < 0.95:
            violations.append(f"Non-numeric values: {((1-numeric_ratio)*100):.1f}%")
        
        match_status = "FAIL" if len(violations) > 0 else "MATCH"
        
        return {
            "match_status": match_status,
            "violations": violations,
            "rules_applied": 1
        }

    def validate_transaction_id(self, series: pd.Series) -> Dict[str, Any]:
        """Validate transaction ID: alphanumeric, should be unique."""
        violations = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return {
                "match_status": "FAIL",
                "violations": ["Column is empty"],
                "rules_applied": 1
            }
        
        # Check if all values are alphanumeric
        alphanumeric_mask = non_null.str.fullmatch(r"[A-Za-z0-9]+")
        alphanumeric_ratio = alphanumeric_mask.mean()
        
        # Check uniqueness
        unique_ratio = non_null.nunique() / len(non_null) if len(non_null) > 0 else 0
        
        if alphanumeric_ratio < 0.95:
            violations.append(f"Non-alphanumeric values: {((1-alphanumeric_ratio)*100):.1f}%")
        
        if unique_ratio < 0.95:
            violations.append(f"Low uniqueness: {unique_ratio*100:.1f}% (should be unique per transaction)")
        
        match_status = "FAIL" if len(violations) > 0 else "MATCH"
        
        return {
            "match_status": match_status,
            "violations": violations,
            "rules_applied": 2
        }

    def generic_validation(self, series: pd.Series, meaning: str) -> Dict[str, Any]:
        """Generic validation for columns that don't have specific rules."""
        violations = []
        non_null = series.dropna()
        
        if len(non_null) == 0:
            return {
                "match_status": "PARTIAL",
                "violations": ["Column is empty"],
                "rules_applied": 0
            }
        
        return {
            "match_status": "MATCH",
            "violations": [],
            "rules_applied": 0
        }

    def apply_cross_column_validations(self, df: pd.DataFrame, col_meanings: Dict[str, str]) -> List[Dict[str, Any]]:
        """Apply validations that involve multiple columns."""
        cross_validations = []
        
        # Check for debit/credit mutual exclusivity across entire dataset
        debit_cols = [col for col, meaning in col_meanings.items() if meaning == "debit"]
        credit_cols = [col for col, meaning in col_meanings.items() if meaning == "credit"]
        
        if debit_cols and credit_cols:
            debit_series = pd.to_numeric(df[debit_cols[0]], errors="coerce").fillna(0)
            credit_series = pd.to_numeric(df[credit_cols[0]], errors="coerce").fillna(0)
            
            both_positive_mask = (debit_series > 0) & (credit_series > 0)
            both_positive_rows = both_positive_mask[both_positive_mask].index.tolist()
            
            if len(both_positive_rows) > 0:
                cross_validations.append({
                    "validation_type": "cross_column",
                    "rule_description": "Debit and credit should be mutually exclusive",
                    "violations": [f"Rows with both debit and credit > 0: {both_positive_rows[:10]}"],  # Limit to first 10
                    "affected_rows": both_positive_rows,
                    "match_status": "FAIL",
                    "rules_applied": 1,
                    "applied_rule_names": ["Cross-Column Validation: Debit/Credit Mutual Exclusivity"]
                })
        
        return cross_validations

    def generate_summary(self, validation_results: List[Dict], column_analysis: Dict, domain: str) -> Dict[str, Any]:
        """Generate overall dataset validation summary."""
        total_columns = len(column_analysis)
        detected_columns = len([r for r in validation_results if r.get("inferred_meaning") and r["inferred_meaning"] != "unknown"])
        rules_applied = sum(r.get("rules_applied", 0) for r in validation_results)
        matches = len([r for r in validation_results if r.get("match_status") == "MATCH"])
        partials = len([r for r in validation_results if r.get("match_status") == "PARTIAL"])
        fails = len([r for r in validation_results if r.get("match_status") == "FAIL"])
        
        # Collect all violations
        all_violations = []
        for result in validation_results:
            if result.get("violations"):
                for violation in result["violations"]:
                    all_violations.append({
                        "column": result.get("column_name", "unknown"),
                        "violation": violation
                    })
        
        return {
            "dataset_summary": {
                "total_columns": total_columns,
                "detected_columns": detected_columns,
                "rules_applied": rules_applied,
                "matches": matches,
                "partials": partials,
                "fails": fails,
                "domain": domain
            },
            "overall_compliance": (matches / total_columns * 100) if total_columns > 0 else 0,
            "violations_list": all_violations
        }

    def validate_dataset(self, csv_path: str) -> Dict[str, Any]:
        """
        Main validation function that orchestrates the entire process.
        
        Args:
            csv_path: Path to the CSV file to validate
            
        Returns:
            Dictionary containing comprehensive validation results
        """
        try:
            # Load the dataset
            df = pd.read_csv(csv_path)
            
            if df.empty:
                return {
                    "error": "Dataset is empty",
                    "summary": {"total_columns": 0, "detected_columns": 0, "rules_applied": 0}
                }
            
            # Step 1: Analyze column semantics
            column_analysis = self.analyze_column_semantics(df)
            
            # Step 2: Detect industry domain
            domain = self.detect_industry_domain(df, column_analysis)
            
            # Step 3: Apply business rules
            validation_results = self.apply_business_rules(df, column_analysis)
            
            # Step 4: Generate summary
            summary = self.generate_summary(validation_results, column_analysis, domain)
            
            # Step 5: Format output according to requirements
            formatted_results = []
            for col_name, analysis in column_analysis.items():
                # Find corresponding validation result
                val_result = next((r for r in validation_results if r.get("column_name") == col_name), {})
                
                # Calculate confidence score for this column if not already present
                if "confidence_score" not in analysis:
                    analysis["confidence_score"] = self.calculate_confidence(analysis)
                
                # Get applied business rule names
                applied_rules = val_result.get("applied_rule_names", [])
                
                formatted_results.append({
                    "column_name": col_name,
                    "inferred_meaning": analysis["inferred_meaning"],
                    "rules_applied": val_result.get("rules_applied", 0),
                    "match_status": val_result.get("match_status", "UNKNOWN"),
                    "confidence_score": f"{analysis['confidence_score']:.1f}%",
                    "sample_values": analysis["sample_values"],
                    "data_type": analysis["data_type"],
                    "violations": val_result.get("violations", []),
                    "applied_rules": applied_rules
                })
            
            return {
                "column_analysis": formatted_results,
                "dataset_summary": summary["dataset_summary"],
                "overall_compliance": f"{summary['overall_compliance']:.1f}%",
                "violations_list": summary["violations_list"],
                "detected_domain": domain,
                "total_records": len(df)
            }
            
        except Exception as e:
            return {
                "error": str(e),
                "summary": {"total_columns": 0, "detected_columns": 0, "rules_applied": 0}
            }


def print_validation_report(results: Dict[str, Any]):
    """
    Print a formatted validation report to console.
    """
    print("=" * 100)
    print("DATA VALIDATION AND BUSINESS RULES REPORT")
    print("=" * 100)
    
    if "error" in results:
        print(f"ERROR: {results['error']}")
        return
    
    print(f"Detected Domain: {results['detected_domain']}")
    print(f"Total Records: {results['total_records']}")
    print(f"Overall Compliance: {results['overall_compliance']}")
    print()
    
    print("COLUMN ANALYSIS:")
    print("-" * 100)
    print(f"{'Column Name':<20} {'Meaning':<20} {'Rules':<5} {'Status':<10} {'Confidence':<10} {'Sample Values'}")
    print("-" * 100)
    
    for col in results["column_analysis"]:
        sample_str = ", ".join(map(str, col["sample_values"][:2]))  # Show first 2 samples
        print(f"{col['column_name']:<20} {col['inferred_meaning']:<20} {col['rules_applied']:<5} "
              f"{col['match_status']:<10} {col['confidence_score']:<10} {sample_str}")
    
    print()
    print("DATASET SUMMARY:")
    print("-" * 100)
    summary = results["dataset_summary"]
    print(f"  Total Columns:       {summary['total_columns']}")
    print(f"  Detected Columns:    {summary['detected_columns']}")
    print(f"  Rules Applied:       {summary['rules_applied']}")
    print(f"  Matches:             {summary['matches']}")
    print(f"  Partials:            {summary['partials']}")
    print(f"  Fails:               {summary['fails']}")
    print()
    
    if results["violations_list"]:
        print("VIOLATIONS FOUND:")
        print("-" * 100)
        for i, violation in enumerate(results["violations_list"][:20], 1):  # Show first 20 violations
            print(f"  {i}. Column '{violation['column']}': {violation['violation']}")
        if len(results["violations_list"]) > 20:
            print(f"  ... and {len(results['violations_list']) - 20} more violations")
    else:
        print("NO VIOLATIONS FOUND - Dataset validation passed!")
    
    print("=" * 100)


if __name__ == "__main__":
    # Example usage
    validator = DataValidationEngine()
    results = validator.validate_dataset("bank.csv")
    print_validation_report(results)