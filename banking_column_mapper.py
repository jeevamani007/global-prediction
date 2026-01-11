"""
Banking Domain Column Mapping Engine

Identifies column purposes using DATA PATTERNS (not just column names).
Assigns confidence scores (0-100%) for each detected column.
Applies banking-specific business rules.
Returns structured output for UI display based on confidence thresholds.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import re


class BankingColumnMapper:
    """
    Banking Domain Column Mapping Engine
    
    Rules:
    - ACCOUNT_NUMBER: Digits only, length 6-18, repeats across rows
    - TRANSACTION_ID: Alphanumeric, ≥90% unique, not used in arithmetic
    - TRANSACTION_AMOUNT: Numeric, used in debit/credit or balance calculation, NOT unique
    - DEBIT/CREDIT: Numeric, only one > 0 per row, impacts balance
    - OPENING_BALANCE/CLOSING_BALANCE: Numeric, Closing = Opening + Credit - Debit
    - TRANSACTION_DATE: Date format, chronological sequence
    """
    
    def __init__(self):
        """Initialize the Banking Column Mapper."""
        self.required_purposes = [
            "ACCOUNT_NUMBER",
            "TRANSACTION_ID",
            "TRANSACTION_AMOUNT",
            "DEBIT",
            "CREDIT",
            "OPENING_BALANCE",
            "CLOSING_BALANCE",
            "TRANSACTION_DATE"
        ]
    
    def normalize(self, text):
        """Normalize text for matching."""
        return str(text).lower().replace(" ", "").replace("_", "").replace("-", "")
    
    def check_account_number_pattern(self, series):
        """
        Check if series matches ACCOUNT_NUMBER pattern:
        - Digits only
        - Length 6-18
        - Repeats across rows (not unique per row, but unique per account)
        """
        try:
            non_null = series.dropna().astype(str)
            if len(non_null) == 0:
                return False, 0.0
            
            # Check if digits only
            digit_only_ratio = non_null.str.fullmatch(r"\d+").mean()
            
            # Check length 6-18
            lengths = non_null.str.len()
            length_ok_ratio = lengths.between(6, 18).mean()
            
            # Check uniqueness (should repeat across rows - not 100% unique)
            unique_ratio = non_null.nunique() / len(non_null)
            # Account numbers repeat, so unique_ratio should be < 1.0 but reasonable
            # Typically 0.1 to 0.9 (10% to 90% unique depending on data)
            
            # Calculate confidence based on pattern match
            confidence = 0.0
            if digit_only_ratio >= 0.9 and length_ok_ratio >= 0.9:
                confidence = 90.0
            elif digit_only_ratio >= 0.8 and length_ok_ratio >= 0.8:
                confidence = 75.0
            elif digit_only_ratio >= 0.7 and length_ok_ratio >= 0.7:
                confidence = 60.0
            
            # Penalize if too unique (account numbers should repeat)
            if unique_ratio > 0.95:
                confidence *= 0.7  # Reduce confidence if too unique
            
            return True, confidence
        except Exception:
            return False, 0.0
    
    def check_transaction_id_pattern(self, series):
        """
        Check if series matches TRANSACTION_ID pattern:
        - Alphanumeric
        - ≥90% unique
        - Not used in arithmetic (typically string-like)
        """
        try:
            non_null = series.dropna().astype(str)
            if len(non_null) == 0:
                return False, 0.0
            
            # Check if alphanumeric
            alphanumeric_ratio = non_null.str.fullmatch(r"[A-Za-z0-9]+").mean()
            
            # Check uniqueness (should be ≥90% unique)
            unique_ratio = non_null.nunique() / len(non_null)
            
            # Check if numeric (transaction IDs are usually NOT purely numeric for transactions)
            # But can be alphanumeric
            numeric_ratio = pd.to_numeric(non_null, errors="coerce").notna().mean()
            
            confidence = 0.0
            if unique_ratio >= 0.9 and alphanumeric_ratio >= 0.9:
                confidence = 95.0
            elif unique_ratio >= 0.85 and alphanumeric_ratio >= 0.85:
                confidence = 80.0
            elif unique_ratio >= 0.75 and alphanumeric_ratio >= 0.75:
                confidence = 65.0
            
            # Penalize if mostly numeric and not unique enough
            if numeric_ratio > 0.9 and unique_ratio < 0.9:
                confidence *= 0.6
            
            return True, confidence
        except Exception:
            return False, 0.0
    
    def check_transaction_amount_pattern(self, series, df=None, debit_col=None, credit_col=None, 
                                        opening_col=None, closing_col=None):
        """
        Check if series matches TRANSACTION_AMOUNT pattern:
        - Numeric
        - Used in debit/credit or balance calculation
        - Can repeat (NOT unique)
        """
        try:
            numeric_series = pd.to_numeric(series, errors="coerce")
            non_null = numeric_series.dropna()
            if len(non_null) == 0:
                return False, 0.0
            
            numeric_ratio = len(non_null) / len(series) if len(series) > 0 else 0
            
            # Check uniqueness (should NOT be unique - amounts repeat)
            unique_ratio = non_null.nunique() / len(non_null)
            
            confidence = 0.0
            if numeric_ratio >= 0.9:
                # Check if used in balance calculations
                usage_bonus = 0.0
                if df is not None:
                    # Check if matches debit or credit columns
                    if debit_col is not None:
                        try:
                            debit_series = pd.to_numeric(df[debit_col], errors="coerce")
                            match_ratio = (non_null.values == debit_series.values).mean()
                            if match_ratio > 0.8:
                                usage_bonus = 10.0
                        except:
                            pass
                    
                    if credit_col is not None:
                        try:
                            credit_series = pd.to_numeric(df[credit_col], errors="coerce")
                            match_ratio = (non_null.values == credit_series.values).mean()
                            if match_ratio > 0.8:
                                usage_bonus = 10.0
                        except:
                            pass
                
                # Transaction amounts should NOT be unique
                if unique_ratio < 0.95:  # Not too unique
                    confidence = 85.0 + usage_bonus
                else:
                    confidence = 70.0 + usage_bonus
                
                # Penalize if too unique (transaction amounts repeat)
                if unique_ratio > 0.98:
                    confidence *= 0.8
            
            return True, confidence
        except Exception:
            return False, 0.0
    
    def check_debit_pattern(self, series):
        """
        Check if series matches DEBIT pattern:
        - Numeric
        - Only one > 0 per row (mutually exclusive with credit)
        """
        try:
            numeric_series = pd.to_numeric(series, errors="coerce")
            non_null = numeric_series.dropna()
            if len(non_null) == 0:
                return False, 0.0
            
            numeric_ratio = len(non_null) / len(series) if len(series) > 0 else 0
            
            # Check if non-negative (debits are typically non-negative)
            non_negative_ratio = (non_null >= 0).mean()
            
            # Check if has zeros (debits can be 0 when credit > 0)
            zero_ratio = (non_null == 0).mean()
            
            confidence = 0.0
            if numeric_ratio >= 0.9 and non_negative_ratio >= 0.95:
                confidence = 85.0
            elif numeric_ratio >= 0.8 and non_negative_ratio >= 0.9:
                confidence = 70.0
            elif numeric_ratio >= 0.7 and non_negative_ratio >= 0.85:
                confidence = 55.0
            
            # Bonus if has zeros (suggests mutual exclusivity with credit)
            if zero_ratio > 0.3:
                confidence = min(confidence + 5.0, 100.0)
            
            return True, confidence
        except Exception:
            return False, 0.0
    
    def check_credit_pattern(self, series):
        """
        Check if series matches CREDIT pattern:
        - Numeric
        - Only one > 0 per row (mutually exclusive with debit)
        """
        try:
            numeric_series = pd.to_numeric(series, errors="coerce")
            non_null = numeric_series.dropna()
            if len(non_null) == 0:
                return False, 0.0
            
            numeric_ratio = len(non_null) / len(series) if len(series) > 0 else 0
            
            # Check if non-negative (credits are typically non-negative)
            non_negative_ratio = (non_null >= 0).mean()
            
            # Check if has zeros (credits can be 0 when debit > 0)
            zero_ratio = (non_null == 0).mean()
            
            confidence = 0.0
            if numeric_ratio >= 0.9 and non_negative_ratio >= 0.95:
                confidence = 85.0
            elif numeric_ratio >= 0.8 and non_negative_ratio >= 0.9:
                confidence = 70.0
            elif numeric_ratio >= 0.7 and non_negative_ratio >= 0.85:
                confidence = 55.0
            
            # Bonus if has zeros (suggests mutual exclusivity with debit)
            if zero_ratio > 0.3:
                confidence = min(confidence + 5.0, 100.0)
            
            return True, confidence
        except Exception:
            return False, 0.0
    
    def check_opening_balance_pattern(self, series, df=None, closing_col=None, 
                                     debit_col=None, credit_col=None):
        """
        Check if series matches OPENING_BALANCE pattern:
        - Numeric
        - Closing = Opening + Credit - Debit
        """
        try:
            numeric_series = pd.to_numeric(series, errors="coerce")
            non_null = numeric_series.dropna()
            if len(non_null) == 0:
                return False, 0.0
            
            numeric_ratio = len(non_null) / len(series) if len(series) > 0 else 0
            non_negative_ratio = (non_null >= 0).mean()
            
            confidence = 0.0
            if numeric_ratio >= 0.9 and non_negative_ratio >= 0.9:
                confidence = 75.0
            elif numeric_ratio >= 0.8 and non_negative_ratio >= 0.85:
                confidence = 60.0
            elif numeric_ratio >= 0.7 and non_negative_ratio >= 0.8:
                confidence = 50.0
            
            # Check balance formula if other columns available
            if df is not None and closing_col and debit_col and credit_col:
                try:
                    opening_vals = pd.to_numeric(df[series.name], errors="coerce").fillna(0)
                    closing_vals = pd.to_numeric(df[closing_col], errors="coerce").fillna(0)
                    debit_vals = pd.to_numeric(df[debit_col], errors="coerce").fillna(0)
                    credit_vals = pd.to_numeric(df[credit_col], errors="coerce").fillna(0)
                    
                    calculated_closing = opening_vals + credit_vals - debit_vals
                    diff = abs(closing_vals - calculated_closing)
                    tolerance = closing_vals.abs() * 0.01 + 0.01
                    matches = (diff <= tolerance).sum()
                    match_ratio = matches / len(df) if len(df) > 0 else 0
                    
                    if match_ratio >= 0.9:
                        confidence = min(confidence + 20.0, 100.0)
                    elif match_ratio >= 0.8:
                        confidence = min(confidence + 15.0, 100.0)
                    elif match_ratio >= 0.7:
                        confidence = min(confidence + 10.0, 100.0)
                except Exception:
                    pass
            
            return True, confidence
        except Exception:
            return False, 0.0
    
    def check_closing_balance_pattern(self, series, df=None, opening_col=None,
                                     debit_col=None, credit_col=None):
        """
        Check if series matches CLOSING_BALANCE pattern:
        - Numeric
        - Closing = Opening + Credit - Debit
        """
        try:
            numeric_series = pd.to_numeric(series, errors="coerce")
            non_null = numeric_series.dropna()
            if len(non_null) == 0:
                return False, 0.0
            
            numeric_ratio = len(non_null) / len(series) if len(series) > 0 else 0
            non_negative_ratio = (non_null >= 0).mean()
            
            confidence = 0.0
            if numeric_ratio >= 0.9 and non_negative_ratio >= 0.9:
                confidence = 75.0
            elif numeric_ratio >= 0.8 and non_negative_ratio >= 0.85:
                confidence = 60.0
            elif numeric_ratio >= 0.7 and non_negative_ratio >= 0.8:
                confidence = 50.0
            
            # Check balance formula if other columns available
            if df is not None and opening_col and debit_col and credit_col:
                try:
                    opening_vals = pd.to_numeric(df[opening_col], errors="coerce").fillna(0)
                    closing_vals = pd.to_numeric(df[series.name], errors="coerce").fillna(0)
                    debit_vals = pd.to_numeric(df[debit_col], errors="coerce").fillna(0)
                    credit_vals = pd.to_numeric(df[credit_col], errors="coerce").fillna(0)
                    
                    calculated_closing = opening_vals + credit_vals - debit_vals
                    diff = abs(closing_vals - calculated_closing)
                    tolerance = closing_vals.abs() * 0.01 + 0.01
                    matches = (diff <= tolerance).sum()
                    match_ratio = matches / len(df) if len(df) > 0 else 0
                    
                    if match_ratio >= 0.9:
                        confidence = min(confidence + 20.0, 100.0)
                    elif match_ratio >= 0.8:
                        confidence = min(confidence + 15.0, 100.0)
                    elif match_ratio >= 0.7:
                        confidence = min(confidence + 10.0, 100.0)
                except Exception:
                    pass
            
            return True, confidence
        except Exception:
            return False, 0.0
    
    def check_customer_id_pattern(self, series):
        """
        Check if series matches CUSTOMER_ID pattern:
        - Alphanumeric
        - Length 3-10
        - Pattern: letters followed by numbers (e.g., C001, C002)
        """
        try:
            non_null = series.dropna().astype(str)
            if len(non_null) == 0:
                return False, 0.0
            
            # Check if alphanumeric
            alphanumeric_ratio = non_null.str.fullmatch(r"[A-Za-z0-9]+").mean()
            
            # Check length 3-10
            length_ok_ratio = non_null.str.len().between(3, 10).mean()
            
            # Pattern: letters followed by numbers (e.g., C001, C002)
            pattern_match = non_null.str.fullmatch(r"[A-Za-z]{1,2}\d{1,4}").mean()
            
            confidence = 0.0
            # CRITICAL: Require pattern_match to be high (letters followed by numbers)
            # Pure numeric values should NOT match customer_id pattern
            if pattern_match >= 0.8 and length_ok_ratio >= 0.9 and alphanumeric_ratio >= 0.9:
                confidence = 90.0
            elif pattern_match >= 0.5 and length_ok_ratio >= 0.8 and alphanumeric_ratio >= 0.8:
                confidence = 75.0
            elif pattern_match >= 0.3 and length_ok_ratio >= 0.7 and alphanumeric_ratio >= 0.7:
                confidence = 60.0
            elif pattern_match >= 0.2 and length_ok_ratio >= 0.6 and alphanumeric_ratio >= 0.6:
                confidence = 50.0
            else:
                # Doesn't match pattern (pattern_match < 0.2) - should be HIDDEN
                confidence = 0.0  # Below 50% threshold - will be HIDDEN
            
            # Only return True if confidence >= 50 (pattern matches)
            if confidence >= 50.0:
                return True, confidence
            else:
                return False, 0.0
        except Exception:
            return False, 0.0
    
    def check_transaction_date_pattern(self, series):
        """
        Check if series matches TRANSACTION_DATE pattern:
        - Date format
        - Chronological sequence (optional check)
        - NOT purely numeric (dates should have separators or be string-like)
        """
        try:
            non_null = series.dropna().astype(str)
            if len(non_null) == 0:
                return False, 0.0
            
            # Check if purely numeric - if yes, it's likely NOT a date
            numeric_only = pd.to_numeric(non_null, errors="coerce").notna().mean()
            if numeric_only > 0.9:
                # Purely numeric values are unlikely to be dates in banking context
                return False, 0.0
            
            # Check if contains date separators (/, -, or spaces) or looks like date format
            has_separators = non_null.str.contains(r'[/-]|^\d{4}-\d{2}-\d{2}', na=False, regex=True).mean()
            
            # Try to parse as date
            date_parsed = pd.to_datetime(non_null, errors="coerce")
            valid_date_ratio = date_parsed.notna().mean()
            
            confidence = 0.0
            # Require both valid date parsing AND separators/format
            if valid_date_ratio >= 0.95 and has_separators >= 0.7:
                confidence = 95.0
            elif valid_date_ratio >= 0.9 and has_separators >= 0.6:
                confidence = 85.0
            elif valid_date_ratio >= 0.8 and has_separators >= 0.5:
                confidence = 75.0
            elif valid_date_ratio >= 0.7 and has_separators >= 0.4:
                confidence = 60.0
            
            # Bonus if chronological (dates are in order)
            if valid_date_ratio >= 0.7 and confidence > 0:
                try:
                    date_parsed_clean = date_parsed.dropna()
                    if len(date_parsed_clean) > 1:
                        sorted_indices = date_parsed_clean.sort_values().index
                        original_indices = date_parsed_clean.index
                        # Check if mostly in order
                        if len(sorted_indices) > 0:
                            confidence = min(confidence + 5.0, 100.0)
                except Exception:
                    pass
            
            if confidence > 0:
                return True, confidence
            else:
                return False, 0.0
        except Exception:
            return False, 0.0
    
    def check_debit_credit_exclusivity(self, debit_series, credit_series):
        """
        Check if debit and credit are mutually exclusive (only one > 0 per row).
        Returns bonus confidence score.
        """
        try:
            debit_numeric = pd.to_numeric(debit_series, errors="coerce").fillna(0)
            credit_numeric = pd.to_numeric(credit_series, errors="coerce").fillna(0)
            
            both_positive = ((debit_numeric > 0) & (credit_numeric > 0)).sum()
            total = len(debit_series)
            both_positive_ratio = both_positive / total if total > 0 else 1.0
            
            # Lower ratio is better (mutually exclusive)
            if both_positive_ratio < 0.05:  # Less than 5% have both > 0
                return 15.0
            elif both_positive_ratio < 0.1:  # Less than 10% have both > 0
                return 10.0
            elif both_positive_ratio < 0.2:  # Less than 20% have both > 0
                return 5.0
            else:
                return 0.0
        except Exception:
            return 0.0
    
    def map_columns(self, csv_path):
        """
        Main function to map columns in a banking dataset.
        
        Returns structured output with:
        - Column Name
        - Identified Purpose
        - Confidence %
        - Status (CONFIRMED / POSSIBLE / HIDDEN)
        - Reason
        """
        try:
            # Load dataset
            df = pd.read_csv(csv_path)
            
            if df.empty:
                return {
                    "error": "Dataset is empty",
                    "columns": []
                }
            
            # First pass: Identify all columns with their patterns
            column_analyses = {}
            
            for col in df.columns:
                series = df[col]
                norm_col = self.normalize(col)
                
                # Initialize analysis for this column
                column_analyses[col] = {
                    "purpose": None,
                    "confidence": 0.0,
                    "scores": {}
                }
                
                # Check each purpose pattern (order matters - check most specific first)
                # TRANSACTION_DATE (check before numeric patterns)
                matches, conf = self.check_transaction_date_pattern(series)
                if matches and conf > 0:
                    column_analyses[col]["scores"]["TRANSACTION_DATE"] = conf
                
                # TRANSACTION_ID (check before account_number to avoid confusion)
                matches, conf = self.check_transaction_id_pattern(series)
                if matches and conf > 0:
                    column_analyses[col]["scores"]["TRANSACTION_ID"] = conf
                
                # ACCOUNT_NUMBER
                matches, conf = self.check_account_number_pattern(series)
                if matches and conf > 0:
                    column_analyses[col]["scores"]["ACCOUNT_NUMBER"] = conf
                
                # CUSTOMER_ID (check after account_number to avoid confusion)
                matches, conf = self.check_customer_id_pattern(series)
                if matches and conf > 0:
                    column_analyses[col]["scores"]["CUSTOMER_ID"] = conf
                
                # DEBIT
                matches, conf = self.check_debit_pattern(series)
                if matches and conf > 0:
                    column_analyses[col]["scores"]["DEBIT"] = conf
                
                # CREDIT
                matches, conf = self.check_credit_pattern(series)
                if matches and conf > 0:
                    column_analyses[col]["scores"]["CREDIT"] = conf
                
                # OPENING_BALANCE
                matches, conf = self.check_opening_balance_pattern(series)
                if matches and conf > 0:
                    column_analyses[col]["scores"]["OPENING_BALANCE"] = conf
                
                # CLOSING_BALANCE
                matches, conf = self.check_closing_balance_pattern(series)
                if matches and conf > 0:
                    column_analyses[col]["scores"]["CLOSING_BALANCE"] = conf
            
            # Second pass: Identify TRANSACTION_AMOUNT (needs context from debit/credit)
            # Also refine balance calculations
            debit_col = None
            credit_col = None
            opening_col = None
            closing_col = None
            
            for col, analysis in column_analyses.items():
                if analysis["scores"]:
                    best_purpose = max(analysis["scores"].items(), key=lambda x: x[1])
                    if best_purpose[0] == "DEBIT" and best_purpose[1] >= 70:
                        debit_col = col
                    elif best_purpose[0] == "CREDIT" and best_purpose[1] >= 70:
                        credit_col = col
                    elif best_purpose[0] == "OPENING_BALANCE" and best_purpose[1] >= 70:
                        opening_col = col
                    elif best_purpose[0] == "CLOSING_BALANCE" and best_purpose[1] >= 70:
                        closing_col = col
            
            # Check TRANSACTION_AMOUNT for columns that might be transaction_amount
            for col in df.columns:
                series = df[col]
                norm_col = self.normalize(col)
                
                # Check if column name suggests transaction_amount
                if "amount" in norm_col and "transaction" in norm_col:
                    # Check if numeric
                    numeric_series = pd.to_numeric(series, errors="coerce")
                    if numeric_series.notna().mean() >= 0.8:
                        matches, conf = self.check_transaction_amount_pattern(
                            series, df, debit_col, credit_col, opening_col, closing_col
                        )
                        if matches:
                            # Add TRANSACTION_AMOUNT score, it will compete with other scores
                            column_analyses[col]["scores"]["TRANSACTION_AMOUNT"] = conf
                elif not column_analyses[col]["scores"]:
                    # For columns without scores, check if they might be transaction_amount
                    numeric_series = pd.to_numeric(series, errors="coerce")
                    if numeric_series.notna().mean() >= 0.8:
                        matches, conf = self.check_transaction_amount_pattern(
                            series, df, debit_col, credit_col, opening_col, closing_col
                        )
                        if matches:
                            column_analyses[col]["scores"]["TRANSACTION_AMOUNT"] = conf
            
            # Refine balance calculations with context
            for col, analysis in column_analyses.items():
                series = df[col]
                if "OPENING_BALANCE" in analysis["scores"]:
                    matches, conf = self.check_opening_balance_pattern(
                        series, df, closing_col, debit_col, credit_col
                    )
                    if matches:
                        analysis["scores"]["OPENING_BALANCE"] = conf
                
                if "CLOSING_BALANCE" in analysis["scores"]:
                    matches, conf = self.check_closing_balance_pattern(
                        series, df, opening_col, debit_col, credit_col
                    )
                    if matches:
                        analysis["scores"]["CLOSING_BALANCE"] = conf
            
            # Add bonus for debit/credit exclusivity
            if debit_col and credit_col:
                bonus = self.check_debit_credit_exclusivity(df[debit_col], df[credit_col])
                if "DEBIT" in column_analyses[debit_col]["scores"]:
                    column_analyses[debit_col]["scores"]["DEBIT"] = min(
                        column_analyses[debit_col]["scores"]["DEBIT"] + bonus, 100.0
                    )
                if "CREDIT" in column_analyses[credit_col]["scores"]:
                    column_analyses[credit_col]["scores"]["CREDIT"] = min(
                        column_analyses[credit_col]["scores"]["CREDIT"] + bonus, 100.0
                    )
            
            # Third pass: Assign best purpose to each column
            # CRITICAL: Ensure transaction_amount is NEVER classified as transaction_id
            for col, analysis in column_analyses.items():
                norm_col = self.normalize(col)
                
                if analysis["scores"]:
                    # CRITICAL RULE: Remove TRANSACTION_ID if column contains "amount" 
                    # (transaction_amount should NEVER be transaction_id)
                    if "amount" in norm_col and "TRANSACTION_ID" in analysis["scores"]:
                        del analysis["scores"]["TRANSACTION_ID"]
                    
                    # CRITICAL RULE: Remove TRANSACTION_DATE if column is clearly numeric
                    # (dates should not match purely numeric columns)
                    if "TRANSACTION_DATE" in analysis["scores"]:
                        numeric_series = pd.to_numeric(df[col], errors="coerce")
                        if numeric_series.notna().mean() > 0.9:
                            # Check if it's actually a date by looking at string format
                            non_null_str = df[col].dropna().astype(str)
                            has_date_format = non_null_str.str.contains(r'[/-]|^\d{4}-\d{2}-\d{2}', na=False, regex=True).mean()
                            if has_date_format < 0.5:  # Doesn't look like date format
                                del analysis["scores"]["TRANSACTION_DATE"]
                    
                    # Use column name hints to break ties and improve accuracy
                    # Check column name for specific keywords
                    if "debit" in norm_col and "DEBIT" in analysis["scores"]:
                        analysis["scores"]["DEBIT"] += 15.0  # Bonus for name match
                        analysis["scores"]["DEBIT"] = min(analysis["scores"]["DEBIT"], 100.0)
                    if "credit" in norm_col and "CREDIT" in analysis["scores"]:
                        analysis["scores"]["CREDIT"] += 15.0  # Bonus for name match
                        analysis["scores"]["CREDIT"] = min(analysis["scores"]["CREDIT"], 100.0)
                    if "opening" in norm_col and "OPENING_BALANCE" in analysis["scores"]:
                        analysis["scores"]["OPENING_BALANCE"] += 15.0
                        analysis["scores"]["OPENING_BALANCE"] = min(analysis["scores"]["OPENING_BALANCE"], 100.0)
                    if ("closing" in norm_col or "current" in norm_col) and "CLOSING_BALANCE" in analysis["scores"]:
                        analysis["scores"]["CLOSING_BALANCE"] += 15.0
                        analysis["scores"]["CLOSING_BALANCE"] = min(analysis["scores"]["CLOSING_BALANCE"], 100.0)
                    if "amount" in norm_col and "TRANSACTION_AMOUNT" in analysis["scores"]:
                        analysis["scores"]["TRANSACTION_AMOUNT"] += 15.0
                        analysis["scores"]["TRANSACTION_AMOUNT"] = min(analysis["scores"]["TRANSACTION_AMOUNT"], 100.0)
                        # Remove DEBIT/CREDIT from transaction_amount candidates
                        if "DEBIT" in analysis["scores"]:
                            analysis["scores"]["DEBIT"] -= 20.0
                        if "CREDIT" in analysis["scores"]:
                            analysis["scores"]["CREDIT"] -= 20.0
                        # Remove balance purposes from transaction_amount
                        if "OPENING_BALANCE" in analysis["scores"]:
                            analysis["scores"]["OPENING_BALANCE"] -= 20.0
                        if "CLOSING_BALANCE" in analysis["scores"]:
                            analysis["scores"]["CLOSING_BALANCE"] -= 20.0
                    # CUSTOMER_ID: Only show if pattern matches (confidence >= 50%)
                    # If column name suggests customer_id but pattern doesn't match, hide it
                    if ("customer" in norm_col or "cust" in norm_col):
                        if "CUSTOMER_ID" in analysis["scores"]:
                            # Give bonus if name matches and pattern matches
                            analysis["scores"]["CUSTOMER_ID"] += 10.0
                            analysis["scores"]["CUSTOMER_ID"] = min(analysis["scores"]["CUSTOMER_ID"], 100.0)
                        else:
                            # Column name suggests customer_id but pattern doesn't match - hide it
                            # Remove all other scores so it becomes UNKNOWN (HIDDEN)
                            analysis["scores"] = {}
                    
                    # Remove negative scores
                    analysis["scores"] = {k: max(0, v) for k, v in analysis["scores"].items() if v > 0}
                    
                    # Get best purpose (highest score)
                    if analysis["scores"]:
                        best_purpose = max(analysis["scores"].items(), key=lambda x: x[1])
                        analysis["purpose"] = best_purpose[0]
                        analysis["confidence"] = best_purpose[1]
                    else:
                        analysis["purpose"] = "UNKNOWN"
                        analysis["confidence"] = 0.0
                else:
                    analysis["purpose"] = "UNKNOWN"
                    analysis["confidence"] = 0.0
            
            # Build output
            results = []
            for col, analysis in column_analyses.items():
                confidence = analysis["confidence"]
                purpose = analysis["purpose"] if analysis["purpose"] else "UNKNOWN"
                
                # Determine status based on confidence
                if confidence >= 70:
                    status = "CONFIRMED"
                elif confidence >= 50:
                    status = "POSSIBLE"
                else:
                    status = "HIDDEN"
                
                # Generate reason
                reason_parts = []
                if purpose != "UNKNOWN":
                    reason_parts.append(f"Pattern matches {purpose}")
                if confidence >= 90:
                    reason_parts.append("High pattern confidence")
                elif confidence >= 70:
                    reason_parts.append("Good pattern match")
                elif confidence >= 50:
                    reason_parts.append("Moderate pattern match")
                else:
                    reason_parts.append("Low pattern confidence")
                
                reason = ". ".join(reason_parts) if reason_parts else "Unable to identify pattern"
                
                results.append({
                    "column_name": col,
                    "identified_purpose": purpose,
                    "confidence_percentage": round(confidence, 2),
                    "status": status,
                    "reason": reason,
                    "all_scores": analysis["scores"]
                })
            
            # Sort by confidence (highest first)
            results.sort(key=lambda x: x["confidence_percentage"], reverse=True)
            
            return {
                "columns": results,
                "total_columns": len(df.columns),
                "confirmed_count": len([r for r in results if r["status"] == "CONFIRMED"]),
                "possible_count": len([r for r in results if r["status"] == "POSSIBLE"]),
                "hidden_count": len([r for r in results if r["status"] == "HIDDEN"])
            }
        
        except Exception as e:
            return {
                "error": str(e),
                "columns": []
            }
