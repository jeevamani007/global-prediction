"""
Core Banking Dataset Validation Engine

A comprehensive validation engine that:
1. Detects industry and dataset purpose
2. Identifies banking columns
3. Applies CRITICAL business rules
4. Applies WARNING rules
5. Makes final decision (REJECT / PASS WITH WARNINGS / PASS)
6. Returns detailed validation results with row-level violations
"""

import pandas as pd
import numpy as np
from datetime import datetime
import re
from typing import Dict, List, Tuple, Optional


class CoreBankingValidator:
    """Core Banking Dataset Validation Engine"""
    
    # Allowed transaction types
    ALLOWED_TRANSACTION_TYPES = ["credit", "debit", "deposit", "withdraw", "withdrawal", "transfer"]
    
    # Account number regex pattern
    ACCOUNT_NUMBER_PATTERN = re.compile(r'^[0-9]{6,18}$')
    
    def __init__(self):
        """Initialize the validator"""
        pass
    
    def normalize_column_name(self, col_name: str) -> str:
        """Normalize column name for matching"""
        return str(col_name).lower().strip().replace(" ", "_").replace("-", "_")
    
    def detect_industry_and_purpose(self, df: pd.DataFrame) -> Dict:
        """
        STEP 1: Detect industry and dataset purpose
        
        If account, transaction, balance columns exist → Banking domain
        """
        detected_columns = []
        banking_keywords = [
            "account", "transaction", "balance", "debit", "credit", 
            "customer", "deposit", "withdraw", "transfer"
        ]
        
        # Check column names for banking keywords
        for col in df.columns:
            norm_col = self.normalize_column_name(col)
            if any(keyword in norm_col for keyword in banking_keywords):
                detected_columns.append(col)
        
        # Check for banking-specific patterns in data
        has_account_like = False
        has_transaction_like = False
        has_balance_like = False
        
        for col in df.columns:
            norm_col = self.normalize_column_name(col)
            series = df[col].dropna()
            
            if len(series) == 0:
                continue
            
            # Check for account number pattern
            if any(kw in norm_col for kw in ["account", "acc", "acct"]):
                if series.astype(str).str.match(r'^\d{6,18}$').mean() > 0.5:
                    has_account_like = True
            
            # Check for transaction pattern
            if any(kw in norm_col for kw in ["transaction", "txn", "trans"]):
                has_transaction_like = True
            
            # Check for balance pattern
            if any(kw in norm_col for kw in ["balance", "bal"]):
                if pd.to_numeric(series, errors="coerce").notna().mean() > 0.5:
                    has_balance_like = True
        
        # Determine domain and confidence
        if has_account_like and (has_transaction_like or has_balance_like):
            domain = "Banking"
            confidence = 95.0
            purpose = "Banking Transaction Dataset"
        elif has_account_like or has_balance_like:
            domain = "Banking"
            confidence = 75.0
            purpose = "Banking Dataset (Partial)"
        else:
            domain = "Unknown"
            confidence = 0.0
            purpose = "Not a banking dataset"
        
        return {
            "domain": domain,
            "confidence": confidence,
            "purpose": purpose,
            "detected_columns": detected_columns,
            "has_account_like": has_account_like,
            "has_transaction_like": has_transaction_like,
            "has_balance_like": has_balance_like
        }
    
    def identify_columns(self, df: pd.DataFrame) -> Dict[str, Optional[str]]:
        """
        STEP 2: Column identification
        
        Identify: account_number, customer_id, transaction_id, transaction_date,
        transaction_type, debit, credit, opening_balance, closing_balance,
        account_type, account_status, phone
        
        NOTE: KYC columns are explicitly ignored (KYC rules removed)
        """
        column_roles = {}
        
        for col in df.columns:
            norm_col = self.normalize_column_name(col)
            
            # CRITICAL: Skip KYC columns completely (KYC rules removed)
            if "kyc" in norm_col:
                column_roles[col] = None
                continue
            
            role = None
            
            # Phone (check early to avoid conflicts)
            if any(kw in norm_col for kw in ["phone", "mobile", "contact", "telephone"]):
                role = "phone"
            
            # PAN (only if not already identified as something else)
            elif any(kw in norm_col for kw in ["pan", "pan_number", "pan_no", "pannumber", "panno", "permanent_account_number"]):
                role = "pan"
            
            # Created/Updated timestamps (audit fields, not transaction dates)
            elif any(kw in norm_col for kw in ["created_at", "created_date", "created", "date_created", "created_on"]):
                if "created" in norm_col:
                    role = "created_at"
            elif any(kw in norm_col for kw in ["updated_at", "updated_date", "updated", "date_updated", "updated_on", "modified_at", "modified"]):
                if "updated" in norm_col or "modified" in norm_col:
                    role = "updated_at"
            
            # Account number
            elif any(kw in norm_col for kw in ["account_number", "account_no", "acc_no", "accno"]):
                if "number" in norm_col or "no" in norm_col:
                    role = "account_number"
            
            # Customer ID
            elif any(kw in norm_col for kw in ["customer_id", "cust_id", "customerid", "custid", "client_id"]):
                role = "customer_id"
            
            # Transaction ID
            elif any(kw in norm_col for kw in ["transaction_id", "txn_id", "trans_id", "transactionid", "txnid"]):
                role = "transaction_id"
            
            # Transaction date (but not created_at/updated_at which are audit fields)
            elif any(kw in norm_col for kw in ["transaction_date", "txn_date", "trans_date", "transactiondate"]):
                if "date" in norm_col and "transaction" in norm_col:
                    role = "transaction_date"
            
            # Transaction type
            elif any(kw in norm_col for kw in ["transaction_type", "txn_type", "trans_type", "transactiontype"]):
                if "type" in norm_col and "transaction" in norm_col:
                    role = "transaction_type"
            
            # Debit
            elif any(kw in norm_col for kw in ["debit", "dr_amount", "debit_amount", "withdraw", "withdrawal"]):
                if "debit" in norm_col or "withdraw" in norm_col:
                    role = "debit"
            
            # Credit
            elif any(kw in norm_col for kw in ["credit", "cr_amount", "credit_amount", "deposit"]):
                if "credit" in norm_col or "deposit" in norm_col:
                    role = "credit"
            
            # Opening balance
            elif any(kw in norm_col for kw in ["opening_balance", "open_balance", "balance_before", "initial_balance", "op_bal"]):
                role = "opening_balance"
            
            # Closing balance
            elif any(kw in norm_col for kw in ["closing_balance", "closing", "balance_after", "final_balance", "current_balance", "cl_bal"]):
                role = "closing_balance"
            
            # Account type
            elif any(kw in norm_col for kw in ["account_type", "acct_type", "accounttype"]):
                role = "account_type"
            
            # Account status (check last to avoid matching kyc_status)
            elif any(kw in norm_col for kw in ["account_status", "acc_status"]):
                if "account" in norm_col and "status" in norm_col:
                    role = "account_status"
            
            column_roles[col] = role
        
        return column_roles
    
    def validate_account_number_rule(self, df: pd.DataFrame, col_name: str) -> Dict:
        """
        CRITICAL RULE 1: Account Number Rule
        - Digits only
        - Length 6-18
        - Unique
        - Regex: ^[0-9]{6,18}$
        """
        violations = []
        series = df[col_name].dropna().astype(str)
        
        if len(series) == 0:
            return {
                "rule_name": "Account Number Rule",
                "status": "FAIL",
                "violations": ["Column is completely empty"],
                "violation_count": 1,
                "row_violations": []
            }
        
        # Check digits only
        non_digit_mask = ~series.str.match(r'^\d+$')
        non_digit_rows = df.index[non_digit_mask].tolist()
        if len(non_digit_rows) > 0:
            violations.append(f"Non-digit values found: {len(non_digit_rows)} rows")
            for idx in non_digit_rows[:10]:  # Limit to first 10
                violations.append(f"Row {idx + 1}: '{series.iloc[non_digit_mask.tolist().index(idx)]}' contains non-digits")
        
        # Check length 6-18
        length_mask = ~series.str.len().between(6, 18)
        length_violation_rows = df.index[length_mask].tolist()
        if len(length_violation_rows) > 0:
            violations.append(f"Invalid length values found: {len(length_violation_rows)} rows")
            for idx in length_violation_rows[:10]:
                violations.append(f"Row {idx + 1}: '{series.iloc[length_mask.tolist().index(idx)]}' has invalid length")
        
        # Check uniqueness - account numbers can repeat (same account, multiple transactions)
        # But we check that each distinct account number is valid
        # No violation for repetition - that's expected in transaction datasets
        
        # Check regex pattern
        pattern_mask = ~series.str.match(self.ACCOUNT_NUMBER_PATTERN)
        pattern_violation_rows = df.index[pattern_mask].tolist()
        if len(pattern_violation_rows) > 0:
            violations.append(f"Pattern violation: {len(pattern_violation_rows)} rows don't match ^[0-9]{{6,18}}$")
        
        status = "PASS" if len(violations) == 0 else "FAIL"
        
        return {
            "rule_name": "Account Number Rule",
            "status": status,
            "violations": violations,
            "violation_count": len(violations),
            "row_violations": pattern_violation_rows[:20]  # Limit to 20 rows
        }
    
    def validate_transaction_rule(self, df: pd.DataFrame, debit_col: str, credit_col: str) -> Dict:
        """
        CRITICAL RULE 2: Transaction Rule
        - Exactly ONE of debit or credit > 0
        - Both > 0 → FAIL
        - Both = 0 → FAIL
        """
        violations = []
        
        debit_series = pd.to_numeric(df[debit_col], errors="coerce").fillna(0)
        credit_series = pd.to_numeric(df[credit_col], errors="coerce").fillna(0)
        
        # Both > 0
        both_positive_mask = (debit_series > 0) & (credit_series > 0)
        both_positive_rows = df.index[both_positive_mask].tolist()
        if len(both_positive_rows) > 0:
            violations.append(f"Both debit and credit > 0: {len(both_positive_rows)} rows")
            for idx in both_positive_rows[:10]:
                violations.append(f"Row {idx + 1}: debit={debit_series.iloc[both_positive_mask.tolist().index(idx)]}, credit={credit_series.iloc[both_positive_mask.tolist().index(idx)]}")
        
        # Both = 0
        both_zero_mask = (debit_series == 0) & (credit_series == 0)
        both_zero_rows = df.index[both_zero_mask].tolist()
        if len(both_zero_rows) > 0:
            violations.append(f"Both debit and credit = 0: {len(both_zero_rows)} rows")
            for idx in both_zero_rows[:10]:
                violations.append(f"Row {idx + 1}: Both values are zero")
        
        status = "PASS" if len(violations) == 0 else "FAIL"
        
        return {
            "rule_name": "Transaction Rule",
            "status": status,
            "violations": violations,
            "violation_count": len(violations),
            "row_violations": (both_positive_rows + both_zero_rows)[:20]
        }
    
    def validate_balance_rule(self, df: pd.DataFrame, opening_col: str, closing_col: str, 
                             debit_col: str, credit_col: str) -> Dict:
        """
        CRITICAL RULE 3: Balance Rule
        closing_balance MUST equal: opening_balance + credit - debit
        - opening_balance >= 0
        - closing_balance >= 0
        - Any mismatch → FAIL dataset
        """
        violations = []
        
        opening_series = pd.to_numeric(df[opening_col], errors="coerce").fillna(0)
        closing_series = pd.to_numeric(df[closing_col], errors="coerce").fillna(0)
        debit_series = pd.to_numeric(df[debit_col], errors="coerce").fillna(0)
        credit_series = pd.to_numeric(df[credit_col], errors="coerce").fillna(0)
        
        # Check opening_balance >= 0
        negative_opening_mask = opening_series < 0
        negative_opening_rows = df.index[negative_opening_mask].tolist()
        if len(negative_opening_rows) > 0:
            violations.append(f"Negative opening_balance: {len(negative_opening_rows)} rows")
            for idx in negative_opening_rows[:10]:
                violations.append(f"Row {idx + 1}: opening_balance={opening_series.iloc[negative_opening_mask.tolist().index(idx)]}")
        
        # Check closing_balance >= 0
        negative_closing_mask = closing_series < 0
        negative_closing_rows = df.index[negative_closing_mask].tolist()
        if len(negative_closing_rows) > 0:
            violations.append(f"Negative closing_balance: {len(negative_closing_rows)} rows")
            for idx in negative_closing_rows[:10]:
                violations.append(f"Row {idx + 1}: closing_balance={closing_series.iloc[negative_closing_mask.tolist().index(idx)]}")
        
        # Check formula: closing = opening + credit - debit
        calculated_closing = opening_series + credit_series - debit_series
        tolerance = 0.01  # Allow small floating point differences
        formula_mismatch_mask = abs(closing_series - calculated_closing) > tolerance
        formula_mismatch_rows = df.index[formula_mismatch_mask].tolist()
        
        if len(formula_mismatch_rows) > 0:
            violations.append(f"Balance formula mismatch: {len(formula_mismatch_rows)} rows")
            for idx in formula_mismatch_rows[:10]:
                row_idx = formula_mismatch_mask.tolist().index(idx) if idx in formula_mismatch_mask.tolist() else None
                if row_idx is not None:
                    violations.append(
                        f"Row {idx + 1}: closing={closing_series.iloc[row_idx]}, "
                        f"expected={calculated_closing.iloc[row_idx]} "
                        f"(opening={opening_series.iloc[row_idx]} + credit={credit_series.iloc[row_idx]} - debit={debit_series.iloc[row_idx]})"
                    )
        
        status = "PASS" if len(violations) == 0 else "FAIL"
        
        return {
            "rule_name": "Balance Rule",
            "status": status,
            "violations": violations,
            "violation_count": len(violations),
            "row_violations": (negative_opening_rows + negative_closing_rows + formula_mismatch_rows)[:20]
        }
    
    def validate_transaction_type_rule(self, df: pd.DataFrame, col_name: str) -> Dict:
        """
        CRITICAL RULE 4: Transaction Type Rule
        Allowed values: credit, debit, deposit, withdraw, withdrawal, transfer
        """
        violations = []
        series = df[col_name].dropna().astype(str).str.lower().str.strip()
        
        if len(series) == 0:
            return {
                "rule_name": "Transaction Type Rule",
                "status": "FAIL",
                "violations": ["Column is completely empty"],
                "violation_count": 1,
                "row_violations": []
            }
        
        invalid_mask = ~series.isin(self.ALLOWED_TRANSACTION_TYPES)
        invalid_rows = df.index[invalid_mask].tolist()
        
        if len(invalid_rows) > 0:
            invalid_values = series[invalid_mask].unique()
            violations.append(f"Invalid transaction types found: {len(invalid_rows)} rows")
            violations.append(f"Invalid values: {list(invalid_values[:10])}")
            for idx in invalid_rows[:10]:
                violations.append(f"Row {idx + 1}: '{series.iloc[invalid_mask.tolist().index(idx)]}' is not in allowed list")
        
        status = "PASS" if len(violations) == 0 else "FAIL"
        
        return {
            "rule_name": "Transaction Type Rule",
            "status": status,
            "violations": violations,
            "violation_count": len(violations),
            "row_violations": invalid_rows[:20]
        }
    
    def validate_warning_rules(self, df: pd.DataFrame, column_roles: Dict) -> List[Dict]:
        """
        STEP 4: Apply WARNING rules
        - Phone number format issues
        - Minor uniqueness issues
        - Case mismatches
        
        NOTE: KYC columns are skipped (KYC rules removed)
        """
        warnings = []
        
        # Skip KYC columns
        filtered_roles = {k: v for k, v in column_roles.items() if v is not None and "kyc" not in self.normalize_column_name(k)}
        
        # Phone number format warning
        if "phone" in filtered_roles.values():
            phone_col = [k for k, v in filtered_roles.items() if v == "phone"]
            if phone_col:
                phone_col = phone_col[0]
                phone_series = df[phone_col].dropna().astype(str)
                
                # Check for common phone format issues
                # Valid formats: 10 digits, may have +, -, spaces, ()
                phone_pattern = re.compile(r'^[\+]?[\d\s\-\(\)]{10,15}$')
                invalid_phones = phone_series[~phone_series.str.match(phone_pattern)]
                
                if len(invalid_phones) > 0:
                    warnings.append({
                        "rule_name": "Phone Number Format Warning",
                        "column": phone_col,
                        "issue": f"{len(invalid_phones)} phone numbers have format issues",
                        "severity": "WARNING"
                    })
        
        # Minor uniqueness issues (for transaction_id)
        if "transaction_id" in filtered_roles.values():
            txn_id_col = [k for k, v in filtered_roles.items() if v == "transaction_id"]
            if txn_id_col:
                txn_id_col = txn_id_col[0]
                txn_id_series = df[txn_id_col].dropna()
                unique_ratio = txn_id_series.nunique() / len(txn_id_series) if len(txn_id_series) > 0 else 0
                
                if unique_ratio < 0.95:
                    warnings.append({
                        "rule_name": "Transaction ID Uniqueness Warning",
                        "column": txn_id_col,
                        "issue": f"Only {unique_ratio*100:.1f}% unique (expected ≥95%)",
                        "severity": "WARNING"
                    })
        
        # Case mismatches (for transaction_type, account_status, etc.)
        if "transaction_type" in filtered_roles.values():
            txn_type_col = [k for k, v in filtered_roles.items() if v == "transaction_type"]
            if txn_type_col:
                txn_type_col = txn_type_col[0]
                txn_type_series = df[txn_type_col].dropna().astype(str)
                
                # Check for case inconsistencies
                lower_values = txn_type_series.str.lower()
                case_mismatches = txn_type_series[txn_type_series != lower_values]
                
                if len(case_mismatches) > 0:
                    warnings.append({
                        "rule_name": "Case Mismatch Warning",
                        "column": txn_type_col,
                        "issue": f"{len(case_mismatches)} values have case inconsistencies",
                        "severity": "WARNING"
                    })
        
        return warnings
    
    def validate(self, csv_path: str) -> Dict:
        """
        Main validation function
        
        Returns comprehensive validation results
        """
        try:
            # Load dataset
            df = pd.read_csv(csv_path)
            
            if df.empty:
                return {
                    "error": "Dataset is empty",
                    "detected_domain": None,
                    "column_identification": {},
                    "critical_rules": [],
                    "warning_rules": [],
                    "final_decision": "REJECT",
                    "final_reason": "Dataset is empty"
                }
            
            # STEP 1: Detect industry and dataset purpose
            industry_detection = self.detect_industry_and_purpose(df)
            
            # STEP 2: Column identification
            column_roles = self.identify_columns(df)
            
            # Filter out KYC columns and non-banking columns from column_roles (don't include in results)
            filtered_roles = {}
            for k, v in column_roles.items():
                norm_col = self.normalize_column_name(k)
                # Skip KYC columns
                if "kyc" in norm_col:
                    continue
                # Skip non-banking columns (currency, country, email, channel, etc.)
                non_banking_keywords = ["currency", "country", "email", "channel", "source", "region", "city", "state_name"]
                if any(kw in norm_col for kw in non_banking_keywords):
                    continue
                # Skip columns with no role identified (unknown columns)
                if v is None:
                    continue
                filtered_roles[k] = v
            
            column_roles = filtered_roles
            
            # STEP 3: Apply CRITICAL business rules
            critical_rules = []
            
            # Rule 1: Account Number Rule
            account_col = [k for k, v in column_roles.items() if v == "account_number"]
            if account_col:
                account_rule = self.validate_account_number_rule(df, account_col[0])
                critical_rules.append(account_rule)
            else:
                critical_rules.append({
                    "rule_name": "Account Number Rule",
                    "status": "FAIL",
                    "violations": ["Account number column not found"],
                    "violation_count": 1,
                    "row_violations": []
                })
            
            # Rule 2: Transaction Rule
            debit_col = [k for k, v in column_roles.items() if v == "debit"]
            credit_col = [k for k, v in column_roles.items() if v == "credit"]
            if debit_col and credit_col:
                transaction_rule = self.validate_transaction_rule(df, debit_col[0], credit_col[0])
                critical_rules.append(transaction_rule)
            else:
                if not debit_col or not credit_col:
                    critical_rules.append({
                        "rule_name": "Transaction Rule",
                        "status": "FAIL",
                        "violations": [f"Missing columns: {'debit' if not debit_col else ''} {'credit' if not credit_col else ''}".strip()],
                        "violation_count": 1,
                        "row_violations": []
                    })
            
            # Rule 3: Balance Rule
            opening_col = [k for k, v in column_roles.items() if v == "opening_balance"]
            closing_col = [k for k, v in column_roles.items() if v == "closing_balance"]
            if opening_col and closing_col and debit_col and credit_col:
                balance_rule = self.validate_balance_rule(
                    df, opening_col[0], closing_col[0], debit_col[0], credit_col[0]
                )
                critical_rules.append(balance_rule)
            else:
                missing = []
                if not opening_col:
                    missing.append("opening_balance")
                if not closing_col:
                    missing.append("closing_balance")
                if missing:
                    critical_rules.append({
                        "rule_name": "Balance Rule",
                        "status": "FAIL",
                        "violations": [f"Missing columns: {', '.join(missing)}"],
                        "violation_count": 1,
                        "row_violations": []
                    })
            
            # Rule 4: Transaction Type Rule
            txn_type_col = [k for k, v in column_roles.items() if v == "transaction_type"]
            if txn_type_col:
                txn_type_rule = self.validate_transaction_type_rule(df, txn_type_col[0])
                critical_rules.append(txn_type_rule)
            
            # STEP 4: Apply WARNING rules
            warning_rules = self.validate_warning_rules(df, column_roles)
            
            # STEP 5: Final Decision Logic
            # If ANY CRITICAL rule fails → REJECT
            # Else if only WARNINGS → PASS WITH WARNINGS
            # Else → PASS
            
            critical_failures = [r for r in critical_rules if r["status"] == "FAIL"]
            
            if len(critical_failures) > 0:
                final_decision = "REJECT"
                failed_rules = [r["rule_name"] for r in critical_failures]
                final_reason = f"CRITICAL rules failed: {', '.join(failed_rules)}"
            elif len(warning_rules) > 0:
                final_decision = "PASS WITH WARNINGS"
                warning_count = len(warning_rules)
                final_reason = f"All CRITICAL rules passed, but {warning_count} warning(s) found"
            else:
                final_decision = "PASS"
                final_reason = "All validation checks passed successfully"
            
            # STEP 6: Output
            # Filter out KYC columns from identified_columns display
            filtered_column_roles = {k: v for k, v in column_roles.items() if "kyc" not in self.normalize_column_name(k)}
            total_cols_excluding_kyc = len([c for c in df.columns if "kyc" not in self.normalize_column_name(c)])
            
            return {
                "detected_domain": industry_detection,
                "column_identification": {
                    "identified_columns": filtered_column_roles,
                    "total_columns": total_cols_excluding_kyc,
                    "identified_count": len([v for v in filtered_column_roles.values() if v is not None])
                },
                "critical_rules": critical_rules,
                "warning_rules": warning_rules,
                "final_decision": final_decision,
                "final_reason": final_reason,
                "dataset_summary": {
                    "total_rows": len(df),
                    "total_columns": len([c for c in df.columns if "kyc" not in self.normalize_column_name(c)]),
                    "critical_violations": sum(r["violation_count"] for r in critical_rules),
                    "warning_count": len(warning_rules)
                }
            }
        
        except Exception as e:
            return {
                "error": str(e),
                "detected_domain": None,
                "column_identification": {},
                "critical_rules": [],
                "warning_rules": [],
                "final_decision": "REJECT",
                "final_reason": f"Validation error: {str(e)}"
            }
