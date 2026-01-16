"""
CORE BANKING DOMAIN BUSINESS RULES ENGINE

Strict flow implementation:
1. Column Profiling (Mandatory)
2. Banking Concept Identification
3. Confidence Scoring
4. Apply Real Banking Business Rules
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
import re
from datetime import datetime


class CoreBankingBusinessRulesEngine:
    """
    CORE BANKING DOMAIN BUSINESS RULES ENGINE
    
    Analyzes banking datasets and applies industry-standard business rules.
    Follows strict 4-step process for each column.
    """
    
    def __init__(self):
        # Banking concept definitions with identification patterns
        self.banking_concepts = self._initialize_banking_concepts()
        
    def _initialize_banking_concepts(self) -> Dict[str, Dict]:
        """Define all supported banking concepts with identification rules"""
        return {
            # CUSTOMER DOMAIN
            "customer_id": {
                "domain": "Customer",
                "name_patterns": ["customer_id", "cust_id", "client_id", "c_id", "customer_number"],
                "data_patterns": {
                    "type": ["numeric", "alphanumeric"],
                    "uniqueness": "high",  # >95%
                    "length": {"min": 4, "max": 20},
                    "nullable": False
                },
                "business_rules": {
                    "unique": True,
                    "mandatory": True,
                    "primary_key": True,
                    "format": "Alphanumeric, 4-20 characters",
                    "reason": "Primary identifier for all customer operations. Must be unique to prevent data corruption.",
                    "violation_impact": "BUSINESS: Cannot link accounts/transactions. FINANCIAL: Risk of duplicate customers. COMPLIANCE: KYC violations."
                }
            },
            "customer_name": {
                "domain": "Customer",
                "name_patterns": ["customer_name", "cust_name", "name", "full_name", "account_holder_name"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "low",  # Names can repeat
                    "length": {"min": 3, "max": 100},
                    "nullable": False
                },
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "format": "Alphabetic characters and spaces only",
                    "reason": "Legal name for KYC compliance. Must match government ID documents.",
                    "violation_impact": "BUSINESS: Cannot verify customer identity. COMPLIANCE: KYC/AML violations. FINANCIAL: Legal liability."
                }
            },
            "date_of_birth": {
                "domain": "Customer",
                "name_patterns": ["dob", "date_of_birth", "birth_date", "birthdate"],
                "data_patterns": {
                    "type": ["date"],
                    "uniqueness": "medium",
                    "nullable": False
                },
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "format": "Valid date, cannot be future date",
                    "reason": "Required for age verification, KYC compliance, and product eligibility.",
                    "violation_impact": "BUSINESS: Cannot determine product eligibility. COMPLIANCE: KYC documentation incomplete."
                }
            },
            "pan": {
                "domain": "Customer",
                "name_patterns": ["pan", "pan_number", "pan_no", "permanent_account_number"],
                "data_patterns": {
                    "type": ["alphanumeric"],
                    "uniqueness": "high",  # PAN is unique per person
                    "length": {"exact": 10},
                    "pattern": "5 letters + 4 digits + 1 letter",
                    "nullable": False
                },
                "business_rules": {
                    "unique": True,
                    "mandatory": True,
                    "format": "5 letters + 4 digits + 1 letter (e.g., ABCDE1234F)",
                    "reason": "Government-issued tax identifier. Required for high-value transactions and compliance.",
                    "violation_impact": "BUSINESS: Cannot process high-value transactions. COMPLIANCE: Tax reporting violations. FINANCIAL: Penalties from tax authorities."
                }
            },
            "phone_number": {
                "domain": "Customer",
                "name_patterns": ["phone", "mobile", "contact_number", "phone_no", "mobile_number"],
                "data_patterns": {
                    "type": ["numeric"],
                    "uniqueness": "medium",
                    "length": {"min": 10, "max": 15},
                    "nullable": False
                },
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "format": "10-15 digits, numeric only",
                    "reason": "Required for OTP verification, transaction alerts, and customer communication.",
                    "violation_impact": "BUSINESS: Cannot send security alerts. FINANCIAL: Increased fraud risk. COMPLIANCE: Communication failures."
                }
            },
            
            # ACCOUNT DOMAIN
            "account_number": {
                "domain": "Account",
                "name_patterns": ["account_number", "acc_no", "account_no", "acct_num", "account_id"],
                "data_patterns": {
                    "type": ["numeric"],
                    "uniqueness": "very_high",  # 100% unique globally
                    "length": {"min": 10, "max": 18},
                    "nullable": False
                },
                "business_rules": {
                    "unique": True,
                    "mandatory": True,
                    "primary_key": True,
                    "format": "Numeric, 10-18 digits, globally unique",
                    "reason": "Globally unique identifier for every bank account. Used in all transactions and statements.",
                    "violation_impact": "BUSINESS: Transaction routing failures. FINANCIAL: Money sent to wrong accounts. COMPLIANCE: Audit trail broken."
                }
            },
            "account_type": {
                "domain": "Account",
                "name_patterns": ["account_type", "acc_type", "acct_type", "account_category"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "very_low",  # Few values: Savings, Current, etc.
                    "cardinality": {"max": 10},  # Limited distinct values
                    "nullable": False
                },
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "format": "Predefined values: Savings, Current, Salary, Fixed Deposit, Recurring Deposit",
                    "domain_values": ["Savings", "Current", "Salary", "Student", "Pension", "Fixed Deposit", "Recurring Deposit"],
                    "reason": "Determines interest rates, fees, transaction limits, and product features.",
                    "violation_impact": "BUSINESS: Wrong interest/fees applied. FINANCIAL: Revenue loss or incorrect charges. COMPLIANCE: Product mis-selling."
                }
            },
            "account_status": {
                "domain": "Account",
                "name_patterns": ["account_status", "acc_status", "status", "account_state"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "very_low",
                    "cardinality": {"max": 5},
                    "nullable": False
                },
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "format": "Predefined values: Active, Inactive, Frozen, Closed",
                    "domain_values": ["Active", "Inactive", "Frozen", "Closed", "Dormant"],
                    "reason": "Controls account operations. Only Active accounts can transact.",
                    "violation_impact": "BUSINESS: Transactions blocked on active accounts or allowed on closed accounts. FINANCIAL: Unauthorized access risk. COMPLIANCE: Operational violations."
                }
            },
            "current_balance": {
                "domain": "Account",
                "name_patterns": ["balance", "current_balance", "available_balance", "account_balance", "closing_balance"],
                "data_patterns": {
                    "type": ["numeric", "decimal"],
                    "uniqueness": "low",
                    "nullable": False
                },
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "format": "Numeric, >= 0 (or negative if overdraft allowed), 2 decimal places",
                    "reason": "Current available funds. Must be accurate for transaction authorization and overdraft prevention.",
                    "violation_impact": "BUSINESS: Overdraft not detected. FINANCIAL: Unauthorized withdrawals, losses. COMPLIANCE: Balance sheet inaccuracies."
                }
            },
            "branch_code": {
                "domain": "Account",
                "name_patterns": ["branch_code", "branch_id", "branch", "branch_number"],
                "data_patterns": {
                    "type": ["alphanumeric", "numeric"],
                    "uniqueness": "low",  # Many accounts per branch
                    "length": {"min": 3, "max": 10},
                    "nullable": False
                },
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "format": "Alphanumeric, 3-10 characters",
                    "reason": "Links account to physical branch for operations, reporting, and customer service.",
                    "violation_impact": "BUSINESS: Cannot route branch operations. FINANCIAL: Incorrect branch reporting. COMPLIANCE: Branch-level audit failures."
                }
            },
            
            # LOAN DOMAIN
            "loan_id": {
                "domain": "Loan",
                "name_patterns": ["loan_id", "loan_number", "loan_no", "loan_account"],
                "data_patterns": {
                    "type": ["numeric", "alphanumeric"],
                    "uniqueness": "high",
                    "length": {"min": 6, "max": 20},
                    "nullable": False
                },
                "business_rules": {
                    "unique": True,
                    "mandatory": True,
                    "primary_key": True,
                    "format": "Alphanumeric, 6-20 characters, unique",
                    "reason": "Unique identifier for each loan. Links to customer and account for EMI processing.",
                    "violation_impact": "BUSINESS: Cannot track loan lifecycle. FINANCIAL: EMI deduction failures. COMPLIANCE: Loan portfolio reporting errors."
                }
            },
            "loan_amount": {
                "domain": "Loan",
                "name_patterns": ["loan_amount", "principal", "loan_principal", "sanctioned_amount"],
                "data_patterns": {
                    "type": ["numeric", "decimal"],
                    "uniqueness": "low",
                    "nullable": False
                },
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "format": "Numeric, > 0, 2 decimal places",
                    "reason": "Principal loan amount. Determines EMI calculation and interest charges.",
                    "violation_impact": "BUSINESS: Incorrect EMI calculation. FINANCIAL: Revenue loss or overcharging. COMPLIANCE: Loan documentation errors."
                }
            },
            "interest_rate": {
                "domain": "Loan",
                "name_patterns": ["interest_rate", "rate", "roi", "annual_rate"],
                "data_patterns": {
                    "type": ["numeric", "decimal"],
                    "uniqueness": "low",
                    "range": {"min": 0, "max": 100},  # Percentage
                    "nullable": False
                },
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "format": "Numeric, 0-100%, typically 6-20% for personal loans",
                    "reason": "Annual interest rate. Must comply with regulatory limits. Determines EMI and total interest.",
                    "violation_impact": "BUSINESS: Incorrect EMI calculation. FINANCIAL: Regulatory penalties for usury. COMPLIANCE: Rate cap violations."
                }
            },
            "emi_amount": {
                "domain": "Loan",
                "name_patterns": ["emi", "emi_amount", "installment", "monthly_payment"],
                "data_patterns": {
                    "type": ["numeric", "decimal"],
                    "uniqueness": "low",
                    "nullable": False
                },
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "format": "Numeric, > 0, 2 decimal places, fixed for loan tenure",
                    "reason": "Fixed monthly installment. Calculated from loan amount, rate, and tenure. Must be consistent.",
                    "violation_impact": "BUSINESS: Incorrect deductions. FINANCIAL: Loan recovery failures. COMPLIANCE: Contractual violations."
                }
            },
            
            # TRANSACTION DOMAIN
            "transaction_id": {
                "domain": "Transaction",
                "name_patterns": ["transaction_id", "txn_id", "trxn_id", "transaction_number", "ref_number"],
                "data_patterns": {
                    "type": ["numeric", "alphanumeric"],
                    "uniqueness": "very_high",  # 100% unique
                    "length": {"min": 8, "max": 30},
                    "nullable": False
                },
                "business_rules": {
                    "unique": True,
                    "mandatory": True,
                    "primary_key": True,
                    "format": "Alphanumeric, 8-30 characters, globally unique",
                    "reason": "Unique identifier for audit trail, dispute resolution, and transaction tracking.",
                    "violation_impact": "BUSINESS: Cannot track transactions. FINANCIAL: Duplicate processing risk. COMPLIANCE: Audit trail broken."
                }
            },
            "transaction_date": {
                "domain": "Transaction",
                "name_patterns": ["transaction_date", "txn_date", "date", "transaction_time", "value_date"],
                "data_patterns": {
                    "type": ["date", "datetime"],
                    "uniqueness": "low",
                    "nullable": False
                },
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "format": "Valid date, cannot be future date (except scheduled transactions)",
                    "reason": "Critical for interest calculation, statement generation, and chronological ordering.",
                    "violation_impact": "BUSINESS: Incorrect statement periods. FINANCIAL: Interest calculation errors. COMPLIANCE: Reporting period violations."
                }
            },
            "transaction_amount": {
                "domain": "Transaction",
                "name_patterns": ["amount", "transaction_amount", "txn_amount", "value"],
                "data_patterns": {
                    "type": ["numeric", "decimal"],
                    "uniqueness": "low",
                    "nullable": False
                },
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "format": "Numeric, > 0, 2 decimal places",
                    "reason": "Transaction value. Must be positive and accurate for balance updates.",
                    "violation_impact": "BUSINESS: Incorrect balance updates. FINANCIAL: Money loss or unauthorized amounts. COMPLIANCE: Transaction reporting errors."
                }
            },
            "debit": {
                "domain": "Transaction",
                "name_patterns": ["debit", "debit_amount", "withdrawal", "dr"],
                "data_patterns": {
                    "type": ["numeric", "decimal"],
                    "uniqueness": "low",
                    "nullable": True  # Can be null if credit transaction
                },
                "business_rules": {
                    "unique": False,
                    "mandatory": False,  # Optional - mutually exclusive with credit
                    "format": "Numeric, >= 0, mutually exclusive with credit",
                    "reason": "Money going out. Must be mutually exclusive with credit. Reduces balance.",
                    "violation_impact": "BUSINESS: Double-entry bookkeeping violation. FINANCIAL: Balance calculation errors. COMPLIANCE: Accounting violations."
                }
            },
            "credit": {
                "domain": "Transaction",
                "name_patterns": ["credit", "credit_amount", "deposit", "cr"],
                "data_patterns": {
                    "type": ["numeric", "decimal"],
                    "uniqueness": "low",
                    "nullable": True  # Can be null if debit transaction
                },
                "business_rules": {
                    "unique": False,
                    "mandatory": False,  # Optional - mutually exclusive with debit
                    "format": "Numeric, >= 0, mutually exclusive with debit",
                    "reason": "Money coming in. Must be mutually exclusive with debit. Increases balance.",
                    "violation_impact": "BUSINESS: Double-entry bookkeeping violation. FINANCIAL: Balance calculation errors. COMPLIANCE: Accounting violations."
                }
            },
            "transaction_type": {
                "domain": "Transaction",
                "name_patterns": ["transaction_type", "txn_type", "type", "transaction_category"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "very_low",
                    "cardinality": {"max": 15},
                    "nullable": False
                },
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "format": "Predefined values: Deposit, Withdrawal, Transfer, Payment, Interest, Fee",
                    "domain_values": ["Deposit", "Withdrawal", "Transfer", "Payment", "Interest", "Fee", "Refund"],
                    "reason": "Categorizes transactions for reporting, fraud detection, and customer statements.",
                    "violation_impact": "BUSINESS: Incorrect transaction categorization. FINANCIAL: Reporting errors. COMPLIANCE: Transaction type misclassification."
                }
            }
        }
    
    def analyze_dataset(self, file_path: str, df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """
        MAIN ENTRY POINT
        
        Follows strict 4-step process:
        1. Column Profiling (Mandatory)
        2. Banking Concept Identification
        3. Confidence Scoring
        4. Apply Real Banking Business Rules
        """
        try:
            # Load DataFrame if not provided
            if df is None:
                df = pd.read_csv(file_path)
            
            if df.empty:
                return {
                    "error": "Dataset is empty",
                    "columns_analyzed": 0
                }
            
            results = {
                "file_name": file_path.split('/')[-1],
                "total_columns": len(df.columns),
                "total_rows": len(df),
                "columns_analysis": [],
                "summary": {}
            }
            
            # STEP 1-4: Process each column
            for column in df.columns:
                column_result = self._process_column(column, df[column], df)
                results["columns_analysis"].append(column_result)
            
            # Generate summary
            results["summary"] = self._generate_summary(results["columns_analysis"])
            
            return results
            
        except Exception as e:
            return {
                "error": f"Analysis failed: {str(e)}",
                "columns_analyzed": 0
            }
    
    def _process_column(self, column_name: str, series: pd.Series, full_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Process single column through all 4 steps
        """
        # STEP 1: Column Profiling (MANDATORY)
        profile = self._step1_column_profiling(column_name, series)
        
        # STEP 2: Banking Concept Identification
        concept_match = self._step2_concept_identification(column_name, profile, series)
        
        # STEP 3: Confidence Scoring
        confidence = self._step3_confidence_scoring(column_name, profile, concept_match, series, full_df)
        
        # STEP 4: Apply Real Banking Business Rules
        business_rules = self._step4_apply_business_rules(concept_match, profile, series)
        
        return {
            "column_name": column_name,
            "step1_profile": profile,
            "step2_identified_as": concept_match["concept"],
            "step2_confidence": confidence,
            "step3_confidence_score": confidence,
            "step4_business_meaning": business_rules["business_meaning"],
            "step4_business_rules": business_rules["rules"],
            "step4_why_rule_exists": business_rules["why_rule_exists"],
            "step4_violation_impact": business_rules["violation_impact"],
            "ui_ready_format": {
                "Column Name": column_name,
                "Identified As": concept_match["concept_display"],
                "Confidence": f"{confidence}%",
                "Business Meaning": business_rules["business_meaning"],
                "Business Rules": business_rules["rules_display"],
                "Why This Rule Exists": business_rules["why_rule_exists"],
                "Violation Impact": business_rules["violation_impact"]
            }
        }
    
    def _step1_column_profiling(self, column_name: str, series: pd.Series) -> Dict[str, Any]:
        """
        STEP 1: COLUMN PROFILING (MANDATORY)
        
        Analyze and record:
        - Column name & keywords
        - Data type
        - Min & max value length
        - Uniqueness percentage
        - Null / empty percentage
        - Pattern detection
        """
        profile = {
            "column_name": column_name,
            "keywords": self._extract_keywords(column_name),
            "data_type": self._detect_data_type(series),
            "total_records": len(series),
            "non_null_count": int(series.notna().sum()),
            "null_count": int(series.isna().sum()),
            "null_percentage": round(float(series.isna().sum() / len(series)) * 100, 2) if len(series) > 0 else 0.0,
            "unique_count": int(series.nunique()),
            "uniqueness_percentage": round(float(series.nunique() / len(series)) * 100, 2) if len(series) > 0 else 0.0,
            "patterns": {}
        }
        
        # Pattern detection
        non_null_series = series.dropna()
        if len(non_null_series) > 0:
            # Length analysis
            if profile["data_type"] in ["text", "alphanumeric"]:
                lengths = non_null_series.astype(str).str.len()
                profile["patterns"]["min_length"] = int(lengths.min())
                profile["patterns"]["max_length"] = int(lengths.max())
                profile["patterns"]["avg_length"] = round(float(lengths.mean()), 2)
            
            # Pattern detection
            sample_values = non_null_series.head(100).astype(str)
            
            # Only digits
            if all(s.isdigit() for s in sample_values):
                profile["patterns"]["only_digits"] = True
            else:
                profile["patterns"]["only_digits"] = False
            
            # Alphanumeric
            if all(re.match(r'^[A-Za-z0-9]+$', s) for s in sample_values):
                profile["patterns"]["alphanumeric"] = True
            else:
                profile["patterns"]["alphanumeric"] = False
            
            # Fixed length
            if profile["data_type"] in ["text", "alphanumeric"]:
                lengths = non_null_series.astype(str).str.len()
                if lengths.nunique() == 1:
                    profile["patterns"]["fixed_length"] = True
                    profile["patterns"]["fixed_length_value"] = int(lengths.iloc[0])
                else:
                    profile["patterns"]["fixed_length"] = False
            
            # Low cardinality (few distinct values)
            unique_ratio = profile["uniqueness_percentage"]
            if unique_ratio < 20:
                profile["patterns"]["low_cardinality"] = True
                profile["patterns"]["distinct_values"] = list(non_null_series.unique()[:10])
            else:
                profile["patterns"]["low_cardinality"] = False
            
            # Date format detection
            if profile["data_type"] == "date":
                profile["patterns"]["date_format"] = self._detect_date_format(non_null_series)
            
            # Numeric range
            if profile["data_type"] in ["numeric", "decimal"]:
                numeric_series = pd.to_numeric(non_null_series, errors='coerce').dropna()
                if len(numeric_series) > 0:
                    profile["patterns"]["min_value"] = float(numeric_series.min())
                    profile["patterns"]["max_value"] = float(numeric_series.max())
                    profile["patterns"]["mean_value"] = round(float(numeric_series.mean()), 2)
                    profile["patterns"]["has_negative"] = bool((numeric_series < 0).any())
                    profile["patterns"]["has_zero"] = bool((numeric_series == 0).any())
        
        return profile
    
    def _step2_concept_identification(self, column_name: str, profile: Dict, series: pd.Series) -> Dict[str, Any]:
        """
        STEP 2: BANKING CONCEPT IDENTIFICATION
        
        Using BOTH column name + data behavior,
        classify each column into EXACTLY ONE banking concept.
        """
        column_lower = column_name.lower()
        best_match = None
        best_score = 0
        
        # Check each banking concept
        for concept_key, concept_def in self.banking_concepts.items():
            score = 0
            
            # 1. Name pattern matching (40% weight)
            name_match = False
            for pattern in concept_def["name_patterns"]:
                if pattern in column_lower:
                    name_match = True
                    score += 40
                    break
            
            # 2. Data pattern matching (60% weight)
            data_patterns = concept_def["data_patterns"]
            
            # Type match
            expected_type = data_patterns.get("type", [])
            if profile["data_type"] in expected_type:
                score += 20
            
            # Uniqueness match
            expected_uniqueness = data_patterns.get("uniqueness", "")
            uniqueness_pct = profile["uniqueness_percentage"]
            if expected_uniqueness == "very_high" and uniqueness_pct >= 99:
                score += 20
            elif expected_uniqueness == "high" and uniqueness_pct >= 95:
                score += 15
            elif expected_uniqueness == "low" and uniqueness_pct < 50:
                score += 15
            elif expected_uniqueness == "very_low" and uniqueness_pct < 20:
                score += 15
            
            # Length match
            if "length" in data_patterns:
                length_def = data_patterns["length"]
                if "exact" in length_def:
                    if profile["patterns"].get("fixed_length") and profile["patterns"].get("fixed_length_value") == length_def["exact"]:
                        score += 10
                elif "min" in length_def and "max" in length_def:
                    avg_len = profile["patterns"].get("avg_length", 0)
                    if length_def["min"] <= avg_len <= length_def["max"]:
                        score += 10
            
            # Pattern match (PAN, etc.)
            if "pattern" in data_patterns:
                pattern_desc = data_patterns["pattern"]
                # Check if pattern matches (e.g., 5 letters + 4 digits + 1 letter)
                if "5 letters" in pattern_desc and "4 digits" in pattern_desc:
                    sample = str(series.dropna().iloc[0]) if len(series.dropna()) > 0 else ""
                    if len(sample) == 10 and sample[:5].isalpha() and sample[5:9].isdigit() and sample[9].isalpha():
                        score += 10
            
            # Cardinality match
            if "cardinality" in data_patterns:
                max_cardinality = data_patterns["cardinality"].get("max", 100)
                distinct_count = profile["unique_count"]
                if distinct_count <= max_cardinality:
                    score += 10
            
            # Nullable match
            expected_nullable = data_patterns.get("nullable", False)
            null_pct = profile["null_percentage"]
            if expected_nullable and null_pct > 0:
                score += 5
            elif not expected_nullable and null_pct == 0:
                score += 5
            
            if score > best_score:
                best_score = score
                best_match = {
                    "concept": concept_key,
                    "concept_display": concept_def["domain"] + " - " + concept_key.replace("_", " ").title(),
                    "domain": concept_def["domain"],
                    "match_score": score,
                    "concept_definition": concept_def
                }
        
        # If no good match found, classify as "Unknown"
        if best_match is None or best_score < 30:
            best_match = {
                "concept": "unknown",
                "concept_display": "Unknown Banking Concept",
                "domain": "General",
                "match_score": 0,
                "concept_definition": None
            }
        
        return best_match
    
    def _step3_confidence_scoring(self, column_name: str, profile: Dict, concept_match: Dict, 
                                  series: pd.Series, full_df: pd.DataFrame) -> float:
        """
        STEP 3: CONFIDENCE SCORING
        
        Assign confidence (0-100%) using:
        - Column name similarity
        - Pattern match accuracy
        - Uniqueness behavior
        - Length consistency
        - Cross-file reuse (if applicable)
        """
        if concept_match["concept"] == "unknown":
            return 0.0
        
        confidence = concept_match["match_score"]  # Base score from step 2
        
        # Adjust based on data quality
        if profile["null_percentage"] == 0:
            confidence += 5  # No nulls = good data quality
        
        if profile["uniqueness_percentage"] > 99:
            confidence += 5  # Very unique = strong identifier
        
        # Pattern consistency
        if profile["patterns"].get("fixed_length"):
            confidence += 5  # Fixed length = strong pattern
        
        # Cap at 100
        confidence = min(100.0, confidence)
        
        return round(confidence, 1)
    
    def _step4_apply_business_rules(self, concept_match: Dict, profile: Dict, series: pd.Series) -> Dict[str, Any]:
        """
        STEP 4: APPLY REAL BANKING BUSINESS RULES
        
        Generate:
        1. Business Meaning
        2. Business Rules
        3. Business Reason
        4. Violation Impact
        """
        if concept_match["concept"] == "unknown":
            # Generic rules for unknown columns
            return {
                "business_meaning": f"The {concept_match['concept_display']} column contains data relevant to banking operations. Exact business meaning requires domain expert review.",
                "rules": {
                    "unique": "Unknown",
                    "mandatory": "Unknown",
                    "format": "Based on data type: " + profile["data_type"]
                },
                "rules_display": "Business rules require domain expert review for this column.",
                "why_rule_exists": "Column purpose not clearly identified. Manual review recommended.",
                "violation_impact": "Impact cannot be determined without proper identification."
            }
        
        concept_def = concept_match["concept_definition"]
        business_rules = concept_def["business_rules"]
        
        # Build rules display
        rules_list = []
        if business_rules.get("unique"):
            rules_list.append("✓ Must be UNIQUE")
        if business_rules.get("mandatory"):
            rules_list.append("✓ MANDATORY (cannot be null)")
        if business_rules.get("primary_key"):
            rules_list.append("✓ PRIMARY KEY")
        if business_rules.get("format"):
            rules_list.append(f"✓ Format: {business_rules['format']}")
        if business_rules.get("domain_values"):
            rules_list.append(f"✓ Allowed values: {', '.join(business_rules['domain_values'][:5])}")
        
        rules_display = "\n".join(rules_list) if rules_list else "Standard banking rules apply."
        
        return {
            "business_meaning": f"This column represents {concept_match['concept_display']} in the banking system. {business_rules.get('reason', '')}",
            "rules": business_rules,
            "rules_display": rules_display,
            "why_rule_exists": business_rules.get("reason", "Standard banking business rule."),
            "violation_impact": business_rules.get("violation_impact", "Business, financial, and compliance risks.")
        }
    
    # Helper methods
    def _extract_keywords(self, column_name: str) -> List[str]:
        """Extract keywords from column name"""
        # Split by underscore, hyphen, or camelCase
        parts = re.split(r'[_\-\s]+|[A-Z]', column_name)
        return [p.lower() for p in parts if p]
    
    def _detect_data_type(self, series: pd.Series) -> str:
        """Detect data type"""
        if pd.api.types.is_datetime64_any_dtype(series):
            return "date"
        elif pd.api.types.is_numeric_dtype(series):
            # Check if it's integer or decimal
            non_null = series.dropna()
            if len(non_null) > 0:
                if (non_null % 1 == 0).all():
                    return "numeric"
                else:
                    return "decimal"
            return "numeric"
        else:
            # Check if alphanumeric
            sample = series.dropna().head(100).astype(str)
            if all(re.match(r'^[A-Za-z0-9]+$', s) for s in sample):
                return "alphanumeric"
            return "text"
    
    def _detect_date_format(self, series: pd.Series) -> str:
        """Detect date format"""
        sample = series.dropna().head(10).astype(str)
        if len(sample) == 0:
            return "Unknown"
        
        # Try to detect common formats
        first_val = str(sample.iloc[0])
        if '/' in first_val:
            return "DD/MM/YYYY or MM/DD/YYYY"
        elif '-' in first_val:
            return "YYYY-MM-DD"
        else:
            return "Unknown"
    
    def _generate_summary(self, columns_analysis: List[Dict]) -> Dict[str, Any]:
        """Generate summary statistics"""
        total = len(columns_analysis)
        identified = len([c for c in columns_analysis if c["step2_identified_as"] != "unknown"])
        avg_confidence = np.mean([c["step3_confidence_score"] for c in columns_analysis]) if columns_analysis else 0
        
        domain_counts = {}
        for col in columns_analysis:
            domain = col.get("step2_identified_as", "Unknown").split(" - ")[0] if " - " in col.get("step2_identified_as", "") else "Unknown"
            domain_counts[domain] = domain_counts.get(domain, 0) + 1
        
        return {
            "total_columns": total,
            "identified_columns": identified,
            "unidentified_columns": total - identified,
            "identification_rate": round((identified / total * 100) if total > 0 else 0, 1),
            "average_confidence": round(avg_confidence, 1),
            "domain_distribution": domain_counts
        }
