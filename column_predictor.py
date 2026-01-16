"""
Column Predictor - Intelligent Column Type and Business Rule Detection

Predicts column types based on:
- Column name patterns
- Data value patterns
- Statistical analysis
- Banking domain knowledge
"""

import pandas as pd
import numpy as np
import re
from typing import Dict, List, Any, Optional, Tuple


class ColumnPredictor:
    """Predict column types and business meanings with confidence scores"""
    
    def __init__(self):
        # Define column type patterns and indicators
        self.column_patterns = {
            "customer_id": {
                "name_keywords": ["customer", "cust", "client", "user_id", "client_id"],
                "exclude_keywords": ["name", "email", "phone", "address"],
                "value_pattern": r"^[A-Za-z]{1,4}\d{1,10}$",  # e.g., CUST001, C12345
                "data_type": "mixed_alphanumeric",
                "uniqueness_threshold": 0.95,
                "business_rule": "Unique identifier for each customer"
            },
            "customer_name": {
                "name_keywords": ["name", "customer_name", "holder_name", "account_holder"],
                "exclude_keywords": ["id", "number", "code"],
                "value_pattern": r"^[A-Za-z\s\.]+$",  # Only letters and spaces
                "data_type": "text",
                "uniqueness_threshold": 0.3,
                "business_rule": "Full legal name of the customer"
            },
            "account_number": {
                "name_keywords": ["account", "acc", "acct", "account_no", "acc_no", "account_number"],
                "exclude_keywords": ["name", "type", "status", "balance"],
                "value_pattern": r"^\d{6,18}$",  # 6-18 digits
                "data_type": "numeric",
                "uniqueness_threshold": 0.95,
                "business_rule": "Unique bank account identifier"
            },
            "account_type": {
                "name_keywords": ["type", "account_type", "acc_type", "category"],
                "exclude_keywords": ["transaction", "payment"],
                "value_pattern": r"^(savings|current|salary|fixed|deposit|fd|rd)$",
                "data_type": "categorical",
                "uniqueness_threshold": 0.01,  # Few unique values
                "business_rule": "Classification of account (Savings, Current, etc.)"
            },
            "account_status": {
                "name_keywords": ["status", "state", "active", "account_status"],
                "exclude_keywords": ["transaction", "payment"],
                "value_pattern": r"^(active|inactive|closed|frozen|open|suspended)$",
                "data_type": "categorical",
                "uniqueness_threshold": 0.01,
                "business_rule": "Current operational state of account"
            },
            "balance": {
                "name_keywords": ["balance", "bal", "amount", "current_balance", "available_balance"],
                "exclude_keywords": ["opening", "closing", "minimum", "debit", "credit"],
                "value_pattern": r"^\d+(\.\d{1,2})?$",
                "data_type": "numeric_decimal",
                "uniqueness_threshold": 0.5,
                "business_rule": "Current account balance amount"
            },
            "transaction_id": {
                "name_keywords": ["transaction", "txn", "trans", "transaction_id", "txn_id", "ref"],
                "exclude_keywords": ["date", "time", "type", "amount"],
                "value_pattern": r"^[A-Za-z0-9]+$",
                "data_type": "mixed_alphanumeric",
                "uniqueness_threshold": 0.98,
                "business_rule": "Unique identifier for each transaction"
            },
            "transaction_date": {
                "name_keywords": ["date", "txn_date", "transaction_date", "trans_date", "time"],
                "exclude_keywords": [],
                "value_pattern": r"^\d{4}-\d{2}-\d{2}",  # YYYY-MM-DD
                "data_type": "date",
                "uniqueness_threshold": 0.05,
                "business_rule": "Date when transaction occurred"
            },
            "transaction_type": {
                "name_keywords": ["type", "txn_type", "transaction_type", "trans_type", "mode"],
                "exclude_keywords": ["account"],
                "value_pattern": r"^(deposit|withdrawal|transfer|payment|debit|credit)$",
                "data_type": "categorical",
                "uniqueness_threshold": 0.01,
                "business_rule": "Category of transaction (Deposit, Withdrawal, etc.)"
            },
            "debit": {
                "name_keywords": ["debit", "dr", "withdrawal", "withdraw", "debit_amount"],
                "exclude_keywords": ["credit", "balance"],
                "value_pattern": r"^\d+(\.\d{1,2})?$",
                "data_type": "numeric_decimal",
                "uniqueness_threshold": 0.5,
                "business_rule": "Amount debited from account"
            },
            "credit": {
                "name_keywords": ["credit", "cr", "deposit", "credit_amount"],
                "exclude_keywords": ["debit", "card"],
                "value_pattern": r"^\d+(\.\d{1,2})?$",
                "data_type": "numeric_decimal",
                "uniqueness_threshold": 0.5,
                "business_rule": "Amount credited to account"
            },
            "opening_balance": {
                "name_keywords": ["opening", "open_balance", "opening_bal", "start_balance"],
                "exclude_keywords": ["closing", "current"],
                "value_pattern": r"^-?\d+(\.\d{1,2})?$",
                "data_type": "numeric_decimal",
                "uniqueness_threshold": 0.3,
                "business_rule": "Balance at start of period"
            },
            "closing_balance": {
                "name_keywords": ["closing", "close_balance", "closing_bal", "end_balance"],
                "exclude_keywords": ["opening", "current"],
                "value_pattern": r"^-?\d+(\.\d{1,2})?$",
                "data_type": "numeric_decimal",
                "uniqueness_threshold": 0.3,
                "business_rule": "Balance at end of period"
            },
            "ifsc_code": {
                "name_keywords": ["ifsc", "ifsc_code", "swift"],
                "exclude_keywords": [],
                "value_pattern": r"^[A-Z]{4}0[A-Z0-9]{6}$",  # IFSC format
                "data_type": "fixed_format",
                "uniqueness_threshold": 0.01,
                "business_rule": "Bank branch identifier code"
            },
            "branch_code": {
                "name_keywords": ["branch", "branch_code", "branch_id"],
                "exclude_keywords": ["name", "address"],
                "value_pattern": r"^[A-Z0-9]{3,6}$",
                "data_type": "fixed_format",
                "uniqueness_threshold": 0.05,
                "business_rule": "Code identifying bank branch"
            },
            "loan_id": {
                "name_keywords": ["loan", "loan_id", "loan_number", "loan_account"],
                "exclude_keywords": ["type", "amount", "status"],
                "value_pattern": r"^[A-Z]{2,4}\d{6,12}$",
                "data_type": "mixed_alphanumeric",
                "uniqueness_threshold": 0.95,
                "business_rule": "Unique loan account identifier"
            },
            "emi_amount": {
                "name_keywords": ["emi", "installment", "monthly_payment", "emi_amount"],
                "exclude_keywords": [],
                "value_pattern": r"^\d+(\.\d{1,2})?$",
                "data_type": "numeric_decimal",
                "uniqueness_threshold": 0.2,
                "business_rule": "Monthly EMI payment amount"
            },
            "interest_rate": {
                "name_keywords": ["interest", "rate", "roi", "interest_rate", "apr"],
                "exclude_keywords": ["amount", "total"],
                "value_pattern": r"^\d{1,2}(\.\d{1,2})?$",  # Usually 0-99%
                "data_type": "numeric_decimal",
                "uniqueness_threshold": 0.05,
                "business_rule": "Interest rate percentage"
            },
            "card_number": {
                "name_keywords": ["card", "card_number", "card_no"],
                "exclude_keywords": ["type", "status", "cvv"],
                "value_pattern": r"^\d{13,19}$",  # Card numbers are 13-19 digits
                "data_type": "numeric",
                "uniqueness_threshold": 0.95,
                "business_rule": "Credit/Debit card number"
            }
        }
    
    def predict_column_type(self, column_name: str, data_series: pd.Series) -> Dict[str, Any]:
        """
        Predict column type with confidence score
        
        Args:
            column_name: Name of the column
            data_series: Pandas Series with column data
            
        Returns:
            Dictionary with prediction results
        """
        predictions = []
        
        # Score each possible column type
        for col_type, pattern_def in self.column_patterns.items():
            score = self._calculate_match_score(column_name, data_series, pattern_def)
            if score > 0:
                predictions.append({
                    "predicted_type": col_type,
                    "confidence": score,
                    "business_rule": pattern_def["business_rule"]
                })
        
        # Sort by confidence
        predictions.sort(key=lambda x: x["confidence"], reverse=True)
        
        # Get best prediction
        if predictions and predictions[0]["confidence"] >= 60:
            best_prediction = predictions[0]
            return {
                "column_name": column_name,
                "predicted_type": best_prediction["predicted_type"],
                "confidence": round(best_prediction["confidence"], 2),
                "business_rule": best_prediction["business_rule"],
                "alternative_predictions": predictions[1:3],  # Top 2 alternatives
                "data_quality": self._assess_data_quality(data_series),
                "sample_values": self._get_sample_values(data_series)
            }
        else:
            # Unknown column type
            return {
                "column_name": column_name,
                "predicted_type": "unknown",
                "confidence": 0,
                "business_rule": "Column type could not be determined confidently",
                "alternative_predictions": predictions[:3],
                "data_quality": self._assess_data_quality(data_series),
                "sample_values": self._get_sample_values(data_series)
            }
    
    def generate_column_business_rule(self, column_type: str, data_series: pd.Series) -> str:
        """
        Generate detailed business rule explanation for a column
        
        Args:
            column_type: Predicted column type
            data_series: Column data
            
        Returns:
            Formatted business rule text
        """
        if column_type in self.column_patterns:
            pattern_def = self.column_patterns[column_type]
            
            # Get statistics
            total_count = len(data_series)
            non_null_count = data_series.notna().sum()
            unique_count = data_series.nunique()
            
            rule_text = f"""**{column_type.replace('_', ' ').title()}**

**Purpose**: {pattern_def['business_rule']}

**Data Characteristics**:
- Total Records: {total_count}
- Non-Null Values: {non_null_count} ({round(non_null_count/total_count*100, 1)}%)
- Unique Values: {unique_count} ({round(unique_count/total_count*100, 1)}%)

**Expected Pattern**: Values should match the pattern of {pattern_def['data_type']} data.

**Business Rule Application**: This column is used to {pattern_def['business_rule'].lower()} and must maintain data integrity for accurate banking operations."""
            
            return rule_text
        else:
            return f"Business rule for {column_type} is not defined in the system."
    
    def calculate_prediction_confidence(self, column_name: str, data_series: pd.Series, 
                                       predicted_type: str) -> float:
        """
        Calculate confidence score for a prediction
        
        Returns:
            Confidence score (0-100)
        """
        if predicted_type not in self.column_patterns:
            return 0.0
        
        pattern_def = self.column_patterns[predicted_type]
        return self._calculate_match_score(column_name, data_series, pattern_def)
    
    def format_column_explanation(self, prediction: Dict[str, Any]) -> str:
        """
        Format column prediction as user-friendly explanation
        
        Args:
            prediction: Dictionary from predict_column_type()
            
        Returns:
            Formatted paragraph explanation
        """
        col_name = prediction["column_name"]
        pred_type = prediction["predicted_type"]
        confidence = prediction["confidence"]
        rule = prediction["business_rule"]
        quality = prediction["data_quality"]
        
        if pred_type == "unknown":
            explanation = f"""**Column: {col_name}**

The system could not confidently identify this column's business purpose (confidence below 60%). 
Based on the data patterns, it appears to contain {quality['data_type_description']} information.

**Data Quality**: {quality['completeness']}% complete, {quality['uniqueness']}% unique values.

**Recommendation**: Please verify this column's purpose with domain experts or system documentation."""
        else:
            explanation = f"""**Column: {col_name}** → Predicted as **{pred_type.replace('_', ' ').title()}** (Confidence: {confidence}%)

**Business Purpose**: {rule}

**Data Quality**: 
- Completeness: {quality['completeness']}% (fewer missing values is better)
- Uniqueness: {quality['uniqueness']}% (depends on column type)
- Overall Quality: {quality['quality_rating']}

**Sample Values**: {', '.join(map(str, prediction['sample_values'][:5]))}

This column is essential for banking operations and should maintain high data quality standards."""
        
        return explanation
    
    # Private helper methods
    
    def _calculate_match_score(self, column_name: str, data_series: pd.Series, 
                               pattern_def: Dict) -> float:
        """Calculate how well a column matches a pattern definition"""
        score = 0.0
        max_score = 100.0
        
        # 1. Column name matching (40% weight)
        name_score = self._score_column_name(column_name, pattern_def)
        score += name_score * 0.4
        
        # 2. Data value pattern matching (35% weight)
        value_score = self._score_value_pattern(data_series, pattern_def)
        score += value_score * 0.35
        
        # 3. Uniqueness matching (15% weight)
        uniqueness_score = self._score_uniqueness(data_series, pattern_def)
        score += uniqueness_score * 0.15
        
        # 4. Data type matching (10% weight)
        dtype_score = self._score_data_type(data_series, pattern_def)
        score += dtype_score * 0.10
        
        return min(score, max_score)
    
    def _score_column_name(self, column_name: str, pattern_def: Dict) -> float:
        """Score based on column name keywords"""
        col_lower = column_name.lower().replace('_', '').replace(' ', '')
        
        # Check for excluded keywords first
        for exclude_kw in pattern_def.get("exclude_keywords", []):
            if exclude_kw in col_lower:
                return 0.0  # Immediate disqualification
        
        # Check for matching keywords
        for keyword in pattern_def["name_keywords"]:
            keyword_clean = keyword.replace('_', '').replace(' ', '')
            if keyword_clean in col_lower or col_lower in keyword_clean:
                return 100.0  # Strong match
        
        return 0.0
    
    def _score_value_pattern(self, data_series: pd.Series, pattern_def: Dict) -> float:
        """Score based on how well data values match expected pattern"""
        non_null = data_series.dropna().astype(str)
        if len(non_null) == 0:
            return 0.0
        
        pattern = pattern_def.get("value_pattern")
        if not pattern:
            return 50.0  # Neutral score if no pattern defined
        
        # Check how many values match the pattern
        sample_size = min(100, len(non_null))
        sample = non_null.head(sample_size)
        
        matches = sample.str.match(pattern, case=False).sum()
        match_ratio = matches / sample_size
        
        return match_ratio * 100.0
    
    def _score_uniqueness(self, data_series: pd.Series, pattern_def: Dict) -> float:
        """Score based on uniqueness threshold"""
        if len(data_series) == 0:
            return 0.0
        
        actual_uniqueness = data_series.nunique() / len(data_series)
        expected_uniqueness = pattern_def.get("uniqueness_threshold", 0.5)
        
        # Calculate difference
        diff = abs(actual_uniqueness - expected_uniqueness)
        
        # Convert to score (closer to expected = higher score)
        if diff < 0.1:
            return 100.0
        elif diff < 0.3:
            return 70.0
        elif diff < 0.5:
            return 40.0
        else:
            return 20.0
    
    def _score_data_type(self, data_series: pd.Series, pattern_def: Dict) -> float:
        """Score based on data type matching"""
        expected_dtype = pattern_def.get("data_type", "unknown")
        
        if pd.api.types.is_numeric_dtype(data_series):
            if expected_dtype in ["numeric", "numeric_decimal"]:
                return 100.0
            else:
                return 30.0
        elif pd.api.types.is_datetime64_any_dtype(data_series):
            if expected_dtype == "date":
                return 100.0
            else:
                return 20.0
        else:
            # String/object type
            if expected_dtype in ["text", "categorical", "mixed_alphanumeric", "fixed_format"]:
                return 80.0
            else:
                return 50.0
    
    def _assess_data_quality(self, data_series: pd.Series) -> Dict[str, Any]:
        """Assess overall data quality of a column"""
        total = int(len(data_series))
        non_null = int(data_series.notna().sum())
        unique = int(data_series.nunique())
        
        completeness = round(float(non_null / total) * 100, 2) if total > 0 else 0.0
        uniqueness = round(float(unique / total) * 100, 2) if total > 0 else 0.0
        
        # Determine quality rating
        if completeness >= 95 and (uniqueness >= 80 or uniqueness <= 10):
            quality_rating = "Excellent"
        elif completeness >= 85:
            quality_rating = "Good"
        elif completeness >= 70:
            quality_rating = "Fair"
        else:
            quality_rating = "Poor"
        
        # Detect data type
        if pd.api.types.is_numeric_dtype(data_series):
            dtype_desc = "numeric"
        elif pd.api.types.is_datetime64_any_dtype(data_series):
            dtype_desc = "date/time"
        else:
            dtype_desc = "text"
        
        return {
            "completeness": completeness,
            "uniqueness": uniqueness,
            "quality_rating": quality_rating,
            "data_type_description": dtype_desc
        }
    
    def _get_sample_values(self, data_series: pd.Series, n: int = 5) -> List:
        """Get sample non-null values from the column"""
        non_null = data_series.dropna()
        if len(non_null) == 0:
            return ["<all null>"]
        
        sample = non_null.head(n).tolist()
        return sample
