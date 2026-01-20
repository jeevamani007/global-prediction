"""
Enhanced Banking Data Validation Engine

A comprehensive banking-grade validation engine that validates all 23 standard banking columns
with field-level error handling, sensitive data masking, and user-friendly explanations.

Follows the Definition-Condition-Action format from banking business rules specification.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import re
from datetime import datetime
from banking_validation_rules import BankingValidationRules


class EnhancedBankingValidationEngine:
    """
    Enhanced Banking Data Validation Engine
    
    Provides comprehensive validation for all 23 standard banking columns:
    - Customer Information (6 columns)
    - Account Information (5 columns)
    - Transaction Module (3 columns)
    - KYC & Compliance (4 columns)
    - Risk & Scoring (2 columns)
    - Nominee & Additional (3 columns)
    """
    
    def __init__(self):
        self.rules = BankingValidationRules()
    
    def validate_file(self, file_path: str, df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """
        Main validation function for a single file
        
        Args:
            file_path: Path to the CSV file to validate
            df: Optional pre-loaded DataFrame
            
        Returns:
            Dictionary with comprehensive validation results
        """
        try:
            # Load dataset if not provided
            if df is None:
                df = pd.read_csv(file_path)
            
            file_name = file_path.split('/')[-1] if '/' in file_path else file_path.split('\\')[-1]
            
            if df.empty:
                return {
                    "file_name": file_name,
                    "status": "INVALID",
                    "columns": [],
                    "summary": {
                        "total_columns": 0,
                        "valid_count": 0,
                        "warning_count": 0,
                        "invalid_count": 0,
                        "message": "The uploaded file is empty. Please upload a file containing banking data."
                    },
                    "can_proceed": False
                }
            
            # Validate each column
            column_validations = []
            for column_name in df.columns:
                validation_result = self._validate_column(column_name, df[column_name], df, file_name)
                column_validations.append(validation_result)
            
            # Calculate summary statistics
            valid_count = sum(1 for col in column_validations if col["status"] == "VALID")
            warning_count = sum(1 for col in column_validations if col["status"] == "WARNING")
            invalid_count = sum(1 for col in column_validations if col["status"] == "INVALID")
            
            # Determine overall status
            if invalid_count > 0:
                overall_status = "INVALID"
                can_proceed = False
                message = f"File '{file_name}' has {invalid_count} field(s) that require correction. Please correct these fields before the upload can proceed. {valid_count} field(s) are valid and {warning_count} field(s) have warnings."
            elif warning_count > 0:
                overall_status = "WARNING"
                can_proceed = True
                message = f"File '{file_name}' is ready for processing. {valid_count} field(s) are valid and {warning_count} field(s) have warnings that should be reviewed."
            else:
                overall_status = "VALID"
                can_proceed = True
                message = f"File '{file_name}' is valid and ready for processing. All {len(column_validations)} field(s) meet banking data requirements."
            
            return {
                "file_name": file_name,
                "status": overall_status,
                "columns": column_validations,
                "summary": {
                    "total_columns": len(column_validations),
                    "valid_count": valid_count,
                    "warning_count": warning_count,
                    "invalid_count": invalid_count,
                    "message": message
                },
                "can_proceed": can_proceed
            }
            
        except Exception as e:
            file_name = file_path.split('/')[-1] if '/' in file_path else file_path.split('\\')[-1]
            return {
                "file_name": file_name,
                "status": "INVALID",
                "columns": [],
                "summary": {
                    "total_columns": 0,
                    "valid_count": 0,
                    "warning_count": 0,
                    "invalid_count": 0,
                    "message": f"The file could not be processed. Please ensure it is a valid CSV file containing banking data. Error: {str(e)}"
                },
                "can_proceed": False,
                "error": str(e)
            }
    
    def validate_multiple_files(self, file_paths: List[str]) -> Dict[str, Any]:
        """
        Validate multiple files
        
        Args:
            file_paths: List of paths to CSV files
            
        Returns:
            Dictionary with validation results for each file
        """
        results = {
            "files": [],
            "overall_status": "VALID",
            "can_proceed": True,
            "summary": {
                "total_files": len(file_paths),
                "valid_files": 0,
                "warning_files": 0,
                "invalid_files": 0
            }
        }
        
        for file_path in file_paths:
            file_result = self.validate_file(file_path)
            results["files"].append(file_result)
            
            if file_result["status"] == "VALID":
                results["summary"]["valid_files"] += 1
            elif file_result["status"] == "WARNING":
                results["summary"]["warning_files"] += 1
            else:
                results["summary"]["invalid_files"] += 1
                results["can_proceed"] = False
        
        # Determine overall status
        if results["summary"]["invalid_files"] > 0:
            results["overall_status"] = "INVALID"
            results["can_proceed"] = False
        elif results["summary"]["warning_files"] > 0:
            results["overall_status"] = "WARNING"
        
        return results
    
    def _validate_column(self, column_name: str, series: pd.Series, df: pd.DataFrame, file_name: str) -> Dict[str, Any]:
        """
        Validate a single column against banking rules
        
        Args:
            column_name: Name of the column
            series: Pandas Series containing the column data
            df: Full DataFrame for context
            file_name: Name of the file being validated
            
        Returns:
            Comprehensive validation result for the column
        """
        # Get validation rule for this column
        rule = self.rules.get_rule(column_name)
        
        if not rule:
            # No specific rule found, apply generic validation
            return self._validate_generic_column(column_name, series, file_name)
        
        # Apply specific banking rule validation
        violations = []
        warnings = []
        
        # Check mandatory field
        if rule.get("mandatory", False):
            null_count = series.isna().sum()
            if null_count > 0:
                violations.append(f"Contains {null_count} missing values but this field is required")
        
        # Check uniqueness
        if rule.get("unique", False):
            non_null_series = series.dropna()
            if len(non_null_series) > 0:
                duplicate_count = len(non_null_series) - non_null_series.nunique()
                if duplicate_count > 0:
                    violations.append(f"Contains {duplicate_count} duplicate values but must be unique")
        
        # Check allowed values
        allowed_values = rule.get("allowed_values")
        if allowed_values:
            non_null_series = series.dropna().astype(str).str.strip().str.upper()
            allowed_upper = [str(v).upper() for v in allowed_values]
            invalid_values = set()
            for val in non_null_series.unique():
                if val not in allowed_upper:
                    invalid_values.add(val)
            
            if invalid_values:
                sample_invalid = list(invalid_values)[:3]
                violations.append(f"Contains invalid values: {', '.join(map(str, sample_invalid))}. Allowed: {', '.join(allowed_values)}")
        
        # Check format constraints
        format_regex = rule.get("format_regex")
        if format_regex and rule.get("data_type") == "text":
            non_null_series = series.dropna().astype(str)
            invalid_format_count = 0
            for val in non_null_series:
                if not re.match(format_regex, str(val).strip()):
                    invalid_format_count += 1
            
            if invalid_format_count > 0:
                violations.append(f"{invalid_format_count} values do not match required format: {rule.get('format_description', 'standard format')}")
        
        # Check length constraints
        min_length = rule.get("min_length")
        max_length = rule.get("max_length")
        if rule.get("data_type") == "text" and (min_length or max_length):
            non_null_series = series.dropna().astype(str)
            
            if min_length:
                too_short = sum(1 for val in non_null_series if len(str(val).strip()) < min_length)
                if too_short > 0:
                    violations.append(f"{too_short} values are shorter than minimum length of {min_length} characters")
            
            if max_length:
                too_long = sum(1 for val in non_null_series if len(str(val).strip()) > max_length)
                if too_long > 0:
                    violations.append(f"{too_long} values exceed maximum length of {max_length} characters")
        
        # Check numeric constraints
        if rule.get("data_type") == "numeric":
            numeric_series = pd.to_numeric(series, errors='coerce')
            non_null_numeric = numeric_series.dropna()
            
            if len(non_null_numeric) > 0:
                min_value = rule.get("min_value")
                max_value = rule.get("max_value")
                
                if min_value is not None:
                    below_min = (non_null_numeric < min_value).sum()
                    if below_min > 0:
                        violations.append(f"{below_min} values are below minimum of {min_value}")
                
                if max_value is not None:
                    above_max = (non_null_numeric > max_value).sum()
                    if above_max > 0:
                        violations.append(f"{above_max} values are above maximum of {max_value}")
        
        # Check date constraints
        if rule.get("data_type") == "date":
            try:
                date_series = pd.to_datetime(series, errors='coerce')
                non_null_dates = date_series.dropna()
                
                if len(non_null_dates) > 0:
                    # Check for future dates where not allowed
                    if column_name.lower() in ["transaction_date", "kyc_verified_date"]:
                        future_dates = (non_null_dates > pd.Timestamp.now()).sum()
                        if future_dates > 0:
                            violations.append(f"{future_dates} dates are in the future but must be past dates")
            except:
                violations.append("Contains invalid date formats")
        
        # Determine status
        if violations:
            status = "INVALID"
        elif warnings:
            status = "WARNING"
        else:
            status = "VALID"
        
        # Get masked value for sensitive fields
        masked_sample = None
        if rule.get("sensitive") and len(series.dropna()) > 0:
            sample_value = str(series.dropna().iloc[0])
            masked_sample = self.rules.mask_sensitive_data(column_name, sample_value)
        
        # Build response
        result = {
            "column_name": column_name,
            "file_name": file_name,
            "status": status,
            "definition": rule.get("definition", ""),
            "condition": rule.get("format_description", "Standard banking validation rules"),
            "violations": violations,
            "warnings": warnings
        }
        
        # Add action based on status
        if status == "VALID":
            action_valid = rule.get("action_valid", {})
            result["action"] = {
                "message": action_valid.get("message", "Field is valid"),
                "usage": action_valid.get("usage", []),
                "next_steps": action_valid.get("next_steps", "Ready for processing")
            }
        else:
            action_invalid = rule.get("action_invalid", {})
            # Determine which specific action message to use
            if "missing" in ' '.join(violations).lower():
                issue_message = action_invalid.get("missing", action_invalid.get("format", "Field has validation issues"))
            elif "duplicate" in ' '.join(violations).lower():
                issue_message = action_invalid.get("duplicate", "Field contains duplicate values")
            elif "format" in ' '.join(violations).lower() or "length" in ' '.join(violations).lower():
                issue_message = action_invalid.get("format", action_invalid.get("invalid_format", "Field format is invalid"))
            elif "invalid values" in ' '.join(violations).lower():
                issue_message = action_invalid.get("invalid_value", "Field contains invalid values")
            else:
                issue_message = action_invalid.get("format", "Field has validation issues")
            
            result["action"] = {
                "issue": issue_message,
                "why_required": action_invalid.get("why_required", "This field is required for banking operations"),
                "what_to_do": "Please correct the issues listed above",
                "blocked_actions": action_invalid.get("blocked_actions", [])
            }
        
        # Add masked value for sensitive fields
        if masked_sample:
            result["masked_sample"] = masked_sample
            result["display_note"] = "Sensitive data is masked for security"
        
        return result
    
    def _validate_generic_column(self, column_name: str, series: pd.Series, file_name: str) -> Dict[str, Any]:
        """
        Generic validation for columns without specific banking rules
        
        Args:
            column_name: Name of the column
            series: Pandas Series containing the column data
            file_name: Name of the file
            
        Returns:
            Basic validation result
        """
        # Basic data quality checks
        null_count = series.isna().sum()
        total_count = len(series)
        null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
        
        violations = []
        warnings = []
        
        # Warn if more than 50% nulls
        if null_percentage > 50:
            warnings.append(f"{null_count} ({null_percentage:.1f}%) values are missing")
        
        # Check if all values are null
        if null_percentage == 100:
            violations.append("All values are missing")
        
        status = "INVALID" if violations else ("WARNING" if warnings else "VALID")
        
        return {
            "column_name": column_name,
            "file_name": file_name,
            "status": status,
            "definition": f"General banking data field: {column_name}",
            "condition": "Basic data quality validation applied",
            "violations": violations,
            "warnings": warnings,
            "action": {
                "message": "Field validated with basic data quality checks" if status == "VALID" else "Field has data quality issues",
                "usage": ["Used in banking data processing and reporting"],
                "next_steps": "Ready for processing" if status == "VALID" else "Please review and correct issues"
            }
        }
    
    def format_ui_response(self, validation_result: Dict) -> str:
        """
        Format validation result as UI-friendly text response
        
        Args:
            validation_result: Validation result dictionary
            
        Returns:
            Formatted string for display
        """
        lines = []
        
        # File summary
        summary = validation_result.get("summary", {})
        lines.append(f"File: {validation_result.get('file_name', 'Unknown')}")
        lines.append(f"Status: {validation_result.get('status', 'UNKNOWN')}")
        lines.append(f"\nSummary:")
        lines.append(f"  Total Columns: {summary.get('total_columns', 0)}")
        lines.append(f"  Valid: {summary.get('valid_count', 0)}")
        lines.append(f"  Warnings: {summary.get('warning_count', 0)}")
        lines.append(f"  Invalid: {summary.get('invalid_count', 0)}")
        lines.append(f"\n{summary.get('message', '')}")
        lines.append("\n" + "="*80 + "\n")
        
        # Column details
        for col in validation_result.get("columns", []):
            lines.append(f"\nColumn: {col['column_name']}")
            lines.append(f"Status: {col['status']}")
            lines.append(f"\nDefinition: {col.get('definition', 'N/A')}")
            lines.append(f"Condition: {col.get('condition', 'N/A')}")
            
            if col['status'] != "VALID":
                if col.get('violations'):
                    lines.append(f"\nIssues:")
                    for violation in col['violations']:
                        lines.append(f"  - {violation}")
                
                action = col.get('action', {})
                if action.get('issue'):
                    lines.append(f"\nIssue: {action['issue']}")
                if action.get('why_required'):
                    lines.append(f"Why Required: {action['why_required']}")
                if action.get('blocked_actions'):
                    lines.append(f"Blocked Actions: {', '.join(action['blocked_actions'])}")
            else:
                action = col.get('action', {})
                if action.get('usage'):
                    lines.append(f"\nUsage:")
                    for usage in action['usage']:
                        lines.append(f"  - {usage}")
            
            lines.append("\n" + "-"*80)
        
        return "\n".join(lines)
