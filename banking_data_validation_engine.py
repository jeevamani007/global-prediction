"""
Banking Data Validation Engine

A banking data validation engine that analyzes CSV files using standard banking business rules.
Applies Definition, Condition, and Action logic in the background.
Returns user-friendly, professional validation results.

INTERNAL PROCESS (NOT EXPOSED):
1. Identify each file and its fields
2. Understand the business meaning of each field
3. Apply appropriate banking business rules
4. Decide VALID / WARNING / INVALID
5. Generate user-facing explanations based on impact
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import re
from datetime import datetime
from core_banking_business_rules_engine import CoreBankingBusinessRulesEngine


class BankingDataValidationEngine:
    """
    Banking Data Validation Engine
    
    Analyzes banking CSV files using standard banking business rules.
    Returns results in a clean, professional, UI-friendly format.
    """
    
    def __init__(self):
        self.core_engine = CoreBankingBusinessRulesEngine()
        self._initialize_collateral_types()
    
    def _initialize_collateral_types(self):
        """Initialize additional banking concepts not in core engine"""
        # Add collateral_type to banking concepts if not present
        if "collateral_type" not in self.core_engine.banking_concepts:
            self.core_engine.banking_concepts["collateral_type"] = {
                "domain": "Loan",
                "name_patterns": ["collateral_type", "collateral", "security_type", "asset_type"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "low",
                    "cardinality": {"max": 10},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Collateral": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Predefined banking collateral types",
                    "allowed_values": ["Property", "Gold", "Vehicle", "Fixed Deposit", "Shares", "Mutual Funds"],
                    "reason": "Type of asset provided as security for loan. Required for collateral valuation and risk assessment.",
                    "violation_impact": "BUSINESS: Cannot assess collateral value. FINANCIAL: Risk calculation errors. COMPLIANCE: Loan security documentation incomplete."
                }
            }
    
    def validate_file(self, file_path: str) -> Dict[str, Any]:
        """
        Main validation function
        
        Args:
            file_path: Path to the CSV file to validate
            
        Returns:
            Dictionary with validation results in UI-friendly format
        """
        try:
            # Load the dataset
            df = pd.read_csv(file_path)
            file_name = file_path.split('/')[-1] if '/' in file_path else file_path.split('\\')[-1]
            
            if df.empty:
                return {
                    "file_name": file_name,
                    "status": "INVALID",
                    "fields": [],
                    "summary": "The uploaded file is empty. Please upload a file containing banking data.",
                    "can_proceed": False
                }
            
            # INTERNAL STEP 1-5: Analyze using core engine (Definition/Condition/Action)
            analysis_result = self.core_engine.analyze_dataset(file_path, df)
            
            # Convert analysis to UI-friendly validation results
            field_validations = []
            has_invalid = False
            has_warning = False
            
            for col_analysis in analysis_result.get("columns_analysis", []):
                field_result = self._convert_to_ui_format(col_analysis, df, file_name)
                field_validations.append(field_result)
                
                if field_result["status"] == "INVALID":
                    has_invalid = True
                elif field_result["status"] == "WARNING":
                    has_warning = True
            
            # Determine overall status
            if has_invalid:
                overall_status = "INVALID"
                can_proceed = False
            elif has_warning:
                overall_status = "WARNING"
                can_proceed = True  # Partial processing allowed
            else:
                overall_status = "VALID"
                can_proceed = True
            
            # Generate summary
            summary = self._generate_summary(field_validations, file_name, can_proceed)
            
            return {
                "file_name": file_name,
                "status": overall_status,
                "fields": field_validations,
                "summary": summary,
                "can_proceed": can_proceed
            }
            
        except Exception as e:
            file_name = file_path.split('/')[-1] if '/' in file_path else file_path.split('\\')[-1]
            return {
                "file_name": file_name,
                "status": "INVALID",
                "fields": [],
                "summary": f"The file could not be processed. Please ensure it is a valid CSV file containing banking data.",
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
            "summary": ""
        }
        
        all_valid = True
        all_invalid = True
        
        for file_path in file_paths:
            file_result = self.validate_file(file_path)
            results["files"].append(file_result)
            
            if file_result["status"] == "INVALID":
                all_valid = False
                results["can_proceed"] = False
            elif file_result["status"] == "WARNING":
                all_valid = False
            else:
                all_invalid = False
        
        if all_invalid:
            results["overall_status"] = "INVALID"
            results["can_proceed"] = False
            results["summary"] = "All files require correction before processing can continue."
        elif not all_valid:
            results["overall_status"] = "WARNING"
            results["summary"] = "Some files have warnings but processing can proceed. Review recommended."
        else:
            results["summary"] = "All files are valid and ready for processing."
        
        return results
    
    def _convert_to_ui_format(self, col_analysis: Dict, df: pd.DataFrame, file_name: str) -> Dict[str, Any]:
        """
        Convert technical analysis to UI-friendly format
        
        INTERNAL: Applies Definition, Condition, Action logic
        """
        column_name = col_analysis.get("column_name", "")
        series = df[column_name]
        
        # Get field definition and business meaning
        concept = col_analysis.get("step3_identified_as", "unknown")
        business_meaning = col_analysis.get("step5_business_meaning", "")
        profile = col_analysis.get("step1_profile", {})
        business_rules = col_analysis.get("step5_business_rules", {})
        
        # CONDITION: Check if field meets business rules
        violations = []
        warnings = []
        
        # Check mandatory field
        if business_rules.get("mandatory", False):
            null_count = profile.get("null_count", 0)
            if null_count > 0:
                violations.append(f"Contains {null_count} missing values but this field is required")
        
        # Check uniqueness
        if business_rules.get("unique", False):
            uniqueness_pct = profile.get("uniqueness_percentage", 0)
            if uniqueness_pct < 95:
                violations.append(f"Must be unique but {100 - uniqueness_pct:.1f}% of values are duplicates")
        
        # Check allowed values
        allowed_values = business_rules.get("allowed_values")
        if allowed_values:
            invalid_values = self._check_allowed_values(series, allowed_values)
            if invalid_values:
                sample_invalid = list(invalid_values)[:3]
                violations.append(f"Contains invalid values: {', '.join(map(str, sample_invalid))}")
        
        # Check format/length constraints
        format_violations = self._check_format_constraints(series, profile, business_rules, concept)
        violations.extend(format_violations)
        
        # Check numeric ranges
        if profile.get("data_type") in ["numeric", "decimal"]:
            range_violations = self._check_numeric_range(series, business_rules, concept)
            violations.extend(range_violations)
        
        # Check date validity
        if profile.get("data_type") == "date":
            date_violations = self._check_date_validity(series, concept)
            violations.extend(date_violations)
        
        # ACTION: Determine status
        if violations:
            status = "INVALID"
        elif warnings:
            status = "WARNING"
        else:
            status = "VALID"
        
        # Generate user-friendly explanations
        field_usage = self._get_field_usage_description(business_meaning, concept, column_name)
        system_check = self._get_system_check_description(business_rules, profile)
        status_meaning = self._get_status_meaning(status, violations, warnings, business_rules)
        correction_guidance = self._get_correction_guidance(status, violations, business_rules, concept)
        
        return {
            "field_name": column_name,
            "file_name": file_name,
            "status": status,
            "field_usage": field_usage,
            "system_check": system_check,
            "status_meaning": status_meaning,
            "correction_guidance": correction_guidance,
            "violations": violations,
            "warnings": warnings
        }
    
    def _check_allowed_values(self, series: pd.Series, allowed_values: List[str]) -> set:
        """Check if series contains only allowed values"""
        invalid = set()
        non_null = series.dropna().astype(str).str.strip()
        
        for val in non_null.unique():
            if val not in allowed_values:
                invalid.add(val)
        
        return invalid
    
    def _check_format_constraints(self, series: pd.Series, profile: Dict, 
                                  business_rules: Dict, concept: str) -> List[str]:
        """Check format and length constraints"""
        violations = []
        
        # Check length constraints for text fields
        if profile.get("data_type") in ["text", "alphanumeric"]:
            patterns = profile.get("patterns", {})
            min_length = patterns.get("min_length")
            max_length = patterns.get("max_length")
            
            # Get expected length from concept definition
            concept_def = self.core_engine.banking_concepts.get(concept, {})
            data_patterns = concept_def.get("data_patterns", {})
            
            expected_length = data_patterns.get("length", {})
            expected_min = expected_length.get("min") if expected_length else None
            expected_max = expected_length.get("max") if expected_length else None
            
            non_null = series.dropna().astype(str)
            
            if expected_min:
                too_short = non_null[non_null.str.len() < expected_min]
                if len(too_short) > 0:
                    violations.append(f"Some values are shorter than required minimum length of {expected_min} characters")
            
            if expected_max:
                too_long = non_null[non_null.str.len() > expected_max]
                if len(too_long) > 0:
                    violations.append(f"Some values exceed maximum length of {expected_max} characters")
        
        # Check pattern constraints (numeric only, alphanumeric, etc.)
        if business_rules.get("format"):
            format_str = str(business_rules["format"])
            if "numeric" in format_str.lower() or "digits" in format_str.lower():
                non_null = series.dropna().astype(str)
                non_numeric = non_null[~non_null.str.isdigit()]
                if len(non_numeric) > 0:
                    violations.append("Contains non-numeric values but only numeric values are allowed")
        
        return violations
    
    def _check_numeric_range(self, series: pd.Series, business_rules: Dict, concept: str) -> List[str]:
        """Check numeric range constraints"""
        violations = []
        
        numeric_series = pd.to_numeric(series, errors='coerce').dropna()
        if len(numeric_series) == 0:
            return violations
        
        # Check for negative values when not allowed
        if business_rules.get("format") and "> 0" in str(business_rules.get("format", "")):
            negative_count = (numeric_series < 0).sum()
            if negative_count > 0:
                violations.append(f"Contains {negative_count} negative values but only positive values are allowed")
        
        # Check for zero when not allowed
        if business_rules.get("format") and "> 0" in str(business_rules.get("format", "")):
            zero_count = (numeric_series == 0).sum()
            if zero_count > 0:
                violations.append(f"Contains {zero_count} zero values but zero is not allowed")
        
        # Check concept-specific ranges
        concept_def = self.core_engine.banking_concepts.get(concept, {})
        data_patterns = concept_def.get("data_patterns", {})
        expected_range = data_patterns.get("range", {})
        
        if expected_range:
            min_val = expected_range.get("min")
            max_val = expected_range.get("max")
            
            if min_val is not None:
                below_min = (numeric_series < min_val).sum()
                if below_min > 0:
                    violations.append(f"Contains {below_min} values below minimum of {min_val}")
            
            if max_val is not None:
                above_max = (numeric_series > max_val).sum()
                if above_max > 0:
                    violations.append(f"Contains {above_max} values above maximum of {max_val}")
        
        return violations
    
    def _check_date_validity(self, series: pd.Series, concept: str) -> List[str]:
        """Check date validity constraints"""
        violations = []
        
        # Check if dates are in the past (for birth dates, transaction dates, etc.)
        if concept in ["dob", "date_of_birth", "birth_date"]:
            non_null = series.dropna()
            try:
                dates = pd.to_datetime(non_null, errors='coerce')
                future_dates = dates[dates > pd.Timestamp.now()]
                if len(future_dates) > 0:
                    violations.append("Contains future dates but birth dates must be in the past")
            except:
                violations.append("Contains invalid date formats")
        
        return violations
    
    def _get_field_usage_description(self, business_meaning: str, concept: str, column_name: str) -> str:
        """Generate description of what the field is used for"""
        if business_meaning:
            # Extract first sentence or first 100 characters
            sentences = business_meaning.split('.')
            if sentences:
                return sentences[0].strip()
        
        # Fallback to concept-based description
        concept_def = self.core_engine.banking_concepts.get(concept, {})
        if concept_def:
            domain = concept_def.get("domain", "")
            return f"This field relates to {domain.lower()} operations in the banking system."
        
        # Generic fallback
        return f"This field is used in banking data processing and reporting."
    
    def _get_system_check_description(self, business_rules: Dict, profile: Dict) -> str:
        """Generate description of what the system checked"""
        checks = []
        
        if business_rules.get("mandatory", False):
            checks.append("mandatory field requirement")
        
        if business_rules.get("unique", False):
            checks.append("uniqueness requirement")
        
        if business_rules.get("allowed_values"):
            allowed_vals = business_rules["allowed_values"][:3]
            checks.append(f"allowed values ({', '.join(map(str, allowed_vals))})")
        
        if business_rules.get("format"):
            checks.append("format and length constraints")
        
        if profile.get("data_type") in ["numeric", "decimal"]:
            checks.append("numeric range validation")
        
        if checks:
            return f"The system checked: {', '.join(checks)}."
        else:
            return "The system checked standard data quality requirements."
    
    def _get_status_meaning(self, status: str, violations: List[str], warnings: List[str], 
                           business_rules: Dict) -> str:
        """Generate explanation of what the status means for the user"""
        if status == "VALID":
            return "This field meets all banking data requirements and is ready for processing."
        
        elif status == "WARNING":
            if warnings:
                return f"This field has {len(warnings)} warning(s) that should be reviewed but will not block processing."
            return "This field has minor issues that should be reviewed but will not block processing."
        
        else:  # INVALID
            if violations:
                mandatory = business_rules.get("mandatory", False)
                if mandatory:
                    return f"This field has {len(violations)} critical issue(s) that must be corrected before processing can continue."
                else:
                    return f"This field has {len(violations)} issue(s) that need to be corrected."
            return "This field does not meet banking data requirements and needs correction."
    
    def _get_correction_guidance(self, status: str, violations: List[str], 
                                business_rules: Dict, concept: str) -> str:
        """Generate guidance on what needs to be corrected"""
        if status == "VALID":
            return ""
        
        guidance_parts = []
        
        if violations:
            # Check for missing values
            missing_violations = [v for v in violations if "missing" in v.lower()]
            if missing_violations:
                guidance_parts.append("Ensure all required values are provided")
            
            # Check for duplicates
            duplicate_violations = [v for v in violations if "duplicate" in v.lower() or "unique" in v.lower()]
            if duplicate_violations:
                guidance_parts.append("Ensure all values are unique")
            
            # Check for invalid values
            invalid_value_violations = [v for v in violations if "invalid values" in v.lower()]
            if invalid_value_violations:
                concept_def = self.core_engine.banking_concepts.get(concept, {})
                rules = concept_def.get("business_rules", {})
                allowed = rules.get("allowed_values", [])
                if allowed:
                    guidance_parts.append(f"Use only these accepted values: {', '.join(map(str, allowed[:5]))}")
            
            # Check for format issues
            format_violations = [v for v in violations if "length" in v.lower() or "format" in v.lower() or "numeric" in v.lower()]
            if format_violations:
                concept_def = self.core_engine.banking_concepts.get(concept, {})
                rules = concept_def.get("business_rules", {})
                format_desc = rules.get("format", "")
                if format_desc:
                    guidance_parts.append(f"Ensure values follow this format: {format_desc}")
        
        if guidance_parts:
            return "Please correct this field to continue processing this file. " + " ".join(guidance_parts) + "."
        
        return "Please review and correct the issues identified above to continue processing this file."
    
    def _generate_summary(self, field_validations: List[Dict], file_name: str, can_proceed: bool) -> str:
        """Generate overall summary"""
        total_fields = len(field_validations)
        valid_count = sum(1 for f in field_validations if f["status"] == "VALID")
        warning_count = sum(1 for f in field_validations if f["status"] == "WARNING")
        invalid_count = sum(1 for f in field_validations if f["status"] == "INVALID")
        
        if invalid_count > 0:
            return f"File '{file_name}' has {invalid_count} field(s) that require correction. Please correct these fields before the upload can proceed. {valid_count} field(s) are valid and {warning_count} field(s) have warnings."
        elif warning_count > 0:
            return f"File '{file_name}' is ready for processing. {valid_count} field(s) are valid and {warning_count} field(s) have warnings that should be reviewed."
        else:
            return f"File '{file_name}' is valid and ready for processing. All {total_fields} field(s) meet banking data requirements."
    
    def format_ui_response(self, validation_result: Dict) -> str:
        """
        Format validation result as UI-friendly text response
        
        Returns line-by-line format suitable for display in UI
        """
        lines = []
        
        # Format each field
        for field in validation_result.get("fields", []):
            lines.append(f"{field['field_name']}")
            lines.append(f"File: {field['file_name']}")
            lines.append(f"Status: {field['status']}")
            lines.append("")
            lines.append(field['field_usage'])
            lines.append(field['system_check'])
            lines.append(field['status_meaning'])
            
            if field['correction_guidance']:
                lines.append(field['correction_guidance'])
            
            lines.append("")
        
        # Add summary
        lines.append(validation_result.get("summary", ""))
        
        return "\n".join(lines)
