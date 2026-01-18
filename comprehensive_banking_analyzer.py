"""
Comprehensive Banking Domain Analyzer

Senior Banking Domain Architect and Data Model Expert System

Implements ALL 9 steps:
1. Column Analysis & Profiling
2. Primary Key Detection (Strict)
3. Foreign Key Detection (Strict)
4. Parent-Child Direction Logic
5. Invalid Relation Filtering
6. Relationship Confidence Classification
7. Final Relation Output
8. Application Structure Correction
9. Error Reporting

Follows banking-domain-accurate logic only.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any, Set
import re
import os
from collections import defaultdict
from rapidfuzz import fuzz


class ComprehensiveBankingAnalyzer:
    """
    Comprehensive Banking Domain Analyzer
    
    Follows strict banking domain rules:
    - Only banking-correct relationships
    - No assumptions based on column similarity alone
    - No relationships using descriptive/status columns
    - Banking flow logic: Customer → Account → Transaction
    """
    
    def __init__(self):
        # Banking entity hierarchy
        self.banking_hierarchy = {
            "Customer": ["Account", "Loan", "Card"],
            "Branch": ["Account", "Employee"],
            "Account": ["Transaction"],
            "Product": ["Account", "Loan"],
            "Loan": ["Collateral", "EMI"],
            "Transaction": []
        }
        
        # Invalid FK columns (descriptive/status only)
        self.invalid_fk_patterns = [
            "status", "city", "currency", "name", "flag", "type", 
            "category", "description", "address", "email", "phone"
        ]
        
        # Columns that should NEVER be treated as PK/unique (even if unique)
        self.invalid_pk_patterns = [
            "dob", "date_of_birth", "birthdate", "birth_date",  # Dates can repeat
            "address", "street", "location", "city", "state",  # Free text, can repeat
            "rate", "percentage", "interest_rate", "rate_percentage",  # Rates can repeat
            "status", "flag", "category", "type",  # Descriptive fields
            "name", "description", "note", "comment"  # Free text
        ]
        
        # Valid PK patterns (ID/number/code only)
        self.valid_pk_patterns = {
            "customer_id": ["customer_id", "cust_id", "client_id", "customer_number", "c_id"],
            "account_number": ["account_number", "account_no", "acc_no", "account_id", "acc_id"],
            "transaction_id": ["transaction_id", "txn_id", "trans_id", "transaction_number"],
            "loan_id": ["loan_id", "loan_number", "loan_ref"],
            "branch_id": ["branch_id", "branch_code", "branch_number"],
            "card_id": ["card_id", "card_number", "card_no"]
        }
    
    def normalize_column_name(self, col_name: str) -> str:
        """Normalize column name for matching"""
        return re.sub(r'[_\s-]+', '_', str(col_name).lower().strip())
    
    # ==================== STEP 1: COLUMN PROFILING (MANDATORY) ====================
    
    def profile_column(self, df: pd.DataFrame, col_name: str) -> Dict[str, Any]:
        """
        Profile every column in every file
        
        Returns:
            - data_type: ID / Code / Amount / Date / Text / Status
            - uniqueness_percentage
            - null_percentage
            - value_pattern: numeric length, prefix, format
            - sample_values: first 10 non-null values
        """
        series = df[col_name]
        norm_col = self.normalize_column_name(col_name)
        
        # Calculate statistics
        total_rows = len(series)
        non_null = series.dropna()
        null_count = series.isna().sum()
        null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
        
        unique_count = non_null.nunique()
        uniqueness_percentage = (unique_count / len(non_null) * 100) if len(non_null) > 0 else 0
        
        # Detect data type
        data_type = self._detect_data_type(series, norm_col)
        
        # Detect value pattern
        value_pattern = self._detect_value_pattern(series, norm_col, data_type)
        
        # Sample values
        sample_values = non_null.head(10).tolist() if len(non_null) > 0 else []
        
        return {
            "column_name": col_name,
            "normalized_name": norm_col,
            "data_type": data_type,
            "uniqueness_percentage": round(uniqueness_percentage, 2),
            "null_percentage": round(null_percentage, 2),
            "total_rows": total_rows,
            "unique_count": unique_count,
            "null_count": null_count,
            "value_pattern": value_pattern,
            "sample_values": sample_values
        }
    
    def _detect_data_type(self, series: pd.Series, norm_col: str) -> str:
        """Detect data type: ID / Code / Amount / Date / Text / Status"""
        non_null = series.dropna()
        if len(non_null) == 0:
            return "Unknown"
        
        # 🔥 FIX: Check if column should be excluded from ID detection
        # These should NEVER be treated as ID even if unique
        if any(invalid in norm_col for invalid in self.invalid_pk_patterns):
            # Check specific cases
            if "account" in norm_col and "number" in norm_col:
                # account_number in transactions = FK, not Amount
                if "transaction" in norm_col or pd.api.types.is_numeric_dtype(series):
                    # Check if it's actually a reference (not used in calculations)
                    return "ID"  # It's a reference/foreign key
            # address, city, rate_percentage, dob should be Text/Amount/Date, not ID
            if any(pattern in norm_col for pattern in ["address", "city", "street", "location"]):
                return "Text"
            if "rate" in norm_col or "percentage" in norm_col or "interest" in norm_col:
                return "Amount"  # Rate is a numeric value, not an identifier
            if "dob" in norm_col or "birth" in norm_col or "date" in norm_col:
                return "Date"
        
        # ID pattern: high uniqueness, alphanumeric, specific naming
        # BUT must match banking PK patterns (customer_id, account_number, etc.)
        if ("id" in norm_col or "number" in norm_col) and not any(invalid in norm_col for invalid in self.invalid_pk_patterns):
            unique_ratio = non_null.nunique() / len(non_null)
            if unique_ratio >= 0.8:
                # Additional check: must look like an identifier (not free text)
                if pd.api.types.is_numeric_dtype(series) or non_null.astype(str).str.match(r'^[A-Za-z0-9]+$').mean() > 0.8:
                    return "ID"
        
        # Code pattern: categorical, moderate uniqueness
        if "code" in norm_col:
            unique_ratio = non_null.nunique() / len(non_null)
            if 0.1 <= unique_ratio <= 0.5:
                return "Code"
        
        # Amount pattern: numeric, positive values
        if "amount" in norm_col or "balance" in norm_col or "price" in norm_col:
            try:
                numeric = pd.to_numeric(non_null, errors='coerce')
                if numeric.notna().mean() > 0.8:
                    if (numeric.dropna() >= 0).all():
                        return "Amount"
            except:
                pass
        
        # Date pattern: parseable as date
        if "date" in norm_col or "time" in norm_col:
            try:
                pd.to_datetime(non_null.head(10), errors='raise')
                return "Date"
            except:
                pass
        
        # Status pattern: few unique values, categorical
        if "status" in norm_col or "flag" in norm_col:
            unique_count = non_null.nunique()
            if unique_count <= 10:
                return "Status"
        
        # Text pattern: default for string data
        if series.dtype == 'object':
            return "Text"
        
        # Numeric pattern
        if pd.api.types.is_numeric_dtype(series):
            return "Amount"
        
        return "Text"
    
    def _detect_value_pattern(self, series: pd.Series, norm_col: str, data_type: str) -> Dict[str, Any]:
        """Detect value pattern: numeric length, prefix, format"""
        non_null = series.dropna().astype(str).head(100)
        
        pattern_info = {
            "prefix": None,
            "length_range": None,
            "format": None
        }
        
        if len(non_null) == 0:
            return pattern_info
        
        # Detect prefix pattern (common prefixes in IDs)
        first_chars = non_null.str[:3].value_counts()
        if len(first_chars) > 0 and first_chars.iloc[0] / len(non_null) > 0.5:
            pattern_info["prefix"] = first_chars.index[0]
        
        # Detect length range
        lengths = non_null.str.len()
        if lengths.min() == lengths.max():
            pattern_info["length_range"] = f"Fixed: {lengths.min()}"
        else:
            pattern_info["length_range"] = f"{lengths.min()}-{lengths.max()}"
        
        # Detect format
        if data_type == "ID":
            # Check if alphanumeric
            if non_null.str.match(r'^[A-Za-z0-9]+$').mean() > 0.8:
                pattern_info["format"] = "Alphanumeric"
            # Check if numeric
            elif non_null.str.match(r'^\d+$').mean() > 0.8:
                pattern_info["format"] = "Numeric"
        
        return pattern_info
    
    # ==================== STEP 2: CANDIDATE KEY DETECTION ====================
    
    def detect_primary_keys(self, file_dataframes: Dict[str, pd.DataFrame]) -> Dict[str, Optional[str]]:
        """
        Mark a column as PRIMARY KEY only if:
        - Uniqueness ≥ 95%
        - Stable identifier (ID / number / code)
        - Represents a real banking entity
        """
        primary_keys = {}
        
        for file_name, df in file_dataframes.items():
            best_pk = None
            best_confidence = 0
            
            for col in df.columns:
                profile = self.profile_column(df, col)
                
                # Requirement 1: Uniqueness ≥ 95%
                if profile["uniqueness_percentage"] < 95:
                    continue
                
                # Requirement 2: Must be ID/Code data type (not Status/Text/Amount)
                if profile["data_type"] not in ["ID", "Code"]:
                    continue
                
                # Requirement 3: Column name must match banking PK patterns
                norm_col = profile["normalized_name"]
                matches_pk_pattern = False
                
                for pk_type, patterns in self.valid_pk_patterns.items():
                    if any(pattern in norm_col for pattern in patterns):
                        matches_pk_pattern = True
                        break
                
                if not matches_pk_pattern:
                    continue
                
                # Calculate confidence based on uniqueness and pattern match
                confidence = profile["uniqueness_percentage"] / 100.0
                
                if confidence > best_confidence:
                    best_confidence = confidence
                    best_pk = col
            
            primary_keys[file_name] = best_pk
        
        return primary_keys
    
    # ==================== STEP 3: FOREIGN KEY DETECTION (STRICT RULES) ====================
    
    def detect_foreign_keys(self, file_dataframes: Dict[str, pd.DataFrame],
                           primary_keys: Dict[str, Optional[str]]) -> List[Dict[str, Any]]:
        """
        Mark a column as FOREIGN KEY only if ALL are true:
        - Appears in another file's PRIMARY KEY
        - Match rate ≥ 70%
        - Child table logically depends on parent table
        - Business flow supports the dependency
        """
        foreign_keys = []
        file_names = list(file_dataframes.keys())
        
        for i, parent_file in enumerate(file_names):
            parent_pk = primary_keys.get(parent_file)
            if not parent_pk:
                continue
            
            parent_df = file_dataframes[parent_file]
            parent_values = set(parent_df[parent_pk].dropna().astype(str))
            
            if len(parent_values) == 0:
                continue
            
            for j, child_file in enumerate(file_names):
                if i == j:
                    continue
                
                child_df = file_dataframes[child_file]
                
                # Check all columns in child file for FK candidate
                for child_col in child_df.columns:
                    # Skip if column name suggests it's NOT an FK (descriptive/status)
                    norm_child_col = self.normalize_column_name(child_col)
                    if any(invalid in norm_child_col for invalid in self.invalid_fk_patterns):
                        continue
                    
                    # Check if column name matches parent PK
                    norm_parent_pk = self.normalize_column_name(parent_pk)
                    
                    # Exact match or similar pattern
                    if norm_child_col == norm_parent_pk or self._columns_match_for_fk(norm_parent_pk, norm_child_col):
                        child_values = set(child_df[child_col].dropna().astype(str))
                        
                        if len(child_values) == 0:
                            continue
                        
                        # Calculate match rate
                        overlap = parent_values & child_values
                        match_rate = len(overlap) / len(child_values) if len(child_values) > 0 else 0
                        
                        # Requirement: Match rate ≥ 70%
                        if match_rate < 0.7:
                            continue
                        
                        # Requirement: Check business flow logic
                        if not self._validate_business_flow(parent_file, child_file, parent_pk, child_col):
                            continue
                        
                        foreign_keys.append({
                            "parent_file": parent_file,
                            "parent_column": parent_pk,
                            "child_file": child_file,
                            "child_column": child_col,
                            "match_rate": round(match_rate, 2),
                            "overlap_count": len(overlap),
                            "parent_unique_count": len(parent_values),
                            "child_unique_count": len(child_values)
                        })
        
        return foreign_keys
    
    def _columns_match_for_fk(self, parent_col: str, child_col: str) -> bool:
        """Check if column names match for FK relationship"""
        # Remove common suffixes
        parent_base = re.sub(r'(_id|_number|_no|_code)$', '', parent_col)
        child_base = re.sub(r'(_id|_number|_no|_code)$', '', child_col)
        
        # Check if base names match
        if parent_base == child_base:
            return True
        
        # Check if one contains the other
        if parent_base in child_base or child_base in parent_base:
            return True
        
        # Check fuzzy match
        if fuzz.ratio(parent_base, child_base) > 85:
            return True
        
        return False
    
    def _validate_business_flow(self, parent_file: str, child_file: str,
                                parent_col: str, child_col: str) -> bool:
        """Validate that business flow supports the dependency"""
        # This is a simplified check - in production, you'd use entity detection
        norm_parent_col = self.normalize_column_name(parent_col)
        norm_child_col = self.normalize_column_name(child_col)
        
        # Common banking flows:
        # customer_id → account_number → transaction_id
        if "customer" in norm_parent_col and "account" in norm_child_col:
            return True
        if "account" in norm_parent_col and "transaction" in norm_child_col:
            return True
        if "customer" in norm_parent_col and "customer" in norm_child_col:
            return True  # Same entity type
        if "account" in norm_parent_col and "account" in norm_child_col:
            return True  # Same entity type
        
        # If column names match closely, likely valid
        if self._columns_match_for_fk(norm_parent_col, norm_child_col):
            return True
        
        return False
    
    # ==================== STEP 4: PARENT–CHILD DIRECTION LOGIC ====================
    
    def determine_relationship_direction(self, foreign_keys: List[Dict[str, Any]],
                                       file_dataframes: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
        """
        Determine direction using banking logic:
        - Customer → Account → Transaction
        - Branch → Account / Employee
        - Product → Account / Loan
        - Loan → Collateral
        """
        directed_relationships = []
        
        for fk in foreign_keys:
            parent_file = fk["parent_file"]
            child_file = fk["child_file"]
            parent_col = fk["parent_column"]
            child_col = fk["child_column"]
            
            # Use banking hierarchy to determine direction
            direction = self._get_banking_direction(parent_file, child_file, parent_col, child_col)
            
            if direction == "correct":
                # Current direction is correct
                relationship_type = self._determine_relationship_type(
                    file_dataframes[parent_file][parent_col],
                    file_dataframes[child_file][child_col]
                )
                
                directed_relationships.append({
                    **fk,
                    "relationship_type": relationship_type,
                    "direction_validated": True
                })
            elif direction == "reversed":
                # Need to reverse direction
                relationship_type = self._determine_relationship_type(
                    file_dataframes[child_file][child_col],
                    file_dataframes[parent_file][parent_col]
                )
                
                directed_relationships.append({
                    "parent_file": child_file,
                    "parent_column": child_col,
                    "child_file": parent_file,
                    "child_column": parent_col,
                    "match_rate": fk["match_rate"],
                    "overlap_count": fk["overlap_count"],
                    "relationship_type": relationship_type,
                    "direction_validated": True,
                    "direction_corrected": True
                })
        
        return directed_relationships
    
    def _get_banking_direction(self, file1: str, file2: str, col1: str, col2: str) -> str:
        """Determine correct banking direction"""
        norm_col1 = self.normalize_column_name(col1)
        norm_col2 = self.normalize_column_name(col2)
        
        # Customer → Account
        if "customer" in norm_col1 and "account" in norm_col2:
            return "correct"
        if "customer" in norm_col2 and "account" in norm_col1:
            return "reversed"
        
        # Account → Transaction
        if "account" in norm_col1 and "transaction" in norm_col2:
            return "correct"
        if "account" in norm_col2 and "transaction" in norm_col1:
            return "reversed"
        
        # If same entity type, check counts (fewer = parent)
        # This is handled by relationship type determination
        
        return "correct"  # Default: assume current direction is correct
    
    def _determine_relationship_type(self, parent_series: pd.Series, child_series: pd.Series) -> str:
        """Determine relationship type: One-to-Many / One-to-One / Many-to-One"""
        parent_unique = parent_series.nunique()
        child_unique = child_series.nunique()
        
        if parent_unique < child_unique * 0.8:  # Parent has fewer unique values
            return "One-to-Many"
        elif child_unique < parent_unique * 0.8:  # Child has fewer unique values
            return "Many-to-One"
        else:
            return "One-to-One"
    
    # ==================== STEP 5: INVALID RELATION FILTERING ====================
    
    def filter_invalid_relationships(self, foreign_keys: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        Explicitly REJECT relationships using:
        - status
        - city
        - currency
        - name fields
        - boolean or flag columns
        """
        valid_relationships = []
        rejected_relationships = []
        
        for fk in foreign_keys:
            parent_col = self.normalize_column_name(fk["parent_column"])
            child_col = self.normalize_column_name(fk["child_column"])
            
            # Check if either column is invalid for FK
            parent_invalid = any(invalid in parent_col for invalid in self.invalid_fk_patterns)
            child_invalid = any(invalid in child_col for invalid in self.invalid_fk_patterns)
            
            if parent_invalid or child_invalid:
                reason = []
                if parent_invalid:
                    reason.append(f"Parent column '{fk['parent_column']}' is descriptive/status (not a key)")
                if child_invalid:
                    reason.append(f"Child column '{fk['child_column']}' is descriptive/status (not a key)")
                
                rejected_relationships.append({
                    **fk,
                    "rejection_reason": "; ".join(reason),
                    "status": "INVALID"
                })
                continue
            
            valid_relationships.append(fk)
        
        return valid_relationships, rejected_relationships
    
    # ==================== STEP 6: RELATIONSHIP CONFIDENCE CLASSIFICATION ====================
    
    def classify_relationship_confidence(self, relationships: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Label each relationship:
        - STRONG (PK–FK + banking logic correct)
        - WEAK (logical but partial data)
        - INVALID (attribute similarity only)
        """
        classified = []
        
        for rel in relationships:
            match_rate = rel.get("match_rate", 0)
            
            # STRONG: High match rate (≥ 90%) + banking logic correct
            if match_rate >= 0.9 and rel.get("direction_validated", False):
                confidence = "STRONG"
            # WEAK: Lower match rate (70-90%) but logical
            elif match_rate >= 0.7 and rel.get("direction_validated", False):
                confidence = "WEAK"
            else:
                confidence = "INVALID"
            
            rel["confidence_level"] = confidence
            classified.append(rel)
        
        return classified
    
    # ==================== STEP 7: FINAL RELATION OUTPUT ====================
    
    def generate_final_relationships(self, relationships: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        For each VALID relationship output:
        - Parent File → Child File
        - PK → FK
        - Relationship type (1–M / 1–1)
        - Banking justification (real-world example)
        """
        final_output = []
        
        for rel in relationships:
            if rel.get("confidence_level") == "INVALID":
                continue
            
            # Generate banking justification
            justification = self._generate_banking_justification(rel)
            
            final_output.append({
                "parent_file": rel["parent_file"],
                "parent_column": rel["parent_column"],
                "child_file": rel["child_file"],
                "child_column": rel["child_column"],
                "relationship_type": rel.get("relationship_type", "One-to-Many"),
                "match_rate": rel.get("match_rate", 0),
                "confidence_level": rel.get("confidence_level", "WEAK"),
                "banking_justification": justification,
                "overlap_count": rel.get("overlap_count", 0)
            })
        
        return final_output
    
    def _generate_banking_justification(self, rel: Dict[str, Any]) -> str:
        """Generate real-world banking justification"""
        parent_col = self.normalize_column_name(rel["parent_column"])
        child_col = self.normalize_column_name(rel["child_column"])
        
        if "customer" in parent_col and "account" in child_col:
            return "One customer can have multiple bank accounts. Each account belongs to exactly one customer."
        elif "account" in parent_col and "transaction" in child_col:
            return "One account can have many transactions. Each transaction belongs to exactly one account."
        elif "customer" in parent_col and "customer" in child_col:
            return "Customer entity relationship - same customer across different files."
        elif "account" in parent_col and "account" in child_col:
            return "Account entity relationship - same account referenced across different files."
        else:
            return f"Banking entity relationship: {rel['parent_file']} → {rel['child_file']} based on matching key values."
    
    # ==================== STEP 8: APPLICATION STRUCTURE CORRECTION ====================
    
    def generate_corrected_structure(self, file_dataframes: Dict[str, pd.DataFrame],
                                    primary_keys: Dict[str, Optional[str]],
                                    relationships: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate FINAL corrected modules:
        - Customer Module
        - Branch Module
        - Account Module
        - Transaction Module
        - Loan & Product Module
        """
        modules = {
            "Customer Module": {"columns": [], "pk": None, "files": []},
            "Branch Module": {"columns": [], "pk": None, "files": []},
            "Account Module": {"columns": [], "pk": None, "files": []},
            "Transaction Module": {"columns": [], "pk": None, "files": []},
            "Loan & Product Module": {"columns": [], "pk": None, "files": []}
        }
        
        # Group columns by module
        for file_name, df in file_dataframes.items():
            pk = primary_keys.get(file_name)
            
            # Determine module based on PK
            module_name = self._identify_module_from_pk(pk, file_name, df.columns.tolist())
            
            if module_name and module_name in modules:
                modules[module_name]["columns"].extend(df.columns.tolist())
                if pk:
                    modules[module_name]["pk"] = pk
                if file_name not in modules[module_name]["files"]:
                    modules[module_name]["files"].append(file_name)
        
        # Add FK relationships
        for rel in relationships:
            if rel.get("confidence_level") != "INVALID":
                parent_file = rel["parent_file"]
                child_file = rel["child_file"]
                
                # Find modules
                parent_module = self._find_module_for_file(parent_file, modules)
                child_module = self._find_module_for_file(child_file, modules)
                
                if parent_module and child_module:
                    if "foreign_keys" not in modules[child_module]:
                        modules[child_module]["foreign_keys"] = []
                    
                    modules[child_module]["foreign_keys"].append({
                        "references": parent_module,
                        "parent_column": rel["parent_column"],
                        "child_column": rel["child_column"]
                    })
        
        return {
            "modules": modules,
            "structure_tree": self._build_structure_tree(modules)
        }
    
    def _identify_module_from_pk(self, pk: Optional[str], file_name: str, columns: List[str]) -> Optional[str]:
        """Identify module based on primary key and columns"""
        if not pk:
            # Try to infer from columns
            all_cols = " ".join([self.normalize_column_name(c) for c in columns])
            if "customer" in all_cols and "account" not in all_cols:
                return "Customer Module"
            elif "transaction" in all_cols:
                return "Transaction Module"
            elif "loan" in all_cols:
                return "Loan & Product Module"
            elif "branch" in all_cols:
                return "Branch Module"
            elif "account" in all_cols:
                return "Account Module"
            return None
        
        norm_pk = self.normalize_column_name(pk)
        
        if "customer" in norm_pk:
            return "Customer Module"
        elif "account" in norm_pk:
            return "Account Module"
        elif "transaction" in norm_pk or "txn" in norm_pk:
            return "Transaction Module"
        elif "loan" in norm_pk:
            return "Loan & Product Module"
        elif "branch" in norm_pk:
            return "Branch Module"
        
        return None
    
    def _find_module_for_file(self, file_name: str, modules: Dict[str, Any]) -> Optional[str]:
        """Find which module contains this file"""
        for module_name, module_info in modules.items():
            if file_name in module_info.get("files", []):
                return module_name
        return None
    
    def _build_structure_tree(self, modules: Dict[str, Any]) -> str:
        """Build structure tree representation"""
        tree_lines = ["Banking Application"]
        
        module_order = ["Customer Module", "Branch Module", "Account Module", 
                       "Transaction Module", "Loan & Product Module"]
        
        for i, module_name in enumerate(module_order):
            if module_name not in modules or not modules[module_name]["files"]:
                continue
            
            is_last = i == len([m for m in module_order if m in modules and modules[m]["files"]]) - 1
            connector = "└──" if is_last else "├──"
            
            module_info = modules[module_name]
            pk_info = f" (PK: {module_info['pk']})" if module_info["pk"] else ""
            
            tree_lines.append(f"{connector} {module_name}{pk_info}")
            
            # Add FK info
            if "foreign_keys" in module_info:
                for fk in module_info["foreign_keys"]:
                    fk_connector = "    " if is_last else "│   "
                    tree_lines.append(
                        f"{fk_connector}    └── FK: {fk['child_column']} → {fk['references']}.{fk['parent_column']}"
                    )
        
        return "\n".join(tree_lines)
    
    # ==================== STEP 9: ERROR REPORTING ====================
    
    def generate_error_report(self, file_dataframes: Dict[str, pd.DataFrame],
                             primary_keys: Dict[str, Optional[str]],
                             relationships: List[Dict[str, Any]],
                             rejected_relationships: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Explicitly list:
        - Wrong relations detected earlier
        - Direction errors
        - Non-key columns wrongly linked
        - Data quality gaps (missing FK values)
        """
        errors = {
            "rejected_relationships": rejected_relationships,
            "missing_primary_keys": [],
            "data_quality_issues": [],
            "direction_corrections": []
        }
        
        # Find files without PK
        for file_name, pk in primary_keys.items():
            if not pk:
                errors["missing_primary_keys"].append({
                    "file": file_name,
                    "issue": "No primary key detected. File needs a unique identifier column.",
                    "suggestion": "Add a column with uniqueness ≥ 95% (e.g., customer_id, account_number)"
                })
        
        # Check for direction corrections
        for rel in relationships:
            if rel.get("direction_corrected", False):
                errors["direction_corrections"].append({
                    "original": f"{rel['child_file']}.{rel['child_column']} → {rel['parent_file']}.{rel['parent_column']}",
                    "corrected": f"{rel['parent_file']}.{rel['parent_column']} → {rel['child_file']}.{rel['child_column']}",
                    "reason": "Banking hierarchy requires parent → child direction"
                })
        
        # Check data quality (FK values not in PK)
        for rel in relationships:
            if rel.get("confidence_level") != "INVALID":
                parent_file = rel["parent_file"]
                child_file = rel["child_file"]
                parent_col = rel["parent_column"]
                child_col = rel["child_column"]
                
                parent_df = file_dataframes[parent_file]
                child_df = file_dataframes[child_file]
                
                parent_values = set(parent_df[parent_col].dropna().astype(str))
                child_values = set(child_df[child_col].dropna().astype(str))
                
                orphaned = child_values - parent_values
                if len(orphaned) > 0:
                    orphan_ratio = len(orphaned) / len(child_values) if len(child_values) > 0 else 0
                    if orphan_ratio > 0.1:  # More than 10% orphaned
                        errors["data_quality_issues"].append({
                            "file": child_file,
                            "column": child_col,
                            "issue": f"{len(orphaned)} values ({orphan_ratio*100:.1f}%) in {child_col} do not exist in parent {parent_file}.{parent_col}",
                            "severity": "HIGH" if orphan_ratio > 0.3 else "MEDIUM"
                        })
        
        return errors
    
    # ==================== MAIN ANALYZER METHOD ====================
    
    def analyze(self, file_paths: List[str]) -> Dict[str, Any]:
        """
        Main analysis method - runs ALL 9 steps
        
        Returns comprehensive banking domain analysis
        """
        # Load files
        file_dataframes = {}
        for file_path in file_paths:
            try:
                df = pd.read_csv(file_path)
                file_name = os.path.basename(file_path)
                file_dataframes[file_name] = df
            except Exception as e:
                print(f"Warning: Could not load {file_path}: {str(e)}")
                continue
        
        if not file_dataframes:
            return {"error": "No valid files could be loaded"}
        
        # STEP 1: Column Profiling
        column_profiles = {}
        for file_name, df in file_dataframes.items():
            column_profiles[file_name] = {
                col: self.profile_column(df, col) for col in df.columns
            }
        
        # STEP 2: Primary Key Detection
        primary_keys = self.detect_primary_keys(file_dataframes)
        
        # STEP 3: Foreign Key Detection
        foreign_keys_candidates = self.detect_foreign_keys(file_dataframes, primary_keys)
        
        # STEP 4: Determine Relationship Direction
        directed_relationships = self.determine_relationship_direction(foreign_keys_candidates, file_dataframes)
        
        # STEP 5: Filter Invalid Relationships
        valid_relationships, rejected_relationships = self.filter_invalid_relationships(directed_relationships)
        
        # STEP 6: Classify Relationship Confidence
        classified_relationships = self.classify_relationship_confidence(valid_relationships)
        
        # STEP 7: Generate Final Relationships
        final_relationships = self.generate_final_relationships(classified_relationships)
        
        # STEP 8: Application Structure Correction
        corrected_structure = self.generate_corrected_structure(
            file_dataframes, primary_keys, classified_relationships
        )
        
        # STEP 9: Error Reporting
        error_report = self.generate_error_report(
            file_dataframes, primary_keys, classified_relationships, rejected_relationships
        )
        
        # Compile final output
        return {
            "step1_column_profiles": column_profiles,
            "step2_primary_keys": primary_keys,
            "step3_foreign_key_candidates": foreign_keys_candidates,
            "step4_directed_relationships": directed_relationships,
            "step5_valid_relationships": valid_relationships,
            "step5_rejected_relationships": rejected_relationships,
            "step6_classified_relationships": classified_relationships,
            "step7_final_relationships": final_relationships,
            "step8_corrected_structure": corrected_structure,
            "step9_error_report": error_report,
            "summary": {
                "total_files": len(file_dataframes),
                "total_columns": sum(len(df.columns) for df in file_dataframes.values()),
                "primary_keys_detected": sum(1 for pk in primary_keys.values() if pk),
                "foreign_keys_detected": len(valid_relationships),
                "valid_relationships": len(final_relationships),
                "rejected_relationships": len(rejected_relationships)
            }
        }
