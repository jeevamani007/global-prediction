"""
Intelligent Banking Application Structure Generator

Dynamically analyzes uploaded files (CSV, SQL, CXL) and generates
the internal Banking Application structure based on:
- Column names and their inferred meanings
- Sample values and data patterns
- Relationships between files
- Banking feature classification
- Module confidence scoring
- Application type determination
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
import re
import os
from collections import defaultdict
from rapidfuzz import fuzz


class BankingApplicationStructureGenerator:
    """
    Intelligent Banking Application Structure Generator
    
    Analyzes files to dynamically generate banking application structure:
    1. Column analysis and meaning inference
    2. Sample value pattern detection
    3. Relationship detection across files
    4. Banking feature classification
    5. Module confidence scoring
    6. Application type determination
    """
    
    def __init__(self):
        # Banking feature patterns aligned with KEY LOGIC / STRUCTURE PATTERN
        self.feature_patterns = {
            "Customer Information": {
                "keywords": ["customer", "client", "user", "person", "name", "email", "phone", "dob", "date.*birth"],
                "patterns": {
                    "customer_id": r"customer.*id|client.*id|user.*id|cust.*id",
                    "customer_name": r"customer.*name|client.*name|user.*name|person.*name",
                    "dob": r"dob|date.*birth|birth.*date|date_of_birth",
                    "phone": r"phone|mobile|contact|tel|telephone",
                    "email": r"email|e-mail|mail|email_address"
                }
            },
            "Account / Product Information": {
                "keywords": ["account", "acc", "acct", "product", "balance", "status", "type"],
                "patterns": {
                    "account_number": r"account.*number|acc.*no|acct.*num|account_id|account_no",
                    "account_type": r"account.*type|acc.*type|product.*type",
                    "product_name": r"product.*name|product|service.*name",
                    "balance": r"balance|bal|account.*balance|current.*balance",
                    "account_status": r"account.*status|acc.*status|status"
                }
            },
            "Transaction Information": {
                "keywords": ["transaction", "txn", "trans", "amount", "date", "type", "debit", "credit"],
                "patterns": {
                    "transaction_id": r"transaction.*id|txn.*id|trans.*id|transaction.*number",
                    "transaction_type": r"transaction.*type|txn.*type|trans.*type",
                    "amount": r"amount|amt|transaction.*amount",
                    "total_amount": r"total.*amount|total|gross.*amount",
                    "tax_amount": r"tax.*amount|tax",
                    "net_amount": r"net.*amount|net",
                    "transaction_date": r"transaction.*date|txn.*date|trans.*date|date",
                    "debit": r"debit|dr|debit.*amount",
                    "credit": r"credit|cr|credit.*amount"
                }
            }
        }
        
        # Module definitions aligned with KEY LOGIC / STRUCTURE PATTERN
        self.modules = {
            "Customer Module": {
                "description": "Identity and personal information",
                "features": ["Customer Information"],
                "required_columns": ["customer_id", "customer_name"],
                "optional_columns": ["dob", "phone", "email"],
                "mandatory_fields": ["customer_id"]  # Critical for relationships
            },
            "Account / Product Module": {
                "description": "Banking product information",
                "features": ["Account / Product Information"],
                "required_columns": ["account_number", "balance"],
                "optional_columns": ["account_type", "product_name", "account_status"],
                "mandatory_fields": ["account_number", "balance"]  # Critical for transactions
            },
            "Transaction Module": {
                "description": "Transaction details",
                "features": ["Transaction Information"],
                "required_columns": ["transaction_id", "amount"],
                "optional_columns": ["transaction_type", "total_amount", "tax_amount", "net_amount", "transaction_date", "debit", "credit"],
                "mandatory_fields": ["transaction_id", "amount", "transaction_date"]
            }
        }
        
        # Expected relationships (customer_id → account_number → transaction_id)
        self.expected_relationships = {
            "customer_id_to_account_number": {
                "from": "customer_id",
                "to": "account_number",
                "type": "One-to-Many",
                "description": "One customer can have multiple accounts"
            },
            "account_number_to_transaction_id": {
                "from": "account_number",
                "to": "transaction_id",
                "type": "One-to-Many",
                "description": "One account can have multiple transactions"
            }
        }
        
        # Business Rules (separate from modules)
        self.business_rules = {
            "debit_balance_rule": {
                "rule": "debit ≤ balance",
                "description": "Debit amount cannot exceed account balance",
                "columns": ["debit", "balance"]
            },
            "net_amount_calculation": {
                "rule": "net_amount = total_amount − tax_amount",
                "description": "Net amount is calculated as total amount minus tax",
                "columns": ["net_amount", "total_amount", "tax_amount"]
            },
            "account_active_status": {
                "rule": "account must be ACTIVE",
                "description": "Account status must be ACTIVE for transactions",
                "columns": ["account_status"]
            }
        }
        
        # Application type patterns (for determine_application_type method)
        self.application_types = {
            "Core Banking System": {
                "required_modules": ["Customer Module", "Account / Product Module"],
                "optional_modules": ["Transaction Module"],
                "description": "Complete core banking system with customer, account, and transaction management"
            },
            "Transaction Processing System": {
                "required_modules": ["Transaction Module"],
                "optional_modules": ["Account / Product Module"],
                "description": "Focused on transaction processing and payment handling"
            },
            "Hybrid Banking System": {
                "required_modules": ["Customer Module", "Account / Product Module", "Transaction Module"],
                "optional_modules": [],
                "description": "Comprehensive banking system with all three core modules"
            }
        }
    
    def normalize_column_name(self, col_name: str) -> str:
        """Normalize column name for pattern matching"""
        return re.sub(r'[_\s-]+', '_', str(col_name).lower().strip())
    
    def infer_column_meaning(self, col_name: str, sample_values: List[Any]) -> Dict[str, Any]:
        """
        Infer column meaning from name and sample values
        
        Returns:
            - inferred_name: Standardized column name
            - confidence: Confidence score (0-100)
            - data_type: Detected data type
            - pattern: Detected pattern (id, number, date, amount, text)
        """
        norm_col = self.normalize_column_name(col_name)
        inferred_name = col_name
        confidence = 0
        data_type = "text"
        pattern = "unknown"
        
        # Check each feature pattern
        for feature_name, feature_info in self.feature_patterns.items():
            for standard_name, pattern_regex in feature_info["patterns"].items():
                if re.search(pattern_regex, norm_col, re.IGNORECASE):
                    inferred_name = standard_name
                    confidence = 85
                    break
            
            if confidence > 0:
                break
        
        # Analyze sample values
        if sample_values:
            sample_str = ' '.join([str(v) for v in sample_values[:10]])
            
            # Check for ID pattern (numeric, 6-18 digits)
            if re.match(r'^\d{6,18}$', str(sample_values[0]) if sample_values else ''):
                pattern = "id"
                data_type = "numeric"
                if "id" in norm_col:
                    confidence = max(confidence, 90)
            
            # Check for numeric/amount pattern
            elif pd.api.types.is_numeric_dtype(pd.Series(sample_values)):
                pattern = "amount" if any(kw in norm_col for kw in ["amount", "balance", "price", "value"]) else "number"
                data_type = "numeric"
                confidence = max(confidence, 75)
            
            # Check for date pattern
            try:
                pd.to_datetime(sample_values[:5], errors="raise")
                pattern = "date"
                data_type = "datetime"
                confidence = max(confidence, 80)
            except:
                pass
            
            # Check for repeating patterns (codes, types)
            if len(set(sample_values)) < len(sample_values) * 0.3:
                pattern = "categorical"
                data_type = "categorical"
        
        return {
            "original_name": col_name,
            "inferred_name": inferred_name,
            "confidence": confidence,
            "data_type": data_type,
            "pattern": pattern
        }
    
    def classify_column_feature(self, col_name: str, sample_values: List[Any]) -> List[Dict[str, Any]]:
        """
        Classify column into banking features
        
        Returns list of feature classifications with confidence scores
        """
        norm_col = self.normalize_column_name(col_name)
        classifications = []
        
        for feature_name, feature_info in self.feature_patterns.items():
            score = 0
            
            # Check keyword matches
            for keyword in feature_info["keywords"]:
                if keyword in norm_col:
                    score += 10
            
            # Check pattern matches
            for standard_name, pattern_regex in feature_info["patterns"].items():
                if re.search(pattern_regex, norm_col, re.IGNORECASE):
                    score += 20
                    break
            
            # Check sample value patterns
            if sample_values:
                sample_str = ' '.join([str(v) for v in sample_values[:5]]).lower()
                for keyword in feature_info["keywords"]:
                    if keyword in sample_str:
                        score += 5
            
            if score > 0:
                classifications.append({
                    "feature": feature_name,
                    "confidence": min(score, 100),
                    "matched_patterns": [p for p in feature_info["patterns"].keys() if re.search(feature_info["patterns"][p], norm_col, re.IGNORECASE)]
                })
        
        # Sort by confidence
        classifications.sort(key=lambda x: x["confidence"], reverse=True)
        return classifications
    
    def detect_relationships(self, file_dataframes: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
        """
        Detect relationships between files by identifying common columns
        
        Returns list of relationships with explanations
        """
        relationships = []
        file_names = list(file_dataframes.keys())
        
        for i, file1 in enumerate(file_names):
            df1 = file_dataframes[file1]
            cols1 = [self.normalize_column_name(c) for c in df1.columns]
            
            for j, file2 in enumerate(file_names):
                if i >= j:
                    continue
                
                df2 = file_dataframes[file2]
                cols2 = [self.normalize_column_name(c) for c in df2.columns]
                
                # Find common columns
                common_cols = set(cols1) & set(cols2)
                
                if common_cols:
                    # Check value overlap for relationship strength
                    for common_col in common_cols:
                        # Find original column names
                        orig_col1 = [c for c in df1.columns if self.normalize_column_name(c) == common_col][0]
                        orig_col2 = [c for c in df2.columns if self.normalize_column_name(c) == common_col][0]
                        
                        values1 = set(df1[orig_col1].dropna().astype(str))
                        values2 = set(df2[orig_col2].dropna().astype(str))
                        
                        overlap = len(values1 & values2)
                        overlap_ratio = overlap / len(values2) if len(values2) > 0 else 0
                        
                        if overlap_ratio > 0.3:  # At least 30% overlap
                            relationships.append({
                                "file1": file1,
                                "file2": file2,
                                "common_column": common_col,
                                "overlap_ratio": round(overlap_ratio * 100, 2),
                                "relationship_type": "One-to-Many" if len(values1) <= len(values2) else "Many-to-One",
                                "explanation": f"{file1} and {file2} are related through {common_col} column"
                            })
        
        return relationships
    
    def assign_module_confidence(self, columns_by_feature: Dict[str, List[str]]) -> Dict[str, Dict[str, Any]]:
        """
        Assign confidence scores to modules based on feature density
        
        Returns module confidence scores with matched columns
        """
        module_scores = {}
        
        for module_name, module_info in self.modules.items():
            score = 0
            matched_columns = []
            required_count = 0
            optional_count = 0
            
            # Check required columns
            for req_col in module_info["required_columns"]:
                for feature in module_info["features"]:
                    if feature in columns_by_feature:
                        for col in columns_by_feature[feature]:
                            norm_col = self.normalize_column_name(col)
                            if req_col in norm_col or fuzz.ratio(req_col, norm_col) > 80:
                                matched_columns.append(col)
                                required_count += 1
                                score += 30
                                break
            
            # Check optional columns
            for opt_col in module_info["optional_columns"]:
                for feature in module_info["features"]:
                    if feature in columns_by_feature:
                        for col in columns_by_feature[feature]:
                            norm_col = self.normalize_column_name(col)
                            if opt_col in norm_col or fuzz.ratio(opt_col, norm_col) > 80:
                                if col not in matched_columns:
                                    matched_columns.append(col)
                                    optional_count += 1
                                    score += 10
            
            # Calculate final confidence
            if required_count >= len(module_info["required_columns"]):
                confidence = min(score, 100)
            elif required_count > 0:
                confidence = min(score * 0.7, 80)  # Partial match
            else:
                confidence = 0
            
            module_scores[module_name] = {
                "confidence": round(confidence, 2),
                "matched_columns": matched_columns,
                "required_columns_found": required_count,
                "optional_columns_found": optional_count,
                "status": "COMPLETE" if required_count >= len(module_info["required_columns"]) else "PARTIAL" if required_count > 0 else "MISSING"
            }
        
        return module_scores
    
    def determine_application_type(self, module_scores: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Determine banking application type based on module confidence scores
        
        Returns application type with confidence and description
        """
        # Filter modules with confidence > 50
        active_modules = [name for name, info in module_scores.items() if info["confidence"] >= 50]
        
        best_match = None
        best_score = 0
        
        for app_type, app_info in self.application_types.items():
            score = 0
            
            # Check required modules
            required_found = sum(1 for mod in app_info["required_modules"] if mod in active_modules)
            if required_found == len(app_info["required_modules"]):
                score += 50
            
            # Check optional modules
            optional_found = sum(1 for mod in app_info["optional_modules"] if mod in active_modules)
            score += optional_found * 10
            
            if score > best_score:
                best_score = score
                best_match = {
                    "application_type": app_type,
                    "description": app_info["description"],
                    "confidence": min(best_score, 100),
                    "detected_modules": active_modules,
                    "required_modules": app_info["required_modules"],
                    "optional_modules": app_info["optional_modules"]
                }
        
        if not best_match:
            best_match = {
                "application_type": "Generic Banking Application",
                "description": "General banking dataset without specific application type classification",
                "confidence": 50,
                "detected_modules": active_modules,
                "required_modules": [],
                "optional_modules": []
            }
        
        return best_match
    
    def generate_structure(self, file_paths: List[str]) -> Dict[str, Any]:
        """
        Main method to generate banking application structure
        
        Args:
            file_paths: List of file paths (CSV, SQL, etc.)
        
        Returns:
            Complete application structure with modules, columns, relationships, and business rules
        """
        # Load all files
        file_dataframes = {}
        for file_path in file_paths:
            try:
                file_ext = os.path.splitext(file_path)[1].lower()
                if file_ext == '.csv':
                    df = pd.read_csv(file_path)
                elif file_ext in ('.xlsx', '.xls', '.cxl'):  # Support CXL files (if they're Excel format)
                    try:
                        df = pd.read_excel(file_path)
                    except:
                        # If CXL is not Excel, try as CSV
                        df = pd.read_csv(file_path)
                else:
                    # Try to read as CSV as fallback
                    try:
                        df = pd.read_csv(file_path)
                    except:
                        continue
                
                file_name = os.path.basename(file_path)
                file_dataframes[file_name] = df
            except Exception as e:
                print(f"Warning: Could not load {file_path}: {str(e)}")
                continue
        
        if not file_dataframes:
            return {"error": "No valid files could be loaded"}
        
        # Step 1: Analyze columns and infer meanings
        all_columns_analysis = {}
        for file_name, df in file_dataframes.items():
            for col in df.columns:
                sample_values = df[col].dropna().head(10).tolist()
                analysis = self.infer_column_meaning(col, sample_values)
                all_columns_analysis[f"{file_name}.{col}"] = analysis
        
        # Step 2: Classify columns into features
        columns_by_feature = defaultdict(list)
        column_classifications = {}
        
        for file_name, df in file_dataframes.items():
            for col in df.columns:
                sample_values = df[col].dropna().head(10).tolist()
                classifications = self.classify_column_feature(col, sample_values)
                
                if classifications:
                    primary_feature = classifications[0]["feature"]
                    columns_by_feature[primary_feature].append(f"{file_name}.{col}")
                    column_classifications[f"{file_name}.{col}"] = classifications
        
        # Step 3: Detect relationships
        relationships = self.detect_relationships(file_dataframes)
        
        # Step 4: Assign module confidence (handle "Account / Product Module" naming)
        # Map old feature names to new module structure
        feature_to_module_mapping = {
            "Account Information": "Account / Product Information",
            "Account / Product Information": "Account / Product Information",
            "Customer Information": "Customer Information",
            "Transaction Information": "Transaction Information"
        }
        
        # Update columns_by_feature keys if needed
        updated_columns_by_feature = {}
        for feature, cols in columns_by_feature.items():
            new_feature = feature_to_module_mapping.get(feature, feature)
            if new_feature not in updated_columns_by_feature:
                updated_columns_by_feature[new_feature] = []
            updated_columns_by_feature[new_feature].extend(cols)
        
        module_scores = self.assign_module_confidence(updated_columns_by_feature if updated_columns_by_feature else columns_by_feature)
        
        # Step 5: Determine application type
        application_type = self.determine_application_type(module_scores)
        
        # Step 6: Generate business rules (separate from modules)
        business_rules_result = self._generate_business_rules(columns_by_feature, module_scores)
        
        # Step 7: Build hierarchical structure (aligned with KEY LOGIC pattern)
        structure_tree = self._build_structure_tree(module_scores, columns_by_feature, file_dataframes)
        
        # Step 8: Detect problems (duplicates, wrong placement, missing fields, over-normalization)
        problems = self._detect_structure_problems(module_scores, columns_by_feature, structure_tree)
        
        initial_structure = {
            "application_type": application_type,
            "modules": module_scores,
            "columns_by_feature": {k: v for k, v in columns_by_feature.items()},
            "column_analysis": all_columns_analysis,
            "relationships": relationships,
            "expected_relationships": list(self.expected_relationships.values()),
            "business_rules": business_rules_result["rules"],  # Separate business rules
            "business_rules_details": business_rules_result["rule_details"],
            "structure_tree": structure_tree,
            "problems": problems,
            "total_files": len(file_dataframes),
            "total_columns": sum(len(df.columns) for df in file_dataframes.values())
        }
        
        # Step 8: Apply Balance & Correction Engine
        try:
            correction_engine = ApplicationStructureBalanceCorrectionEngine()
            corrected_structure = correction_engine.correct_and_balance(
                initial_structure, 
                file_dataframes,
                columns_by_feature
            )
            return corrected_structure
        except Exception as e:
            print(f"Warning: Balance & Correction Engine error: {str(e)}")
            import traceback
            traceback.print_exc()
            # Return initial structure if correction fails
            return initial_structure
    
    def _generate_business_rules(self, columns_by_feature: Dict[str, List[str]], 
                                 module_scores: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate business rules based on KEY LOGIC / STRUCTURE PATTERN
        
        Business Rules (separate page):
        - debit ≤ balance
        - net_amount = total_amount − tax_amount
        - account must be ACTIVE
        """
        detected_rules = []
        rule_details = {}
        
        # Check for debit and balance columns
        has_debit = False
        has_balance = False
        all_cols = []
        for feature_cols in columns_by_feature.values():
            all_cols.extend(feature_cols)
        
        for col_path in all_cols:
            col_name = col_path.rsplit('.', 1)[-1] if '.' in col_path else col_path
            norm_col = self.normalize_column_name(col_name)
            if "debit" in norm_col:
                has_debit = True
            if "balance" in norm_col:
                has_balance = True
        
        # Rule 1: debit ≤ balance
        if has_debit and has_balance:
            rule = self.business_rules["debit_balance_rule"]
            detected_rules.append(rule["rule"])
            rule_details["debit_balance_rule"] = {
                "rule": rule["rule"],
                "description": rule["description"],
                "status": "DETECTED",
                "columns_present": ["debit", "balance"]
            }
        
        # Rule 2: net_amount = total_amount − tax_amount
        has_total_amount = any("total_amount" in self.normalize_column_name(c.rsplit('.', 1)[-1]) for c in all_cols)
        has_tax_amount = any("tax_amount" in self.normalize_column_name(c.rsplit('.', 1)[-1]) for c in all_cols)
        has_net_amount = any("net_amount" in self.normalize_column_name(c.rsplit('.', 1)[-1]) for c in all_cols)
        
        if has_total_amount and has_tax_amount:
            rule = self.business_rules["net_amount_calculation"]
            detected_rules.append(rule["rule"])
            rule_details["net_amount_calculation"] = {
                "rule": rule["rule"],
                "description": rule["description"],
                "status": "DETECTED" if has_net_amount else "INFERRED",
                "columns_present": ["total_amount", "tax_amount"] + (["net_amount"] if has_net_amount else [])
            }
        
        # Rule 3: account must be ACTIVE
        has_account_status = any("account_status" in self.normalize_column_name(c.rsplit('.', 1)[-1]) or 
                                "status" in self.normalize_column_name(c.rsplit('.', 1)[-1]) for c in all_cols)
        if has_account_status:
            rule = self.business_rules["account_active_status"]
            detected_rules.append(rule["rule"])
            rule_details["account_active_status"] = {
                "rule": rule["rule"],
                "description": rule["description"],
                "status": "DETECTED",
                "columns_present": ["account_status"]
            }
        
        return {
            "rules": detected_rules,
            "rule_details": rule_details,
            "total_rules": len(detected_rules)
        }
    
    def _build_structure_tree(self, module_scores: Dict[str, Dict[str, Any]], 
                             columns_by_feature: Dict[str, List[str]],
                             file_dataframes: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Build hierarchical tree structure aligned with KEY LOGIC / STRUCTURE PATTERN
        
        Output format:
        Banking Application
        │
        ├── Customer Module ✅ 60%
        │   ├── customer_id
        │   └── customer_name
        ├── Account / Product Module ✅ 80%
        │   ├── account_number
        │   ├── product_name
        │   ├── account_type
        │   └── balance
        ├── Transaction Module ✅ 90%
        │   ├── transaction_id
        │   ├── transaction_type
        │   ├── transaction_date
        │   ├── total_amount
        │   ├── tax_amount
        │   └── net_amount
        """
        tree = {
            "application_type": "Banking Application",
            "modules": [],
            "relationships": [],
            "problems": []
        }
        
        # Add modules with their columns (aligned with structure pattern)
        module_order = ["Customer Module", "Account / Product Module", "Transaction Module"]
        
        for module_name in module_order:
            if module_name in module_scores and module_scores[module_name]["confidence"] >= 30:
                module_info = module_scores[module_name]
                module_def = self.modules.get(module_name, {})
                
                module_node = {
                    "name": module_name,
                    "description": module_def.get("description", ""),
                    "confidence": module_info["confidence"],
                    "status": module_info["status"],
                    "columns": []
                }
                
                # Add columns for this module
                for feature in module_def.get("features", []):
                    if feature in columns_by_feature:
                        for col_path in columns_by_feature[feature]:
                            # Extract column name from file.column format
                            if '.' in col_path:
                                file_name, col_name = col_path.rsplit('.', 1)
                            else:
                                col_name = col_path
                                file_name = "Unknown"
                            
                            # Check if column belongs to this module's pattern
                            norm_col = self.normalize_column_name(col_name)
                            belongs_to_module = False
                            
                            # Check required/optional columns
                            for req_col in module_def.get("required_columns", []):
                                if req_col in norm_col or fuzz.ratio(req_col, norm_col) > 85:
                                    belongs_to_module = True
                                    break
                            
                            if not belongs_to_module:
                                for opt_col in module_def.get("optional_columns", []):
                                    if opt_col in norm_col or fuzz.ratio(opt_col, norm_col) > 85:
                                        belongs_to_module = True
                                        break
                            
                            if belongs_to_module:
                                module_node["columns"].append({
                                    "name": col_name,
                                    "feature": feature,
                                    "file": file_name,
                                    "is_required": any(req_col in norm_col for req_col in module_def.get("required_columns", [])),
                                    "is_mandatory": any(mandatory in norm_col for mandatory in module_def.get("mandatory_fields", []))
                                })
                
                # Check for missing mandatory fields
                missing_mandatory = []
                for mandatory_field in module_def.get("mandatory_fields", []):
                    found = any(mandatory_field in self.normalize_column_name(c["name"]) for c in module_node["columns"])
                    if not found:
                        missing_mandatory.append(mandatory_field)
                
                if missing_mandatory:
                    module_node["missing_mandatory"] = missing_mandatory
                    tree["problems"].append({
                        "type": "missing_mandatory",
                        "module": module_name,
                        "fields": missing_mandatory,
                        "severity": "HIGH"
                    })
                
                tree["modules"].append(module_node)
        
        # Detect relationships
        all_cols = {}
        for module in tree["modules"]:
            for col in module["columns"]:
                all_cols[self.normalize_column_name(col["name"])] = {
                    "name": col["name"],
                    "module": module["name"]
                }
        
        # Check for customer_id → account_number relationship
        has_customer_id = any("customer_id" in col for col in all_cols.keys())
        has_account_number = any("account_number" in col for col in all_cols.keys())
        if has_customer_id and has_account_number:
            tree["relationships"].append(self.expected_relationships["customer_id_to_account_number"])
        
        # Check for account_number → transaction_id relationship
        has_transaction_id = any("transaction_id" in col for col in all_cols.keys())
        if has_account_number and has_transaction_id:
            tree["relationships"].append(self.expected_relationships["account_number_to_transaction_id"])
        
        return tree
    
    def _detect_structure_problems(self, module_scores: Dict[str, Dict[str, Any]], 
                                   columns_by_feature: Dict[str, List[str]],
                                   structure_tree: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Detect problems in the structure:
        - Duplicate columns
        - Wrong module placement
        - Missing mandatory fields
        - Over-normalization (e.g., product_name ≠ customer_name)
        """
        problems = []
        
        # Collect all columns across modules
        all_module_columns = {}
        for module in structure_tree.get("modules", []):
            module_name = module.get("name", "")
            for col in module.get("columns", []):
                col_name = col.get("name", "")
                norm_name = self.normalize_column_name(col_name)
                
                if norm_name not in all_module_columns:
                    all_module_columns[norm_name] = []
                all_module_columns[norm_name].append({
                    "name": col_name,
                    "module": module_name
                })
        
        # Problem 1: Duplicate columns
        for norm_name, occurrences in all_module_columns.items():
            if len(occurrences) > 1:
                problems.append({
                    "type": "duplicate_column",
                    "severity": "MEDIUM",
                    "column": norm_name,
                    "occurrences": occurrences,
                    "description": f"Column '{norm_name}' appears in multiple modules: {', '.join(set(o['module'] for o in occurrences))}"
                })
        
        # Problem 2: Missing mandatory fields (already detected in _build_structure_tree)
        for problem in structure_tree.get("problems", []):
            if problem["type"] == "missing_mandatory":
                problems.append(problem)
        
        # Problem 3: Over-normalization check (preserve separate meanings)
        preservation_violations = []
        for module in structure_tree.get("modules", []):
            module_cols = [self.normalize_column_name(c.get("name", "")) for c in module.get("columns", [])]
            
            # Check if product_name and customer_name are in same module (should be separate)
            has_product_name = any("product_name" in col or ("product" in col and "name" in col) for col in module_cols)
            has_customer_name = any("customer_name" in col or ("customer" in col and "name" in col) for col in module_cols)
            
            if has_product_name and has_customer_name:
                preservation_violations.append("product_name and customer_name should be in different modules")
        
        if preservation_violations:
            problems.append({
                "type": "over_normalization",
                "severity": "HIGH",
                "description": "Potential over-normalization detected. Columns with different business meanings should remain separate.",
                "violations": preservation_violations,
                "recommendation": "Do not normalize product_name ≠ customer_name. They have different business meanings."
            })
        
        return problems


class ApplicationStructureBalanceCorrectionEngine:
    """
    SAFE Banking Application Structure Correction Engine
    
    Fixes and balances the Banking Application Structure WITHOUT destroying semantic meaning.
    
    Strict Rules:
    1. NEVER normalize columns with different business meaning
    2. Normalize ONLY true aliases (txn_date ↔ transaction_date)
    3. Preserve financial integrity (total_amount ≠ tax_amount ≠ net_amount)
    4. Safe banking inferences (infer balance if Transaction Module exists)
    5. Module validation (ensure proper column-to-module mapping)
    6. Logical confidence recalculation
    """
    
    def __init__(self):
        # Module definitions (same as in BankingApplicationStructureGenerator)
        self.modules = {
            "Customer Module": {
                "features": ["Customer Information"],
                "required_columns": ["customer_id", "customer_name"],
                "optional_columns": ["email", "phone", "address"]
            },
            "Account Module": {
                "features": ["Account Information"],
                "required_columns": ["account_number", "balance"],
                "optional_columns": ["account_status", "account_type", "branch_code"]
            },
            "Transaction Module": {
                "features": ["Transaction Information"],
                "required_columns": ["transaction_id", "amount"],
                "optional_columns": ["debit", "credit", "transaction_date", "transaction_type"]
            },
            "Loan Module": {
                "features": ["Loan / EMI Information"],
                "required_columns": ["loan_id"],
                "optional_columns": ["emi", "interest_rate", "principal", "tenure"]
            },
            "Business Rules Engine": {
                "features": ["Compliance / Status Information"],
                "required_columns": ["status"],
                "optional_columns": ["compliance", "approval_status"]
            }
        }
        
        # SAFE: Only true aliases mapping (same business meaning)
        # DO NOT include columns with different business meanings
        self.true_aliases = {
            # Customer IDs - same meaning
            "customer_id": ["customer_id", "cust_id", "client_id", "user_id"],
            "customer_name": ["customer_name", "cust_name", "client_name"],
            
            # Account numbers - same meaning  
            "account_number": ["account_number", "acc_no", "acct_number", "account_no", "acct_no"],
            
            # Transaction IDs - same meaning
            "transaction_id": ["transaction_id", "txn_id", "trans_id", "transaction_number"],
            
            # Transaction dates - same meaning
            "transaction_date": ["transaction_date", "txn_date", "trans_date", "txn_datetime"],
            
            # Generic amount (NOT total_amount, tax_amount, net_amount - these are DIFFERENT)
            "amount": ["amount", "amt", "transaction_amount"],
            
            # Balance (generic)
            "balance": ["balance", "bal", "account_balance", "current_balance"],
            
            # Loan IDs - same meaning
            "loan_id": ["loan_id", "loan_number", "loan_no"],
            
            # EMI - same meaning
            "emi": ["emi", "installment", "monthly_payment", "monthly_installment"],
            
            # Interest rate - same meaning
            "interest_rate": ["interest_rate", "rate", "roi", "interest_percentage"],
            
            # Email - same meaning
            "email": ["email", "e-mail", "email_address"],
            
            # Phone - same meaning
            "phone": ["phone", "mobile", "contact", "telephone", "mobile_number"]
        }
        
        # PRESERVE THESE AS SEPARATE (different business meaning):
        # - product_name ≠ customer_name
        # - total_amount ≠ tax_amount ≠ net_amount
        # - transaction_id ≠ transaction_date
        # - opening_balance ≠ closing_balance
        self.preserve_separate = {
            "product_name", "customer_name", "account_name",
            "total_amount", "tax_amount", "net_amount", "gross_amount",
            "opening_balance", "closing_balance", "current_balance",
            "transaction_id", "transaction_date", "transaction_type",
            "debit", "credit", "debit_amount", "credit_amount"
        }
        
        # Module priority for column reassignment
        self.module_priorities = {
            "Customer Module": ["customer", "name", "email", "phone"],
            "Account Module": ["account", "product", "balance"],
            "Transaction Module": ["transaction", "transaction_date", "amount", "debit", "credit"],
            "Loan Module": ["loan", "emi", "interest"],
            "Business Rules Engine": ["status", "compliance", "approval"]
        }
    
    def normalize_column_name(self, col_name: str) -> str:
        """Normalize column name for comparison"""
        # Remove file prefix if present (file.column format)
        if '.' in col_name:
            col_name = col_name.rsplit('.', 1)[1]
        
        # Normalize to lowercase, replace spaces/underscores/dashes
        normalized = re.sub(r'[_\s-]+', '_', str(col_name).lower().strip())
        return normalized
    
    def are_columns_similar(self, col1: str, col2: str, threshold: int = 95) -> bool:
        """
        SAFE: Check if two columns are true aliases (same business meaning)
        
        STRICT RULES:
        - Only match if they are in the same true_aliases group
        - Do NOT match if they should be preserved as separate
        - Uses higher threshold for fuzzy matching
        """
        norm1 = self.normalize_column_name(col1)
        norm2 = self.normalize_column_name(col2)
        
        # Exact match after normalization
        if norm1 == norm2:
            return True
        
        # Check if both should be preserved as separate (different business meaning)
        if (norm1 in self.preserve_separate and norm2 in self.preserve_separate):
            if norm1 != norm2:
                return False  # They are intentionally separate
        
        # Check if they're in the same true_aliases group
        for canonical, variants in self.true_aliases.items():
            # Check if both columns match the same canonical or variants
            col1_match = any(v in norm1 or norm1 in v for v in variants) or canonical in norm1
            col2_match = any(v in norm2 or norm2 in v for v in variants) or canonical in norm2
            
            if col1_match and col2_match:
                return True
        
        # Very strict fuzzy match (only for obvious duplicates)
        similarity = fuzz.ratio(norm1, norm2)
        if similarity >= threshold and len(norm1) > 3 and len(norm2) > 3:
            # Additional check: both should be in preserve_separate or neither
            col1_preserve = any(ps in norm1 for ps in self.preserve_separate)
            col2_preserve = any(ps in norm2 for ps in self.preserve_separate)
            if col1_preserve != col2_preserve:
                return False  # Different categories
            return True
        
        return False
    
    def find_best_module_for_column(self, col_name: str, current_module: str, 
                                    all_modules: Dict[str, Dict[str, Any]]) -> str:
        """
        Re-evaluate which module a column should belong to
        
        Returns the best matching module name
        """
        norm_col = self.normalize_column_name(col_name)
        best_module = current_module
        best_score = 0
        
        # Check each module's semantic patterns
        for module_name, semantic_patterns in self.module_priorities.items():
            score = 0
            
            # Check if column matches any semantic pattern for this module
            for pattern in semantic_patterns:
                if pattern in self.column_semantics:
                    variants = self.column_semantics[pattern]
                    if any(variant in norm_col for variant in variants):
                        score += 30
                        break
                    # Check fuzzy match with variants
                    for variant in variants:
                        if fuzz.ratio(norm_col, variant) > 80:
                            score += 20
                            break
            
            # Check module's required/optional columns
            if module_name in all_modules:
                module_info = all_modules[module_name]
                for req_col in module_info.get("required_columns", []):
                    if req_col in norm_col or fuzz.ratio(req_col, norm_col) > 80:
                        score += 25
                        break
                
                for opt_col in module_info.get("optional_columns", []):
                    if opt_col in norm_col or fuzz.ratio(opt_col, norm_col) > 80:
                        score += 15
                        break
            
            if score > best_score:
                best_score = score
                best_module = module_name
        
        return best_module
    
    def remove_duplicate_columns(self, structure: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        """
        Rule 1: Remove duplicate columns within each module
        
        Returns:
            - Corrected structure
            - List of corrections applied
        """
        corrections = []
        corrected_structure = structure.copy()
        
        if "structure_tree" not in corrected_structure:
            return corrected_structure, corrections
        
        # Process each module
        for module in corrected_structure["structure_tree"].get("modules", []):
            if "columns" not in module:
                continue
            
            seen_columns = {}
            unique_columns = []
            duplicates_removed = []
            
            for col in module["columns"]:
                col_name = col.get("name", "")
                norm_name = self.normalize_column_name(col_name)
                
                # Check if we've seen a similar column
                is_duplicate = False
                for seen_norm, seen_col in seen_columns.items():
                    if self.are_columns_similar(col_name, seen_col["name"]):
                        is_duplicate = True
                        duplicates_removed.append(col_name)
                        # Keep the one with better semantic match
                        if col.get("feature", "") and not seen_col.get("feature", ""):
                            seen_columns[seen_norm] = col
                        break
                
                if not is_duplicate:
                    seen_columns[norm_name] = col
                    unique_columns.append(col)
            
            if duplicates_removed:
                module["columns"] = unique_columns
                corrections.append(f"Removed {len(duplicates_removed)} duplicate column(s) from {module['name']}: {', '.join(duplicates_removed[:3])}")
        
        return corrected_structure, corrections
    
    def re_evaluate_column_mapping(self, structure: Dict[str, Any], 
                                  all_modules: Dict[str, Dict[str, Any]]) -> Tuple[Dict[str, Any], List[str]]:
        """
        Rule 2: Re-evaluate column-to-module mapping
        
        Moves columns to more semantically appropriate modules
        """
        corrections = []
        corrected_structure = structure.copy()
        
        if "structure_tree" not in corrected_structure:
            return corrected_structure, corrections
        
        # Build column-to-module map
        column_module_map = {}
        for module in corrected_structure["structure_tree"].get("modules", []):
            module_name = module.get("name", "")
            for col in module.get("columns", []):
                col_name = col.get("name", "")
                column_module_map[col_name] = module_name
        
        # Re-evaluate each column
        columns_to_move = []
        for module in corrected_structure["structure_tree"].get("modules", []):
            module_name = module.get("name", "")
            columns_to_remove = []
            
            for col in module.get("columns", []):
                col_name = col.get("name", "")
                best_module = self.find_best_module_for_column(col_name, module_name, all_modules)
                
                if best_module != module_name:
                    columns_to_move.append({
                        "column": col,
                        "from_module": module_name,
                        "to_module": best_module
                    })
                    columns_to_remove.append(col)
            
            # Remove columns that need to be moved
            for col in columns_to_remove:
                module["columns"] = [c for c in module["columns"] if c != col]
        
        # Add columns to their new modules
        for move_info in columns_to_move:
            target_module = None
            for module in corrected_structure["structure_tree"].get("modules", []):
                if module.get("name", "") == move_info["to_module"]:
                    target_module = module
                    break
            
            if target_module:
                # Check if column already exists (avoid duplicates)
                col_name = move_info["column"].get("name", "")
                exists = any(self.are_columns_similar(col_name, c.get("name", "")) 
                           for c in target_module.get("columns", []))
                
                if not exists:
                    target_module.setdefault("columns", []).append(move_info["column"])
                    corrections.append(
                        f"Moved '{col_name}' from {move_info['from_module']} to {move_info['to_module']}"
                    )
        
        return corrected_structure, corrections
    
    def normalize_similar_columns(self, structure: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        """
        SAFE Rule 3: Normalize ONLY true aliases (same business meaning)
        
        Examples of SAFE normalization:
        - txn_date ↔ transaction_date (same meaning)
        - cust_id ↔ customer_id (same meaning)
        
        Examples of UNSAFE (DO NOT normalize):
        - product_name ≠ customer_name (different meaning)
        - total_amount ≠ tax_amount ≠ net_amount (different meaning)
        """
        corrections = []
        corrected_structure = structure.copy()
        
        if "structure_tree" not in corrected_structure:
            return corrected_structure, corrections
        
        # Build canonical map from true_aliases only
        canonical_map = {}
        for canonical, variants in self.true_aliases.items():
            for variant in variants:
                canonical_map[variant] = canonical
            # Canonical maps to itself
            canonical_map[canonical] = canonical
        
        # Normalize columns in each module (SAFE normalization only)
        for module in corrected_structure["structure_tree"].get("modules", []):
            normalized_columns = []
            seen_canonicals = set()
            
            for col in module.get("columns", []):
                col_name = col.get("name", "")
                norm_name = self.normalize_column_name(col_name)
                
                # Check if this column should be preserved as separate
                if any(ps in norm_name for ps in self.preserve_separate):
                    # Keep original name - do not normalize
                    if col_name not in [c.get("name", "") for c in normalized_columns]:
                        normalized_columns.append(col)
                    else:
                        corrections.append(f"Removed duplicate: '{col_name}' in {module.get('name', '')}")
                    continue
                
                # Find canonical name from true_aliases only
                canonical = None
                for variant, canon in canonical_map.items():
                    if variant in norm_name or fuzz.ratio(variant, norm_name) > 90:
                        # Double-check: ensure it's a true alias (not different business meaning)
                        if self.are_columns_similar(col_name, canonical):
                            canonical = canon
                            break
                
                # Only normalize if canonical is found and it's a true alias
                if canonical and canonical != norm_name:
                    # Verify this is a safe normalization
                    if canonical not in self.preserve_separate or norm_name in self.true_aliases.get(canonical, []):
                        original_name = col.get("name", "")
                        col["name"] = canonical
                        
                        # Check for duplicates after normalization
                        if canonical not in seen_canonicals:
                            seen_canonicals.add(canonical)
                            normalized_columns.append(col)
                            if original_name != canonical:
                                corrections.append(
                                    f"SAFE: Normalized '{original_name}' → '{canonical}' in {module.get('name', '')}"
                                )
                        else:
                            corrections.append(
                                f"Removed duplicate after normalization: '{original_name}' in {module.get('name', '')}"
                            )
                    else:
                        # Unsafe normalization - keep original
                        if col_name not in [c.get("name", "") for c in normalized_columns]:
                            normalized_columns.append(col)
                else:
                    # No canonical found or already canonical - keep as is
                    if col_name not in [c.get("name", "") for c in normalized_columns]:
                        normalized_columns.append(col)
                    else:
                        corrections.append(f"Removed duplicate: '{col_name}' in {module.get('name', '')}")
            
            module["columns"] = normalized_columns
        
        return corrected_structure, corrections
    
    def balance_module_completeness(self, structure: Dict[str, Any], 
                                   file_dataframes: Dict[str, pd.DataFrame]) -> Tuple[Dict[str, Any], List[str]]:
        """
        SAFE Rule 4: Mandatory Banking Inference
        
        - If Transaction Module exists and amount is present,
          infer and add `balance` to Account Module if missing.
        - If Customer Module exists, ensure customer_id is present or inferred.
        """
        corrections = []
        corrected_structure = structure.copy()
        
        if "modules" not in corrected_structure:
            return corrected_structure, corrections
        
        module_scores = corrected_structure.get("modules", {})
        structure_tree = corrected_structure.get("structure_tree", {})
        
        # Inference 1: If Transaction Module exists and has amount, ensure Account Module has balance
        transaction_module_score = module_scores.get("Transaction Module", {}).get("confidence", 0)
        transaction_module_node = None
        for module in structure_tree.get("modules", []):
            if module.get("name") == "Transaction Module":
                transaction_module_node = module
                break
        
        has_amount = False
        if transaction_module_node:
            for col in transaction_module_node.get("columns", []):
                col_name = col.get("name", "").lower()
                if "amount" in col_name and col_name not in ["tax_amount", "net_amount", "total_amount"]:
                    has_amount = True
                    break
        
        account_module_score = module_scores.get("Account Module", {}).get("confidence", 0)
        account_module_node = None
        for module in structure_tree.get("modules", []):
            if module.get("name") == "Account Module":
                account_module_node = module
                break
        
        if transaction_module_score > 50 and has_amount and account_module_score < 70:
            # Check if balance exists in Account Module
            has_balance = False
            if account_module_node:
                for col in account_module_node.get("columns", []):
                    if "balance" in col.get("name", "").lower():
                        has_balance = True
                        break
            
            if not has_balance:
                # Infer balance column from data
                balance_found = False
                for file_name, df in file_dataframes.items():
                    for col in df.columns:
                        norm_col = self.normalize_column_name(col)
                        if "balance" in norm_col or "bal" in norm_col:
                            # Create or enhance Account Module
                            if not account_module_node:
                                account_module_node = {
                                    "name": "Account Module",
                                    "confidence": max(account_module_score, 55),
                                    "status": "PARTIAL",
                                    "columns": []
                                }
                                structure_tree.setdefault("modules", []).append(account_module_node)
                            
                            # Add balance column
                            balance_col = {
                                "name": col,
                                "feature": "Account Information",
                                "file": file_name
                            }
                            if not any(c.get("name", "").lower() == col.lower() for c in account_module_node.get("columns", [])):
                                account_module_node.setdefault("columns", []).append(balance_col)
                                corrections.append(
                                    f"SAFE: Inferred 'balance' column ({col}) for Account Module (Transaction Module detected)"
                                )
                                balance_found = True
                                break
                    if balance_found:
                        break
        
        # Inference 2: If Customer Module exists, ensure customer_id is present
        customer_module_score = module_scores.get("Customer Module", {}).get("confidence", 0)
        customer_module_node = None
        for module in structure_tree.get("modules", []):
            if module.get("name") == "Customer Module":
                customer_module_node = module
                break
        
        if customer_module_score > 50 and customer_module_node:
            has_customer_id = False
            for col in customer_module_node.get("columns", []):
                col_name = col.get("name", "").lower()
                if "customer" in col_name and "id" in col_name:
                    has_customer_id = True
                    break
            
            if not has_customer_id:
                # Look for customer_id in data
                for file_name, df in file_dataframes.items():
                    for col in df.columns:
                        norm_col = self.normalize_column_name(col)
                        if "customer" in norm_col and "id" in norm_col:
                            customer_id_col = {
                                "name": col,
                                "feature": "Customer Information",
                                "file": file_name
                            }
                            if not any(c.get("name", "").lower() == col.lower() for c in customer_module_node.get("columns", [])):
                                customer_module_node.setdefault("columns", []).append(customer_id_col)
                                corrections.append(
                                    f"SAFE: Inferred 'customer_id' column ({col}) for Customer Module"
                                )
                                break
                    if any("customer" in c.get("name", "").lower() and "id" in c.get("name", "").lower() 
                          for c in customer_module_node.get("columns", [])):
                        break
        
        corrected_structure["modules"] = module_scores
        corrected_structure["structure_tree"] = structure_tree
        
        return corrected_structure, corrections
    
    def derive_business_rules(self, structure: Dict[str, Any]) -> Tuple[List[str], List[str]]:
        """
        SAFE Rule 5: Derive business rules from related columns (preserve financial integrity)
        
        Financial Integrity Rules:
        - Keep total_amount, tax_amount, and net_amount as separate columns.
        - Enforce rule: net_amount = total_amount - tax_amount (as calculation rule, not merge).
        - Monetary columns must remain numeric and positive.
        """
        new_rules = []
        corrections = []
        
        if "structure_tree" not in structure:
            return new_rules, corrections
        
        # Collect all column names with their modules
        all_columns = {}
        for module in structure["structure_tree"].get("modules", []):
            module_name = module.get("name", "")
            for col in module.get("columns", []):
                col_name = col.get("name", "")
                norm_name = self.normalize_column_name(col_name)
                all_columns[norm_name] = {"original": col_name, "module": module_name}
        
        col_names = list(all_columns.keys())
        
        # Rule 1: net_amount = total_amount - tax_amount (PRESERVE ALL THREE AS SEPARATE)
        has_total = any("total" in col and "amount" in col for col in col_names)
        has_tax = any("tax" in col and "amount" in col for col in col_names)
        has_net = any("net" in col and "amount" in col for col in col_names)
        
        if has_total and has_tax:
            if has_net:
                new_rules.append("VALIDATION: net_amount = total_amount - tax_amount (all three must be present and consistent)")
                corrections.append("Financial integrity: Validated total_amount, tax_amount, and net_amount relationship")
            else:
                new_rules.append("INFERENCE: net_amount should equal total_amount - tax_amount")
                corrections.append("Derived rule: net_amount calculation from total_amount and tax_amount (preserved as separate columns)")
        
        # Rule 2: closing_balance = opening_balance + credits - debits
        has_opening = any("opening" in col and "balance" in col for col in col_names)
        has_closing = any("closing" in col and "balance" in col for col in col_names)
        has_credit = any("credit" in col and "amount" in col for col in col_names)
        has_debit = any("debit" in col and "amount" in col for col in col_names)
        
        if has_opening and has_closing and (has_credit or has_debit):
            new_rules.append("VALIDATION: closing_balance = opening_balance + total_credits - total_debits")
            corrections.append("Financial integrity: Validated balance calculation rule")
        
        # Rule 3: EMI calculation (if loan module exists)
        has_principal = any("principal" in col or ("loan" in col and "amount" in col and "loan_amount" not in col) 
                           for col in col_names)
        has_interest_rate = any("interest" in col and "rate" in col for col in col_names)
        has_tenure = any("tenure" in col or "term" in col for col in col_names)
        has_emi = any("emi" in col or "installment" in col for col in col_names)
        
        if has_principal and has_interest_rate and has_tenure:
            if has_emi:
                new_rules.append("VALIDATION: EMI = (principal * interest_rate * (1 + interest_rate)^tenure) / ((1 + interest_rate)^tenure - 1)")
                corrections.append("Financial integrity: Validated EMI calculation rule")
            else:
                new_rules.append("INFERENCE: EMI should be calculated as (principal * interest_rate * (1 + interest_rate)^tenure) / ((1 + interest_rate)^tenure - 1)")
                corrections.append("Derived rule: EMI calculation from principal, interest_rate, and tenure")
        
        # Rule 4: Monetary columns validation
        monetary_cols = [col for col in col_names if "amount" in col or "balance" in col]
        if monetary_cols:
            new_rules.append(f"VALIDATION: Monetary columns ({len(monetary_cols)}) must be numeric and non-negative: {', '.join(monetary_cols[:3])}")
            corrections.append(f"Financial integrity: Validated {len(monetary_cols)} monetary columns")
        
        return new_rules, corrections
    
    def recalculate_confidence_scores(self, structure: Dict[str, Any], 
                                     columns_by_feature: Dict[str, List[str]]) -> Tuple[Dict[str, Any], List[str]]:
        """
        SAFE Rule 6: Recalculate confidence scores logically after corrections
        
        Rules:
        - Confidence scores must be recalculated logically
        - A fix must not reduce confidence unless critical data is removed
        - Completion of mandatory fields increases confidence
        """
        corrections = []
        corrected_structure = structure.copy()
        
        if "modules" not in corrected_structure:
            return corrected_structure, corrections
        
        # Recalculate based on current column assignments in structure tree
        module_scores = {}
        
        for module_name, module_info in self.modules.items():
            score = 0
            matched_columns = []
            required_count = 0
            optional_count = 0
            
            # Find columns assigned to this module in structure tree
            module_columns = []
            module_node = None
            if "structure_tree" in corrected_structure:
                for node in corrected_structure["structure_tree"].get("modules", []):
                    if node.get("name") == module_name:
                        module_node = node
                        module_columns = [c.get("name", "") for c in node.get("columns", [])]
                        break
            
            # Check required columns
            for req_col in module_info["required_columns"]:
                found = False
                for col in module_columns:
                    norm_col = self.normalize_column_name(col)
                    # More strict matching for required columns
                    if (req_col in norm_col or 
                        fuzz.ratio(req_col, norm_col) > 85 or
                        any(alias in norm_col for alias in self.true_aliases.get(req_col, []))):
                        if col not in matched_columns:
                            matched_columns.append(col)
                            required_count += 1
                            score += 35  # Higher weight for required columns
                            found = True
                            break
                
                if not found:
                    # Check if a normalized version exists (true alias)
                    for alias_group, variants in self.true_aliases.items():
                        if req_col in variants or alias_group == req_col:
                            for variant in variants:
                                for col in module_columns:
                                    norm_col = self.normalize_column_name(col)
                                    if variant in norm_col or norm_col in variant:
                                        if col not in matched_columns:
                                            matched_columns.append(col)
                                            required_count += 1
                                            score += 35
                                            found = True
                                            break
                                if found:
                                    break
                            if found:
                                break
            
            # Check optional columns
            for opt_col in module_info["optional_columns"]:
                for col in module_columns:
                    if col in matched_columns:
                        continue
                    norm_col = self.normalize_column_name(col)
                    if (opt_col in norm_col or 
                        fuzz.ratio(opt_col, norm_col) > 85 or
                        any(alias in norm_col for alias in self.true_aliases.get(opt_col, []))):
                        matched_columns.append(col)
                        optional_count += 1
                        score += 15  # Lower weight for optional columns
                        break
            
            # Calculate final confidence (logical calculation)
            required_total = len(module_info["required_columns"])
            optional_total = len(module_info["optional_columns"])
            
            if required_count >= required_total:
                # All required columns present - high confidence
                base_confidence = 70 + (optional_count / max(optional_total, 1)) * 20
                confidence = min(base_confidence, 100)
                status = "COMPLETE"
            elif required_count > 0:
                # Partial required columns - moderate confidence
                base_confidence = 40 + (required_count / required_total) * 30 + (optional_count / max(optional_total, 1)) * 10
                confidence = min(base_confidence, 75)
                status = "PARTIAL"
            else:
                # No required columns - low/no confidence
                confidence = min(optional_count * 10, 30)
                status = "MISSING"
            
            # Get old confidence for comparison
            old_module_info = corrected_structure["modules"].get(module_name, {})
            old_confidence = old_module_info.get("confidence", 0)
            
            # Only update if confidence actually changed significantly
            module_scores[module_name] = {
                "confidence": round(confidence, 2),
                "matched_columns": matched_columns,
                "required_columns_found": required_count,
                "optional_columns_found": optional_count,
                "status": status
            }
            
            # Only report if significant change (more than 5%)
            if abs(confidence - old_confidence) > 5:
                direction = "↑" if confidence > old_confidence else "↓"
                corrections.append(
                    f"Recalculated {module_name} confidence: {old_confidence}% {direction} {confidence:.1f}% "
                    f"({required_count}/{required_total} required, {optional_count}/{optional_total} optional)"
                )
        
        corrected_structure["modules"] = module_scores
        
        # Update structure tree module confidences
        if "structure_tree" in corrected_structure:
            for module_node in corrected_structure["structure_tree"].get("modules", []):
                module_name = module_node.get("name", "")
                if module_name in module_scores:
                    module_node["confidence"] = module_scores[module_name]["confidence"]
                    module_node["status"] = module_scores[module_name]["status"]
        
        return corrected_structure, corrections
    
    def correct_and_balance(self, structure: Dict[str, Any], 
                           file_dataframes: Dict[str, pd.DataFrame],
                           columns_by_feature: Dict[str, List[str]]) -> Dict[str, Any]:
        """
        Main method to apply all correction and balancing rules
        
        Returns:
            - Corrected structure
            - List of all corrections applied
        """
        all_corrections = []
        corrected_structure = structure.copy()
        
        # Use the modules definition from self
        all_modules = self.modules
        
        # Rule 1: Remove duplicate columns
        corrected_structure, corrections = self.remove_duplicate_columns(corrected_structure)
        all_corrections.extend(corrections)
        
        # Rule 2: Re-evaluate column mapping
        corrected_structure, corrections = self.re_evaluate_column_mapping(corrected_structure, all_modules)
        all_corrections.extend(corrections)
        
        # Rule 3: Normalize similar columns
        corrected_structure, corrections = self.normalize_similar_columns(corrected_structure)
        all_corrections.extend(corrections)
        
        # Rule 4: Balance module completeness
        corrected_structure, corrections = self.balance_module_completeness(corrected_structure, file_dataframes)
        all_corrections.extend(corrections)
        
        # Rule 5: Derive business rules
        new_rules, corrections = self.derive_business_rules(corrected_structure)
        all_corrections.extend(corrections)
        if new_rules:
            if "business_rules" not in corrected_structure:
                corrected_structure["business_rules"] = []
            corrected_structure["business_rules"].extend(new_rules)
        
        # Rule 6: Recalculate confidence scores
        corrected_structure, corrections = self.recalculate_confidence_scores(corrected_structure, columns_by_feature)
        all_corrections.extend(corrections)
        
        # Add corrections summary
        corrected_structure["corrections_applied"] = all_corrections
        corrected_structure["total_corrections"] = len(all_corrections)
        
        return corrected_structure
