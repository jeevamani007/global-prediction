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
        # Banking feature patterns for classification
        self.feature_patterns = {
            "Customer Information": {
                "keywords": ["customer", "client", "user", "person", "name", "email", "phone", "address", "dob", "age"],
                "patterns": {
                    "customer_id": r"customer.*id|client.*id|user.*id",
                    "customer_name": r"customer.*name|client.*name|user.*name|name",
                    "email": r"email|e-mail|mail",
                    "phone": r"phone|mobile|contact|tel",
                    "address": r"address|location|city|state|country|zip|pincode"
                }
            },
            "Account Information": {
                "keywords": ["account", "acc", "acct", "balance", "opening", "closing", "status", "type", "branch"],
                "patterns": {
                    "account_number": r"account.*number|acc.*no|acct.*num|account_id",
                    "balance": r"balance|bal|amount",
                    "opening_balance": r"opening.*balance|initial.*balance|start.*balance",
                    "closing_balance": r"closing.*balance|final.*balance|end.*balance",
                    "account_status": r"account.*status|acc.*status|status",
                    "account_type": r"account.*type|acc.*type|type",
                    "branch_code": r"branch.*code|branch.*id|ifsc|branch"
                }
            },
            "Transaction Information": {
                "keywords": ["transaction", "txn", "trans", "debit", "credit", "amount", "payment", "transfer"],
                "patterns": {
                    "transaction_id": r"transaction.*id|txn.*id|trans.*id|transaction_number",
                    "amount": r"amount|amt|value|sum",
                    "debit": r"debit|dr|withdraw|outgoing",
                    "credit": r"credit|cr|deposit|incoming",
                    "transaction_date": r"transaction.*date|txn.*date|date|timestamp",
                    "transaction_type": r"transaction.*type|txn.*type|type",
                    "purpose": r"purpose|description|narration|remarks|note"
                }
            },
            "Loan / EMI Information": {
                "keywords": ["loan", "emi", "interest", "principal", "tenure", "disbursement", "repayment"],
                "patterns": {
                    "loan_id": r"loan.*id|loan.*number|loan.*no",
                    "emi": r"emi|installment|monthly.*payment",
                    "interest_rate": r"interest.*rate|rate|roi",
                    "principal": r"principal|loan.*amount|disbursement.*amount",
                    "tenure": r"tenure|term|duration|period",
                    "loan_status": r"loan.*status|status"
                }
            },
            "Compliance / Status Information": {
                "keywords": ["status", "compliance", "kyc", "verification", "approval", "rejection"],
                "patterns": {
                    "status": r"status|state|condition",
                    "compliance": r"compliance|kyc|verification|approval",
                    "approval_status": r"approval|approved|rejected|pending"
                }
            }
        }
        
        # Module definitions with required features
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
        
        # Application type patterns
        self.application_types = {
            "Core Banking": {
                "required_modules": ["Customer Module", "Account Module"],
                "optional_modules": ["Transaction Module"],
                "description": "Complete core banking system with customer and account management"
            },
            "Transaction Processing": {
                "required_modules": ["Transaction Module"],
                "optional_modules": ["Account Module"],
                "description": "Focused on transaction processing and payment handling"
            },
            "Loan Management": {
                "required_modules": ["Loan Module"],
                "optional_modules": ["Customer Module"],
                "description": "Loan and EMI management system"
            },
            "Hybrid Banking System": {
                "required_modules": ["Customer Module", "Account Module", "Transaction Module"],
                "optional_modules": ["Loan Module"],
                "description": "Comprehensive banking system with multiple modules"
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
        
        # Step 4: Assign module confidence
        module_scores = self.assign_module_confidence(columns_by_feature)
        
        # Step 5: Determine application type
        application_type = self.determine_application_type(module_scores)
        
        # Step 6: Generate business rules suggestions
        business_rules = self._generate_business_rules(columns_by_feature, module_scores)
        
        # Step 7: Build hierarchical structure
        structure_tree = self._build_structure_tree(module_scores, columns_by_feature, file_dataframes)
        
        return {
            "application_type": application_type,
            "modules": module_scores,
            "columns_by_feature": {k: v for k, v in columns_by_feature.items()},
            "column_analysis": all_columns_analysis,
            "relationships": relationships,
            "business_rules": business_rules,
            "structure_tree": structure_tree,
            "total_files": len(file_dataframes),
            "total_columns": sum(len(df.columns) for df in file_dataframes.values())
        }
    
    def _generate_business_rules(self, columns_by_feature: Dict[str, List[str]], 
                                 module_scores: Dict[str, Dict[str, Any]]) -> List[str]:
        """Generate suggested business rules based on detected modules and columns"""
        rules = []
        
        # Account Module rules
        if module_scores.get("Account Module", {}).get("confidence", 0) >= 50:
            rules.append("Account numbers must be unique across all accounts")
            rules.append("Balance cannot be negative unless overdraft facility is enabled")
            rules.append("Account status must be 'active' for transactions")
        
        # Transaction Module rules
        if module_scores.get("Transaction Module", {}).get("confidence", 0) >= 50:
            rules.append("Transaction IDs must be unique")
            rules.append("Debit amount cannot exceed account balance")
            rules.append("All transactions must have a timestamp")
        
        # Customer Module rules
        if module_scores.get("Customer Module", {}).get("confidence", 0) >= 50:
            rules.append("Customer IDs must be unique")
            rules.append("Each account must be linked to exactly one customer")
        
        # Loan Module rules
        if module_scores.get("Loan Module", {}).get("confidence", 0) >= 50:
            rules.append("EMI amount must be calculated based on principal, interest rate, and tenure")
            rules.append("Loan status controls EMI deductions")
        
        return rules
    
    def _build_structure_tree(self, module_scores: Dict[str, Dict[str, Any]], 
                             columns_by_feature: Dict[str, List[str]],
                             file_dataframes: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Build hierarchical tree structure for display"""
        tree = {
            "application_type": "Banking Application",
            "modules": []
        }
        
        # Add modules with their columns
        for module_name, module_info in module_scores.items():
            if module_info["confidence"] >= 30:  # Only show modules with some confidence
                module_node = {
                    "name": module_name,
                    "confidence": module_info["confidence"],
                    "status": module_info["status"],
                    "columns": []
                }
                
                # Add columns for this module
                for feature in self.modules[module_name]["features"]:
                    if feature in columns_by_feature:
                        for col_path in columns_by_feature[feature]:
                            # Extract column name from file.column format
                            if '.' in col_path:
                                file_name, col_name = col_path.rsplit('.', 1)
                            else:
                                col_name = col_path
                            
                            module_node["columns"].append({
                                "name": col_name,
                                "feature": feature,
                                "file": file_name if '.' in col_path else "Unknown"
                            })
                
                tree["modules"].append(module_node)
        
        return tree
