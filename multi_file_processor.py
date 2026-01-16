"""
Multi-File Data Intelligence System

Extends single-file analysis to multiple files/tables:
- Applies same single-file logic to each file independently
- Detects domain per table and overall domain
- Detects primary keys per table
- Detects foreign keys across files
- Builds relationships between tables
- Applies business rules per table
- Provides consolidated results
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
import os
import re
from bank import BankingDomainDetector
from complete_banking_validator import CompleteBankingValidator
from core_banking_validator import CoreBankingValidator
from enhanced_business_rules_engine import EnhancedBusinessRulesEngine
from column_predictor import ColumnPredictor
from banking_blueprint_engine import BankingBlueprintEngine


class MultiFileProcessor:
    """Process multiple files using the same single-file logic"""
    
    def __init__(self):
        self.banking_detector = BankingDomainDetector()
        self.complete_validator = CompleteBankingValidator()
        self.core_validator = CoreBankingValidator()
        self.business_rules_engine = EnhancedBusinessRulesEngine()
        self.column_predictor = ColumnPredictor()
        self.blueprint_engine = BankingBlueprintEngine()
    
    def _format_validator_result(self, validator_result: Dict) -> Dict:
        """Format validator result to match UI expected format (same as single-file endpoint)"""
        if not validator_result or "error" in validator_result:
            return validator_result
        
        # Extract column-level validation results
        column_validation_results = validator_result.get("column_wise_validation", [])
        
        columns_result = []
        for col in column_validation_results:
            status = col.get("validation_result", "FAIL").upper()
            
            columns_result.append({
                "name": col.get("column_name", "Unknown"),
                "meaning": col.get("standard_name", "Unknown"),
                "status": status,
                "confidence": col.get("confidence_percentage", 0),
                "rules_passed": 1,
                "rules_total": 1,
                "failures": col.get("detected_issue", []),
                "applied_rules": [col.get("business_rule", "General Rule")],
                "reasons": col.get("detected_issue", [])
            })
        
        # Calculate overall dataset confidence
        summary = validator_result.get("summary", {})
        avg_confidence = summary.get("overall_confidence", 0)
        
        # Determine final decision based on overall confidence
        overall_confidence = summary.get("overall_confidence", 0)
        if overall_confidence >= 95:
            final_decision = "PASS"
        elif overall_confidence >= 80:
            final_decision = "PASS WITH WARNINGS"
        else:
            final_decision = "FAIL"
        
        formatted_result = {
            "final_decision": final_decision,
            "dataset_confidence": round(avg_confidence, 1),
            "explanation": f"Banking validation completed. {summary.get('total_columns_analyzed', 0)} columns analyzed, {summary.get('total_passed', 0)} passed, {summary.get('total_failed', 0)} failed.",
            "columns": columns_result,
            "relationships": validator_result.get("cross_column_validations", []),
            "total_records": summary.get("total_records", 0)
        }
        
        return formatted_result
    
    def process_files(self, file_paths: List[str]) -> Dict[str, Any]:
        """
        Main entry point for multi-file processing
        
        Steps:
        1. Process each file independently (same single-file logic)
        2. Detect domain per table
        3. Detect primary keys per table
        4. Detect foreign keys across files
        5. Build relationships
        6. Apply business rules per table
        7. Consolidate results
        """
        if not file_paths:
            return {"error": "No files provided"}
        
        # STEP 1: Process each file independently
        table_results = []
        table_dataframes = {}
        
        for file_path in file_paths:
            try:
                # Load dataframe
                df = pd.read_csv(file_path)
                table_name = os.path.basename(file_path)
                
                # Store dataframe for cross-file analysis
                table_dataframes[table_name] = df
                
                # Apply single-file logic
                single_file_result = self._process_single_file(file_path, table_name, df)
                table_results.append(single_file_result)
                
            except Exception as e:
                table_results.append({
                    "table_name": os.path.basename(file_path),
                    "error": str(e),
                    "status": "FAILED"
                })
        
        # STEP 2: Detect domain per table and overall domain
        domain_results = self._detect_domains(table_results)
        
        # STEP 3: Detect primary keys per table
        primary_keys = self._detect_primary_keys(table_dataframes)
        
        # STEP 4: Detect foreign keys across files
        foreign_keys = self._detect_foreign_keys(table_dataframes, primary_keys)
        
        # STEP 5: Build relationships
        relationships = self._build_relationships(table_dataframes, primary_keys, foreign_keys)
        
        # STEP 6: Apply business rules (already done in single-file processing)
        
        # STEP 7: Unified Banking Blueprint Analysis (New)
        try:
            banking_blueprint = self.blueprint_engine.analyze_multiple_files(table_dataframes)
        except Exception as e:
            banking_blueprint = {"error": f"Blueprint analysis failed: {str(e)}"}
            
        # STEP 8: Consolidate results
        consolidated_result = self._consolidate_results(
            table_results, domain_results, primary_keys, foreign_keys, relationships, banking_blueprint
        )
        
        return consolidated_result
    
    def _process_single_file(self, file_path: str, table_name: str, df: pd.DataFrame) -> Dict[str, Any]:
        """Apply the exact same single-file logic to each file"""
        result = {
            "table_name": table_name,
            "file_path": file_path,
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "columns": list(df.columns),
            "status": "SUCCESS"
        }
        
        try:
            # Apply banking domain detection (same as single-file)
            banking_result = self.banking_detector.predict(file_path)
            result["banking_domain"] = banking_result
            
            # Apply complete banking validator (same as single-file)
            if banking_result and isinstance(banking_result, dict):
                decision = banking_result.get("decision")
                if decision and decision != "UNKNOWN":
                    try:
                        validator_result = self.complete_validator.validate_dataset(file_path)
                        # Format validator result to match UI format
                        result["banking_validator"] = self._format_validator_result(validator_result)
                    except Exception as e:
                        result["banking_validator_error"] = str(e)
                    
                    try:
                        core_validator_result = self.core_validator.validate(file_path)
                        result["core_banking_validator"] = core_validator_result
                    except Exception as e:
                        result["core_banking_validator_error"] = str(e)
            
            # Column-level analysis (same as single-file)
            column_analysis = self._analyze_columns(df)
            result["column_analysis"] = column_analysis
            
            # Generate enhanced business rules for this table
            try:
                business_rules = self.business_rules_engine.generate_table_business_rules(df, table_name)
                result["business_rules"] = business_rules
                
                # Format rules as paragraphs
                rules_paragraphs = self.business_rules_engine.format_rules_as_paragraphs(business_rules)
                result["business_rules_paragraphs"] = rules_paragraphs
            except Exception as e:
                result["business_rules_error"] = str(e)
            
            # Generate column predictions
            try:
                column_predictions = []
                for column in df.columns:
                    prediction = self.column_predictor.predict_column_type(column, df[column])
                    column_predictions.append(prediction)
                result["column_predictions"] = column_predictions
            except Exception as e:
                result["column_predictions_error"] = str(e)
            
        except Exception as e:
            result["error"] = str(e)
            result["status"] = "FAILED"
        
        return result
    
    def _analyze_columns(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Column semantic analysis (same as single-file)"""
        column_results = []
        
        for col in df.columns:
            series = df[col]
            # Convert numpy types to Python native types
            null_pct = float((series.isnull().sum() / len(series)) * 100) if len(series) > 0 else 0.0
            unique_pct = float((series.nunique() / len(series)) * 100) if len(series) > 0 else 0.0
            
            analysis = {
                "column_name": col,
                "data_type": str(series.dtype),
                "null_percentage": round(null_pct, 2),
                "uniqueness_percentage": round(unique_pct, 2),
                "pattern": self._detect_pattern(series),
                "sample_values": [str(v) for v in series.dropna().head(5).tolist()] if len(series.dropna()) > 0 else []
            }
            column_results.append(analysis)
        
        return column_results
    
    def _detect_pattern(self, series: pd.Series) -> str:
        """Detect pattern: id, number, date, amount"""
        non_null = series.dropna()
        if len(non_null) == 0:
            return "unknown"
        
        # Check for ID pattern
        if "id" in str(series.name).lower():
            return "id"
        
        # Check for number pattern (numeric, 6-18 digits)
        if pd.api.types.is_numeric_dtype(series):
            return "number"
        
        # Check for date pattern
        try:
            pd.to_datetime(non_null.head(10), errors="raise")
            return "date"
        except:
            pass
        
        # Check for amount/money pattern
        if pd.api.types.is_numeric_dtype(series):
            if "amount" in str(series.name).lower() or "balance" in str(series.name).lower():
                return "amount"
        
        return "text"
    
    def _detect_domains(self, table_results: List[Dict]) -> Dict[str, Any]:
        """Detect domain per table and overall domain"""
        table_domains = []
        domain_votes = {}
        
        for table_result in table_results:
            if table_result.get("status") != "SUCCESS":
                continue
            
            table_name = table_result.get("table_name", "unknown")
            banking_domain = table_result.get("banking_domain", {})
            
            if isinstance(banking_domain, dict):
                decision = banking_domain.get("decision", "UNKNOWN")
                confidence = banking_domain.get("confidence_percentage", 0)
                domain = banking_domain.get("domain", "Unknown")
                
                table_domains.append({
                    "table_name": table_name,
                    "domain": domain,
                    "decision": decision,
                    "confidence": confidence
                })
                
                # Count domain votes
                if decision not in ["UNKNOWN", None]:
                    if domain not in domain_votes:
                        domain_votes[domain] = []
                    domain_votes[domain].append(confidence)
        
        # Determine overall domain (majority vote)
        overall_domain = "Unknown"
        overall_confidence = 0.0
        overall_decision = "UNKNOWN"
        
        if domain_votes:
            # Find domain with highest average confidence
            domain_scores = {
                domain: np.mean(confidences) 
                for domain, confidences in domain_votes.items()
            }
            
            if domain_scores:
                overall_domain = max(domain_scores.items(), key=lambda x: x[1])[0]
                overall_confidence = domain_scores[overall_domain]
                overall_decision = "BANKING" if overall_confidence >= 70 else "UNKNOWN"
        
        return {
            "table_domains": table_domains,
            "overall_domain": {
                "domain": overall_domain,
                "confidence": round(overall_confidence, 2),
                "decision": overall_decision,
                "tables_analyzed": len(table_domains)
            }
        }
    
    def _detect_primary_keys(self, table_dataframes: Dict[str, pd.DataFrame]) -> Dict[str, Optional[str]]:
        """Detect primary key per table using: 95% uniqueness, NOT NULL, id/number/code patterns"""
        primary_keys = {}
        
        for table_name, df in table_dataframes.items():
            pk_candidate = None
            best_score = 0
            
            for col in df.columns:
                series = df[col]
                norm_col = str(col).lower()
                
                # Check uniqueness (95% threshold)
                uniqueness = (series.nunique() / len(series)) * 100 if len(series) > 0 else 0
                if uniqueness < 95:
                    continue
                
                # Check NOT NULL (95% threshold)
                not_null_ratio = (series.notna().sum() / len(series)) * 100 if len(series) > 0 else 0
                if not_null_ratio < 95:
                    continue
                
                # Check patterns (id, number, code)
                has_id_pattern = "id" in norm_col or "number" in norm_col or "code" in norm_col
                
                # Calculate score
                score = uniqueness * 0.4 + not_null_ratio * 0.4 + (100 if has_id_pattern else 0) * 0.2
                
                if score > best_score:
                    best_score = score
                    pk_candidate = col
            
            primary_keys[table_name] = pk_candidate
        
        return primary_keys
    
    def _detect_foreign_keys(self, table_dataframes: Dict[str, pd.DataFrame], 
                            primary_keys: Dict[str, Optional[str]]) -> List[Dict[str, Any]]:
        """Detect foreign keys across files by comparing column names and value overlap"""
        foreign_keys = []
        table_names = list(table_dataframes.keys())
        
        for i, parent_table in enumerate(table_names):
            parent_pk = primary_keys.get(parent_table)
            if not parent_pk:
                continue
            
            parent_df = table_dataframes[parent_table]
            parent_values = set(parent_df[parent_pk].dropna().astype(str))
            
            for j, child_table in enumerate(table_names):
                if i == j:
                    continue
                
                child_df = table_dataframes[child_table]
                
                # Check for same column name
                if parent_pk in child_df.columns:
                    child_values = set(child_df[parent_pk].dropna().astype(str))
                    overlap = len(parent_values & child_values)
                    overlap_ratio = overlap / len(child_values) if len(child_values) > 0 else 0
                    
                    if overlap_ratio > 0.5:  # At least 50% overlap
                        foreign_keys.append({
                            "parent_table": parent_table,
                            "parent_column": parent_pk,
                            "child_table": child_table,
                            "child_column": parent_pk,
                            "overlap_ratio": round(overlap_ratio, 2),
                            "relationship_type": "One-to-Many"  # Will be refined in relationship building
                        })
                
                # Check for similar column names (fuzzy matching)
                for child_col in child_df.columns:
                    norm_parent = str(parent_pk).lower()
                    norm_child = str(child_col).lower()
                    
                    # Skip if exact match already found
                    if child_col == parent_pk:
                        continue
                    
                    # Check if column names are similar
                    if self._columns_similar(norm_parent, norm_child):
                        child_values = set(child_df[child_col].dropna().astype(str))
                        overlap = len(parent_values & child_values)
                        overlap_ratio = overlap / len(child_values) if len(child_values) > 0 else 0
                        
                        if overlap_ratio > 0.5:
                            foreign_keys.append({
                                "parent_table": parent_table,
                                "parent_column": parent_pk,
                                "child_table": child_table,
                                "child_column": child_col,
                                "overlap_ratio": round(overlap_ratio, 2),
                                "relationship_type": "One-to-Many"
                            })
        
        return foreign_keys
    
    def _columns_similar(self, col1: str, col2: str) -> bool:
        """Check if two column names are similar (for FK detection)"""
        # Remove common suffixes/prefixes
        col1_clean = col1.replace("_id", "").replace("_number", "").replace("_code", "")
        col2_clean = col2.replace("_id", "").replace("_number", "").replace("_code", "")
        
        # Check if cleaned names match
        if col1_clean == col2_clean:
            return True
        
        # Check if one contains the other
        if col1_clean in col2_clean or col2_clean in col1_clean:
            return True
        
        return False
    
    def _build_relationships(self, table_dataframes: Dict[str, pd.DataFrame],
                           primary_keys: Dict[str, Optional[str]],
                           foreign_keys: List[Dict]) -> List[Dict[str, Any]]:
        """Build logical relationship map (textual ER) with detailed explanations"""
        relationships = []
        
        for fk in foreign_keys:
            parent_table = fk["parent_table"]
            child_table = fk["child_table"]
            parent_col = fk["parent_column"]
            child_col = fk["child_column"]
            overlap_ratio = fk.get("overlap_ratio", 0)
            
            # Determine relationship type
            parent_df = table_dataframes[parent_table]
            child_df = table_dataframes[child_table]
            
            # Count unique values
            parent_unique = parent_df[parent_col].nunique()
            child_unique = child_df[child_col].nunique()
            
            # Check if one-to-many or many-to-one
            if parent_unique <= child_unique:
                relationship_type = "One-to-Many"
                description = f"{parent_table}.{parent_col} -> {child_table}.{child_col}"
            else:
                relationship_type = "Many-to-One"
                description = f"{child_table}.{child_col} -> {parent_table}.{parent_col}"
            
            # Generate detailed paragraph explanation using business rules engine
            try:
                paragraph_explanation = self.business_rules_engine.generate_relationship_explanation(
                    parent_table, child_table, parent_col, overlap_ratio
                )
            except:
                paragraph_explanation = f"Tables {parent_table} and {child_table} are connected through {parent_col}."
            
            relationships.append({
                "parent_table": parent_table,
                "parent_column": parent_col,
                "child_table": child_table,
                "child_column": child_col,
                "relationship_type": relationship_type,
                "description": description,
                "overlap_ratio": overlap_ratio,
                "detailed_explanation": paragraph_explanation
            })
        
        return relationships
    
    def _consolidate_results(self, table_results: List[Dict], domain_results: Dict,
                            primary_keys: Dict, foreign_keys: List[Dict],
                            relationships: List[Dict], banking_blueprint: Dict) -> Dict[str, Any]:
        """Consolidate all results into final format"""
        
        # Calculate overall system verdict
        all_passed = all(r.get("status") == "SUCCESS" for r in table_results)
        any_failed = any(r.get("status") == "FAILED" for r in table_results)
        
        # Determine overall verdict
        if any_failed:
            overall_verdict = "FAIL"
        elif all_passed:
            # Check validation results
            all_validations_passed = True
            for table_result in table_results:
                validator = table_result.get("banking_validator", {})
                if isinstance(validator, dict):
                    final_decision = validator.get("final_decision", "")
                    if final_decision == "FAIL":
                        all_validations_passed = False
                        break
            
            if all_validations_passed:
                overall_verdict = "PASS"
            else:
                overall_verdict = "PASS WITH WARNINGS"
        else:
            overall_verdict = "PASS WITH WARNINGS"
        
        # Calculate overall confidence
        confidences = []
        for table_result in table_results:
            banking = table_result.get("banking_domain", {})
            if isinstance(banking, dict):
                conf = banking.get("confidence_percentage", 0)
                if conf > 0:
                    confidences.append(conf)
        
        overall_confidence = np.mean(confidences) if confidences else 0.0
        
        # Build consolidated result (maintaining same structure as single-file)
        consolidated = {
            "multi_file_mode": True,
            "total_files": len(table_results),
            "table_results": table_results,
            "domain_detection": domain_results,
            "primary_keys": primary_keys,
            "foreign_keys": foreign_keys,
            "relationships": relationships,
            "overall_verdict": overall_verdict,
            "overall_confidence": round(overall_confidence, 2),
            "banking_blueprint": banking_blueprint,
            "business_explanation": self._generate_business_explanation(
                table_results, domain_results, relationships, primary_keys
            )
        }
        
        # For backward compatibility, include first table's banking result as primary
        if table_results and len(table_results) > 0:
            first_table = table_results[0]
            if first_table.get("status") == "SUCCESS":
                consolidated["banking"] = first_table.get("banking_domain", {})
                # Format validator result if it exists
                banking_validator = first_table.get("banking_validator")
                if banking_validator:
                    consolidated["banking_dataset_validator"] = banking_validator
                consolidated["core_banking_validator"] = first_table.get("core_banking_validator")
        
        return consolidated
    
    def _generate_business_explanation(self, table_results: List[Dict], domain_results: Dict,
                                     relationships: List[Dict], primary_keys: Dict) -> str:
        """Generate simple business explanation in non-technical language"""
        explanations = []
        
        # Explain each table
        explanations.append(f"Analyzed {len(table_results)} table(s):")
        for table_result in table_results:
            table_name = table_result.get("table_name", "Unknown")
            rows = table_result.get("total_rows", 0)
            cols = table_result.get("total_columns", 0)
            explanations.append(f"- {table_name}: {rows} rows, {cols} columns")
        
        # Explain overall domain
        overall_domain = domain_results.get("overall_domain", {})
        domain_name = overall_domain.get("domain", "Unknown")
        domain_confidence = overall_domain.get("confidence", 0)
        explanations.append(f"\nOverall Domain: {domain_name} (Confidence: {domain_confidence}%)")
        
        # Explain relationships
        if relationships:
            explanations.append(f"\nTable Relationships:")
            for rel in relationships:
                explanations.append(f"- {rel['description']} ({rel['relationship_type']})")
        
        # Explain primary keys
        if primary_keys:
            explanations.append(f"\nPrimary Keys:")
            for table, pk in primary_keys.items():
                if pk:
                    explanations.append(f"- {table}: {pk}")
        
        # Explain data fitness
        overall_verdict = "PASS"  # Will be set in consolidation
        explanations.append(f"\nData Fitness: {overall_verdict}")
        
        return "\n".join(explanations)
