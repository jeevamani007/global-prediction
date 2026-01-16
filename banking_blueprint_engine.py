"""
Banking Blueprint Engine - Complete 8-Step Analysis Flow

Implements the full banking domain analysis blueprint:
1. User Upload Files (CSV/Excel/SQL)
2. Column Scan (Banking Keyword Detection)
3. Banking Domain Confirmation
4. Application Type Prediction
5. Business Rules Application
6. Multi-File Relationship Logic
7. Data Flow Analysis
8. Final Structured Output
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
import re
from collections import defaultdict
from enhanced_business_rules_engine import EnhancedBusinessRulesEngine


class BankingBlueprintEngine:
    """
    Complete Banking Domain Analysis Engine based on Blueprint Logic
    """
    
    def __init__(self):
        # Initialize Enhanced Business Rules Engine
        self.enhanced_rules_engine = EnhancedBusinessRulesEngine()
        
        # Step 2: Banking Keyword Signals
        self.banking_signals = {
            "customer_id": {"meaning": "Customer master", "weight": 3},
            "account_number": {"meaning": "Bank account", "weight": 5},
            "account_no": {"meaning": "Bank account", "weight": 5},
            "balance": {"meaning": "Core banking", "weight": 5},
            "opening_balance": {"meaning": "Core banking", "weight": 4},
            "closing_balance": {"meaning": "Core banking", "weight": 4},
            "transaction_id": {"meaning": "Transactions", "weight": 4},
            "txn_id": {"meaning": "Transactions", "weight": 4},
            "debit": {"meaning": "Money movement", "weight": 4},
            "credit": {"meaning": "Money movement", "weight": 4},
            "amount": {"meaning": "Money movement", "weight": 3},
            "loan_id": {"meaning": "Loans", "weight": 4},
            "interest": {"meaning": "Lending", "weight": 3},
            "emi": {"meaning": "Lending", "weight": 3},
            "ifsc": {"meaning": "Bank identity", "weight": 4},
            "branch": {"meaning": "Bank identity", "weight": 3},
            "branch_code": {"meaning": "Bank identity", "weight": 3},
        }
        
        # Step 4: Application Type Patterns
        self.application_patterns = {
            "Core Banking System": {
                "required": ["account_number", "balance"],
                "optional": ["customer_id", "opening_balance", "account_status", "branch_code"],
                "priority": 1,
                "description": "Central system for managing accounts, balances, and customer master data."
            },
            "Transaction Processing System": {
                "required": ["transaction_id", "amount", "account_number"],
                "optional": ["date", "time", "transaction_type", "debit", "credit", "terminal_id"],
                "priority": 2,
                "description": "System focused on daily financial movements and audit trails."
            },
            "Loan Management System": {
                "required": ["loan_id", "emi", "loan_amount"],
                "optional": ["interest_rate", "tenure", "disbursement_date", "loan_status", "collateral"],
                "priority": 3,
                "description": "Lending platform for processing loans, EMI schedules, and interest calculations."
            },
            "Card Management System": {
                "required": ["card_number", "customer_id"],
                "optional": ["expiry_date", "cvv", "card_type", "card_limit", "card_status"],
                "priority": 4,
                "description": "Platform for managing credit/debit cards, limits, and cardholder information."
            },
            "Payment Gateway": {
                "required": ["payment_id", "amount", "merchant_id"],
                "optional": ["payment_method", "transaction_status", "gateway_response", "currency"],
                "priority": 5,
                "description": "Interface for processing online payments between customers and merchants."
            },
            "Mortgage System": {
                "required": ["mortgage_id", "property_value", "loan_id"],
                "optional": ["appraisal_date", "ltv_ratio", "escrow_balance", "lien_status"],
                "priority": 6,
                "description": "Specialized lending system for real-estate backed loans and property tracking."
            },
            "Wealth Management": {
                "required": ["portfolio_id", "asset_class", "valuation"],
                "optional": ["investment_type", "dividend_yield", "brokerage_fee", "risk_profile"],
                "priority": 7,
                "description": "System for tracking investments, portfolios, and high-net-worth individual assets."
            }
        }
        
        # Step 5: Business Rules per Column Type
        self.business_rules = {
            "balance": [
                "Must be numeric (decimal/integer)",
                "Cannot be NULL for active accounts",
                "Debit amount must not exceed balance (no overdraft unless allowed)",
                "Negative balance allowed only if overdraft facility exists",
                "Account status must be 'active' for transactions"
            ],
            "account_number": [
                "Must be unique across all accounts",
                "Fixed length (typically 12-16 digits)",
                "Cannot be changed after account creation",
                "Must be linked to exactly one customer_id",
                "Must follow bank's numbering pattern"
            ],
            "transaction_amount": [
                "Must be greater than 0",
                "Debit transactions cannot exceed account balance",
                "Credit transactions are always allowed",
                "Must be stored with timestamp for audit",
                "Requires transaction type (debit/credit) specification"
            ],
            "loan_amount": [
                "Must be approved before disbursement",
                "Interest rate must be applied",
                "EMI must be calculated based on tenure and interest",
                "Loan status controls EMI deductions",
                "Cannot exceed customer's credit limit"
            ],
            "customer_id": [
                "Must be unique per customer",
                "Cannot be NULL",
                "Primary key in customer master table",
                "Links to accounts, loans, and transactions",
                "Immutable after creation"
            ],
            "transaction_id": [
                "Must be unique per transaction",
                "Auto-generated sequential or UUID",
                "Cannot be modified after creation",
                "Links transaction to account_number",
                "Must have timestamp for chronological order"
            ],
            "opening_balance": [
                "Initial balance when account is created",
                "Must be >= minimum balance requirement",
                "Cannot be negative (exceptions for special accounts)",
                "Set only once during account creation",
                "Current balance is calculated from opening_balance + transactions"
            ],
            "emi": [
                "Equal Monthly Installment for loans",
                "Calculated: [P × r × (1+r)^n] / [(1+r)^n-1]",
                "Must be deducted on specific date each month",
                "Cannot be zero or negative",
                "Failure to pay EMI affects loan status"
            ]
        }
        
        # Step 6: Multi-File Relationship Patterns
        self.relationship_patterns = {
            "customer_to_account": {
                "parent": "customer",
                "child": "account",
                "key": "customer_id",
                "relationship": "One customer can have multiple accounts (1:N)"
            },
            "account_to_transaction": {
                "parent": "account",
                "child": "transaction",
                "key": "account_number",
                "relationship": "One account can have multiple transactions (1:N)"
            },
            "customer_to_loan": {
                "parent": "customer",
                "child": "loan",
                "key": "customer_id",
                "relationship": "One customer can have multiple loans (1:N)"
            },
            "account_to_loan": {
                "parent": "account",
                "child": "loan",
                "key": "account_number",
                "relationship": "Loans are disbursed to specific accounts (N:1)"
            }
        }
    
    # ========== STEP 1: File Upload (handled by FastAPI) ==========
    
    # ========== STEP 2: Column Scan ==========
    def extract_column_names(self, df: pd.DataFrame) -> List[str]:
        """Extract and normalize column names"""
        return [col.lower().strip().replace(" ", "_") for col in df.columns]
    
    def scan_banking_signals(self, columns: List[str]) -> Dict[str, Any]:
        """
        Scan columns for banking keyword signals
        Returns detected signals with their meanings and weights
        """
        detected_signals = {}
        total_weight = 0
        
        for col in columns:
            col_normalized = col.lower().strip()
            
            # Exact match
            if col_normalized in self.banking_signals:
                detected_signals[col] = self.banking_signals[col_normalized]
                total_weight += self.banking_signals[col_normalized]["weight"]
            else:
                # Partial match (fuzzy matching)
                for signal, info in self.banking_signals.items():
                    if signal in col_normalized or col_normalized in signal:
                        detected_signals[col] = info
                        total_weight += info["weight"]
                        break
        
        return {
            "detected_signals": detected_signals,
            "signal_count": len(detected_signals),
            "total_weight": total_weight,
            "columns_scanned": len(columns)
        }
    
    # ========== STEP 3: Banking Domain Confirmation ==========
    def confirm_banking_domain(self, signal_scan: Dict[str, Any]) -> Dict[str, Any]:
        """
        Confirm if data belongs to banking domain
        Threshold: 3+ strong banking columns OR weight >= 12
        """
        signal_count = signal_scan["signal_count"]
        total_weight = signal_scan["total_weight"]
        
        # Threshold logic
        threshold_count = 3
        threshold_weight = 12
        
        if signal_count >= threshold_count or total_weight >= threshold_weight:
            domain = "Banking"
            confidence = "High"
            
            # Calculate confidence percentage
            if total_weight >= 20:
                confidence_percentage = 95
            elif total_weight >= 15:
                confidence_percentage = 90
            elif total_weight >= 12:
                confidence_percentage = 85
            else:
                confidence_percentage = 80
        elif signal_count >= 2 or total_weight >= 8:
            domain = "Banking"
            confidence = "Medium"
            confidence_percentage = 70
        else:
            domain = "Unknown"
            confidence = "Low"
            confidence_percentage = 40
        
        return {
            "domain": domain,
            "confidence": confidence,
            "confidence_percentage": confidence_percentage,
            "signal_count": signal_count,
            "total_weight": total_weight,
            "threshold_met": signal_count >= threshold_count or total_weight >= threshold_weight
        }
    
    # ========== STEP 4: Application Type Prediction ==========
    def predict_application_type(self, columns: List[str], df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """
        Predict banking application type based on column patterns.
        Nuanced scoring considering required and optional columns.
        """
        columns_normalized = [c.lower().strip() for c in columns]
        detected_apps = []
        
        for app_name, pattern in self.application_patterns.items():
            required_cols = pattern["required"]
            optional_cols = pattern["optional"]
            
            # Count matches for required columns
            matched_required = []
            for req in required_cols:
                if any(req in col for col in columns_normalized):
                    matched_required.append(req)
            
            # Application is a candidate if most required columns are present (relaxed for multi-file)
            # For strictness, if this is single file, we might want 100% required.
            # But in multi-file, columns might be spread.
            required_match_ratio = len(matched_required) / len(required_cols)
            
            if required_match_ratio >= 0.6:  # At least 60% of required columns found
                # Count optional matches
                matched_optional = []
                for opt in optional_cols:
                    if any(opt in col for col in columns_normalized):
                        matched_optional.append(opt)
                
                optional_match_ratio = len(matched_optional) / len(optional_cols) if optional_cols else 0
                
                # Dynamic scoring
                # 70% weight to required, 30% to optional
                score = (required_match_ratio * 70) + (optional_match_ratio * 30)
                
                detected_apps.append({
                    "application_type": app_name,
                    "score": round(score, 2),
                    "priority": pattern["priority"],
                    "description": pattern.get("description", ""),
                    "matched_required": matched_required,
                    "missing_required": list(set(required_cols) - set(matched_required)),
                    "matched_optional": matched_optional
                })
        
        # Sort by score (desc) and priority (asc)
        detected_apps.sort(key=lambda x: (-x["score"], x["priority"]))
        
        if detected_apps:
            primary_app = detected_apps[0]
            # Higher threshold for confidence display
            display_confidence = min(99, primary_app["score"] + 10) if primary_app["score"] > 80 else primary_app["score"]
            
            return {
                "application_type": primary_app["application_type"],
                "description": primary_app["description"],
                "confidence": round(display_confidence, 2),
                "modules": [app["application_type"] for app in detected_apps[:3]], # Top 3 modules
                "is_multi_module": len(detected_apps) > 1,
                "all_detected": detected_apps,
                "recommended_actions": [
                    f"Validate {primary_app['application_type']} business rules",
                    "Check for missing mandatory fields: " + ", ".join(primary_app["missing_required"]) if primary_app["missing_required"] else "All mandatory fields present"
                ]
            }
        else:
            return {
                "application_type": "Generic Banking Application",
                "description": "Analysis indicates a general banking dataset without specific modular mapping.",
                "confidence": 50,
                "modules": ["Data Management"],
                "is_multi_module": False,
                "all_detected": [],
                "recommended_actions": ["Review column mappings", "Upload more context-rich files"]
            }
    
    # ========== STEP 5: Business Rules Application ==========
    def apply_business_rules(self, columns: List[str], df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """
        Apply business rules to each column based on its type
        """
        column_rules = {}
        
        for col in columns:
            # Use Enhanced Business Rules Engine for richer descriptions
            data_series = df[col] if df is not None and col in df.columns else pd.Series()
            enhanced_rule = self.enhanced_rules_engine.explain_column_business_meaning(col, data_series)
            
            column_rules[col] = {
                "column_name": col,
                "title": enhanced_rule.get("title", f"{col} Rule"),
                "explanation": enhanced_rule.get("detailed_explanation", "No specific rules found."),
                "workflow": enhanced_rule.get("business_workflow", ""),
                "icon": enhanced_rule.get("icon", "📊"),
                "simple_rules": enhanced_rule.get("simple_rules", []),
                "data_rules": enhanced_rule.get("data_rules", [])
            }
        
        return {
            "column_wise_rules": column_rules,
            "total_rules_applied": len(column_rules)
        }
    
    # ========== STEP 6: Multi-File Relationship Logic ==========
    def detect_multi_file_relationships(
        self, 
        file_dataframes: Dict[str, pd.DataFrame]
    ) -> Dict[str, Any]:
        """
        Detect relationships across multiple files
        Customer → Account → Transaction → Loan
        """
        if len(file_dataframes) <= 1:
            return {
                "multi_file_mode": False,
                "relationships": [],
                "architecture": "Single Table Analysis"
            }
        
        relationships = []
        
        # Detect table types
        table_types = {}
        for table_name, df in file_dataframes.items():
            table_types[table_name] = self._detect_table_type(df)
        
        # Build relationships
        for parent_table, parent_df in file_dataframes.items():
            parent_cols = [c.lower() for c in parent_df.columns]
            
            for child_table, child_df in file_dataframes.items():
                if parent_table == child_table:
                    continue
                
                child_cols = [c.lower() for c in child_df.columns]
                
                # Find common columns (potential FK)
                common_cols = set(parent_cols).intersection(set(child_cols))
                
                for common_col in common_cols:
                    if "id" in common_col or "number" in common_col or "code" in common_col:
                        relationships.append({
                            "parent_table": parent_table,
                            "child_table": child_table,
                            "linking_column": common_col,
                            "relationship_type": "1:N",
                            "explanation": f"{parent_table} is linked to {child_table} via {common_col}"
                        })
        
        # Determine architecture
        architecture = self._determine_architecture(table_types, relationships)
        
        return {
            "multi_file_mode": True,
            "relationships": relationships,
            "table_types": table_types,
            "architecture": architecture,
            "relationship_count": len(relationships)
        }
    
    def _detect_table_type(self, df: pd.DataFrame) -> str:
        """Detect what type of banking table this is"""
        cols = [c.lower() for c in df.columns]
        
        if any("customer" in c for c in cols):
            return "Customer Master"
        elif any("account" in c and "balance" in c for c in cols):
            return "Account Master"
        elif any("transaction" in c or "txn" in c for c in cols):
            return "Transaction Table"
        elif any("loan" in c for c in cols):
            return "Loan Table"
        elif any("card" in c for c in cols):
            return "Card Table"
        else:
            return "Generic Banking Table"
    
    def _determine_architecture(self, table_types: Dict, relationships: List) -> str:
        """Determine overall banking architecture"""
        if "Customer Master" in table_types.values() and "Account Master" in table_types.values():
            return "Core Banking Architecture"
        elif "Loan Table" in table_types.values():
            return "Loan Management Architecture"
        elif "Transaction Table" in table_types.values():
            return "Transaction Processing Architecture"
        else:
            return "Banking Data Architecture"
    
    # ========== STEP 7: Data Flow Analysis ==========
    def analyze_data_flow(
        self, 
        file_dataframes: Dict[str, pd.DataFrame],
        relationships: List[Dict]
    ) -> Dict[str, Any]:
        """
        Analyze data flow: Customer → Account → Transaction → Balance Update
        """
        flow_steps = []
        
        # Detect flow based on table types
        table_names = list(file_dataframes.keys())
        
        has_customer = any("customer" in t.lower() for t in table_names)
        has_account = any("account" in t.lower() for t in table_names)
        has_transaction = any("transaction" in t.lower() or "txn" in t.lower() for t in table_names)
        has_loan = any("loan" in t.lower() for t in table_names)
        
        if has_customer:
            flow_steps.append({
                "step": 1,
                "action": "Customer Creation",
                "description": "Customer record is created with unique customer_id"
            })
        
        if has_account:
            flow_steps.append({
                "step": 2,
                "action": "Account Opening",
                "description": "Account is opened for customer with account_number and opening_balance"
            })
        
        if has_transaction:
            flow_steps.append({
                "step": 3,
                "action": "Transaction Processing",
                "description": "Transactions (debit/credit) are processed against account"
            })
            flow_steps.append({
                "step": 4,
                "action": "Balance Update",
                "description": "Account balance is updated: Balance = Opening Balance + Credits - Debits"
            })
        
        if has_loan:
            flow_steps.append({
                "step": 5,
                "action": "Loan EMI Deduction",
                "description": "EMI is deducted from customer account on scheduled date"
            })
        
        # Generate flow diagram
        flow_diagram = self._generate_flow_diagram(flow_steps)
        
        return {
            "data_flow": flow_steps,
            "flow_diagram": flow_diagram,
            "flow_description": self._generate_flow_description(flow_steps)
        }
    
    def _generate_flow_diagram(self, flow_steps: List[Dict]) -> str:
        """Generate text-based flow diagram"""
        if not flow_steps:
            return "No data flow detected"
        
        diagram = "\n"
        for step in flow_steps:
            diagram += f"  [{step['step']}] {step['action']}\n"
            diagram += f"      ↓\n"
        diagram += "  [END] Final State\n"
        
        return diagram
    
    def _generate_flow_description(self, flow_steps: List[Dict]) -> str:
        """Generate human-readable flow description"""
        if not flow_steps:
            return "No data flow detected"
        
        descriptions = [step["description"] for step in flow_steps]
        return " → ".join(descriptions)
    
    # ========== STEP 8: Final Output Generation ==========
    def generate_blueprint_output(
        self,
        domain_confirmation: Dict,
        app_prediction: Dict,
        business_rules: Dict,
        relationships: Dict,
        data_flow: Dict
    ) -> Dict[str, Any]:
        """
        Generate final structured output matching blueprint format
        """
        return {
            "domain": domain_confirmation["domain"],
            "domain_type": f"{domain_confirmation['domain']} ({relationships.get('architecture', 'General')})",
            "confidence_percentage": domain_confirmation["confidence_percentage"],
            
            "application": app_prediction["application_type"],
            "application_confidence": app_prediction["confidence"],
            "app_prediction": app_prediction,
            "app_description": app_prediction.get("description", ""),
            
            "modules": app_prediction["modules"],
            "is_multi_module": app_prediction["is_multi_module"],
            
            "business_rules": business_rules,
            "total_rules_applied": business_rules.get("total_rules_applied", 0),
            
            "multi_file_analysis": {
                "enabled": relationships.get("multi_file_mode", False),
                "relationships": relationships.get("relationships", []),
                "architecture": relationships.get("architecture", "Single Table"),
                "relationship_count": relationships.get("relationship_count", 0)
            },
            
            "data_flow": {
                "flow_steps": data_flow.get("data_flow", []),
                "flow_diagram": data_flow.get("flow_diagram", ""),
                "flow_description": data_flow.get("flow_description", "")
            },
            
            "summary": {
                "domain": domain_confirmation["domain"],
                "application_type": app_prediction["application_type"],
                "modules_detected": len(app_prediction["modules"]),
                "rules_applied": business_rules["total_rules_applied"],
                "confidence": f"{domain_confirmation['confidence_percentage']}%"
            }
        }
    
    # ========== MAIN ANALYSIS METHOD ==========
    def analyze_file(
        self, 
        file_path: str, 
        df: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Main entry point - analyze single file
        """
        try:
            # Step 2: Column Scan
            columns = self.extract_column_names(df)
            signal_scan = self.scan_banking_signals(columns)
            
            # Step 3: Domain Confirmation
            domain_confirmation = self.confirm_banking_domain(signal_scan)
            
            # Step 4: Application Type Prediction
            app_prediction = self.predict_application_type(columns, df)
            
            # Step 5: Business Rules Application
            business_rules = self.apply_business_rules(columns, df)
            
            # Single file - no relationships
            relationships = {
                "multi_file_mode": False,
                "relationships": [],
                "architecture": "Single Table Analysis"
            }
            
            # Single file - simple data flow
            data_flow = {
                "data_flow": [],
                "flow_diagram": "Single table - no complex flow",
                "flow_description": "Single table analysis - use multi-file upload for flow analysis"
            }
            
            # Step 8: Generate Final Output
            final_output = self.generate_blueprint_output(
                domain_confirmation,
                app_prediction,
                business_rules,
                relationships,
                data_flow
            )
            
            return final_output
            
        except Exception as e:
            return {
                "error": f"Blueprint analysis failed: {str(e)}",
                "domain": "Unknown",
                "confidence_percentage": 0
            }
    
    def analyze_multiple_files(
        self,
        file_dataframes: Dict[str, pd.DataFrame]
    ) -> Dict[str, Any]:
        """
        Main entry point - analyze multiple files and provide a unified banking blueprint.
        """
        try:
            # Aggregate all columns from all files
            all_columns_map = {} # column_name -> [file_list]
            all_columns_flat = []
            
            for file_name, df in file_dataframes.items():
                cols = self.extract_column_names(df)
                all_columns_flat.extend(cols)
                for col in cols:
                    if col not in all_columns_map:
                        all_columns_map[col] = []
                    all_columns_map[col].append(file_name)
            
            # Unique columns for scan
            unique_columns = list(set(all_columns_flat))
            
            # Step 2: Column Scan (across all files)
            signal_scan = self.scan_banking_signals(unique_columns)
            
            # Step 3: Domain Confirmation
            domain_confirmation = self.confirm_banking_domain(signal_scan)
            
            # Step 4: Application Type Prediction (Unified across all columns)
            # We don't pass a single DF here as it's a multi-file context
            app_prediction = self.predict_application_type(unique_columns)
            
            # Step 5: Business Rules Application (Detailed for each file)
            file_rules = {}
            for file_name, df in file_dataframes.items():
                cols = self.extract_column_names(df)
                file_rules[file_name] = {
                    "rules": self.apply_business_rules(cols, df)["column_wise_rules"],
                    "table_purpose": self.enhanced_rules_engine._detect_table_purpose(df, file_name)
                }
            
            business_rules = {
                "files": file_rules,
                "column_wise_rules": self.apply_business_rules(unique_columns, None)["column_wise_rules"], # Legacy/Consolidated
                "total_rules_applied": sum(len(f["rules"]) for f in file_rules.values())
            }
            
            # Step 6: Multi-File Relationships
            relationships = self.detect_multi_file_relationships(file_dataframes)
            
            # Step 7: Data Flow Analysis
            data_flow = self.analyze_data_flow(
                file_dataframes,
                relationships.get("relationships", [])
            )
            
            # Step 8: Generate Final Output
            final_output = self.generate_blueprint_output(
                domain_confirmation,
                app_prediction,
                business_rules,
                relationships,
                data_flow
            )
            
            # Add multi-file specific metadata
            final_output["multi_file_metadata"] = {
                "file_count": len(file_dataframes),
                "total_unique_columns": len(unique_columns),
                "column_distribution": all_columns_map
            }
            
            return final_output
            
        except Exception as e:
            return {
                "error": f"Multi-file blueprint analysis failed: {str(e)}",
                "domain": "Unknown",
                "confidence_percentage": 0
            }
