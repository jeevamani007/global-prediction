"""
Column Relationship Analyzer
Analyzes column-to-column relationships between files and explains banking domain connections.
Shows what kind of banking domain each file belongs to and how columns relate across files.
"""

import pandas as pd
from typing import Dict, List, Any, Optional
import os


class ColumnRelationshipAnalyzer:
    """
    Analyzes column relationships between files and identifies banking domains.
    Provides detailed explanations of column connections and their banking purpose.
    """
    
    def __init__(self):
        # Banking domain patterns - what columns indicate which banking domain
        self.banking_domain_patterns = {
            "Customer Management": {
                "keywords": ["customer_id", "customer_name", "cust_name", "name", "phone", "mobile", "email", 
                            "dob", "date_of_birth", "gender", "address", "city", "state", "kyc", "pan", "aadhar"],
                "description": "Contains customer master data including personal information, contact details, and KYC documents.",
                "icon": "👤"
            },
            "Account Management": {
                "keywords": ["account_id", "account_number", "account_type", "account_status", "balance", 
                            "opening_balance", "closing_balance", "ifsc", "branch_id", "branch_code", "opened_date"],
                "description": "Contains account information including account numbers, types, balances, and branch details.",
                "icon": "🏦"
            },
            "Transaction Management": {
                "keywords": ["transaction_id", "txn_id", "transaction_date", "transaction_amount", "amount", 
                            "debit", "credit", "transaction_type", "balance_after", "narration", "description"],
                "description": "Contains transaction records including payments, transfers, deposits, and withdrawals.",
                "icon": "💸"
            },
            "Loan Management": {
                "keywords": ["loan_id", "loan_number", "loan_amount", "loan_type", "interest_rate", "tenure", 
                            "emi", "loan_status", "disbursement_date", "due_date", "credit_score", "eligibility"],
                "description": "Contains loan account information including loan details, payments, and credit assessments.",
                "icon": "💰"
            },
            "Card Management": {
                "keywords": ["card_id", "card_number", "card_type", "expiry_date", "cvv", "card_status", 
                            "limit_amount", "available_limit", "card_holder_name"],
                "description": "Contains payment card information including debit cards, credit cards, and card transactions.",
                "icon": "💳"
            },
            "Branch Management": {
                "keywords": ["branch_id", "branch_code", "branch_name", "branch_address", "ifsc_code", 
                            "branch_manager", "branch_location"],
                "description": "Contains branch master data including branch codes, addresses, and IFSC codes.",
                "icon": "🏢"
            },
            "Fraud Detection": {
                "keywords": ["fraud_flag", "suspicious", "risk_score", "device_type", "location", "ip_address", 
                            "transaction_time", "unusual_pattern"],
                "description": "Contains fraud detection and risk assessment data for transaction security.",
                "icon": "🔒"
            }
        }
        
        # Column relationship types and explanations
        self.column_relationship_types = {
            "Customer ID Link": {
                "pattern": ["customer_id", "cust_id", "client_id"],
                "explanation": "Customer ID links multiple banking entities together. When two files share customer_id, they represent different aspects of the same customer's banking relationship. For example, a customer can have accounts, loans, and cards - all linked through customer_id.",
                "business_value": "Enables comprehensive customer view across all banking products and services."
            },
            "Account Number Link": {
                "pattern": ["account_number", "account_id", "acc_id"],
                "explanation": "Account number connects transactions, balances, and account details. Files sharing account_number show different aspects of the same account - transactions, statements, and account master data.",
                "business_value": "Maintains complete account history and enables accurate balance calculations."
            },
            "Transaction Reference": {
                "pattern": ["transaction_id", "txn_id", "reference_number"],
                "explanation": "Transaction IDs link transaction details across multiple files. This enables transaction tracking, reconciliation, and audit trails.",
                "business_value": "Ensures transaction integrity and enables dispute resolution."
            },
            "Loan Account Link": {
                "pattern": ["loan_id", "loan_number", "loan_account"],
                "explanation": "Loan IDs connect loan master data with loan transactions, payments, and schedules. This enables complete loan lifecycle management.",
                "business_value": "Tracks loan performance, payment history, and enables loan servicing."
            },
            "Branch Code Link": {
                "pattern": ["branch_id", "branch_code", "ifsc"],
                "explanation": "Branch codes link accounts and transactions to specific bank branches. This enables branch-wise reporting and regional analysis.",
                "business_value": "Enables branch performance tracking and regulatory branch-level reporting."
            }
        }
    
    def analyze_file_banking_domain(self, df: pd.DataFrame, file_name: str) -> Dict[str, Any]:
        """
        Analyze a file to determine its banking domain based on column patterns.
        Returns domain information and matched columns.
        """
        columns = list(df.columns)
        column_lower = [str(col).lower() for col in columns]
        
        domain_scores = {}
        matched_columns_by_domain = {}
        
        # Score each domain based on keyword matches
        for domain_name, domain_info in self.banking_domain_patterns.items():
            score = 0
            matched_cols = []
            
            for keyword in domain_info["keywords"]:
                for idx, col in enumerate(column_lower):
                    if keyword in col:
                        score += 1
                        if columns[idx] not in matched_cols:
                            matched_cols.append(columns[idx])
            
            if score > 0:
                domain_scores[domain_name] = score
                matched_columns_by_domain[domain_name] = matched_cols
        
        # Determine primary domain (highest score)
        primary_domain = max(domain_scores.items(), key=lambda x: x[1])[0] if domain_scores else "General Banking"
        primary_domain_info = self.banking_domain_patterns.get(primary_domain, {
            "description": "General banking data file",
            "icon": "📊"
        })
        
        return {
            "file_name": os.path.basename(file_name),
            "primary_domain": primary_domain,
            "domain_icon": primary_domain_info.get("icon", "📊"),
            "domain_description": primary_domain_info.get("description", "Banking data file"),
            "domain_confidence": min(100, (domain_scores.get(primary_domain, 0) / len(columns)) * 100) if columns else 0,
            "all_domains": list(domain_scores.keys()),
            "domain_scores": domain_scores,
            "matched_columns": matched_columns_by_domain.get(primary_domain, []),
            "all_columns": columns,
            "total_columns": len(columns),
            "total_rows": len(df)
        }
    
    def analyze_column_relationships(self, file_dataframes: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Analyze column-to-column relationships across multiple files.
        Returns detailed relationship information with explanations.
        """
        if len(file_dataframes) < 2:
            return {
                "file_domains": [],
                "column_relationships": [],
                "summary": "Upload at least 2 files to see column relationships."
            }
        
        # Step 1: Analyze each file's banking domain
        file_domains = []
        for file_name, df in file_dataframes.items():
            domain_info = self.analyze_file_banking_domain(df, file_name)
            file_domains.append(domain_info)
        
        # Step 2: Find column relationships between files
        column_relationships = []
        file_names = list(file_dataframes.keys())
        
        for i, file1_name in enumerate(file_names):
            df1 = file_dataframes[file1_name]
            domain1 = file_domains[i]
            
            for j, file2_name in enumerate(file_names):
                if i >= j:
                    continue
                
                df2 = file_dataframes[file2_name]
                domain2 = file_domains[j]
                
                # Find common or related columns
                relationships = self._find_column_relationships(
                    file1_name, df1, domain1,
                    file2_name, df2, domain2
                )
                
                column_relationships.extend(relationships)
        
        # Generate summary
        summary = self._generate_summary(file_domains, column_relationships)
        
        return {
            "file_domains": file_domains,
            "column_relationships": column_relationships,
            "summary": summary
        }
    
    def _find_column_relationships(
        self, 
        file1_name: str, df1: pd.DataFrame, domain1: Dict,
        file2_name: str, df2: pd.DataFrame, domain2: Dict
    ) -> List[Dict[str, Any]]:
        """Find relationships between columns in two files."""
        relationships = []
        
        # Normalize column names for matching
        df1_cols = {str(col).lower(): col for col in df1.columns}
        df2_cols = {str(col).lower(): col for col in df2.columns}
        
        # Find exact matches
        common_cols = set(df1_cols.keys()) & set(df2_cols.keys())
        
        for col_lower in common_cols:
            col1 = df1_cols[col_lower]
            col2 = df2_cols[col_lower]
            
            # Calculate value overlap
            overlap_info = self._calculate_overlap(df1[col1], df2[col2])
            
            if overlap_info["overlap_ratio"] > 0.2:  # At least 20% overlap
                # Determine relationship type
                rel_type = self._identify_relationship_type(col_lower)
                
                relationship = {
                    "file1": os.path.basename(file1_name),
                    "file1_domain": domain1["primary_domain"],
                    "file1_column": col1,
                    "file2": os.path.basename(file2_name),
                    "file2_domain": domain2["primary_domain"],
                    "file2_column": col2,
                    "relationship_type": rel_type["name"],
                    "explanation": rel_type["explanation"],
                    "business_value": rel_type["business_value"],
                    "overlap_info": overlap_info,
                    "connection_strength": "Strong" if overlap_info["overlap_ratio"] > 0.7 else 
                                          "Moderate" if overlap_info["overlap_ratio"] > 0.4 else "Weak"
                }
                
                relationships.append(relationship)
        
        # Find similar column names (fuzzy matching)
        for col1_lower, col1 in df1_cols.items():
            for col2_lower, col2 in df2_cols.items():
                if col1_lower == col2_lower:
                    continue  # Already checked
                
                # Check if columns are similar (e.g., "customer_id" and "cust_id")
                if self._columns_are_similar(col1_lower, col2_lower):
                    overlap_info = self._calculate_overlap(df1[col1], df2[col2])
                    
                    if overlap_info["overlap_ratio"] > 0.3:
                        rel_type = self._identify_relationship_type(col1_lower)
                        
                        relationship = {
                            "file1": os.path.basename(file1_name),
                            "file1_domain": domain1["primary_domain"],
                            "file1_column": col1,
                            "file2": os.path.basename(file2_name),
                            "file2_domain": domain2["primary_domain"],
                            "file2_column": col2,
                            "relationship_type": rel_type["name"],
                            "explanation": rel_type["explanation"],
                            "business_value": rel_type["business_value"],
                            "overlap_info": overlap_info,
                            "connection_strength": "Strong" if overlap_info["overlap_ratio"] > 0.7 else 
                                                  "Moderate" if overlap_info["overlap_ratio"] > 0.4 else "Weak"
                        }
                        
                        relationships.append(relationship)
        
        return relationships
    
    def _identify_relationship_type(self, column_name: str) -> Dict[str, str]:
        """Identify what type of relationship a column represents."""
        col_lower = column_name.lower()
        
        for rel_name, rel_info in self.column_relationship_types.items():
            for pattern in rel_info["pattern"]:
                if pattern in col_lower:
                    return {
                        "name": rel_name,
                        "explanation": rel_info["explanation"],
                        "business_value": rel_info["business_value"]
                    }
        
        # Default
        return {
            "name": "Data Link",
            "explanation": f"This column ({column_name}) links data between the two files, enabling cross-file analysis and maintaining referential integrity.",
            "business_value": "Enables data integration and comprehensive banking analytics."
        }
    
    def _columns_are_similar(self, col1: str, col2: str) -> bool:
        """Check if two column names are similar (e.g., customer_id and cust_id)."""
        # Remove common suffixes/prefixes
        clean1 = col1.replace("_id", "").replace("_number", "").replace("_code", "").replace("_", "")
        clean2 = col2.replace("_id", "").replace("_number", "").replace("_code", "").replace("_", "")
        
        # Check if one contains the other
        if clean1 in clean2 or clean2 in clean1:
            return True
        
        # Check for common prefixes
        prefixes = ["customer", "cust", "account", "acc", "transaction", "txn", "loan", "branch", "card"]
        for prefix in prefixes:
            if prefix in clean1 and prefix in clean2:
                return True
        
        return False
    
    def _calculate_overlap(self, series1: pd.Series, series2: pd.Series) -> Dict[str, Any]:
        """Calculate overlap between two series."""
        set1 = set(series1.dropna().astype(str))
        set2 = set(series2.dropna().astype(str))
        
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        overlap_ratio = intersection / union if union > 0 else 0
        
        return {
            "overlap_count": intersection,
            "total_unique_file1": len(set1),
            "total_unique_file2": len(set2),
            "overlap_ratio": round(overlap_ratio, 3),
            "overlap_percentage": round(overlap_ratio * 100, 1)
        }
    
    def _generate_summary(self, file_domains: List[Dict], column_relationships: List[Dict]) -> str:
        """Generate a summary paragraph of the analysis."""
        if not file_domains:
            return "No files analyzed."
        
        summary_parts = []
        summary_parts.append(f"Analyzed {len(file_domains)} file(s) and found {len(column_relationships)} column relationship(s).")
        
        # List file domains
        summary_parts.append("\n\nFile Banking Domains:")
        for domain_info in file_domains:
            summary_parts.append(
                f"• {domain_info['file_name']}: {domain_info['domain_icon']} {domain_info['primary_domain']} "
                f"({domain_info['total_columns']} columns, {domain_info['total_rows']} rows)"
            )
        
        # Summarize relationships
        if column_relationships:
            summary_parts.append("\n\nColumn Relationships Found:")
            for rel in column_relationships[:5]:  # Show first 5
                summary_parts.append(
                    f"• {rel['file1']}.{rel['file1_column']} ↔ {rel['file2']}.{rel['file2_column']} "
                    f"({rel['connection_strength']} connection, {rel['overlap_info']['overlap_percentage']}% match)"
                )
            if len(column_relationships) > 5:
                summary_parts.append(f"  ... and {len(column_relationships) - 5} more relationships")
        
        return "\n".join(summary_parts)
