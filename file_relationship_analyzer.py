"""
File Relationship Analyzer
Analyzes relationships between uploaded files based on predefined Primary Key (PK) and Foreign Key (FK) patterns.
Provides detailed explanations of why files are connected and their business purpose.
"""

import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
import os


class FileRelationshipAnalyzer:
    """
    Analyzes file-to-file relationships using predefined banking PK/FK patterns.
    Matches uploaded columns to predefined concepts and explains connections.
    """
    
    def __init__(self):
        # Define Primary Keys (PK) patterns - these identify unique entities
        self.primary_key_patterns = {
            "customer_id": {
                "patterns": ["customer_id", "cust_id", "client_id", "c_id", "customer_number"],
                "entity": "Customer",
                "description": "Unique identifier for each customer in the system"
            },
            "account_id": {
                "patterns": ["account_id", "account_number", "acc_id", "account_no", "acc_number"],
                "entity": "Account",
                "description": "Unique identifier for each bank account"
            },
            "transaction_id": {
                "patterns": ["transaction_id", "txn_id", "transaction_number", "txn_number", "transaction_ref"],
                "entity": "Transaction",
                "description": "Unique identifier for each financial transaction"
            },
            "loan_id": {
                "patterns": ["loan_id", "loan_number", "loan_ref", "loan_account"],
                "entity": "Loan",
                "description": "Unique identifier for each loan account"
            },
            "card_id": {
                "patterns": ["card_id", "card_number", "card_no", "card_ref"],
                "entity": "Card",
                "description": "Unique identifier for each payment card"
            },
            "branch_id": {
                "patterns": ["branch_id", "branch_code", "branch_number", "branch_code"],
                "entity": "Branch",
                "description": "Unique identifier for each bank branch"
            }
        }
        
        # Define Foreign Key (FK) Relationships - these show how entities connect
        self.foreign_key_relationships = {
            "Account → Customer": {
                "foreign_key_patterns": ["customer_id", "cust_id", "client_id"],
                "primary_key_entity": "Customer",
                "child_entity": "Account",
                "purpose": "Links each account to its owner (customer). This enables the bank to track which customer owns which accounts, essential for account management, statements, and customer service.",
                "business_impact": "Without this connection, the bank cannot identify account owners, process transactions correctly, or generate customer statements. Critical for regulatory compliance and fraud prevention.",
                "connection_explanation": "The Account table contains a customer_id column that references the Customer table's primary key. This creates a one-to-many relationship where one customer can have multiple accounts (savings, checking, fixed deposits, etc.)."
            },
            "Account → Branch": {
                "foreign_key_patterns": ["branch_id", "branch_code"],
                "primary_key_entity": "Branch",
                "child_entity": "Account",
                "purpose": "Associates each account with the branch where it was opened. This is required for branch-wise reporting, managing account operations, and regulatory compliance.",
                "business_impact": "Essential for branch performance analysis, transaction routing, and meeting regulatory requirements for branch-level reporting.",
                "connection_explanation": "The Account table includes a branch_id that references the Branch table. This links every account to its originating branch, enabling branch-level analytics and operations."
            },
            "Transaction → Account": {
                "foreign_key_patterns": ["account_id", "account_number", "acc_id"],
                "primary_key_entity": "Account",
                "child_entity": "Transaction",
                "purpose": "Links every transaction to the account it affects. This is fundamental for maintaining account balance, generating statements, and tracking financial activity.",
                "business_impact": "Without this connection, transactions cannot be properly recorded, balances cannot be calculated, and account statements cannot be generated. Core to banking operations.",
                "connection_explanation": "Every transaction record must reference an account_id to indicate which account the transaction belongs to. This enables the system to calculate running balances and maintain account history."
            },
            "Loan → Customer & Account": {
                "foreign_key_patterns": ["customer_id", "account_number"],
                "primary_key_entity": ["Customer", "Account"],
                "child_entity": "Loan",
                "purpose": "Connects loans to both the customer (borrower) and the associated account. This ensures loan payments are correctly applied and customer credit history is maintained.",
                "business_impact": "Critical for loan management, payment processing, credit risk assessment, and regulatory reporting. Links credit operations to core banking.",
                "connection_explanation": "Loan records typically reference both customer_id (to identify the borrower) and account_number (the loan account where payments are credited). This creates a comprehensive loan-to-customer-to-account relationship."
            },
            "Card → Customer & Account": {
                "foreign_key_patterns": ["customer_id", "account_number"],
                "primary_key_entity": ["Customer", "Account"],
                "child_entity": "Card",
                "purpose": "Links payment cards to both the customer and the account they are tied to. Essential for card transactions, fraud detection, and account linking.",
                "business_impact": "Enables card-based transactions to automatically update account balances and maintains the relationship between physical cards and digital accounts.",
                "connection_explanation": "Card records reference both customer_id (cardholder) and account_number (the account the card is linked to). This allows card transactions to directly impact account balances."
            },
            "Beneficiary → Customer": {
                "foreign_key_patterns": ["customer_id", "cust_id"],
                "primary_key_entity": "Customer",
                "child_entity": "Beneficiary",
                "purpose": "Links beneficiaries (people who can receive transfers) to the customer who registered them. Required for managing transfer beneficiaries and payment authorizations.",
                "business_impact": "Enables customers to save and manage their frequent transfer recipients, improving user experience and transaction security.",
                "connection_explanation": "The Beneficiary table contains customer_id to identify which customer has registered each beneficiary. This creates a one-to-many relationship for transfer management."
            }
        }
    
    def match_column_to_pattern(self, column_name: str) -> Optional[Dict[str, Any]]:
        """
        Match a column name to predefined PK/FK patterns.
        Returns the matched pattern info or None.
        """
        col_lower = str(column_name).lower().strip()
        
        # Check Primary Key patterns
        for pk_key, pk_info in self.primary_key_patterns.items():
            for pattern in pk_info["patterns"]:
                if pattern in col_lower or col_lower in pattern:
                    return {
                        "type": "PRIMARY_KEY",
                        "key": pk_key,
                        "entity": pk_info["entity"],
                        "description": pk_info["description"],
                        "column_name": column_name
                    }
        
        # Check Foreign Key patterns
        for fk_relation, fk_info in self.foreign_key_relationships.items():
            for pattern in fk_info["foreign_key_patterns"]:
                if pattern in col_lower or col_lower in pattern:
                    return {
                        "type": "FOREIGN_KEY",
                        "relationship": fk_relation,
                        "parent_entity": fk_info["primary_key_entity"],
                        "child_entity": fk_info["child_entity"],
                        "column_name": column_name,
                        "purpose": fk_info["purpose"]
                    }
        
        return None
    
    def identify_file_entity_type(self, df: pd.DataFrame, file_name: str) -> Dict[str, Any]:
        """
        Identify what type of entity a file represents based on its columns.
        Returns entity type, detected PKs, and detected FKs.
        """
        columns = list(df.columns)
        detected_pks = []
        detected_fks = []
        entity_types = []
        
        for col in columns:
            match = self.match_column_to_pattern(col)
            if match:
                if match["type"] == "PRIMARY_KEY":
                    detected_pks.append(match)
                    if match["entity"] not in entity_types:
                        entity_types.append(match["entity"])
                elif match["type"] == "FOREIGN_KEY":
                    detected_fks.append(match)
                    if match["child_entity"] not in entity_types:
                        entity_types.append(match["child_entity"])
        
        # Determine primary entity type
        primary_entity = entity_types[0] if entity_types else "Unknown"
        if not detected_pks and detected_fks:
            # If no PK but has FK, infer from FK
            primary_entity = detected_fks[0]["child_entity"]
        
        return {
            "file_name": file_name,
            "primary_entity": primary_entity,
            "detected_primary_keys": detected_pks,
            "detected_foreign_keys": detected_fks,
            "all_columns": columns
        }
    
    def analyze_file_relationships(self, file_dataframes: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
        """
        Analyze relationships between multiple files.
        Returns detailed relationship information with explanations.
        """
        if len(file_dataframes) < 2:
            return []
        
        # Step 1: Identify entity type for each file
        file_entities = {}
        for file_name, df in file_dataframes.items():
            file_entities[file_name] = self.identify_file_entity_type(df, file_name)
        
        # Step 2: Detect relationships between files
        relationships = []
        file_names = list(file_dataframes.keys())
        
        for i, file1_name in enumerate(file_names):
            file1_info = file_entities[file1_name]
            df1 = file_dataframes[file1_name]
            
            for j, file2_name in enumerate(file_names):
                if i >= j:  # Avoid duplicates and self-comparison
                    continue
                
                file2_info = file_entities[file2_name]
                df2 = file_dataframes[file2_name]
                
                # Check for FK relationships using pattern matching
                relationship = self._detect_file_to_file_relationship(
                    file1_name, file1_info, df1,
                    file2_name, file2_info, df2
                )
                
                if relationship:
                    relationships.append(relationship)
                else:
                    # Fallback: Try direct column matching
                    relationship = self._detect_direct_column_match(
                        file1_name, file1_info, df1,
                        file2_name, file2_info, df2
                    )
                    if relationship:
                        relationships.append(relationship)
        
        return relationships
    
    def _detect_direct_column_match(
        self,
        file1_name: str, file1_info: Dict, df1: pd.DataFrame,
        file2_name: str, file2_info: Dict, df2: pd.DataFrame
    ) -> Optional[Dict[str, Any]]:
        """Fallback: Detect relationships by matching column names directly."""
        # Find common column names (case-insensitive)
        df1_cols_lower = {str(c).lower(): c for c in df1.columns}
        df2_cols_lower = {str(c).lower(): c for c in df2.columns}
        
        common_cols = set(df1_cols_lower.keys()) & set(df2_cols_lower.keys())
        
        # Filter to ID-like columns
        id_patterns = ["_id", "id", "_number", "number", "_code", "code"]
        id_columns = [col for col in common_cols if any(pattern in col for pattern in id_patterns)]
        
        for col_lower in id_columns:
            col1 = df1_cols_lower[col_lower]
            col2 = df2_cols_lower[col_lower]
            
            # Calculate overlap
            overlap_info = self._calculate_value_overlap(df1[col1], df2[col2])
            
            if overlap_info["overlap_ratio"] > 0.5:  # At least 50% overlap for direct match
                # Determine which file is parent (has fewer unique values = parent)
                unique1 = df1[col1].nunique()
                unique2 = df2[col2].nunique()
                
                if unique1 <= unique2:
                    # file1 is parent
                    parent_file, parent_info, parent_df = file1_name, file1_info, df1
                    child_file, child_info, child_df = file2_name, file2_info, df2
                    parent_col, child_col = col1, col2
                else:
                    # file2 is parent
                    parent_file, parent_info, parent_df = file2_name, file2_info, df2
                    child_file, child_info, child_df = file1_name, file1_info, df1
                    parent_col, child_col = col2, col1
                
                # Infer entities
                parent_entity = self._infer_entity_from_column(parent_col) or parent_info.get("primary_entity", "Unknown")
                child_entity = self._infer_entity_from_column(child_col) or child_info.get("primary_entity", "Unknown")
                
                # Create synthetic PK/FK info
                parent_pk_info = {
                    "entity": parent_entity,
                    "column_name": parent_col,
                    "key": str(parent_col).lower()
                }
                child_fk_info = {
                    "child_entity": child_entity,
                    "column_name": child_col,
                    "parent_entity": parent_entity
                }
                
                return self._build_relationship_explanation(
                    parent_file, parent_info, parent_col, parent_pk_info,
                    child_file, child_info, child_col, child_fk_info,
                    overlap_info
                )
        
        return None
    
    def _detect_file_to_file_relationship(
        self, 
        file1_name: str, file1_info: Dict, df1: pd.DataFrame,
        file2_name: str, file2_info: Dict, df2: pd.DataFrame
    ) -> Optional[Dict[str, Any]]:
        """Detect if two files have a relationship based on PK/FK patterns."""
        
        # Get PKs from file1
        file1_pks = {pk["key"]: pk for pk in file1_info.get("detected_primary_keys", [])}
        
        # Get FKs from file2 that might reference file1's PKs
        for fk in file2_info.get("detected_foreign_keys", []):
            fk_col = fk["column_name"]
            parent_entity = fk["parent_entity"]
            
            # Handle case where parent_entity is a list (e.g., ["Customer", "Account"])
            parent_entities = parent_entity if isinstance(parent_entity, list) else [parent_entity]
            
            # Check if file1 has a matching PK
            for pk_key, pk_info in file1_pks.items():
                if pk_info["entity"] in parent_entities:
                    pk_col = pk_info["column_name"]
                    
                    # Check if columns exist and have overlapping values
                    if pk_col in df1.columns and fk_col in df2.columns:
                        overlap_info = self._calculate_value_overlap(df1[pk_col], df2[fk_col])
                        
                        if overlap_info["overlap_ratio"] > 0.3:  # At least 30% overlap
                            return self._build_relationship_explanation(
                                file1_name, file1_info, pk_col, pk_info,
                                file2_name, file2_info, fk_col, fk,
                                overlap_info
                            )
        
        # Reverse check: file2's PKs referenced by file1's FKs
        file2_pks = {pk["key"]: pk for pk in file2_info.get("detected_primary_keys", [])}
        
        for fk in file1_info.get("detected_foreign_keys", []):
            fk_col = fk["column_name"]
            parent_entity = fk["parent_entity"]
            
            # Handle case where parent_entity is a list
            parent_entities = parent_entity if isinstance(parent_entity, list) else [parent_entity]
            
            for pk_key, pk_info in file2_pks.items():
                if pk_info["entity"] in parent_entities:
                    pk_col = pk_info["column_name"]
                    
                    if pk_col in df2.columns and fk_col in df1.columns:
                        overlap_info = self._calculate_value_overlap(df2[pk_col], df1[fk_col])
                        
                        if overlap_info["overlap_ratio"] > 0.3:
                            return self._build_relationship_explanation(
                                file2_name, file2_info, pk_col, pk_info,
                                file1_name, file1_info, fk_col, fk,
                                overlap_info
                            )
        
        # Fallback: Direct column name matching (if patterns didn't match but columns have same name and overlapping values)
        for col1 in df1.columns:
            col1_lower = str(col1).lower()
            # Check if this looks like an ID column
            if any(pattern in col1_lower for pattern in ["_id", "id", "_number", "number"]):
                for col2 in df2.columns:
                    col2_lower = str(col2).lower()
                    # If column names are similar
                    if col1_lower == col2_lower or (col1_lower.replace("_", "") == col2_lower.replace("_", "")):
                        overlap_info = self._calculate_value_overlap(df1[col1], df2[col2])
                        if overlap_info["overlap_ratio"] > 0.5:  # Higher threshold for fallback
                            # Try to infer entity types from column names
                            parent_entity = self._infer_entity_from_column(col1)
                            child_entity = self._infer_entity_from_column(col2) or file2_info.get("primary_entity", "Unknown")
                            
                            # Create synthetic PK/FK info
                            parent_pk_info = {
                                "entity": parent_entity,
                                "column_name": col1,
                                "key": col1_lower
                            }
                            child_fk_info = {
                                "child_entity": child_entity,
                                "column_name": col2,
                                "parent_entity": parent_entity
                            }
                            
                            return self._build_relationship_explanation(
                                file1_name, file1_info, col1, parent_pk_info,
                                file2_name, file2_info, col2, child_fk_info,
                                overlap_info
                            )
        
        return None
    
    def _infer_entity_from_column(self, column_name: str) -> str:
        """Infer entity type from column name."""
        col_lower = str(column_name).lower()
        if "customer" in col_lower or "cust" in col_lower:
            return "Customer"
        elif "account" in col_lower or "acc" in col_lower:
            return "Account"
        elif "transaction" in col_lower or "txn" in col_lower:
            return "Transaction"
        elif "loan" in col_lower:
            return "Loan"
        elif "card" in col_lower:
            return "Card"
        elif "branch" in col_lower:
            return "Branch"
        return "Unknown"
    
    def _calculate_value_overlap(self, series1: pd.Series, series2: pd.Series) -> Dict[str, Any]:
        """Calculate overlap ratio between two series."""
        set1 = set(series1.dropna().astype(str))
        set2 = set(series2.dropna().astype(str))
        
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        overlap_ratio = intersection / union if union > 0 else 0
        
        return {
            "overlap_count": intersection,
            "total_unique_file1": len(set1),
            "total_unique_file2": len(set2),
            "overlap_ratio": round(overlap_ratio, 3)
        }
    
    def _build_relationship_explanation(
        self,
        parent_file: str, parent_info: Dict, parent_pk_col: str, parent_pk_info: Dict,
        child_file: str, child_info: Dict, child_fk_col: str, child_fk_info: Dict,
        overlap_info: Dict
    ) -> Dict[str, Any]:
        """Build detailed relationship explanation with purpose and business impact."""
        
        # Use just the filename (basename) for cleaner display
        parent_file_display = os.path.basename(parent_file)
        child_file_display = os.path.basename(child_file)
        
        parent_entity = parent_pk_info["entity"]
        child_entity = child_fk_info["child_entity"]
        
        # Get relationship details from predefined patterns
        relationship_key = None
        relationship_details = None
        
        for rel_key, rel_info in self.foreign_key_relationships.items():
            pk_entity = rel_info["primary_key_entity"]
            # Handle both string and list cases
            pk_entities = pk_entity if isinstance(pk_entity, list) else [pk_entity]
            
            if (parent_entity in pk_entities and 
                child_entity == rel_info["child_entity"]):
                relationship_key = rel_key
                relationship_details = rel_info
                break
        
        # Build explanation paragraph
        if relationship_details:
            explanation = relationship_details["purpose"]
            business_impact = relationship_details["business_impact"]
            connection_details = relationship_details["connection_explanation"]
        else:
            explanation = f"The {parent_file} file contains the primary key ({parent_pk_col}) that is referenced by the {child_file} file's foreign key ({child_fk_col})."
            business_impact = "This connection enables data linking and referential integrity between the two files."
            connection_details = f"Values in {child_file}.{child_fk_col} must exist in {parent_file}.{parent_pk_col}."
        
        # Build dynamic 2-line short explanation based on observed data (not hardcoded)
        overlap_count = overlap_info.get('overlap_count', 0)
        overlap_pct = overlap_info.get('overlap_ratio', 0) * 100
        unique_parent = overlap_info.get('total_unique_file1', 0)
        unique_child = overlap_info.get('total_unique_file2', 0)
        
        # Line 1: What connects them (data-driven)
        short_line1 = f"Found {overlap_count} matching '{parent_pk_col}' values linking {parent_file_display} to {child_file_display} ({overlap_pct:.0f}% overlap)."
        
        # Line 2: Why it matters (data-driven inference)
        if unique_parent > 0 and unique_child > unique_parent:
            ratio_desc = f"Each {parent_entity} record links to ~{unique_child // max(1, unique_parent)} {child_entity} records on average."
        else:
            ratio_desc = f"This connection allows linking {parent_entity} data with related {child_entity} records."
        short_line2 = ratio_desc
        
        short_explanation = f"{short_line1} {short_line2}"
        
        # Build full explanation paragraph (kept for detailed view)
        full_explanation = f"""
        <strong>Connection Detected:</strong> {parent_file_display} (contains {parent_entity} data) ↔ {child_file_display} (contains {child_entity} data)<br><br>
        
        <strong>Why They Are Connected:</strong><br>
        {explanation}<br><br>
        
        <strong>Technical Details:</strong><br>
        {connection_details} The connection is established through the column {parent_pk_col} in {parent_file_display} and {child_fk_col} in {child_file_display}. 
        Analysis shows {overlap_info['overlap_count']} matching values between the two files, with an overlap ratio of {overlap_pct:.1f}%.<br><br>
        
        <strong>Business Purpose:</strong><br>
        {business_impact}<br><br>
        
        <strong>Data Relationship Type:</strong> This is a "One-to-Many" relationship, meaning one record in {parent_file_display} can be linked to multiple records in {child_file_display}. 
        This is a standard pattern in banking systems where, for example, one customer can have multiple accounts or one account can have multiple transactions.
        """
        
        return {
            "parent_file": parent_file_display,
            "parent_entity": parent_entity,
            "parent_column": parent_pk_col,
            "parent_column_type": "PRIMARY_KEY",
            "child_file": child_file_display,
            "child_entity": child_entity,
            "child_column": child_fk_col,
            "child_column_type": "FOREIGN_KEY",
            "relationship_type": relationship_key or f"{parent_entity} → {child_entity}",
            "overlap_info": overlap_info,
            "short_explanation": short_explanation,
            "detailed_explanation": full_explanation,
            "purpose": explanation,
            "business_impact": business_impact,
            "connection_details": connection_details
        }
