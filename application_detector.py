"""
Banking Application Type Detector

Detects the type of banking application based on column patterns and data structure.
"""

from typing import List, Dict
from rapidfuzz import fuzz


class BankingApplicationDetector:
    """Detects banking application type based on column patterns"""
    
    def __init__(self):
        # Define column patterns for each application type
        self.application_patterns = {
            "Core Banking System": {
                "required": ["account_number", "customer_id", "balance"],
                "optional": ["transaction_date", "transaction_id", "debit", "credit", "branch_code"],
                "weight": 10,
                "description": "Complete banking system managing accounts, customers, and transactions"
            },
            "Loan Management System": {
                "required": ["loan_id", "loan_amount"],
                "optional": ["emi_amount", "interest_rate", "loan_type", "tenure", "outstanding_amount", "repayment"],
                "weight": 9,
                "description": "System for managing loans, EMIs, and loan accounts"
            },
            "Account Opening Application": {
                "required": ["application_id", "customer_name"],
                "optional": ["kyc_status", "approval_status", "document_verification", "account_type", "branch"],
                "weight": 8,
                "description": "Application for opening new bank accounts with KYC verification"
            },
            "Transaction Processing System": {
                "required": ["transaction_id", "amount"],
                "optional": ["debit", "credit", "transaction_type", "transaction_date", "status", "mode_of_transaction"],
                "weight": 9,
                "description": "System focused on processing and recording financial transactions"
            },
            "Customer Relationship Management": {
                "required": ["customer_id", "customer_name"],
                "optional": ["contact_number", "email", "address", "interaction_date", "service_request"],
                "weight": 7,
                "description": "CRM system for managing customer data and interactions"
            },
            "Fixed Deposit System": {
                "required": ["fd_number", "deposit_amount"],
                "optional": ["maturity_date", "interest_rate", "tenure", "maturity_amount", "customer_id"],
                "weight": 8,
                "description": "System for managing fixed deposits and recurring deposits"
            },
            "Credit Card Management": {
                "required": ["card_number", "customer_id"],
                "optional": ["credit_limit", "outstanding_balance", "due_date", "minimum_payment", "transaction"],
                "weight": 8,
                "description": "System for managing credit cards and card transactions"
            }
        }
    
    def normalize(self, text: str) -> str:
        """Normalize text for comparison"""
        return str(text).lower().replace(" ", "").replace("_", "")
    
    def column_matches(self, column: str, pattern: str, threshold: int = 85) -> bool:
        """Check if a column matches a pattern using fuzzy matching"""
        norm_col = self.normalize(column)
        norm_pattern = self.normalize(pattern)
        
        # Direct match
        if norm_pattern in norm_col or norm_col in norm_pattern:
            return True
        
        # Fuzzy match
        if fuzz.ratio(norm_col, norm_pattern) >= threshold:
            return True
        
        # Partial match for common variations
        variations = {
            "account_number": ["acct", "acc", "account", "accountno"],
            "customer_id": ["cust", "customer", "client"],
            "transaction_id": ["txn", "trans", "transaction"],
            "loan_id": ["loan", "loanno", "loannumber"],
            "application_id": ["app", "application", "appno"],
            "deposit_amount": ["deposit", "amount"],
            "card_number": ["card", "cardno"],
        }
        
        if pattern in variations:
            for var in variations[pattern]:
                if var in norm_col:
                    return True
        
        return False
    
    def detect_application_type(self, columns: List[str]) -> Dict:
        """
        Detect the banking application type based on columns
        
        Args:
            columns: List of column names from the dataset
            
        Returns:
            Dictionary with application type, confidence, reasoning, and key indicators
        """
        if not columns:
            return {
                "application_type": "Unknown",
                "confidence": "LOW",
                "reasoning": "No columns provided for detection",
                "key_indicators": [],
                "description": ""
            }
        
        scores = {}
        matched_columns = {}
        
        # Calculate match score for each application type
        for app_type, patterns in self.application_patterns.items():
            score = 0
            matches = []
            
            # Check required columns
            required_matches = 0
            for req_pattern in patterns["required"]:
                for col in columns:
                    if self.column_matches(col, req_pattern):
                        required_matches += 1
                        matches.append(col)
                        score += patterns["weight"]
                        break
            
            # Check optional columns (bonus points)
            for opt_pattern in patterns["optional"]:
                for col in columns:
                    if self.column_matches(col, opt_pattern):
                        matches.append(col)
                        score += patterns["weight"] * 0.5
                        break
            
            # Only consider if at least some required columns are present
            if required_matches > 0:
                scores[app_type] = score
                matched_columns[app_type] = list(set(matches))
        
        # No matches found
        if not scores:
            return {
                "application_type": "General Banking Data",
                "confidence": "LOW",
                "reasoning": "Could not identify specific banking application type. Data contains banking-related columns but doesn't match known application patterns.",
                "key_indicators": columns[:5] if len(columns) > 5 else columns,
                "description": "General banking dataset with unclassified structure"
            }
        
        # Find the best match
        best_app = max(scores.items(), key=lambda x: x[1])
        app_type = best_app[0]
        score = best_app[1]
        indicators = matched_columns[app_type]
        
        # Determine confidence level
        required_count = len(self.application_patterns[app_type]["required"])
        matched_required = sum(1 for col in indicators if any(
            self.column_matches(col, req) for req in self.application_patterns[app_type]["required"]
        ))
        
        if matched_required == required_count and score > 20:
            confidence = "HIGH"
        elif matched_required >= required_count // 2:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"
        
        # Generate reasoning
        reasoning = self._generate_reasoning(app_type, indicators, matched_required, required_count)
        
        return {
            "application_type": app_type,
            "confidence": confidence,
            "reasoning": reasoning,
            "key_indicators": indicators,
            "description": self.application_patterns[app_type]["description"]
        }
    
    def _generate_reasoning(self, app_type: str, indicators: List[str], matched: int, total: int) -> str:
        """Generate human-readable reasoning for the detection"""
        
        reasoning_parts = [
            f"Detected as '{app_type}' because the dataset contains {matched} out of {total} required columns."
        ]
        
        if indicators:
            key_cols = ", ".join(indicators[:5])
            reasoning_parts.append(f"Key identifying columns found: {key_cols}.")
        
        # Add application-specific reasoning
        app_specific = {
            "Core Banking System": "This appears to be a comprehensive banking system with account management, customer data, and transaction processing capabilities.",
            "Loan Management System": "The presence of loan-related columns indicates this system manages loan accounts, EMI payments, and loan lifecycle.",
            "Account Opening Application": "This appears to be a customer onboarding system for opening new bank accounts with KYC verification.",
            "Transaction Processing System": "This system is primarily focused on recording and processing financial transactions.",
            "Customer Relationship Management": "This appears to be a CRM system for managing customer information and service interactions.",
            "Fixed Deposit System": "This system manages fixed deposit accounts and maturity calculations.",
            "Credit Card Management": "This appears to be a credit card management system tracking card usage and payments."
        }
        
        if app_type in app_specific:
            reasoning_parts.append(app_specific[app_type])
        
        return " ".join(reasoning_parts)
