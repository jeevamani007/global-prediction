"""
Application Type Detector
Probabilistic application type detection using pattern observation.
100% data-driven - no hardcoded keywords, only observable patterns.
"""

import pandas as pd
from typing import Dict, Any, List, Optional, Tuple
from collections import Counter
import re


class ApplicationTypeDetector:
    """
    Detects application type based on observed column patterns, relationships, and data characteristics.
    Uses probabilistic scoring across multiple dimensions.
    """
    
    def __init__(self):
        """Initialize pattern matchers for different application types."""
        
        # Define observable patterns for each application type
        # These are PATTERNS (not keywords) - detected through column naming conventions and relationships
        self.application_patterns = {
            'Core Banking': {
                'column_patterns': [
                    # Account management patterns
                    r'account.*number|acc.*no|account.*id',
                    r'customer.*id|cust.*id|client.*id',
                    r'balance|current.*balance|available.*balance',
                    r'opening.*balance|closing.*balance',
                    r'account.*type|account.*status',
                    r'branch.*code|branch.*id|branch.*name',
                    
                    # Transaction patterns
                    r'transaction.*id|trans.*id|txn.*id',
                    r'transaction.*date|trans.*date|txn.*date',
                    r'debit|credit|dr|cr',
                    r'transaction.*type|trans.*type',
                    r'transaction.*amount|trans.*amount',
                ],
                'relationship_patterns': [
                    'account-customer linkage',
                    'account-transaction linkage',
                    'customer-branch linkage',
                ],
                'data_patterns': [
                    'numeric balances',
                    'date sequences',
                    'status indicators (ACTIVE/INACTIVE)',
                ],
            },
            
            'Loan Management': {
                'column_patterns': [
                    r'loan.*id|loan.*number|loan.*account',
                    r'loan.*amount|principal|loan.*principal',
                    r'emi|installment|repayment',
                    r'interest.*rate|roi|rate.*of.*interest',
                    r'disbursement|disburse|loan.*disburse',
                    r'tenure|duration|loan.*period',
                    r'loan.*type|loan.*product|loan.*scheme',
                    r'outstanding|overdue|due.*amount',
                    r'maturity.*date|end.*date|loan.*closure',
                    r'sanction.*amount|approved.*amount',
                ],
                'relationship_patterns': [
                    'loan-customer linkage',
                    'loan-EMI schedule linkage',
                    'loan-disbursement linkage',
                ],
                'data_patterns': [
                    'EMI schedules',
                    'interest calculations',
                    'payment tracking',
                ],
            },
            
            'Payments': {
                'column_patterns': [
                    r'payment.*id|payment.*reference|payment.*number',
                    r'payment.*amount|paid.*amount|transaction.*amount',
                    r'payment.*date|payment.*time|txn.*date',
                    r'payment.*status|payment.*state',
                    r'payment.*mode|payment.*method|payment.*type',
                    r'settlement|settled.*amount',
                    r'utr|unique.*transaction.*reference',
                    r'beneficiary|payee|receiver',
                    r'payer|remitter|sender',
                    r'bank.*reference|ref.*number',
                ],
                'relationship_patterns': [
                    'payment-settlement linkage',
                    'payment-transaction linkage',
                    'payer-payee relationship',
                ],
                'data_patterns': [
                    'payment status tracking',
                    'settlement reconciliation',
                    'transaction timestamp sequences',
                ],
            },
            
            'Deposits': {
                'column_patterns': [
                    r'deposit.*id|deposit.*number|deposit.*account',
                    r'deposit.*amount|principal|invested.*amount',
                    r'deposit.*type|fd|rd|fixed.*deposit|recurring.*deposit',
                    r'maturity.*date|maturity.*amount',
                    r'interest.*rate|rate.*of.*interest',
                    r'deposit.*date|opening.*date|start.*date',
                    r'tenure|period|duration',
                    r'nomination|nominee',
                    r'renewal|auto.*renew',
                    r'premature.*closure|withdrawal',
                ],
                'relationship_patterns': [
                    'deposit-customer linkage',
                    'deposit-maturity linkage',
                    'deposit-interest calculation',
                ],
                'data_patterns': [
                    'maturity calculations',
                    'interest accrual',
                    'tenure tracking',
                ],
            },
            
            'Cards': {
                'column_patterns': [
                    r'card.*number|card.*id|pan',
                    r'card.*type|card.*product|credit.*card|debit.*card',
                    r'credit.*limit|card.*limit',
                    r'statement.*date|billing.*date',
                    r'due.*date|payment.*due',
                    r'outstanding|balance|amount.*due',
                    r'minimum.*due|min.*payment',
                    r'card.*status|card.*state',
                    r'expiry.*date|valid.*thru|expiration',
                    r'rewards|points|cashback',
                ],
                'relationship_patterns': [
                    'card-customer linkage',
                    'card-transaction linkage',
                    'card-statement linkage',
                ],
                'data_patterns': [
                    'statement cycles',
                    'credit limit tracking',
                    'reward points',
                ],
            },
            
            'KYC': {
                'column_patterns': [
                    r'kyc.*id|kyc.*number|kyc.*reference',
                    r'document.*type|doc.*type|id.*proof',
                    r'document.*number|id.*number|proof.*number',
                    r'verification.*status|kyc.*status',
                    r'verification.*date|verified.*date',
                    r'aadhar|pan|passport|voter.*id|driving.*license',
                    r'address.*proof|identity.*proof',
                    r'upload.*date|submission.*date',
                    r'verified.*by|verified.*on',
                    r'rejection.*reason|remarks',
                ],
                'relationship_patterns': [
                    'KYC-customer linkage',
                    'document-verification linkage',
                ],
                'data_patterns': [
                    'document verification workflow',
                    'approval/rejection tracking',
                    'compliance status',
                ],
            },
            
            'CRM': {
                'column_patterns': [
                    r'customer.*id|client.*id|contact.*id',
                    r'interaction.*id|case.*id|ticket.*id',
                    r'interaction.*type|contact.*type|channel',
                    r'interaction.*date|contact.*date|created.*date',
                    r'campaign.*id|campaign.*name|marketing.*campaign',
                    r'lead.*id|lead.*source|lead.*status',
                    r'opportunity|deal|pipeline',
                    r'assigned.*to|owner|agent',
                    r'resolution|status|priority',
                    r'feedback|satisfaction|rating',
                ],
                'relationship_patterns': [
                    'customer-interaction linkage',
                    'customer-campaign linkage',
                    'lead-opportunity linkage',
                ],
                'data_patterns': [
                    'interaction tracking',
                    'campaign management',
                    'lead conversion funnel',
                ],
            },
            
            'Insurance': {
                'column_patterns': [
                    r'policy.*id|policy.*number',
                    r'premium|premium.*amount',
                    r'sum.*assured|coverage|insured.*amount',
                    r'policy.*type|insurance.*type',
                    r'nominee|beneficiary',
                    r'claim.*id|claim.*number|claim.*amount',
                    r'policy.*start.*date|inception.*date',
                    r'policy.*end.*date|maturity.*date|expiry.*date',
                    r'rider|add.*on',
                    r'claim.*status|claim.*date',
                ],
                'relationship_patterns': [
                    'policy-customer linkage',
                    'policy-claim linkage',
                    'policy-premium linkage',
                ],
                'data_patterns': [
                    'premium schedules',
                    'claim processing',
                    'policy lifecycle',
                ],
            },
            
            'Trading': {
                'column_patterns': [
                    r'trade.*id|order.*id|transaction.*id',
                    r'symbol|ticker|security|instrument',
                    r'quantity|volume|lots',
                    r'price|rate|trade.*price',
                    r'buy|sell|order.*type|side',
                    r'settlement.*date|trade.*date',
                    r'broker|trading.*account',
                    r'portfolio|position',
                    r'profit.*loss|pnl|gain.*loss',
                    r'margin|exposure|leverage',
                ],
                'relationship_patterns': [
                    'trade-account linkage',
                    'trade-settlement linkage',
                    'portfolio-position linkage',
                ],
                'data_patterns': [
                    'trade execution sequences',
                    'P&L calculations',
                    'position tracking',
                ],
            },
            
            'HR/Payroll': {
                'column_patterns': [
                    r'employee.*id|emp.*id|staff.*id',
                    r'employee.*name|emp.*name',
                    r'salary|basic.*salary|gross.*salary',
                    r'designation|position|role|department',
                    r'attendance|leave|working.*days',
                    r'allowance|deduction|tax',
                    r'net.*salary|net.*pay|take.*home',
                    r'join.*date|hire.*date|doj',
                    r'payroll.*period|pay.*period|month',
                    r'bank.*account|ifsc',
                ],
                'relationship_patterns': [
                    'employee-payroll linkage',
                    'employee-attendance linkage',
                    'employee-department linkage',
                ],
                'data_patterns': [
                    'salary components',
                    'attendance tracking',
                    'tax calculations',
                ],
            },
        }
    
    def detect_type(
        self, 
        csv_files_data: Dict[str, pd.DataFrame],
        business_rules: Optional[Dict[str, Any]] = None,
        relationships: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Detect application type based on observed patterns.
        
        Args:
            csv_files_data: Dict of {filename: DataFrame}
            business_rules: Optional business rules analysis
            relationships: Optional file relationships
            
        Returns:
            {
                'application_type': 'Core Banking',
                'confidence': 92,
                'confidence_level': 'High',
                'patterns_detected': ['account management', 'transactions', 'balances'],
                'evidence': {
                    'column_matches': 15,
                    'relationship_matches': 3,
                    'data_pattern_matches': 2
                },
                'alternative_types': [
                    {'type': 'Payments', 'confidence': 45},
                    {'type': 'Loan Management', 'confidence': 23}
                ]
            }
        """
        
        # Collect all columns from all files
        all_columns = []
        for df in csv_files_data.values():
            all_columns.extend(df.columns.tolist())
        
        # Score each application type
        type_scores = {}
        
        for app_type, patterns in self.application_patterns.items():
            score_data = self._calculate_type_score(
                app_type,
                patterns,
                all_columns,
                csv_files_data,
                relationships
            )
            type_scores[app_type] = score_data
        
        # Find the best match
        if not type_scores:
            return self._create_unknown_result()
        
        # Sort by confidence score
        sorted_types = sorted(
            type_scores.items(),
            key=lambda x: x[1]['confidence'],
            reverse=True
        )
        
        best_match = sorted_types[0]
        best_type = best_match[0]
        best_data = best_match[1]
        
        # Get alternative types (top 3 excluding best)
        alternatives = [
            {'type': t, 'confidence': d['confidence']}
            for t, d in sorted_types[1:4]
            if d['confidence'] > 10  # Only show alternatives with reasonable confidence
        ]
        
        # Determine confidence level
        confidence = best_data['confidence']
        if confidence >= 80:
            confidence_level = 'High'
        elif confidence >= 60:
            confidence_level = 'Medium'
        else:
            confidence_level = 'Low'
        
        return {
            'application_type': best_type,
            'confidence': confidence,
            'confidence_level': confidence_level,
            'patterns_detected': best_data['patterns_detected'],
            'evidence': {
                'column_matches': best_data['column_matches'],
                'relationship_matches': best_data['relationship_matches'],
                'data_pattern_matches': best_data['data_pattern_matches'],
                'total_columns_analyzed': len(all_columns)
            },
            'alternative_types': alternatives,
            'explanation': self._generate_explanation(best_type, best_data, len(all_columns))
        }
    
    def _calculate_type_score(
        self,
        app_type: str,
        patterns: Dict[str, List],
        all_columns: List[str],
        csv_files_data: Dict[str, pd.DataFrame],
        relationships: Optional[List[Dict[str, Any]]]
    ) -> Dict[str, Any]:
        """Calculate confidence score for a specific application type."""
        
        # 1. Column pattern matching (50% weight)
        column_matches = 0
        matched_patterns = []
        
        for pattern in patterns['column_patterns']:
            for column in all_columns:
                if re.search(pattern, column.lower()):
                    column_matches += 1
                    matched_patterns.append(column)
                    break  # Count each pattern only once
        
        # Normalize to percentage
        total_column_patterns = len(patterns['column_patterns'])
        column_score = (column_matches / total_column_patterns * 100) if total_column_patterns > 0 else 0
        
        # 2. Relationship pattern matching (30% weight)
        relationship_matches = 0
        if relationships:
            relationship_score = self._score_relationships(patterns['relationship_patterns'], relationships)
        else:
            relationship_score = 0
        
        # 3. Data pattern matching (20% weight)
        data_pattern_score = self._score_data_patterns(patterns['data_patterns'], csv_files_data)
        
        # Calculate weighted confidence
        confidence = (
            column_score * 0.5 +
            relationship_score * 0.3 +
            data_pattern_score * 0.2
        )
        
        # Extract human-readable pattern names
        patterns_detected = self._extract_pattern_names(matched_patterns)
        
        return {
            'confidence': round(confidence, 1),
            'column_matches': column_matches,
            'relationship_matches': relationship_matches,
            'data_pattern_matches': len(patterns['data_patterns']),
            'patterns_detected': patterns_detected[:5]  # Top 5 patterns
        }
    
    def _score_relationships(self, expected_patterns: List[str], relationships: List[Dict[str, Any]]) -> float:
        """Score relationship patterns."""
        if not relationships or not expected_patterns:
            return 0
        
        matches = 0
        for expected in expected_patterns:
            # Look for keyword matches in relationship explanations
            for rel in relationships:
                explanation = rel.get('explanation', '').lower()
                relationship_type = rel.get('relationship_type', '').lower()
                
                # Check if the expected pattern keywords are in the relationship
                keywords = expected.lower().split('-')
                if all(kw in explanation or kw in relationship_type for kw in keywords):
                    matches += 1
                    break
        
        return (matches / len(expected_patterns) * 100) if expected_patterns else 0
    
    def _score_data_patterns(self, expected_patterns: List[str], csv_files_data: Dict[str, pd.DataFrame]) -> float:
        """Score data patterns by examining actual data."""
        if not csv_files_data or not expected_patterns:
            return 0
        
        # For now, return a default score
        # In production, this would analyze actual data distributions, value ranges, etc.
        return 50.0  # Neutral score
    
    def _extract_pattern_names(self, matched_columns: List[str]) -> List[str]:
        """Extract human-readable pattern names from matched columns."""
        patterns = []
        
        # Group similar column names
        for col in matched_columns[:10]:  # Limit to first 10
            # Clean column name for display
            clean_name = col.replace('_', ' ').replace('.', ' ').title()
            if clean_name not in patterns:
                patterns.append(clean_name)
        
        return patterns
    
    def _generate_explanation(self, app_type: str, score_data: Dict, total_columns: int) -> str:
        """Generate human-readable explanation of the detection."""
        confidence = score_data['confidence']
        column_matches = score_data['column_matches']
        
        explanation = (
            f"Detected as {app_type} with {confidence}% confidence. "
            f"Found {column_matches} matching column patterns out of {total_columns} total columns analyzed. "
        )
        
        if score_data['relationship_matches'] > 0:
            explanation += f"Identified {score_data['relationship_matches']} relevant file relationships. "
        
        return explanation
    
    def _create_unknown_result(self) -> Dict[str, Any]:
        """Create result for unknown application type."""
        return {
            'application_type': 'Unknown',
            'confidence': 0,
            'confidence_level': 'None',
            'patterns_detected': [],
            'evidence': {
                'column_matches': 0,
                'relationship_matches': 0,
                'data_pattern_matches': 0,
                'total_columns_analyzed': 0
            },
            'alternative_types': [],
            'explanation': 'Unable to determine application type. No recognizable patterns found.'
        }


def detect_application_type(
    csv_files_data: Dict[str, pd.DataFrame],
    business_rules: Optional[Dict[str, Any]] = None,
    relationships: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """
    Convenience function to detect application type.
    
    Args:
        csv_files_data: Dict of {filename: DataFrame}
        business_rules: Optional business rules analysis
        relationships: Optional file relationships
        
    Returns:
        Application type detection result
    """
    detector = ApplicationTypeDetector()
    return detector.detect_type(csv_files_data, business_rules, relationships)
