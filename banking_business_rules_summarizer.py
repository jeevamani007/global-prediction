"""
Banking Business Rules Summarizer

Converts dynamically inferred column-level validation rules into high-level business rules.
Focuses on overall system behavior, controls, and data integrity patterns.
Groups findings into clear business themes suitable for senior stakeholders.
"""

from typing import Dict, Any, List, Optional
from collections import defaultdict
import re


class BankingBusinessRulesSummarizer:
    """
    Summarizes column-level validation rules into high-level business rules.
    Groups findings by business themes and highlights risks in business terms.
    """
    
    def __init__(self):
        # Business theme patterns - based on column name patterns and validation results
        self.theme_patterns = {
            'Account Design': [
                r'account.*(number|id|identifier)',
                r'account.*(type|status|category)',
                r'account.*(open|closure|close).*date',
                r'balance',
                r'ifsc|branch.*code',
            ],
            'Customer Identity Management': [
                r'customer.*(id|identifier)',
                r'customer.*name',
                r'kyc.*(status|date|verified)',
                r'pan.*number|aadhaar',
                r'age',
            ],
            'Transaction Controls': [
                r'transaction.*(amount|amt)',
                r'transaction.*(date|dt)',
                r'transaction.*(type|category)',
                r'transaction.*(status|state)',
                r'transaction.*channel',
                r'debit|credit',
                r'reversal.*flag',
            ],
            'Security & Access Controls': [
                r'otp.*verified',
                r'login.*attempt',
                r'account.*lock',
                r'device.*id',
                r'ip.*address',
                r'last.*login',
            ],
            'Risk Management & Compliance': [
                r'risk.*(level|score)',
                r'aml.*alert',
                r'suspicious.*(txn|transaction)',
                r'freeze.*reason',
                r'daily.*limit|monthly.*limit',
                r'atm.*limit',
            ],
            'Product Standardization': [
                r'account.*type',
                r'customer.*category',
                r'product.*(type|category)',
                r'interest.*rate',
            ],
            'Communication & Notifications': [
                r'email',
                r'phone|mobile|contact',
                r'notification.*preference',
                r'customer.*consent',
            ],
            'Operational Controls': [
                r'charge.*amount',
                r'tax.*deduct',
                r'interest.*credit.*date',
                r'statement.*cycle',
                r'standing.*instruction',
            ],
            'Relationship Management': [
                r'nominee.*(name|relation)',
                r'branch.*(name|code)',
                r'customer.*category',
            ],
            'Data Integrity': [
                r'.*duplicate',
                r'.*missing',
                r'.*null',
                r'.*unique',
            ]
        }
    
    def _classify_column_theme(self, column_name: str, analysis: Dict[str, Any]) -> List[str]:
        """Classify a column into one or more business themes based on name and validation results."""
        col_lower = column_name.lower()
        themes = []
        
        # Check each theme pattern
        for theme, patterns in self.theme_patterns.items():
            for pattern in patterns:
                if re.search(pattern, col_lower):
                    themes.append(theme)
                    break
        
        # If no theme matched, try to infer from validation results
        if not themes:
            issues = analysis.get('issues', [])
            status = analysis.get('status', '')
            
            # Data integrity issues
            if any('duplicate' in str(i).lower() or 'missing' in str(i).lower() for i in issues):
                themes.append('Data Integrity')
            
            # Critical validation failures
            if status == 'INVALID' and analysis.get('importance') == 'CRITICAL':
                if 'id' in col_lower or 'identifier' in col_lower:
                    themes.append('Account Design')
                elif 'customer' in col_lower:
                    themes.append('Customer Identity Management')
        
        return themes if themes else ['General Data Quality']
    
    def _extract_business_insight(self, columns: List[Dict[str, Any]], theme: str) -> Dict[str, Any]:
        """Extract business insights from columns belonging to a theme."""
        theme_columns = []
        critical_issues = []
        warning_issues = []
        data_patterns = defaultdict(int)
        
        for col in columns:
            col_name = col.get('column_name', '')
            themes = self._classify_column_theme(col_name, col)
            
            if theme in themes:
                theme_columns.append(col)
                
                # Collect issues
                issues = col.get('issues', [])
                importance = col.get('importance', 'SAFE')
                status = col.get('status', 'VALID')
                
                if importance == 'CRITICAL' or status == 'INVALID':
                    critical_issues.extend(issues)
                elif importance == 'WARNING' or status == 'WARNING':
                    warning_issues.extend(issues)
                
                # Track data patterns
                null_pct = col.get('null_percentage', 0)
                unique_ratio = col.get('unique_ratio', 1.0)
                
                if null_pct > 10:
                    data_patterns['high_missing_data'] += 1
                if unique_ratio < 0.95:
                    data_patterns['duplicate_values'] += 1
                if col.get('pattern_consistency', 100) < 80:
                    data_patterns['inconsistent_format'] += 1
        
        return {
            'columns': theme_columns,
            'critical_issues': critical_issues,
            'warning_issues': warning_issues,
            'data_patterns': dict(data_patterns),
            'column_count': len(theme_columns)
        }
    
    def _generate_business_rule(self, theme: str, insight: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Generate a high-level business rule from theme insights."""
        columns = insight['columns']
        critical_issues = insight['critical_issues']
        warning_issues = insight['warning_issues']
        data_patterns = insight['data_patterns']
        
        if not columns:
            return None
        
        # Generate business rule based on theme and issues
        rule = {
            'title': '',
            'interpretation': '',
            'impact': None
        }
        
        # Account Design Theme
        if theme == 'Account Design':
            rule['title'] = 'Account Identification and Lifecycle Management'
            
            has_duplicates = data_patterns.get('duplicate_values', 0) > 0
            has_missing = data_patterns.get('high_missing_data', 0) > 0
            
            if has_duplicates:
                rule['interpretation'] = (
                    "Account identifiers must be unique across all records. "
                    "Duplicate account numbers detected may cause transaction routing errors and balance calculation issues."
                )
                rule['impact'] = "CRITICAL: Transaction processing and account reconciliation may fail. Customer funds could be misrouted."
            elif has_missing:
                rule['interpretation'] = (
                    "Account identification fields must be complete. "
                    "Missing account numbers prevent proper account linking and transaction processing."
                )
                rule['impact'] = "CRITICAL: Incomplete account data blocks core banking operations."
            else:
                rule['interpretation'] = (
                    "Account identification structure is consistent and unique. "
                    "System can reliably route transactions and maintain account balances."
                )
        
        # Customer Identity Management Theme
        elif theme == 'Customer Identity Management':
            rule['title'] = 'Customer Identity Verification and KYC Compliance'
            
            has_duplicates = data_patterns.get('duplicate_values', 0) > 0
            has_missing = data_patterns.get('high_missing_data', 0) > 0
            
            if has_duplicates:
                rule['interpretation'] = (
                    "Customer identifiers must be unique. "
                    "Duplicate customer IDs detected may cause account linking errors and compliance reporting issues."
                )
                rule['impact'] = "CRITICAL: Customer relationship mapping may fail. Regulatory reporting accuracy compromised."
            elif has_missing:
                rule['interpretation'] = (
                    "Customer identification data must be complete for KYC compliance. "
                    "Missing customer identifiers prevent proper identity verification and regulatory reporting."
                )
                rule['impact'] = "CRITICAL: KYC compliance requirements may not be met. Account activation may be blocked."
            else:
                rule['interpretation'] = (
                    "Customer identity data is properly structured. "
                    "System can maintain accurate customer records and support KYC compliance requirements."
                )
        
        # Transaction Controls Theme
        elif theme == 'Transaction Controls':
            rule['title'] = 'Transaction Processing and Settlement Controls'
            
            has_missing = data_patterns.get('high_missing_data', 0) > 0
            has_inconsistent = data_patterns.get('inconsistent_format', 0) > 0
            
            if has_missing:
                rule['interpretation'] = (
                    "Transaction records must include complete amount, date, and type information. "
                    "Missing transaction data prevents accurate balance updates and statement generation."
                )
                rule['impact'] = "CRITICAL: Transaction settlement may fail. Account balances may be incorrect."
            elif has_inconsistent:
                rule['interpretation'] = (
                    "Transaction data formats must be consistent. "
                    "Inconsistent transaction types or statuses may cause reconciliation errors."
                )
                rule['impact'] = "WARNING: Transaction reporting and reconciliation may require manual intervention."
            else:
                rule['interpretation'] = (
                    "Transaction data structure supports reliable processing. "
                    "System can accurately update balances and generate transaction reports."
                )
        
        # Security & Access Controls Theme
        elif theme == 'Security & Access Controls':
            rule['title'] = 'Security Authentication and Access Management'
            
            has_missing = data_patterns.get('high_missing_data', 0) > 0
            
            if has_missing:
                rule['interpretation'] = (
                    "Security and access control fields must be properly maintained. "
                    "Missing authentication flags or device identifiers weaken fraud prevention capabilities."
                )
                rule['impact'] = "WARNING: Security monitoring and fraud detection capabilities may be reduced."
            else:
                rule['interpretation'] = (
                    "Security controls are properly configured. "
                    "System can enforce access restrictions and monitor suspicious activity effectively."
                )
        
        # Risk Management & Compliance Theme
        elif theme == 'Risk Management & Compliance':
            rule['title'] = 'Risk Assessment and Regulatory Compliance'
            
            has_missing = data_patterns.get('high_missing_data', 0) > 0
            
            if has_missing:
                rule['interpretation'] = (
                    "Risk and compliance data must be complete for regulatory reporting. "
                    "Missing risk scores or AML flags may prevent proper risk assessment and regulatory compliance."
                )
                rule['impact'] = "WARNING: Risk-based decision making and regulatory reporting may be incomplete."
            else:
                rule['interpretation'] = (
                    "Risk and compliance data structure supports regulatory requirements. "
                    "System can perform risk assessments and generate compliance reports."
                )
        
        # Product Standardization Theme
        elif theme == 'Product Standardization':
            rule['title'] = 'Product Classification and Standardization'
            
            has_inconsistent = data_patterns.get('inconsistent_format', 0) > 0
            
            if has_inconsistent:
                rule['interpretation'] = (
                    "Product types and categories must follow standardized values. "
                    "Inconsistent product classifications prevent proper interest calculation and fee application."
                )
                rule['impact'] = "WARNING: Product-specific rules (interest rates, fees) may not apply correctly."
            else:
                rule['interpretation'] = (
                    "Product classification is standardized. "
                    "System can apply product-specific rules for pricing, interest, and fees correctly."
                )
        
        # Communication & Notifications Theme
        elif theme == 'Communication & Notifications':
            rule['title'] = 'Customer Communication and Consent Management'
            
            has_inconsistent = data_patterns.get('inconsistent_format', 0) > 0
            has_missing = data_patterns.get('high_missing_data', 0) > 0
            
            if has_inconsistent:
                rule['interpretation'] = (
                    "Contact information must follow standard formats. "
                    "Invalid email or phone formats prevent customer notifications and OTP delivery."
                )
                rule['impact'] = "WARNING: Customer communication channels may be unavailable."
            elif has_missing:
                rule['interpretation'] = (
                    "Customer contact information should be maintained for service delivery. "
                    "Missing contact details limit notification capabilities."
                )
                rule['impact'] = None  # Optional field, no critical impact
            else:
                rule['interpretation'] = (
                    "Customer communication data is properly formatted. "
                    "System can deliver notifications and maintain customer consent preferences."
                )
        
        # Operational Controls Theme
        elif theme == 'Operational Controls':
            rule['title'] = 'Operational Fee and Interest Management'
            
            has_missing = data_patterns.get('high_missing_data', 0) > 0
            
            if has_missing:
                rule['interpretation'] = (
                    "Operational data (fees, interest, charges) must be complete for accurate billing. "
                    "Missing fee or interest data may cause incorrect statement generation."
                )
                rule['impact'] = "WARNING: Billing accuracy and statement generation may be affected."
            else:
                rule['interpretation'] = (
                    "Operational controls are properly maintained. "
                    "System can accurately calculate fees, interest, and generate billing statements."
                )
        
        # Relationship Management Theme
        elif theme == 'Relationship Management':
            rule['title'] = 'Customer Relationship and Branch Management'
            
            has_missing = data_patterns.get('high_missing_data', 0) > 0
            
            if has_missing:
                rule['interpretation'] = (
                    "Relationship data (nominee, branch) should be maintained for complete customer records. "
                    "Missing relationship information may affect service delivery and legal compliance."
                )
                rule['impact'] = None  # Optional fields, no critical impact
            else:
                rule['interpretation'] = (
                    "Customer relationship data is complete. "
                    "System can support branch-level reporting and nominee management."
                )
        
        # Data Integrity Theme
        elif theme == 'Data Integrity':
            rule['title'] = 'Data Quality and Integrity Standards'
            
            has_duplicates = data_patterns.get('duplicate_values', 0) > 0
            has_missing = data_patterns.get('high_missing_data', 0) > 0
            
            if has_duplicates and has_missing:
                rule['interpretation'] = (
                    "Data integrity issues detected: duplicate values and missing data across multiple fields. "
                    "These issues may cause operational errors and reporting inaccuracies."
                )
                rule['impact'] = "CRITICAL: Data quality issues may cause system-wide operational failures."
            elif has_duplicates:
                rule['interpretation'] = (
                    "Duplicate values detected in key identifier fields. "
                    "This may cause data linking errors and reporting inaccuracies."
                )
                rule['impact'] = "CRITICAL: Duplicate identifiers prevent reliable data relationships."
            elif has_missing:
                rule['interpretation'] = (
                    "Significant missing data detected across multiple fields. "
                    "Incomplete records may limit system functionality and reporting accuracy."
                )
                rule['impact'] = "WARNING: Missing data may affect reporting completeness and operational efficiency."
            else:
                rule['interpretation'] = (
                    "Data integrity standards are met. "
                    "Records are complete, unique, and ready for processing."
                )
        
        # General Data Quality Theme
        elif theme == 'General Data Quality':
            rule['title'] = 'Overall Data Quality Standards'
            
            has_issues = len(critical_issues) > 0 or len(warning_issues) > 0
            
            if has_issues:
                rule['interpretation'] = (
                    "Data quality validation identified issues requiring attention. "
                    "Addressing these issues will improve system reliability and reporting accuracy."
                )
                rule['impact'] = "WARNING: Data quality issues may affect system performance and reporting."
            else:
                rule['interpretation'] = (
                    "Data quality meets banking standards. "
                    "System can process records reliably and generate accurate reports."
                )
        
        return rule if rule['title'] else None
    
    def summarize(self, dynamic_rules_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Summarize column-level validation rules into high-level business rules.
        
        Args:
            dynamic_rules_result: Output from generate_dynamic_business_rules()
        
        Returns:
            Dictionary with business rules grouped by theme
        """
        if not dynamic_rules_result or not isinstance(dynamic_rules_result, dict):
            return {
                'business_rules': [],
                'summary': {
                    'total_rules': 0,
                    'critical_rules': 0,
                    'warning_rules': 0
                }
            }
        
        columns = dynamic_rules_result.get('columns', [])
        if not columns:
            return {
                'business_rules': [],
                'summary': {
                    'total_rules': 0,
                    'critical_rules': 0,
                    'warning_rules': 0
                }
            }
        
        # Group columns by theme
        theme_insights = defaultdict(lambda: {'columns': [], 'critical_issues': [], 'warning_issues': [], 'data_patterns': defaultdict(int), 'column_count': 0})
        
        for col in columns:
            col_name = col.get('column_name', '')
            themes = self._classify_column_theme(col_name, col)
            
            for theme in themes:
                insight = theme_insights[theme]
                insight['columns'].append(col)
                
                # Collect issues
                issues = col.get('issues', [])
                importance = col.get('importance', 'SAFE')
                status = col.get('status', 'VALID')
                
                if importance == 'CRITICAL' or status == 'INVALID':
                    insight['critical_issues'].extend(issues)
                elif importance == 'WARNING' or status == 'WARNING':
                    insight['warning_issues'].extend(issues)
                
                # Track data patterns
                null_pct = col.get('null_percentage', 0)
                unique_ratio = col.get('unique_ratio', 1.0)
                pattern_consistency = col.get('pattern_consistency', 100)
                
                if null_pct > 10:
                    insight['data_patterns']['high_missing_data'] += 1
                if unique_ratio < 0.95:
                    insight['data_patterns']['duplicate_values'] += 1
                if pattern_consistency < 80:
                    insight['data_patterns']['inconsistent_format'] += 1
                
                insight['column_count'] = len(insight['columns'])
        
        # Generate business rules for each theme
        business_rules = []
        critical_count = 0
        warning_count = 0
        
        for theme, insight in theme_insights.items():
            rule = self._generate_business_rule(theme, insight)
            if rule:
                # Determine if rule has critical impact
                if rule.get('impact') and 'CRITICAL' in rule['impact']:
                    critical_count += 1
                elif rule.get('impact'):
                    warning_count += 1
                
                business_rules.append({
                    'theme': theme,
                    'title': rule['title'],
                    'interpretation': rule['interpretation'],
                    'impact': rule['impact'],
                    'column_count': insight['column_count'],
                    'critical_issues_count': len(set(insight['critical_issues'])),
                    'warning_issues_count': len(set(insight['warning_issues']))
                })
        
        # Sort rules by criticality (critical first, then warning, then safe)
        def sort_key(rule):
            impact = rule.get('impact', '')
            if 'CRITICAL' in impact:
                return (0, rule['critical_issues_count'])
            elif impact:
                return (1, rule['warning_issues_count'])
            else:
                return (2, 0)
        
        business_rules.sort(key=sort_key)
        
        return {
            'business_rules': business_rules,
            'summary': {
                'total_rules': len(business_rules),
                'critical_rules': critical_count,
                'warning_rules': warning_count,
                'safe_rules': len(business_rules) - critical_count - warning_count,
                'total_columns_analyzed': len(columns),
                'themes_covered': len(set(r['theme'] for r in business_rules))
            }
        }


def summarize_banking_business_rules(dynamic_rules_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convenience function to summarize banking business rules.
    
    Args:
        dynamic_rules_result: Output from generate_dynamic_business_rules()
    
    Returns:
        Dictionary with business rules grouped by theme
    """
    summarizer = BankingBusinessRulesSummarizer()
    return summarizer.summarize(dynamic_rules_result)
