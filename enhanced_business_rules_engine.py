"""
Enhanced Business Rules Engine for Banking Data Analysis

Generates comprehensive, paragraph-style business rule explanations for:
- Individual tables
- Column-level business meanings
- Multi-table relationships
- Data quality rules
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import re


class EnhancedBusinessRulesEngine:
    """Generate detailed business rule explanations in paragraph format"""
    
    def __init__(self):
        # Define comprehensive business rules for banking columns
        self.column_business_rules = {
            "customer_id": {
                "title": "Customer Identification Rule",
                "section": "Customer Details",
                "icon": "👤",
                "purpose": "Uniquely identifies each customer in the banking system",
                "simple_rules": [
                    "Customer ID must be unique",
                    "Required for all customer operations",
                    "Cannot be changed once assigned"
                ],
                "explanation": """The customer_id column serves as the primary identifier for each customer across all banking operations. Each customer is assigned a unique alphanumeric code (typically 4-10 characters) when they open their first account. This identifier remains constant throughout the customer's relationship with the bank and is used to link all their accounts, transactions, and services. The system ensures no two customers can have the same ID, and this field is mandatory for every customer record. Customer IDs typically follow a pattern like 'CUST0001' or 'C12345' and contain no special characters to ensure compatibility across all banking systems.""",
                "business_workflow": "When a new customer visits the bank, the system automatically generates a unique customer ID. This ID is then used to open accounts, process transactions, and track the customer's complete banking history.",
                "data_rules": [
                    "Must be unique across all customer records",
                    "Cannot be null or empty",
                    "Typically alphanumeric (letters + numbers)",
                    "Length between 4-10 characters",
                    "No special characters allowed",
                    "Remains constant for customer's lifetime"
                ]
            },
            "customer_name": {
                "title": "Customer Name Validation Rule",
                "section": "Customer Details",
                "icon": "👤",
                "purpose": "Stores the legal full name of the account holder",
                "simple_rules": [
                    "Name must match government ID",
                    "Only alphabetic characters allowed",
                    "Required for KYC compliance"
                ],
                "explanation": """The customer_name field contains the customer's full legal name as it appears on their government-issued identification documents. This information is critical for KYC (Know Your Customer) compliance and legal documentation. The name should match exactly with the customer's ID proof and signature cards. Banking regulations require this field to be verified during account opening and updated if the customer legally changes their name. The system stores the complete name including first name, middle name (if any), and last name to ensure accurate identification during transactions and communications.""",
                "business_workflow": "During account opening, bank staff verifies the customer's name against government ID (like Aadhar, PAN, or Passport). The verified name is stored and used on all account statements, checkbooks, and official correspondence.",
                "data_rules": [
                    "Must contain only alphabetic characters and spaces",
                    "Minimum length of 3 characters",
                    "Maximum length typically 50-100 characters",
                    "Cannot be null for active accounts",
                    "Should match government ID documents",
                    "May contain titles (Mr., Mrs., Dr.) at the beginning"
                ]
            },
            "account_number": {
                "title": "Account Number Uniqueness Rule",
                "section": "Account Details",
                "icon": "💳",
                "purpose": "Uniquely identifies each bank account in the system",
                "simple_rules": [
                    "Account number must be unique",
                    "Numeric only (10-18 digits)",
                    "Cannot be changed once created"
                ],
                "explanation": """The account_number is the most critical identifier in banking operations, serving as a unique reference for every bank account. This number is automatically generated when a new account is opened and follows specific banking standards (typically 10-18 digits). The account number is used in all financial transactions including deposits, withdrawals, fund transfers, and statement generation. Banks use account numbers to route transactions, maintain ledgers, and reconcile balances. Each account number is globally unique within the bank's system and often includes embedded information like branch code, account type code, and a sequential customer number.""",
                "business_workflow": "When a customer opens a new account, the core banking system generates a unique account number based on the branch code, product type, and sequential numbering. This number is printed on checkbooks, debit cards, and passbooks.",
                "data_rules": [
                    "Must be unique across all accounts in the bank",
                    "Typically 10-18 digits (numeric only)",
                    "Cannot be null or empty",
                    "Cannot contain letters or special characters",
                    "Once assigned, never changes for that account",
                    "Used as primary key for account transactions"
                ]
            },
            "account_type": {
                "title": "Account Classification Rule",
                "section": "Account Details",
                "icon": "💳",
                "purpose": "Categorizes accounts based on their operational characteristics",
                "simple_rules": [
                    "Must be valid type (Savings, Current, etc.)",
                    "Determines interest and fee structure",
                    "Controls transaction limits"
                ],
                "explanation": """The account_type field classifies bank accounts into different categories, each with distinct features, rules, and benefits. Common types include Savings (for personal savings with interest), Current (for business transactions with no interest), Salary (for employee salary credits), Fixed Deposit (for locked-in deposits with higher interest), and Recurring Deposit (for regular monthly savings). Each account type has specific regulations regarding minimum balance, transaction limits, interest rates, and withdrawal restrictions. The account type determines what services are available, what fees apply, and how the account operates on a daily basis.""",
                "business_workflow": "Customers choose account type based on their needs. Savings accounts are for individuals to save money, current accounts are for businesses with frequent transactions, and salary accounts are opened by employers for their employees.",
                "data_rules": [
                    "Must be one of predefined types (Savings, Current, Salary, etc.)",
                    "Cannot be null",
                    "Determines interest rate calculation",
                    "Controls transaction limits and fees",
                    "May restrict certain operations based on type",
                    "Can be changed only through formal application"
                ]
            },
            "account_status": {
                "title": "Account Status Management Rule",
                "section": "Account Details",
                "icon": "💳",
                "purpose": "Indicates whether an account is operational or restricted",
                "simple_rules": [
                    "Account status must be Active for transactions",
                    "Frozen accounts block withdrawals",
                    "Closed accounts cannot be reactivated"
                ],
                "explanation": """The account_status field tracks the current operational state of a bank account. An 'Active' status means the account is fully functional for all transactions. 'Inactive' typically indicates the account hasn't been used for a specific period but can be reactivated. 'Frozen' means the account is temporarily blocked due to suspicious activity, court orders, or customer request. 'Closed' indicates the account has been permanently shut down and cannot be reactivated. This status directly controls what operations can be performed - active accounts allow deposits and withdrawals, frozen accounts may allow deposits only, and closed accounts allow no transactions. The status is crucial for compliance, fraud prevention, and customer service.""",
                "business_workflow": "New accounts start as Active. If unused for 2+ years, they may become Inactive. Bank can freeze accounts for investigation. Customers can request closure, after which the account becomes Closed permanently.",
                "data_rules": [
                    "Must be one of: Active, Inactive, Frozen, Closed",
                    "Cannot be null",
                    "Active accounts allow all transactions",
                    "Frozen accounts block most operations",
                    "Closed accounts cannot process any transactions",
                    "Status changes must be logged with reasons"
                ]
            },
            "balance": {
                "title": "Account Balance Calculation Rule",
                "section": "Account Details",
                "icon": "💳",
                "purpose": "Maintains the current available amount in the account",
                "simple_rules": [
                    "Balance cannot be below minimum requirement",
                    "Updated after each transaction",
                    "Negative balance triggers overdraft alert"
                ],
                "explanation": """The balance represents the current amount of money available in the account after considering all credits (deposits) and debits (withdrawals). This is a calculated field that changes with every transaction. For savings accounts, the balance earns interest. For current accounts, it must typically stay above a minimum threshold to avoid penalties. The balance is calculated using the formula: Current Balance = Opening Balance + Total Credits - Total Debits. Banks maintain this in real-time to prevent overdrafts and ensure customers have accurate information. The balance is shown on every bank statement, ATM receipt, and mobile banking app. Negative balances may trigger overdraft fees or account restrictions.""",
                "business_workflow": "Every transaction updates the balance. When a customer deposits cash, balance increases. When they withdraw or transfer money, balance decreases. Interest is added monthly based on the average balance.",
                "data_rules": [
                    "Must be numeric (decimal allowed for cents/paise)",
                    "Can be positive, zero, or negative (if overdraft allowed)",
                    "Updated in real-time with every transaction",
                    "Minimum balance requirements vary by account type",
                    "Calculated as: opening + credits - debits",
                    "Precision typically up to 2 decimal places"
                ]
            },
            "transaction_id": {
                "title": "Transaction Uniqueness Rule",
                "section": "Transaction Details",
                "icon": "💸",
                "purpose": "Uniquely identifies each financial transaction",
                "simple_rules": [
                    "Transaction ID must be unique",
                    "Generated automatically for each transaction",
                    "Used for tracking and disputes"
                ],
                "explanation": """The transaction_id is a unique identifier assigned to every financial transaction that occurs in the banking system. This ID ensures traceability, auditability, and non-repudiation. Each deposit, withdrawal, transfer, or payment gets a unique transaction ID that can be used for tracking, disputes, and reconciliation. Transaction IDs are sequential or timestamp-based to ensure uniqueness and are often visible to customers on receipts, statements, and transaction confirmations. This identifier is crucial for customer service inquiries ("What happened to transaction TX12345?"), fraud investigation, and regulatory compliance. Once assigned, a transaction ID never changes and serves as a permanent record of that specific financial event.""",
                "business_workflow": "When a customer performs any transaction (ATM withdrawal, online transfer, branch deposit), the system generates a unique transaction ID and provides it as a reference number for the customer to track.",
                "data_rules": [
                    "Must be unique across all transactions",
                    "Typically alphanumeric (TX12345, TRN20240115001)",
                    "Cannot be null",
                    "Generated automatically by the system",
                    "Includes date/time information (often encoded)",
                    "Used for transaction tracking and disputes"
                ]
            },
            "transaction_date": {
                "title": "Transaction Timing Rule",
                "section": "Transaction Details",
                "icon": "💸",
                "purpose": "Records when a transaction actually occurred",
                "simple_rules": [
                    "Date must be valid and not in future",
                    "Used for statement generation",
                    "Critical for interest calculation"
                ],
                "explanation": """The transaction_date captures the exact date (and often time) when a financial transaction was executed. This timestamp is critical for interest calculations, statement generation, audit trails, and dispute resolution. The date helps determine the order of transactions, calculate end-of-day balances, and generate monthly statements. For interest-bearing accounts, the transaction date determines from when interest starts or stops. Banks use this field to track transaction patterns, detect fraud (unusual late-night transactions), and comply with regulatory reporting deadlines. The date must reflect the actual processing time, not just when the customer initiated the request, as some transactions may be processed the next business day.""",
                "business_workflow": "Every transaction is timestamped when processed. ATM withdrawals show real-time date. Checks may show the date when cleared. Online transfers show when the bank processed them, which might be next business day.",
                "data_rules": [
                    "Must be valid date format (YYYY-MM-DD or DD/MM/YYYY)",
                    "Cannot be null",
                    "Cannot be future date (except scheduled transactions)",
                    "Used for chronological transaction ordering",
                    "Critical for interest calculation",
                    "Determines which statement period it appears in"
                ]
            },
            "transaction_type": {
                "title": "Transaction Classification Rule",
                "section": "Transaction Details",
                "icon": "💸",
                "purpose": "Categorizes transactions by their operational nature",
                "simple_rules": [
                    "Must be valid type (Deposit, Withdrawal, Transfer)",
                    "Determines debit or credit application",
                    "Used for fraud detection patterns"
                ],
                "explanation": """The transaction_type field classifies each transaction into predefined categories like Deposit, Withdrawal, Transfer, Payment, Interest Credit, or Fee Debit. This classification helps customers understand their account activity, enables banks to generate meaningful reports, and supports fraud detection systems. For example, multiple 'Withdrawal' transactions at odd hours might indicate card theft. The transaction type determines how the amount is applied (debit or credit), what limits apply, and how it's displayed on statements. This field also drives automated processes like tax reporting (interest credits must be reported to tax authorities) and customer notifications (large withdrawals trigger SMS alerts).""",
                "business_workflow": "When a customer visits an ATM and withdraws cash, the system records it as 'Withdrawal'. When they receive salary, it's recorded as 'Deposit' or 'Credit'. When they pay a bill online, it's recorded as 'Payment' or 'Transfer'.",
                "data_rules": [
                    "Must be from predefined list (Deposit, Withdrawal, Transfer, etc.)",
                    "Cannot be null",
                    "Determines if amount is debited or credited",
                    "Used for transaction categorization in statements",
                    "Affects account balance calculation logic",
                    "Used in fraud detection and spending analysis"
                ]
            },
            "debit": {
                "title": "Debit Amount Validation Rule",
                "section": "Transaction Details",
                "icon": "💸",
                "purpose": "Records money going out of the account",
                "simple_rules": [
                    "Amount must be available before transfer",
                    "Must be positive value or zero",
                    "Large amounts may trigger alerts"
                ],
                "explanation": """The debit column contains the amount of money leaving the account in a transaction. In double-entry bookkeeping used by banks, debits represent outflows like withdrawals, transfers to other accounts, bill payments, and fees. When a customer withdraws cash from an ATM, buys something with their debit card, or pays a bill online, the amount appears in the debit column. This field is mutually exclusive with the credit column - a single transaction row will have either a debit value OR a credit value, but not both. The sum of all debits reduces the account balance. Banks monitor debit patterns to detect fraud, enforce daily withdrawal limits, and maintain sufficient balances.""",
                "business_workflow": "When a customer withdraws ₹5000 from ATM, the transaction record shows ₹5000 in the debit column and 0 in credit column. The account balance decreases by ₹5000.",
                "data_rules": [
                    "Must be numeric (positive values only)",
                    "Cannot be negative",
                    "Zero is allowed (indicates credit transaction)",
                    "Mutually exclusive with credit (one must be zero)",
                    "Reduces account balance when applied",
                    "Precision up to 2 decimal places for currency"
                ]
            },
            "credit": {
                "title": "Credit Amount Validation Rule",
                "section": "Transaction Details",
                "icon": "💸",
                "purpose": "Records money coming into the account",
                "simple_rules": [
                    "Increases account balance",
                    "Must be positive value or zero",
                    "Regular credits indicate stable income"
                ],
                "explanation": """The credit column records amounts being added to the account through deposits, incoming transfers, salary credits, interest additions, or refunds. When a customer deposits cash, receives their monthly salary, or gets a refund for a returned product, the amount appears in the credit column. Like the debit field, this follows double-entry bookkeeping principles. A transaction will have either a credit value OR a debit value, never both. The sum of all credits increases the account balance. Banks use credit patterns for customer profiling (regular salary credits indicate stable income), fraud detection (sudden large credits might indicate money laundering), and relationship building (high-value customers get special services).""",
                "business_workflow": "When an employer transfers ₹50,000 as salary to the employee's account, the transaction shows ₹50,000 in the credit column and 0 in debit column. The account balance increases by ₹50,000.",
                "data_rules": [
                    "Must be numeric (positive values only)",
                    "Cannot be negative",
                    "Zero is allowed (indicates debit transaction)",
                    "Mutually exclusive with debit (one must be zero)",
                    "Increases account balance when applied",
                    "Precision up to 2 decimal places for currency"
                ]
            },
            "opening_balance": {
                "title": "Opening Balance Initialization Rule",
                "section": "Account Details",
                "icon": "💳",
                "purpose": "Records the starting balance for a statement period",
                "simple_rules": [
                    "Equals previous period's closing balance",
                    "Used for reconciliation",
                    "Cannot change once period starts"
                ],
                "explanation": """The opening_balance represents the account's balance at the start of a specific period (beginning of day, month, or statement cycle). This is essential for reconciliation and statement generation. For daily processing, the opening balance is yesterday's closing balance. For monthly statements, it's the balance at the end of the previous month. This field helps customers verify their statements - they can calculate: Closing Balance = Opening Balance + Total Credits - Total Debits. If this doesn't match, there's an error. Banks use opening balance for audit trails, ensuring no money is created or lost in the system. For new accounts, the opening balance is zero (or the initial deposit amount if required).""",
                "business_workflow": "At the start of each day, the system copies yesterday's closing balance as today's opening balance. When generating monthly statements, it shows the balance from the last day of previous month as opening balance.",
                "data_rules": [
                    "Must be numeric (can be positive, zero, or negative)",
                    "Must equal previous period's closing balance",
                    "Used as starting point for balance calculations",
                    "Critical for reconciliation and auditing",
                    "Should not change once period starts",
                    "New accounts start with zero or initial deposit"
                ]
            },
            "closing_balance": {
                "title": "Closing Balance Calculation Rule",
                "purpose": "Shows the final balance after all period transactions",
                "explanation": """The closing_balance is the calculated result of applying all transactions to the opening balance. It represents what the customer has in their account at the end of a specific period. The fundamental banking equation is: Closing Balance = Opening Balance + Total Credits - Total Debits. This calculation must balance perfectly, or it indicates data corruption or fraud. The closing balance becomes next period's opening balance, creating an unbroken chain of accountability. Banks verify closing balances multiple times daily for accuracy. This field appears prominently on bank statements, passbooks, and mobile apps, showing customers their current financial position. Discrepancies in closing balance calculations trigger immediate investigation.""",
                "business_workflow": "At end of each day, the system calculates: Today's Closing Balance = Today's Opening Balance + All Credits Today - All Debits Today. This closing balance becomes tomorrow's opening balance.",
                "data_rules": [
                    "Must be calculated, not manually entered",
                    "Formula: Opening + Credits - Debits",
                    "Must match to the penny/paisa",
                    "Becomes next period's opening balance",
                    "Used for minimum balance checks",
                    "Critical for interest calculation and reporting"
                ]
            },
            "loan_id": {
                "title": "Loan Identification Rule",
                "section": "Loan Details",
                "icon": "🏛️",
                "purpose": "Uniquely identifies each loan account",
                "simple_rules": [
                    "Loan ID must be unique",
                    "Links to borrower's account",
                    "Used for EMI tracking"
                ],
                "explanation": """The loan_id is a unique identifier for each loan disbursed by the bank. This ID links to the customer's account and tracks all aspects of the loan including principal amount, interest charges, EMI payments, and outstanding balance. Each loan gets a unique identifier when approved and disbursed.""",
                "business_workflow": "When a loan is approved, the system generates a unique loan ID that appears on all loan documents, EMI receipts, and statements.",
                "data_rules": [
                    "Must be unique across all loans",
                    "Typically alphanumeric format",
                    "Cannot be null",
                    "Links to customer account",
                    "Used for EMI deduction tracking"
                ]
            },
            "emi_amount": {
                "title": "EMI Payment Rule",
                "section": "Loan Details",
                "icon": "🏛️",
                "purpose": "Monthly installment payment amount",
                "simple_rules": [
                    "Loan amount and tenure fixed upfront",
                    "EMI calculated based on interest rate",
                    "Must be paid monthly"
                ],
                "explanation": """The emi_amount is the fixed monthly installment that borrowers must pay to repay their loan. This amount is calculated based on the loan amount, interest rate, and tenure using EMI formula.""",
                "business_workflow": "EMI is auto-debited from customer's account on a fixed date each month. Formula: EMI = [P x R x (1+R)^N]/[(1+R)^N-1]",
                "data_rules": [
                    "Must be positive numeric value",
                    "Calculated at loan disbursement",
                    "Fixed for loan tenure",
                    "Includes principal + interest"
                ]
            },
            "interest_rate": {
                "title": "Interest Rate Rule",
                "section": "Loan Details",
                "icon": "🏛️",
                "purpose": "Annual interest rate percentage",
                "simple_rules": [
                    "Eligibility based on income & credit score",
                    "Rate determined by loan type",
                    "Affects total repayment amount"
                ],
                "explanation": """The interest_rate determines how much extra the borrower pays on top of the principal amount. It's expressed as annual percentage and varies based on credit score, loan type, and market conditions.""",
                "business_workflow": "Interest rates are set based on customer's credit score, income level, and loan product. Higher credit scores get lower rates.",
                "data_rules": [
                    "Must be between 0-100%",
                    "Typically 6-20% for personal loans",
                    "Cannot be negative",
                    "Determines EMI calculation"
                ]
            },
            "card_number": {
                "title": "Card Number Security Rule",
                "section": "Card Details",
                "icon": "💳",
                "purpose": "Unique identifier for credit/debit cards",
                "simple_rules": [
                    "Card number must be secure",
                    "16 digits following Luhn algorithm",
                    "Unique per card issued"
                ],
                "explanation": """The card_number is a 16-digit unique identifier embossed on credit and debit cards. It follows international standards (Luhn algorithm) and is used for all card transactions.""",
                "business_workflow": "When a card is issued, system generates a valid 16-digit number. First 6 digits identify the bank, remaining digits identify the account and card.",
                "data_rules": [
                    "Must be exactly 16 digits",
                    "Must pass Luhn check",
                    "Cannot be null",
                    "Unique per card",
                    "Encrypted in storage"
                ]
            },
            "card_expiry": {
                "title": "Card Expiry Validation Rule",
                "section": "Card Details",
                "icon": "💳",
                "purpose": "Valid thru date on cards",
                "simple_rules": [
                    "Valid thru date must be correct",
                    "Cards expire after 3-5 years",
                    "Expired cards cannot transact"
                ],
                "explanation": """The card expiry date indicates until when the card is valid for transactions. Cards are issued with typically 3-5 year validity and must be renewed before expiry.""",
                "business_workflow": "System checks expiry date for every transaction. Expired cards are automatically blocked. New card issued before expiry.",
                "data_rules": [
                    "Format: MM/YY",
                    "Cannot be in the past",
                    "Required for all transactions",
                    "Renewed before expiry"
                ]
            },
            "credit_limit": {
                "title": "Credit Limit Management Rule",
                "section": "Card Details",
                "icon": "💳",
                "purpose": "Maximum credit allowed on card",
                "simple_rules": [
                    "Credit limit cannot be exceeded",
                    "Based on income and credit score",
                    "Available limit reduces with transactions"
                ],
                "explanation": """The credit_limit is the maximum amount a cardholder can borrow on their credit card. It's determined by income, credit score, and payment history.""",
                "business_workflow": "Bank sets credit limit at card issuance. Every transaction reduces available limit. Payments restore the limit.",
                "data_rules": [
                    "Must be positive amount",
                    "Cannot exceed approved limit",
                    "Transactions blocked if limit exceeded",
                    "Can be increased based on usage"
                ]
            },
            "ifsc_code": {
                "title": "IFSC Code Validation Rule",
                "section": "Account Details",
                "icon": "🏛️",
                "purpose": "Identifies the specific bank branch for transactions",
                "simple_rules": [
                    "11-character alphanumeric code",
                    "Required for fund transfers",
                    "Identifies bank and branch"
                ],
                "explanation": """The IFSC (Indian Financial System Code) is an 11-character alphanumeric code that uniquely identifies each bank branch in India. The first 4 characters represent the bank code, the 5th character is always 0 (reserved for future use), and the last 6 characters identify the specific branch. This code is mandatory for all electronic fund transfers including NEFT, RTGS, and IMPS. When a customer wants to receive money from another bank, they provide their account number and IFSC code. The code ensures money reaches exactly the right branch and account. Every branch has exactly one IFSC code, and the Reserve Bank of India maintains the master list of all IFSC codes. Wrong IFSC codes cause transaction failures or money going to wrong branches.""",
                "business_workflow": "When setting up online transfers, customers enter recipient's account number and IFSC code. The banking system uses this to route money to the correct branch. For example, SBIN0001234 represents State Bank of India, branch number 001234.",
                "data_rules": [
                    "Must be exactly 11 characters",
                    "First 4 characters: alphabetic (bank code)",
                    "5th character: always zero",
                    "Last 6 characters: alphanumeric (branch code)",
                    "Must match RBI-approved codes",
                    "Required for all electronic transfers"
                ]
            },
            "branch_code": {
                "title": "Branch Identification Rule",
                "section": "Account Details",
                "icon": "🏛️",
                "purpose": "Links accounts to their home branch",
                "simple_rules": [
                    "Links account to physical branch",
                    "Used for branch-wise reporting",
                    "Cannot change without transfer request"
                ],
                "explanation": """The branch_code identifies which physical bank branch an account belongs to. This code is crucial for operations, reporting, and customer service. Each branch has a unique code assigned by the bank's head office. When customers open an account, it's tagged with their branch code. This determines which branch handles their passbook updates, checkbook requests, and in-person services. Branch codes are used in reports to track performance (which branch has highest deposits), resource allocation (busy branches get more staff), and compliance (each branch must maintain certain ratios). Some banks use geographic branch codes (Mumbai branches start with 'MUM'), while others use sequential numbers.""",  
                "business_workflow": "When a customer opens account at ABC Bank's Connaught Place branch, their account gets branch code 'CP001'. All their account statements, checkbooks, and correspondence show this branch code for identification.",
                "data_rules": [
                    "Must match bank's branch master list",
                    "Typically 3-6 characters (letters or numbers)",
                    "Cannot be null for all accounts",
                    "Links account to physical branch location",
                    "Used in branch performance reporting",
                    "May be embedded in account number structure"
                ]
            }
        }
    
    def generate_table_business_rules(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Generate comprehensive business rules explanation for an entire table
        
        Args:
            df: DataFrame containing the table data
            table_name: Name of the table/file
            
        Returns:
            Dictionary with table-level business rules and column rules
        """
        rules = {
            "table_name": table_name,
            "overall_purpose": self._detect_table_purpose(df, table_name),
            "business_context": self._generate_business_context(df, table_name),
            "column_rules": [],
            "data_quality_summary": self._generate_data_quality_summary(df)
        }
        
        # Generate rules for each column
        for column in df.columns:
            column_rule = self.explain_column_business_meaning(column, df[column])
            rules["column_rules"].append(column_rule)
        
        return rules
    
    def explain_column_business_meaning(self, column_name: str, data_series: pd.Series) -> Dict[str, Any]:
        """
        Generate detailed business explanation for a single column
        
        Args:
            column_name: Name of the column
            data_series: Pandas Series containing the column data
            
        Returns:
            Dictionary with comprehensive column explanation
        """
        # Normalize column name for matching
        normalized_name = self._normalize_column_name(column_name)
        
        # Find matching business rule
        matched_rule = None
        for rule_key, rule_def in self.column_business_rules.items():
            if rule_key in normalized_name or normalized_name in rule_key:
                matched_rule = rule_def
                break
        
        if matched_rule:
            explanation = {
                "column_name": column_name,
                "title": matched_rule["title"],
                "purpose": matched_rule["purpose"],
                "detailed_explanation": matched_rule["explanation"],
                "business_workflow": matched_rule["business_workflow"],
                "data_rules": matched_rule["data_rules"],
                "data_statistics": self._calculate_column_statistics(data_series),
                "quality_score": self._calculate_quality_score(data_series, matched_rule["data_rules"])
            }
        else:
            # Generate generic explanation for unknown columns
            explanation = self._generate_generic_explanation(column_name, data_series)
        
        return explanation
    
    def generate_relationship_explanation(self, parent_table: str, child_table: str, 
                                         connection_column: str, overlap_ratio: float) -> str:
        """
        Generate paragraph explaining how two tables are connected
        
        Args:
            parent_table: Name of parent table
            child_table: Name of child table
            connection_column: Column that connects them
            overlap_ratio: How much data overlaps (0.0 to 1.0)
            
        Returns:
            Formatted paragraph explanation
        """
        # Clean table names (remove file extensions)
        parent_clean = parent_table.replace('.csv', '').replace('_', ' ').title()
        child_clean = child_table.replace('.csv', '').replace('_', ' ').title()
        
        # Determine relationship strength
        if overlap_ratio >= 0.9:
            strength = "strong"
            reliability = "very reliable"
        elif overlap_ratio >= 0.7:
            strength = "good"
            reliability = "reliable"
        elif overlap_ratio >= 0.5:
            strength = "moderate"
            reliability = "reasonably reliable"
        else:
            strength = "weak"
            reliability = "potentially unreliable"
        
        # Generate explanation paragraph
        explanation = f"""The **{parent_clean}** table connects to the **{child_clean}** table through the **{connection_column}** column. 
This creates a {strength} relationship ({int(overlap_ratio * 100)}% data overlap) between the two tables. 
In business terms, this means that each record in {parent_clean} can be linked to one or more records in {child_clean} 
using the {connection_column} as the common identifier. This connection is {reliability} for data analysis and reporting purposes. 

**Business Impact**: When you look up a {connection_column} in the {parent_clean} table, you can find all related 
records in the {child_clean} table, allowing you to see the complete picture of how these data sets interact."""
        
        return explanation
    
    def format_rules_as_paragraphs(self, table_rules: Dict[str, Any]) -> str:
        """
        Format all table rules as readable paragraphs
        
        Args:
            table_rules: Dictionary from generate_table_business_rules()
            
        Returns:
            Formatted text with all rules as paragraphs
        """
        output = []
        
        # Table overview
        output.append(f"# Business Rules for {table_rules['table_name']}\n")
        output.append(f"## Purpose\n{table_rules['overall_purpose']}\n")
        output.append(f"## Business Context\n{table_rules['business_context']}\n")
        
        # Column rules
        output.append("## Column-Level Business Rules\n")
        for col_rule in table_rules['column_rules']:
            output.append(f"### {col_rule['column_name']}: {col_rule['title']}\n")
            output.append(f"**Purpose**: {col_rule['purpose']}\n")
            output.append(f"**Explanation**: {col_rule['detailed_explanation']}\n")
            
            if 'business_workflow' in col_rule:
                output.append(f"**How It's Used**: {col_rule['business_workflow']}\n")
            
            if 'data_rules' in col_rule and col_rule['data_rules']:
                output.append("**Data Rules**:")
                for rule in col_rule['data_rules']:
                    output.append(f"- {rule}")
                output.append("")
        
        # Data quality summary
        output.append(f"## Data Quality Summary\n{table_rules['data_quality_summary']}\n")
        
        return "\n".join(output)
    
    # Helper methods
    
    def _normalize_column_name(self, column_name: str) -> str:
        """Normalize column name for matching"""
        return column_name.lower().replace('_', '').replace(' ', '').replace('-', '')
    
    def _detect_table_purpose(self, df: pd.DataFrame, table_name: str) -> str:
        """Detect what business purpose the table serves"""
        columns = [col.lower() for col in df.columns]
        
        # Check for customer data
        customer_indicators = ['customer', 'client', 'name', 'email', 'phone']
        if any(ind in ' '.join(columns) for ind in customer_indicators):
            if 'transaction' not in ' '.join(columns):
                return "This table stores customer master data, containing information about bank customers and their profiles."
        
        # Check for account data
        account_indicators = ['account', 'balance', 'type', 'status']
        if any(ind in ' '.join(columns) for ind in account_indicators):
            return "This table manages bank account information, tracking account details, balances, and status."
        
        # Check for transaction data
        transaction_indicators = ['transaction', 'debit', 'credit', 'amount', 'payment']
        if any(ind in ' '.join(columns) for ind in transaction_indicators):
            return "This table records all financial transactions, capturing every debit and credit operation."
        
        # Generic
        return f"This table ({table_name}) contains {len(df.columns)} columns and {len(df)} records for banking operations."
    
    def _generate_business_context(self, df: pd.DataFrame, table_name: str) -> str:
        """Generate business context explanation"""
        return f"""This data table is part of the banking system and contains {len(df)} rows and {len(df.columns)} columns. 
Each row represents a distinct record, and the columns capture different attributes relevant to banking operations. 
The data in this table is used for daily operations, reporting, compliance, and decision-making within the bank."""
    
    def _calculate_column_statistics(self, series: pd.Series) -> Dict[str, Any]:
        """Calculate basic statistics for a column"""
        # Convert numpy types to Python native types for JSON serialization
        stats = {
            "total_records": int(len(series)),
            "non_null_count": int(series.notna().sum()),
            "null_count": int(series.isna().sum()),
            "null_percentage": round(float(series.isna().sum() / len(series)) * 100, 2) if len(series) > 0 else 0.0,
            "unique_count": int(series.nunique()),
            "uniqueness_percentage": round(float(series.nunique() / len(series)) * 100, 2) if len(series) > 0 else 0.0
        }
        
        # Add numeric statistics if applicable
        if pd.api.types.is_numeric_dtype(series):
            non_null = series.dropna()
            if len(non_null) > 0:
                stats["min_value"] = float(non_null.min())
                stats["max_value"] = float(non_null.max())
                stats["mean_value"] = round(float(non_null.mean()), 2)
        
        return stats
    
    def _calculate_quality_score(self, series: pd.Series, expected_rules: List[str]) -> int:
        """Calculate data quality score (0-100)"""
        score = 100
        
        # Penalize for null values
        null_ratio = series.isna().sum() / len(series) if len(series) > 0 else 0
        score -= int(null_ratio * 30)  # Max 30 points penalty
        
        # Check uniqueness for ID fields
        if any(keyword in str(series.name).lower() for keyword in ['id', 'number', 'code']):
            uniqueness = series.nunique() / len(series) if len(series) > 0 else 0
            if uniqueness < 0.95:
                score -= 20
        
        return max(0, min(100, score))
    
    def _generate_generic_explanation(self, column_name: str, series: pd.Series) -> Dict[str, Any]:
        """Generate basic explanation for columns without predefined rules"""
        # Detect column type
        column_type = "Unknown"
        if pd.api.types.is_numeric_dtype(series):
            column_type = "Numeric Amount or Identifier"
        elif pd.api.types.is_datetime64_any_dtype(series):
            column_type = "Date/Time Field"
        else:
            column_type = "Text/Categorical Field"
        
        return {
            "column_name": column_name,
            "title": f"{column_name} Data Field",
            "purpose": f"Stores {column_type.lower()} information",
            "detailed_explanation": f"""The {column_name} column contains {column_type.lower()} data that is part of this banking table. 
Based on the data patterns, this field appears to store information relevant to banking operations. The exact business meaning 
should be confirmed with domain experts or documentation.""",
            "business_workflow": f"This field is used in banking operations where {column_name} information is required.",
            "data_rules": [
                f"Data type: {column_type}",
                f"Contains {series.notna().sum()} non-null values out of {len(series)} total records"
            ],
            "data_statistics": self._calculate_column_statistics(series),
            "quality_score": self._calculate_quality_score(series, [])
        }
    
    def _generate_data_quality_summary(self, df: pd.DataFrame) -> str:
        """Generate overall data quality summary for the table"""
        total_cells = len(df) * len(df.columns)
        null_cells = df.isnull().sum().sum()
        completeness = round(((total_cells - null_cells) / total_cells) * 100, 2) if total_cells > 0 else 0
        
        return f"""**Data Completeness**: {completeness}% of all data fields are filled (non-null). 
The table has {len(df)} rows and {len(df.columns)} columns, making a total of {total_cells} data points. 
Out of these, {null_cells} are missing/null. Good data quality is essential for accurate business insights and decision-making."""
