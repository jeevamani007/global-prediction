"""
CORE BANKING DOMAIN BUSINESS RULES ENGINE (FIXED)

STRICT PROCESS (DO NOT SKIP):
STEP 1: COLUMN PROFILING (MANDATORY)
STEP 2: IDENTIFIER ELIGIBILITY CHECK (CRITICAL)
STEP 3: BANKING CONCEPT IDENTIFICATION
STEP 4: CONFIDENCE SCORING
STEP 5: APPLY REAL BANKING BUSINESS RULES
STEP 6: BANKING DATA WORKFLOW EXPLANATION
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
import re
from datetime import datetime
from column_definitions_parser import get_column_parser, get_column_definition


class CoreBankingBusinessRulesEngine:
    """
    CORE BANKING DOMAIN BUSINESS RULES ENGINE
    
    Analyzes banking datasets and applies industry-standard business rules.
    Follows strict 6-step process for each column.
    """
    
    def __init__(self):
        # Banking concept definitions with identification patterns
        self.banking_concepts = self._initialize_banking_concepts()
        # Track dataset context for table-aware rules
        self.dataset_context = {
            "detected_tables": [],
            "primary_keys": {},
            "foreign_keys": {}
        }
        
    def _initialize_banking_concepts(self) -> Dict[str, Dict]:
        """Define all supported banking concepts with identification rules"""
        return {
            # CUSTOMER DOMAIN
            "customer_id": {
                "domain": "Customer",
                "name_patterns": ["customer_id", "cust_id", "client_id", "c_id", "customer_number","CustomerId"],
                "data_patterns": {
                    "type": ["numeric", "alphanumeric"],
                    "uniqueness": "high",  # ≥95%
                    "length": {"min": 4, "max": 20},
                    "nullable": False
                },
                "is_identifier": True,
                "table_role": {"Customer Master": "PK", "Account": "FK", "Loan": "FK", "Transaction": "FK"},
                "business_rules": {
                    "unique": True,
                    "mandatory": True,
                    "primary_key": True,
                    "foreign_key": True,
                    "format": "Alphanumeric, 4-20 characters",
                    "allowed_values": None,
                    "reason": "Primary identifier for all customer operations. Must be unique to prevent data corruption.",
                    "violation_impact": "BUSINESS: Cannot link accounts/transactions. FINANCIAL: Risk of duplicate customers. COMPLIANCE: KYC violations."
                }
            },
            "customer_name": {
                "domain": "Customer",
                "name_patterns": ["customer_name", "cust_name", "name", "full_name", "account_holder_name"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "low",  # Names can repeat
                    "length": {"min": 3, "max": 100},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Customer Master": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Alphabetic characters and spaces only",
                    "allowed_values": None,
                    "reason": "Legal name for KYC compliance. Must match government ID documents.",
                    "violation_impact": "BUSINESS: Cannot verify customer identity. COMPLIANCE: KYC/AML violations. FINANCIAL: Legal liability."
                }
            },
            "phone_number": {
                "domain": "Customer",
                "name_patterns": ["phone", "mobile", "contact_number", "phone_no", "mobile_number"],
                "data_patterns": {
                    "type": ["numeric"],
                    "uniqueness": "medium",
                    "length": {"min": 10, "max": 15},
                    "nullable": False
                },
                "is_identifier": False,  # Contact field, never PK
                "table_role": {"Customer Master": "contact"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "10-15 digits, numeric only",
                    "allowed_values": None,
                    "reason": "Required for OTP verification, transaction alerts, and customer communication.",
                    "violation_impact": "BUSINESS: Cannot send security alerts. FINANCIAL: Increased fraud risk. COMPLIANCE: Communication failures."
                }
            },
            "email": {
                "domain": "Customer",
                "name_patterns": ["email", "email_address", "e_mail"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "medium",
                    "length": {"min": 5, "max": 100},
                    "nullable": True
                },
                "is_identifier": False,  # Contact field, never PK
                "table_role": {"Customer Master": "contact"},
                "business_rules": {
                    "unique": False,
                    "mandatory": False,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Valid email format",
                    "allowed_values": None,
                    "reason": "Optional contact method for statements and notifications.",
                    "violation_impact": "BUSINESS: Cannot send email notifications. COMPLIANCE: Communication preference not honored."
                }
            },
            "address": {
                "domain": "Customer",
                "name_patterns": ["address", "street_address", "residential_address"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "low",
                    "length": {"min": 10, "max": 200},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Customer Master": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Alphanumeric with spaces, commas, and special characters",
                    "allowed_values": None,
                    "reason": "Required for KYC verification and communication.",
                    "violation_impact": "BUSINESS: Cannot verify customer location. COMPLIANCE: KYC documentation incomplete."
                }
            },
            "city": {
                "domain": "Customer",
                "name_patterns": ["city", "city_name"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "low",
                    "length": {"min": 2, "max": 50},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Customer Master": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Alphabetic characters and spaces",
                    "allowed_values": None,
                    "reason": "Required for geographic reporting and branch assignment.",
                    "violation_impact": "BUSINESS: Cannot assign branch. COMPLIANCE: Geographic reporting incomplete."
                }
            },
            "dob": {
                "domain": "Customer",
                "name_patterns": ["dob", "date_of_birth", "birth_date", "birthdate"],
                "data_patterns": {
                    "type": ["date"],
                    "uniqueness": "low",
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Customer Master": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Valid date (YYYY-MM-DD or DD/MM/YYYY), must be in the past",
                    "allowed_values": None,
                    "reason": "Customer date of birth required for age verification, KYC compliance, and age-based product eligibility.",
                    "violation_impact": "BUSINESS: Cannot verify customer age. COMPLIANCE: KYC documentation incomplete. FINANCIAL: Age-based product violations."
                }
            },
            "state": {
                "domain": "Customer",
                "name_patterns": ["state", "state_name", "province"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "low",
                    "length": {"min": 2, "max": 50},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Customer Master": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Alphabetic characters and spaces",
                    "allowed_values": None,
                    "reason": "Customer state required for geographic reporting, regulatory compliance, and state-specific banking regulations.",
                    "violation_impact": "BUSINESS: Geographic reporting incomplete. COMPLIANCE: State-specific regulatory violations. FINANCIAL: Tax reporting errors."
                }
            },
            "country": {
                "domain": "Customer",
                "name_patterns": ["country", "country_name", "nation"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "very_low",
                    "length": {"min": 2, "max": 50},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Customer Master": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Country name or ISO code (e.g., India, USA, IN, US)",
                    "allowed_values": None,
                    "reason": "Customer country required for international compliance, KYC verification, and cross-border transaction regulations.",
                    "violation_impact": "BUSINESS: International compliance failures. COMPLIANCE: KYC/AML violations. FINANCIAL: Cross-border transaction restrictions."
                }
            },
            "customer_type": {
                "domain": "Customer",
                "name_patterns": ["customer_type", "cust_type", "client_type", "customer_category", "customer_segment"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "very_low",
                    "cardinality": {"max": 10},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Customer Master": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Predefined values: Regular, VIP, Premium, Corporate, Individual, Business",
                    "allowed_values": ["Regular", "VIP", "Premium", "Corporate", "Individual", "Business", "Student", "Senior"],
                    "reason": "Customer category determines service levels, fee structures, transaction limits, and product eligibility.",
                    "violation_impact": "BUSINESS: Incorrect service levels applied. FINANCIAL: Fee calculation errors. COMPLIANCE: Customer segmentation violations."
                }
            },
            
            # ACCOUNT DOMAIN
            "account_number": {
                "domain": "Account",
                "name_patterns": ["account_number", "acc_no", "account_no", "acct_num", "account_id"],
                "data_patterns": {
                    "type": ["numeric"],
                    "uniqueness": "very_high",  # 100% unique globally
                    "length": {"min": 10, "max": 18},
                    "nullable": False
                },
                "is_identifier": True,
                "table_role": {"Account Master": "PK", "Transaction": "FK"},
                "business_rules": {
                    "unique": True,
                    "mandatory": True,
                    "primary_key": True,
                    "foreign_key": True,
                    "format": "Numeric, 10-18 digits, globally unique",
                    "allowed_values": None,
                    "reason": "Globally unique identifier for every bank account. Used in all transactions and statements.",
                    "violation_impact": "BUSINESS: Transaction routing failures. FINANCIAL: Money sent to wrong accounts. COMPLIANCE: Audit trail broken."
                }
            },
            "account_type": {
                "domain": "Account",
                "name_patterns": ["account_type", "acc_type", "acct_type", "account_category"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "very_low",  # Few values: Savings, Current, etc.
                    "cardinality": {"max": 10},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Account Master": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Predefined values: Savings, Current, Salary, Fixed Deposit, Recurring Deposit",
                    "allowed_values": ["Savings", "Current", "Salary", "Student", "Pension", "Fixed Deposit", "Recurring Deposit"],
                    "reason": "Determines interest rates, fees, transaction limits, and product features.",
                    "violation_impact": "BUSINESS: Wrong interest/fees applied. FINANCIAL: Revenue loss or incorrect charges. COMPLIANCE: Product mis-selling."
                }
            },
            "account_status": {
                "domain": "Account",
                "name_patterns": ["account_status", "acc_status", "status", "account_state"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "very_low",
                    "cardinality": {"max": 5},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Account Master": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Predefined values: Active, Inactive, Frozen, Closed",
                    "allowed_values": ["Active", "Inactive", "Frozen", "Closed", "Dormant"],
                    "reason": "Controls account operations. Only Active accounts can transact.",
                    "violation_impact": "BUSINESS: Transactions blocked on active accounts or allowed on closed accounts. FINANCIAL: Unauthorized access risk. COMPLIANCE: Operational violations."
                }
            },
            "current_balance": {
                "domain": "Account",
                "name_patterns": ["balance", "current_balance", "available_balance", "account_balance", "closing_balance"],
                "data_patterns": {
                    "type": ["numeric", "decimal"],
                    "uniqueness": "low",
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Account Master": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Numeric, >= 0 (or negative if overdraft allowed), 2 decimal places",
                    "allowed_values": None,
                    "reason": "Current available funds. Must be accurate for transaction authorization and overdraft prevention.",
                    "violation_impact": "BUSINESS: Overdraft not detected. FINANCIAL: Unauthorized withdrawals, losses. COMPLIANCE: Balance sheet inaccuracies."
                }
            },
            "branch_code": {
                "domain": "Account",
                "name_patterns": ["branch_code", "branch_id", "branch", "branch_number"],
                "data_patterns": {
                    "type": ["alphanumeric", "numeric"],
                    "uniqueness": "low",  # Many accounts per branch
                    "length": {"min": 3, "max": 10},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Account Master": "reference"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": True,
                    "format": "Alphanumeric, 3-10 characters",
                    "allowed_values": None,
                    "reason": "Links account to physical branch for operations, reporting, and customer service.",
                    "violation_impact": "BUSINESS: Cannot route branch operations. FINANCIAL: Incorrect branch reporting. COMPLIANCE: Branch-level audit failures."
                }
            },
            "created_date": {
                "domain": "Account",
                "name_patterns": ["created_date", "account_created_date", "open_date", "opening_date", "account_open_date"],
                "data_patterns": {
                    "type": ["date", "datetime"],
                    "uniqueness": "low",
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Account Master": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Valid date, cannot be future date",
                    "allowed_values": None,
                    "reason": "Account creation date required for account age calculation, product maturity tracking, and regulatory reporting.",
                    "violation_impact": "BUSINESS: Cannot calculate account age. FINANCIAL: Interest calculation errors. COMPLIANCE: Account lifecycle reporting incomplete."
                }
            },
            "currency": {
                "domain": "Transaction",
                "name_patterns": ["currency", "currency_type", "curr", "currency_code"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "very_low",
                    "cardinality": {"max": 50},
                    "length": {"min": 3, "max": 3},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Transaction": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "ISO 4217 currency code (3 letters: INR, USD, EUR, GBP, etc.)",
                    "allowed_values": ["INR", "USD", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "CNY", "SGD"],
                    "reason": "Currency type required for multi-currency accounts, foreign exchange operations, and international transaction processing.",
                    "violation_impact": "BUSINESS: Currency conversion failures. FINANCIAL: Incorrect exchange rates applied. COMPLIANCE: Multi-currency reporting violations."
                }
            },
            "channel": {
                "domain": "Transaction",
                "name_patterns": ["channel", "transaction_channel", "channel_type", "txn_channel"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "very_low",
                    "cardinality": {"max": 10},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Transaction": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Predefined values: ATM, Online, Branch, Mobile, Phone Banking, UPI, NEFT, RTGS",
                    "allowed_values": ["ATM", "Online", "Branch", "Mobile", "Phone Banking", "UPI", "NEFT", "RTGS", "Cheque", "Card"],
                    "reason": "Transaction channel required for channel-wise reporting, fee calculation, fraud detection, and customer behavior analysis.",
                    "violation_impact": "BUSINESS: Channel-specific fee calculation errors. FINANCIAL: Incorrect channel reporting. COMPLIANCE: Channel-wise audit trail incomplete."
                }
            },
            
            # LOAN DOMAIN
            "loan_id": {
                "domain": "Loan",
                "name_patterns": ["loan_id", "loan_number", "loan_no", "loan_account"],
                "data_patterns": {
                    "type": ["numeric", "alphanumeric"],
                    "uniqueness": "high",  # ≥95%
                    "length": {"min": 6, "max": 20},
                    "nullable": False
                },
                "is_identifier": True,
                "table_role": {"Loan Master": "PK", "Transaction": "FK"},
                "business_rules": {
                    "unique": True,
                    "mandatory": True,
                    "primary_key": True,
                    "foreign_key": True,
                    "format": "Alphanumeric, 6-20 characters, unique",
                    "allowed_values": None,
                    "reason": "Unique identifier for each loan. Links to customer and account for EMI processing.",
                    "violation_impact": "BUSINESS: Cannot track loan lifecycle. FINANCIAL: EMI deduction failures. COMPLIANCE: Loan portfolio reporting errors."
                }
            },
            "loan_amount": {
                "domain": "Loan",
                "name_patterns": ["loan_amount", "principal", "loan_principal", "sanctioned_amount"],
                "data_patterns": {
                    "type": ["numeric", "decimal"],
                    "uniqueness": "low",
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Loan Master": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Numeric, > 0, 2 decimal places",
                    "allowed_values": None,
                    "reason": "Principal loan amount. Determines EMI calculation and interest charges.",
                    "violation_impact": "BUSINESS: Incorrect EMI calculation. FINANCIAL: Revenue loss or overcharging. COMPLIANCE: Loan documentation errors."
                }
            },
            "interest_rate": {
                "domain": "Loan",
                "name_patterns": ["interest_rate", "rate", "roi", "annual_rate"],
                "data_patterns": {
                    "type": ["numeric", "decimal"],
                    "uniqueness": "low",
                    "range": {"min": 0, "max": 100},  # Percentage
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Loan Master": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Numeric, 0-100%, typically 6-20% for personal loans",
                    "allowed_values": None,
                    "reason": "Annual interest rate. Must comply with regulatory limits. Determines EMI and total interest.",
                    "violation_impact": "BUSINESS: Incorrect EMI calculation. FINANCIAL: Regulatory penalties for usury. COMPLIANCE: Rate cap violations."
                }
            },
            "emi_amount": {
                "domain": "Loan",
                "name_patterns": ["emi", "emi_amount", "installment", "monthly_payment"],
                "data_patterns": {
                    "type": ["numeric", "decimal"],
                    "uniqueness": "low",
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Loan Master": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Numeric, > 0, 2 decimal places, fixed for loan tenure",
                    "allowed_values": None,
                    "reason": "Fixed monthly installment. Calculated from loan amount, rate, and tenure. Must be consistent.",
                    "violation_impact": "BUSINESS: Incorrect deductions. FINANCIAL: Loan recovery failures. COMPLIANCE: Contractual violations."
                }
            },
            
            # TRANSACTION DOMAIN
            "transaction_id": {
                "domain": "Transaction",
                "name_patterns": ["transaction_id", "txn_id", "trxn_id", "transaction_number", "ref_number"],
                "data_patterns": {
                    "type": ["numeric", "alphanumeric"],
                    "uniqueness": "very_high",  # 100% unique
                    "length": {"min": 8, "max": 30},
                    "nullable": False
                },
                "is_identifier": True,
                "table_role": {"Transaction": "PK"},
                "business_rules": {
                    "unique": True,
                    "mandatory": True,
                    "primary_key": True,
                    "foreign_key": False,
                    "format": "Alphanumeric, 8-30 characters, globally unique",
                    "allowed_values": None,
                    "reason": "Unique identifier for audit trail, dispute resolution, and transaction tracking.",
                    "violation_impact": "BUSINESS: Cannot track transactions. FINANCIAL: Duplicate processing risk. COMPLIANCE: Audit trail broken."
                }
            },
            "transaction_date": {
                "domain": "Transaction",
                "name_patterns": ["transaction_date", "txn_date", "value_date"],
                "data_patterns": {
                    "type": ["date", "datetime"],
                    "uniqueness": "low",
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Transaction": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Valid date, cannot be future date (except scheduled transactions)",
                    "allowed_values": None,
                    "reason": "Critical for interest calculation, statement generation, and chronological ordering.",
                    "violation_impact": "BUSINESS: Incorrect statement periods. FINANCIAL: Interest calculation errors. COMPLIANCE: Reporting period violations."
                }
            },
            "transaction_time": {
                "domain": "Transaction",
                "name_patterns": ["transaction_time", "txn_time", "time"],
                "data_patterns": {
                    "type": ["text", "time"],
                    "uniqueness": "low",
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Transaction": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Time format (HH:MM:SS) for accurate transaction ordering",
                    "allowed_values": None,
                    "reason": "Critical for accurate transaction ordering within the same day. Enables chronological sorting and fraud detection based on timing patterns.",
                    "violation_impact": "BUSINESS: Incorrect transaction ordering. FINANCIAL: Dispute resolution failures. COMPLIANCE: Audit trail incomplete."
                }
            },
            "transaction_amount": {
                "domain": "Transaction",
                "name_patterns": ["amount", "transaction_amount", "txn_amount", "value"],
                "data_patterns": {
                    "type": ["numeric", "decimal"],
                    "uniqueness": "low",
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Transaction": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Numeric, > 0, 2 decimal places",
                    "allowed_values": None,
                    "reason": "Transaction value. Must be positive and accurate for balance updates.",
                    "violation_impact": "BUSINESS: Incorrect balance updates. FINANCIAL: Money loss or unauthorized amounts. COMPLIANCE: Transaction reporting errors."
                }
            },
            "debit": {
                "domain": "Transaction",
                "name_patterns": ["debit", "debit_amount", "withdrawal", "dr"],
                "data_patterns": {
                    "type": ["numeric", "decimal"],
                    "uniqueness": "low",
                    "nullable": True  # Can be null if credit transaction
                },
                "is_identifier": False,
                "table_role": {"Transaction": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": False,  # Optional - mutually exclusive with credit
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Numeric, >= 0, mutually exclusive with credit",
                    "allowed_values": None,
                    "reason": "Money going out. Must be mutually exclusive with credit. Reduces balance.",
                    "violation_impact": "BUSINESS: Double-entry bookkeeping violation. FINANCIAL: Balance calculation errors. COMPLIANCE: Accounting violations."
                }
            },
            "credit": {
                "domain": "Transaction",
                "name_patterns": ["credit", "credit_amount", "deposit", "cr"],
                "data_patterns": {
                    "type": ["numeric", "decimal"],
                    "uniqueness": "low",
                    "nullable": True  # Can be null if debit transaction
                },
                "is_identifier": False,
                "table_role": {"Transaction": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": False,  # Optional - mutually exclusive with debit
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Numeric, >= 0, mutually exclusive with debit",
                    "allowed_values": None,
                    "reason": "Money coming in. Must be mutually exclusive with debit. Increases balance.",
                    "violation_impact": "BUSINESS: Double-entry bookkeeping violation. FINANCIAL: Balance calculation errors. COMPLIANCE: Accounting violations."
                }
            },
            "transaction_type": {
                "domain": "Transaction",
                "name_patterns": ["transaction_type", "txn_type", "type", "transaction_category"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "very_low",
                    "cardinality": {"max": 15},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Transaction": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Predefined values: Deposit, Withdrawal, Transfer, Payment, Interest, Fee",
                    "allowed_values": ["Deposit", "Withdrawal", "Transfer", "Payment", "Interest", "Fee", "Refund"],
                    "reason": "Categorizes transactions for reporting, fraud detection, and customer statements.",
                    "violation_impact": "BUSINESS: Incorrect transaction categorization. FINANCIAL: Reporting errors. COMPLIANCE: Transaction type misclassification."
                }
            },
            
            # ============================================
            # CHURN PREDICTION / CUSTOMER RETENTION DOMAIN
            # ============================================
            "row_number": {
                "domain": "General",
                "name_patterns": ["rownumber", "row_number", "row_num", "record_number", "serial_number"],
                "data_patterns": {
                    "type": ["numeric"],
                    "uniqueness": "very_high",  # Sequential numbering
                    "nullable": False
                },
                "is_identifier": False,  # Technical ID, not business identifier
                "table_role": {"General": "technical"},
                "business_rules": {
                    "unique": True,
                    "mandatory": False,  # Not mandatory for business logic
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Sequential integer starting from 1",
                    "allowed_values": None,
                    "reason": "Technical row identifier for data ordering and reference. Not used in business calculations or customer operations.",
                    "violation_impact": "BUSINESS: No direct business impact. TECHNICAL: Data ordering issues in reports or exports."
                }
            },
            "surname": {
                "domain": "Customer",
                "name_patterns": ["surname", "last_name", "lastname", "family_name"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "low",  # Surnames can repeat
                    "length": {"min": 2, "max": 50},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Customer Master": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Alphabetic characters, spaces, hyphens allowed (e.g., Smith, O'Brien, Lee-Jones)",
                    "allowed_values": None,
                    "reason": "Customer surname required for KYC compliance, legal documentation, and customer identification. Must match government-issued ID.",
                    "violation_impact": "BUSINESS: Cannot verify customer identity. COMPLIANCE: KYC/AML documentation incomplete. LEGAL: Contract validity issues."
                }
            },
            "credit_score": {
                "domain": "Risk",
                "name_patterns": ["creditscore", "credit_score", "credit_rating", "fico_score", "cibil_score"],
                "data_patterns": {
                    "type": ["numeric"],
                    "uniqueness": "low",  # Scores can repeat
                    "range": {"min": 300, "max": 850},  # Standard credit score range
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Customer Risk": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Numeric, 300-850 range. 750+ = Excellent, 700-749 = Good, 650-699 = Fair, 600-649 = Poor, <600 = Very Poor",
                    "allowed_values": None,
                    "reason": "Credit score determines loan eligibility, interest rates, credit limits, and product approval. Higher scores indicate lower default risk and enable better terms.",
                    "violation_impact": "BUSINESS: Incorrect loan approvals or rejections. FINANCIAL: Wrong interest rates applied, increased default risk. COMPLIANCE: Fair lending violations."
                }
            },
            "geography": {
                "domain": "Customer",
                "name_patterns": ["geography", "country", "region", "location", "geo"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "very_low",  # Limited number of countries/regions
                    "cardinality": {"max": 50},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Customer Master": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Country or region name (e.g., France, Spain, Germany, USA, India)",
                    "allowed_values": None,  # Depends on bank's operational geography
                    "reason": "Geography determines regulatory compliance requirements, product availability, tax reporting, currency options, and regional marketing strategies. Different countries have different banking regulations.",
                    "violation_impact": "BUSINESS: Wrong product offerings for region. COMPLIANCE: Regulatory violations, incorrect tax reporting. FINANCIAL: Foreign exchange errors."
                }
            },
            "gender": {
                "domain": "Customer",
                "name_patterns": ["gender", "sex", "customer_gender"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "very_low",  # Limited values
                    "cardinality": {"max": 5},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Customer Master": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Predefined values: Male, Female, Other, Prefer not to say",
                    "allowed_values": ["Male", "Female", "M", "F", "Other", "Prefer not to say"],
                    "reason": "Gender used for demographic analysis, customer segmentation, personalized marketing, and regulatory reporting. Required for KYC compliance in many jurisdictions.",
                    "violation_impact": "BUSINESS: Inaccurate demographic analysis. COMPLIANCE: KYC documentation incomplete. FINANCIAL: Marketing campaign targeting errors."
                }
            },
            "age": {
                "domain": "Customer",
                "name_patterns": ["age", "customer_age", "years"],
                "data_patterns": {
                    "type": ["numeric"],
                    "uniqueness": "low",  # Ages repeat
                    "range": {"min": 18, "max": 100},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Customer Master": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Integer, 18-100 years. Customers must be 18+ for most banking products (legal age requirement)",
                    "allowed_values": None,
                    "reason": "Age determines product eligibility (student accounts for 18-24, senior citizen benefits for 60+, retirement products for 50+), interest rates, insurance premiums, and loan tenure. Legal requirement for age verification.",
                    "violation_impact": "BUSINESS: Wrong product offerings, age-based benefit errors. COMPLIANCE: Underage account opening violations. FINANCIAL: Incorrect interest rates for senior citizens."
                }
            },
            "tenure": {
                "domain": "Customer",
                "name_patterns": ["tenure", "customer_tenure", "years_with_bank", "relationship_years"],
                "data_patterns": {
                    "type": ["numeric"],
                    "uniqueness": "low",
                    "range": {"min": 0, "max": 50},  # Years with bank
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Customer Relationship": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Non-negative integer representing years as customer (0 = new customer, 10+ = loyal customer)",
                    "allowed_values": None,
                    "reason": "Tenure measures customer loyalty and relationship strength. Longer tenure customers receive loyalty rewards, preferential interest rates, fee waivers, and priority service. Key metric for churn prediction.",
                    "violation_impact": "BUSINESS: Incorrect loyalty program benefits. FINANCIAL: Wrong interest rates or fee structures. RISK: Churn prediction model errors."
                }
            },
            "num_of_products": {
                "domain": "Product",
                "name_patterns": ["numofproducts", "num_of_products", "product_count", "number_of_products", "products_held"],
                "data_patterns": {
                    "type": ["numeric"],
                    "uniqueness": "very_low",  # Limited range (typically 0-6)
                    "range": {"min": 0, "max": 10},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Product Management": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Non-negative integer (0 = no products, 1 = single product, 2+ = multiple products). Typical products: Savings Account, Current Account, Credit Card, Loan, Fixed Deposit, Insurance",
                    "allowed_values": None,
                    "reason": "Number of products indicates cross-sell success and customer engagement. Customers with multiple products have lower churn rates and higher lifetime value. Used for upsell/cross-sell targeting.",
                    "violation_impact": "BUSINESS: Incorrect cross-sell opportunities. FINANCIAL: Revenue calculation errors. RISK: Churn prediction inaccuracies."
                }
            },
            "has_cr_card": {
                "domain": "Product",
                "name_patterns": ["hascrcard", "has_cr_card", "credit_card", "has_credit_card", "cc_flag"],
                "data_patterns": {
                    "type": ["numeric", "boolean"],
                    "uniqueness": "very_low",  # Binary
                    "cardinality": {"max": 2},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Product Management": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Binary: 0 = No Credit Card, 1 = Has Credit Card. Alternative formats: Yes/No, True/False",
                    "allowed_values": [0, 1, "0", "1", "Yes", "No", "Y", "N", True, False],
                    "reason": "Credit card ownership indicates customer engagement with premium products. Credit card holders typically have higher revenue contribution (interest, fees, merchant charges) and lower churn rates.",
                    "violation_impact": "BUSINESS: Incorrect product portfolio analysis. FINANCIAL: Revenue miscalculation. RISK: Churn model inaccuracies."
                }
            },
            "is_active_member": {
                "domain": "Customer",
                "name_patterns": ["isactivemember", "is_active_member", "active_status", "active_flag", "customer_active"],
                "data_patterns": {
                    "type": ["numeric", "boolean"],
                    "uniqueness": "very_low",  # Binary
                    "cardinality": {"max": 2},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Customer Relationship": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Binary: 0 = Inactive Member, 1 = Active Member. Active means recent transactions, logins, or product usage within past 3-6 months",
                    "allowed_values": [0, 1, "0", "1", "Yes", "No", "Y", "N", True, False],
                    "reason": "Active membership indicates customer engagement and usage patterns. Inactive customers have significantly higher churn risk. Used to trigger retention campaigns, reactivation offers, and account dormancy procedures.",
                    "violation_impact": "BUSINESS: Missed retention opportunities. FINANCIAL: Dormant account compliance violations. RISK: High churn risk for inactive members."
                }
            },
            "estimated_salary": {
                "domain": "Customer",
                "name_patterns": ["estimatedsalary", "estimated_salary", "annual_income", "salary", "income"],
                "data_patterns": {
                    "type": ["numeric", "decimal"],
                    "uniqueness": "low",  # Many different values
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Customer Master": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Positive numeric value representing annual salary (e.g., 50000, 100000, 150000). Currency depends on Geography",
                    "allowed_values": None,
                    "reason": "Estimated salary determines creditworthiness, loan eligibility, loan amount limits, credit card limits, and product recommendations. Higher salary enables larger loans and premium products.",
                    "violation_impact": "BUSINESS: Wrong product offerings, incorrect credit limits. FINANCIAL: Overlending risk, revenue loss. COMPLIANCE: Responsible lending violations."
                }
            },
            "exited": {
                "domain": "Risk",
                "name_patterns": ["exited", "churned", "churn", "left_bank", "customer_exit"],
                "data_patterns": {
                    "type": ["numeric", "boolean"],
                    "uniqueness": "very_low",  # Binary
                    "cardinality": {"max": 2},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Risk Management": "outcome"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Binary: 0 = Customer Retained (Active), 1 = Customer Exited (Churned). Churned means customer closed all accounts and left the bank",
                    "allowed_values": [0, 1, "0", "1", "Yes", "No", "Y", "N", True, False],
                    "reason": "Exited (churn) status is the TARGET OUTCOME for customer retention analysis. Indicates whether customer left the bank. Business goal is to predict and prevent churn. High churn rate impacts revenue, market share, and customer acquisition costs.",
                    "violation_impact": "BUSINESS: Loss of customers and revenue. FINANCIAL: Acquisition cost wasted, lifetime value loss. STRATEGIC: Market share reduction, competitive disadvantage."
                }
            },
            
            # ============================================
            # NEW BANKING CONCEPTS (EXPANSION)
            # ============================================
            "card_present_flag": {
                "domain": "Transaction",
                "name_patterns": ["card_present_flag", "cp_flag", "card_present", "is_card_present"],
                "data_patterns": {
                    "type": ["text", "boolean"],
                    "uniqueness": "very_low",
                    "cardinality": {"max": 5},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Transaction": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Boolean (Y/N, 1/0, Yes/No)",
                    "allowed_values": ["Y", "N", "Yes", "No", "1", "0"],
                    "reason": "Indicates if the physical card was present during the transaction. different fraud rules apply for CP (Card Present) vs CNP (Card Not Present).",
                    "violation_impact": "BUSINESS: Incorrect fraud detection logic. FINANCIAL: Increased fraud risk for CNP transactions."
                }
            },
            "bpay_biller_code": {
                "domain": "Transaction",
                "name_patterns": ["bpay_biller_code", "biller_code", "bpay_code"],
                "data_patterns": {
                    "type": ["numeric", "alphanumeric"],
                    "uniqueness": "low",
                    "length": {"min": 3, "max": 10},
                    "nullable": True
                },
                "is_identifier": False,
                "table_role": {"Transaction": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": False,  # Only for BPAY txns
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Numeric or Alphanumeric code",
                    "allowed_values": None,
                    "reason": "Identifies the biller for BPAY transactions. Essential for routing payments to the correct utility or merchant organization.",
                    "violation_impact": "BUSINESS: Payment routing failures. FINANCIAL: Incorrect settlements. CUSTOMER: Bill payment failures."
                }
            },
            "txn_description": {
                "domain": "Transaction",
                "name_patterns": ["txn_description", "transaction_description", "narration", "description", "details"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "medium",
                    "length": {"min": 5, "max": 200},
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"Transaction": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Free text description",
                    "allowed_values": None,
                    "reason": "Provides context for the transaction (e.g., 'Woolworths Suburb', 'Netflix Subscription'). Critical for customer understanding and statement narration.",
                    "violation_impact": "CUSTOMER: Unclear statements. BUSINESS: Customer support calls increase."
                }
            },
            "merchant_id": {
                "domain": "Merchant",
                "name_patterns": ["merchant_id", "merch_id", "mid", "merchant_identifier"],
                "data_patterns": {
                    "type": ["alphanumeric"],
                    "uniqueness": "low",  # Repeats across transactions
                    "length": {"min": 5, "max": 30},
                    "nullable": True  # Only for merchant txns
                },
                "is_identifier": True,
                "table_role": {"Merchant": "PK", "Transaction": "FK"},
                "business_rules": {
                    "unique": False,
                    "mandatory": False,
                    "primary_key": False,
                    "foreign_key": True,
                    "format": "Alphanumeric identifier",
                    "allowed_values": None,
                    "reason": "Uniquely identifies the merchant. Used for settlement, fee calculation, and merchant analytics.",
                    "violation_impact": "BUSINESS: Valid merchant identification failure. FINANCIAL: Settlement errors."
                }
            },
            "merchant_code": {
                "domain": "Merchant",
                "name_patterns": ["merchant_code", "mcc", "merchant_category_code", "category_code"],
                "data_patterns": {
                    "type": ["numeric"],
                    "uniqueness": "low",
                    "length": {"min": 4, "max": 4},
                    "nullable": True
                },
                "is_identifier": False,
                "table_role": {"Merchant": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": False,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "4-digit Numeric Code (Standard MCC)",
                    "allowed_values": None,
                    "reason": "Classifies the merchant's business type (e.g., 5411 for Grocery Stores). Used for rewards, cashback logic, and risk categorization.",
                    "violation_impact": "BUSINESS: Incorrect reward calculation. APPLICATION: Spend categorization errors."
                }
            },
            "merchant_suburb": {
                "domain": "Merchant",
                "name_patterns": ["merchant_suburb", "suburb", "merchant_city"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "low",
                    "nullable": True
                },
                "is_identifier": False,
                "table_role": {"Merchant": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": False,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Text (Suburb Name)",
                    "allowed_values": None,
                    "reason": "Location of the merchant. Used for geo-analysis and location-based fraud detection.",
                    "violation_impact": "BUSINESS: Inaccurate location insights. RISK: Reduced fraud detection capability."
                }
            },
            "merchant_state": {
                "domain": "Merchant",
                "name_patterns": ["merchant_state", "state_code", "merchant_province"],
                "data_patterns": {
                    "type": ["text"],
                    "uniqueness": "very_low",
                    "length": {"min": 2, "max": 3},
                    "nullable": True
                },
                "is_identifier": False,
                "table_role": {"Merchant": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": False,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Valid State Code (e.g., NSW, VIC, CA, NY)",
                    "allowed_values": None,
                    "reason": "State/Province of the merchant. Used for regional analysis and tax compliance.",
                    "violation_impact": "COMPLIANCE: Regional tax reporting errors. BUSINESS: Regional spend analysis failure."
                }
            },
            "long_lat": {
                "domain": "Customer",
                "name_patterns": ["long_lat", "lat_long", "coordinates", "geo_location", "customer_location"],
                "data_patterns": {
                    "type": ["numeric", "text"],
                    "uniqueness": "medium",
                    "nullable": True
                },
                "is_identifier": False,
                "table_role": {"Customer": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": False,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Decimal Degrees (Latitude, Longitude)",
                    "allowed_values": None,
                    "reason": "Geographic coordinates of the customer or transaction origin. Critical for real-time location checks and fraud anomalies.",
                    "violation_impact": "RISK: Fraud detection bypass. BUSINESS: Location intelligence missing."
                }
            },
            "merchant_long_lat": {
                "domain": "Merchant",
                "name_patterns": ["merchant_long_lat", "merchant_coordinates", "store_location"],
                "data_patterns": {
                    "type": ["numeric", "text"],
                    "uniqueness": "medium",
                    "nullable": True
                },
                "is_identifier": False,
                "table_role": {"Merchant": "descriptive"},
                "business_rules": {
                    "unique": False,
                    "mandatory": False,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "Decimal Degrees (Latitude, Longitude)",
                    "allowed_values": None,
                    "reason": "Geographic coordinates of the merchant. Used to calculate distance from customer home location for fraud rules.",
                    "violation_impact": "RISK: Distance-based fraud rules won't fail. BUSINESS: Merchant mapping features fail."
                }
            },
            "extraction": {
                "domain": "System",
                "name_patterns": ["extraction", "extraction_id", "batch_id", "run_id", "load_id"],
                "data_patterns": {
                    "type": ["numeric", "alphanumeric"],
                    "uniqueness": "low",
                    "nullable": False
                },
                "is_identifier": False,
                "table_role": {"System": "metadata"},
                "business_rules": {
                    "unique": False,
                    "mandatory": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "format": "System generated ID",
                    "allowed_values": None,
                    "reason": "Technical metadata field tracking the ETL batch or extraction run. Used for data lineage and auditing.",
                    "violation_impact": "TECHNICAL: Data lineage broken. COMPLIANCE: Audit trail gaps."
                }
            }
        }
    
    def analyze_dataset(self, file_path: str, df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """
        MAIN ENTRY POINT
        
        Follows strict 6-step process:
        1. Column Profiling (Mandatory)
        2. Identifier Eligibility Check (Critical)
        3. Banking Concept Identification
        4. Confidence Scoring
        5. Apply Real Banking Business Rules
        6. Banking Data Workflow Explanation
        """
        try:
            # Load DataFrame if not provided
            if df is None:
                df = pd.read_csv(file_path)
            
            if df.empty:
                return {
                    "error": "Dataset is empty",
                    "total_columns": 0,
                    "columns_analysis": []
                }
            
            # Reset dataset context
            self.dataset_context = {
                "detected_tables": [],
                "primary_keys": {},
                "foreign_keys": {}
            }
            
            results = {
                "file_name": file_path.split('/')[-1] if '/' in file_path else file_path.split('\\')[-1],
                "total_columns": len(df.columns),
                "total_rows": len(df),
                "columns_analysis": [],
                "workflow_explanation": {},
                "summary": {}
            }
            
            # STEP 1-5: Process each column
            for column in df.columns:
                column_result = self._process_column(column, df[column], df)
                results["columns_analysis"].append(column_result)
            
            # STEP 6: Banking Data Workflow Explanation
            results["workflow_explanation"] = self._step6_workflow_explanation(results["columns_analysis"], df)
            
            # Generate summary
            results["summary"] = self._generate_summary(results["columns_analysis"])
            
            return results
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {
                "error": f"Analysis failed: {str(e)}",
                "total_columns": 0,
                "columns_analysis": []
            }
    
    def _process_column(self, column_name: str, series: pd.Series, full_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Process single column through all 6 steps
        """
        # STEP 1: Column Profiling (MANDATORY)
        profile = self._step1_column_profiling(column_name, series)
        
        # STEP 2: Identifier Eligibility Check (CRITICAL)
        identifier_check = self._step2_identifier_eligibility(column_name, profile, series)
        
        # STEP 3: Banking Concept Identification
        concept_match = self._step3_concept_identification(column_name, profile, series, identifier_check)
        
        # STEP 4: Confidence Scoring
        confidence = self._step4_confidence_scoring(column_name, profile, concept_match, series, full_df, identifier_check)
        
        # STEP 5: Apply Real Banking Business Rules
        business_rules = self._step5_apply_business_rules(concept_match, profile, series, identifier_check)
        
        # Build UI-ready format
        ui_format = {
            "Column Name": column_name,
            "Identified As": concept_match["concept_display"],
            "Confidence": f"{confidence}%",
            "Business Meaning": business_rules["business_meaning"],
            "Business Rules": business_rules["rules_display"],
            "Why This Rule Exists": business_rules["why_rule_exists"],
            "Violation Impact": business_rules["violation_impact"],
            "Data Workflow Role": business_rules["workflow_role"]
        }
        
        return {
            "column_name": column_name,
            "step1_profile": profile,
            "step2_identifier_eligible": identifier_check["is_eligible"],
            "step2_identifier_reason": identifier_check["reason"],
            "step3_identified_as": concept_match["concept"],
            "step3_confidence": confidence,
            "step4_confidence_score": confidence,
            "step5_business_meaning": business_rules["business_meaning"],
            "step5_md_description": business_rules.get("md_description", ""),
            "step5_md_section": business_rules.get("md_section", ""),
            "step5_business_rules": business_rules["rules"],
            "step5_why_rule_exists": business_rules["why_rule_exists"],
            "step5_violation_impact": business_rules["violation_impact"],
            "step5_workflow_role": business_rules["workflow_role"],
            "ui_ready_format": ui_format
        }
    
    def _step1_column_profiling(self, column_name: str, series: pd.Series) -> Dict[str, Any]:
        """
        STEP 1: COLUMN PROFILING (MANDATORY)
        
        For each column, analyze:
        - Column name & keywords
        - Data type (numeric / text / date / decimal)
        - Min & max length
        - Uniqueness percentage
        - Null / empty percentage
        - Pattern: digits / alphanumeric / fixed length / low cardinality / date formats
        """
        profile = {
            "column_name": column_name,
            "keywords": self._extract_keywords(column_name),
            "data_type": self._detect_data_type(series),
            "total_records": len(series),
            "non_null_count": int(series.notna().sum()),
            "null_count": int(series.isna().sum()),
            "null_percentage": round(float(series.isna().sum() / len(series)) * 100, 2) if len(series) > 0 else 0.0,
            "empty_count": int((series.astype(str).str.strip() == "").sum()),
            "empty_percentage": round(float((series.astype(str).str.strip() == "").sum() / len(series)) * 100, 2) if len(series) > 0 else 0.0,
            "unique_count": int(series.nunique()),
            "uniqueness_percentage": round(float(series.nunique() / len(series)) * 100, 2) if len(series) > 0 else 0.0,
            "patterns": {}
        }
        
        # Pattern detection
        non_null_series = series.dropna()
        non_empty_series = series[series.astype(str).str.strip() != ""]
        
        if len(non_null_series) > 0:
            # Length analysis
            if profile["data_type"] in ["text", "alphanumeric"]:
                lengths = non_null_series.astype(str).str.len()
                profile["patterns"]["min_length"] = int(lengths.min())
                profile["patterns"]["max_length"] = int(lengths.max())
                profile["patterns"]["avg_length"] = round(float(lengths.mean()), 2)
                profile["patterns"]["std_length"] = round(float(lengths.std()), 2) if len(lengths) > 1 else 0.0
            
            # Pattern detection
            sample_values = non_null_series.head(100).astype(str)
            
            # Only digits
            digit_count = sum(1 for s in sample_values if s.isdigit())
            profile["patterns"]["only_digits"] = (digit_count / len(sample_values)) >= 0.9 if len(sample_values) > 0 else False
            
            # Alphanumeric
            alphanumeric_count = sum(1 for s in sample_values if re.match(r'^[A-Za-z0-9]+$', s))
            profile["patterns"]["alphanumeric"] = (alphanumeric_count / len(sample_values)) >= 0.9 if len(sample_values) > 0 else False
            
            # Fixed length
            if profile["data_type"] in ["text", "alphanumeric"]:
                lengths = non_null_series.astype(str).str.len()
                unique_lengths = lengths.nunique()
                if unique_lengths == 1:
                    profile["patterns"]["fixed_length"] = True
                    profile["patterns"]["fixed_length_value"] = int(lengths.iloc[0])
                else:
                    profile["patterns"]["fixed_length"] = False
                    # Check if length is consistent (low std dev)
                    if len(lengths) > 1:
                        std_dev = lengths.std()
                        if std_dev < 1.0:  # Very consistent length
                            profile["patterns"]["near_fixed_length"] = True
                            profile["patterns"]["typical_length"] = int(lengths.mode().iloc[0]) if len(lengths.mode()) > 0 else int(lengths.median())
            
            # Low cardinality (few distinct values)
            unique_ratio = profile["uniqueness_percentage"]
            if unique_ratio < 20:
                profile["patterns"]["low_cardinality"] = True
                # Convert numpy types to python native types for JSON serialization
                unique_vals = non_null_series.unique()[:10]
                profile["patterns"]["distinct_values"] = [
                    val.item() if hasattr(val, 'item') else val 
                    for val in unique_vals
                ]
                profile["patterns"]["distinct_count"] = int(non_null_series.nunique())
            else:
                profile["patterns"]["low_cardinality"] = False
            
            # Date format detection
            if profile["data_type"] == "date":
                profile["patterns"]["date_format"] = self._detect_date_format(non_null_series)
            
            # Numeric range
            if profile["data_type"] in ["numeric", "decimal"]:
                numeric_series = pd.to_numeric(non_null_series, errors='coerce').dropna()
                if len(numeric_series) > 0:
                    profile["patterns"]["min_value"] = float(numeric_series.min())
                    profile["patterns"]["max_value"] = float(numeric_series.max())
                    profile["patterns"]["mean_value"] = round(float(numeric_series.mean()), 2)
                    profile["patterns"]["median_value"] = round(float(numeric_series.median()), 2)
                    profile["patterns"]["has_negative"] = bool((numeric_series < 0).any())
                    profile["patterns"]["has_zero"] = bool((numeric_series == 0).any())
                    profile["patterns"]["has_positive"] = bool((numeric_series > 0).any())
        
        return profile
    
    def _step2_identifier_eligibility(self, column_name: str, profile: Dict, series: pd.Series) -> Dict[str, Any]:
        """
        STEP 2: IDENTIFIER ELIGIBILITY CHECK (CRITICAL)
        
        A column can be an IDENTIFIER ONLY IF:
        - Uniqueness ≥ 95% AND
        - Fixed length or strict pattern AND
        - Column name is NOT descriptive (city, name, description)
        
        If any condition fails → NOT an identifier, treat as descriptive / contact / reference field
        
        Table-aware rules:
        - customer_id → PK only in Customer Master; FK in Account / Loan / Transaction tables
        - account_number → Globally unique, numeric, fixed length
        - loan_id → Unique, primary key in Loan table
        - Contact fields (phone, email) → Never PK, optional uniqueness
        """
        column_lower = column_name.lower()
        
        # Check if column name is descriptive (exclude from identifier)
        descriptive_keywords = ["name", "city", "description", "remarks", "note", "comment", "address", "street"]
        is_descriptive = any(keyword in column_lower for keyword in descriptive_keywords)
        
        # Check if it's a contact field (phone, email) - never PK
        contact_keywords = ["phone", "mobile", "email", "contact"]
        is_contact = any(keyword in column_lower for keyword in contact_keywords)
        
        # Check uniqueness
        uniqueness_pct = profile["uniqueness_percentage"]
        is_unique = uniqueness_pct >= 95.0
        
        # Check fixed length or strict pattern
        has_fixed_length = profile["patterns"].get("fixed_length", False)
        has_near_fixed_length = profile["patterns"].get("near_fixed_length", False)
        has_strict_pattern = profile["patterns"].get("only_digits", False) or profile["patterns"].get("alphanumeric", False)
        
        # Determine eligibility
        is_eligible = False
        reason_parts = []
        
        if is_descriptive:
            reason_parts.append("Column name is descriptive (contains name/city/description)")
            is_eligible = False
        elif is_contact:
            reason_parts.append("Contact field (phone/email) - never primary key")
            is_eligible = False
        elif not is_unique:
            reason_parts.append(f"Uniqueness {uniqueness_pct:.1f}% < 95% threshold")
            is_eligible = False
        elif not (has_fixed_length or has_near_fixed_length or has_strict_pattern):
            reason_parts.append("No fixed length or strict pattern detected")
            is_eligible = False
        else:
            is_eligible = True
            reason_parts.append("Meets all identifier criteria")
        
        return {
            "is_eligible": is_eligible,
            "reason": ". ".join(reason_parts) if reason_parts else "Unknown",
            "uniqueness_pct": uniqueness_pct,
            "has_fixed_length": has_fixed_length,
            "has_strict_pattern": has_strict_pattern,
            "is_descriptive": is_descriptive,
            "is_contact": is_contact
        }
    
    def _normalize_column_name(self, column_name: str) -> str:
        """
        Normalize column names to handle different naming conventions:
        - CustomerId → customer_id
        - CreditScore → credit_score
        - NumOfProducts → num_of_products
        """
        import re
        # Convert camelCase to snake_case
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', column_name)
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
        return s2.lower().strip()
    
    def _step3_concept_identification(self, column_name: str, profile: Dict, series: pd.Series, 
                                     identifier_check: Dict) -> Dict[str, Any]:
        """
        STEP 3: BANKING CONCEPT IDENTIFICATION
        
        Using column name + data behavior + identifier eligibility, 
        map each column to exactly ONE banking concept.
        
        Table-aware rules:
        - customer_id → PK only in Customer Master; FK in Account / Loan / Transaction tables
        - account_number → Globally unique, numeric, fixed length
        - loan_id → Unique, primary key in Loan table
        - Contact fields (phone, email) → Never PK, optional uniqueness
        """
        # Normalize column name (CustomerId → customer_id, CreditScore → credit_score)
        column_normalized = self._normalize_column_name(column_name)
        column_lower = column_name.lower().strip()
        best_match = None
        best_score = 0
        
        # Check each banking concept
        for concept_key, concept_def in self.banking_concepts.items():
            score = 0
            
            # 1. Name pattern matching (40% weight) - Enhanced with exact match bonus
            # Use normalized name for better matching (CustomerId → customer_id)
            name_match = False
            exact_match = False
            for pattern in concept_def["name_patterns"]:
                # Check both normalized and original lowercase for flexibility
                if pattern == column_normalized or pattern == column_lower:
                    exact_match = True
                    name_match = True
                    score += 50  # Bonus for exact match
                    break
                elif pattern in column_normalized or pattern in column_lower:
                    name_match = True
                    score += 40
                    break
            
            # 2. Data pattern matching (60% weight)
            data_patterns = concept_def["data_patterns"]
            
            # Type match - Enhanced
            expected_type = data_patterns.get("type", [])
            if profile["data_type"] in expected_type:
                score += 20
            # Partial type match (e.g., numeric vs decimal)
            elif profile["data_type"] == "numeric" and "decimal" in expected_type:
                score += 15
            elif profile["data_type"] == "decimal" and "numeric" in expected_type:
                score += 15
            
            # Uniqueness match - Enhanced with tighter ranges
            expected_uniqueness = data_patterns.get("uniqueness", "")
            uniqueness_pct = profile["uniqueness_percentage"]
            if expected_uniqueness == "very_high" and uniqueness_pct >= 99.5:
                score += 25  # Very high uniqueness
            elif expected_uniqueness == "very_high" and uniqueness_pct >= 99:
                score += 20
            elif expected_uniqueness == "high" and uniqueness_pct >= 95:
                score += 15
            elif expected_uniqueness == "low" and uniqueness_pct < 50:
                score += 15
            elif expected_uniqueness == "very_low" and uniqueness_pct < 20:
                score += 15
            
            # Length match - Enhanced
            if "length" in data_patterns:
                length_def = data_patterns["length"]
                if "exact" in length_def:
                    if profile["patterns"].get("fixed_length") and profile["patterns"].get("fixed_length_value") == length_def["exact"]:
                        score += 15  # Exact length match
                    elif profile["patterns"].get("near_fixed_length") and abs(profile["patterns"].get("typical_length", 0) - length_def["exact"]) <= 2:
                        score += 10  # Near exact match
                elif "min" in length_def and "max" in length_def:
                    avg_len = profile["patterns"].get("avg_length", 0)
                    min_len = profile["patterns"].get("min_length", 0)
                    max_len = profile["patterns"].get("max_length", 0)
                    if length_def["min"] <= avg_len <= length_def["max"]:
                        score += 10
                    # Check if most values are in range
                    if length_def["min"] <= min_len and max_len <= length_def["max"]:
                        score += 5  # All values in range
            
            # Cardinality match - Enhanced
            if "cardinality" in data_patterns:
                max_cardinality = data_patterns["cardinality"].get("max", 100)
                distinct_count = profile["unique_count"]
                if distinct_count <= max_cardinality:
                    score += 10
                elif distinct_count <= max_cardinality * 1.5:  # Allow some flexibility
                    score += 5
            
            # Nullable match - Enhanced
            expected_nullable = data_patterns.get("nullable", False)
            null_pct = profile["null_percentage"]
            if expected_nullable and null_pct > 0:
                score += 5
            elif not expected_nullable and null_pct == 0:
                score += 10  # Bonus for mandatory fields with no nulls
            elif not expected_nullable and null_pct < 5:  # Allow small tolerance
                score += 3
            
            # Pattern consistency bonus
            if profile["patterns"].get("fixed_length") or profile["patterns"].get("near_fixed_length"):
                if concept_def.get("is_identifier", False):
                    score += 5  # Fixed length is good for identifiers
            
            # CRITICAL: Identifier eligibility check
            # If concept expects identifier but column is not eligible, reduce score significantly
            if concept_def.get("is_identifier", False) and not identifier_check["is_eligible"]:
                score *= 0.3  # Reduce score more aggressively
            
            # CRITICAL: Contact fields should never be identifiers
            if identifier_check["is_contact"] and concept_def.get("is_identifier", False):
                score = 0  # Zero score for contact fields as identifiers
            
            # CRITICAL: Descriptive fields should not be identifiers
            if identifier_check["is_descriptive"] and concept_def.get("is_identifier", False):
                score = 0  # Zero score for descriptive fields as identifiers
            
            if score > best_score:
                best_score = score
                best_match = {
                    "concept": concept_key,
                    "concept_display": concept_def["domain"] + " - " + concept_key.replace("_", " ").title(),
                    "domain": concept_def["domain"],
                    "match_score": score,
                    "concept_definition": concept_def
                }
        
        # If no good match found, classify as "Unknown"
        # Lower threshold to 25 to catch more matches, but still require reasonable confidence
        if best_match is None or best_score < 25:
            best_match = {
                "concept": "unknown",
                "concept_display": "Unknown Banking Concept",
                "domain": "General",
                "match_score": 0,
                "concept_definition": None
            }
        
        return best_match
    
    def _step4_confidence_scoring(self, column_name: str, profile: Dict, concept_match: Dict,
                                  series: pd.Series, full_df: pd.DataFrame, identifier_check: Dict) -> float:
        """
        STEP 4: CONFIDENCE SCORING
        
        Use: Name match, pattern match, uniqueness, length consistency, cross-file reuse
        
        Confidence < 60% → treat as descriptive / reference field, not an identifier
        """
        if concept_match["concept"] == "unknown":
            return 0.0
        
        confidence = concept_match["match_score"]  # Base score from step 3
        
        # Adjust based on data quality
        if profile["null_percentage"] == 0:
            confidence += 5  # No nulls = good data quality
        
        if profile["uniqueness_percentage"] > 99.5:
            confidence += 8  # Very unique = strong identifier
        elif profile["uniqueness_percentage"] > 99:
            confidence += 5
        
        # Pattern consistency
        if profile["patterns"].get("fixed_length"):
            confidence += 5  # Fixed length = strong pattern
        elif profile["patterns"].get("near_fixed_length"):
            confidence += 3  # Near fixed length
        
        # Data type consistency
        if profile["patterns"].get("only_digits") and profile["data_type"] in ["numeric", "alphanumeric"]:
            confidence += 3  # Consistent numeric pattern
        
        # Identifier eligibility bonus
        if identifier_check["is_eligible"] and concept_match["concept_definition"] and concept_match["concept_definition"].get("is_identifier", False):
            confidence += 10  # Bonus for confirmed identifier
        
        # Penalty for mismatches
        if identifier_check["is_contact"] and concept_match["concept_definition"] and concept_match["concept_definition"].get("is_identifier", False):
            confidence = 0  # Contact fields cannot be identifiers
        
        if identifier_check["is_descriptive"] and concept_match["concept_definition"] and concept_match["concept_definition"].get("is_identifier", False):
            confidence = 0  # Descriptive fields cannot be identifiers
        
        # Penalty for low confidence
        if confidence < 60 and identifier_check["is_eligible"]:
            confidence = max(confidence * 0.8, 0)  # Reduce confidence if below threshold
        
        # Normalize to 0-100 scale if score exceeds 100
        if confidence > 100:
            confidence = 100
        
        return round(confidence, 1)
    
    def _step5_apply_business_rules(self, concept_match: Dict, profile: Dict, series: pd.Series,
                                   identifier_check: Dict) -> Dict[str, Any]:
        """
        STEP 5: APPLY REAL BANKING BUSINESS RULES
        
        For each column, generate:
        - Business Meaning (WHAT) - Uses .md file definitions
        - Business Rules (Unique / Not Unique, Mandatory / Optional, Format & length, PK / FK, Allowed values)
        - Business Reason (WHY)
        - Violation Impact (BUSINESS / FINANCIAL / COMPLIANCE)
        - Data Workflow Role
        """
        # Get column name from profile
        column_name = profile.get("column_name", "")
        
        # Try to get definition from .md file
        md_definition = None
        try:
            md_definition = get_column_definition(column_name)
        except Exception as e:
            print(f"Warning: Could not load .md definition for {column_name}: {str(e)}")
        
        if concept_match["concept"] == "unknown":
            # Use .md definition if available, otherwise generic
            if md_definition:
                business_meaning = md_definition.get("description", "")
            else:
                business_meaning = f"This column contains data relevant to banking operations. Exact business meaning requires domain expert review."
            
            return {
                "business_meaning": business_meaning,
                "md_description": md_definition.get("description", "") if md_definition else "",
                "md_section": md_definition.get("section", "") if md_definition else "",
                "rules": {
                    "unique": "Unknown",
                    "mandatory": "Unknown",
                    "format": f"Based on data type: {profile['data_type']}",
                    "primary_key": False,
                    "foreign_key": False,
                    "allowed_values": None
                },
                "rules_display": "Business rules require domain expert review for this column.",
                "why_rule_exists": "Column purpose not clearly identified. Manual review recommended." if not md_definition else md_definition.get("description", ""),
                "violation_impact": "Impact cannot be determined without proper identification.",
                "workflow_role": "Role in banking workflow requires expert review."
            }
        
        concept_def = concept_match["concept_definition"]
        business_rules = concept_def["business_rules"]
        
        # Determine PK/FK based on table role and identifier eligibility
        is_pk = business_rules.get("primary_key", False) and identifier_check["is_eligible"]
        is_fk = business_rules.get("foreign_key", False)
        
        # Build rules display
        rules_list = []
        if business_rules.get("unique"):
            rules_list.append("✓ Must be UNIQUE")
        else:
            rules_list.append("✗ Not unique (duplicates allowed)")
        
        if business_rules.get("mandatory"):
            rules_list.append("✓ MANDATORY (cannot be null)")
        else:
            rules_list.append("○ Optional (can be null)")
        
        if is_pk:
            rules_list.append("✓ PRIMARY KEY")
        if is_fk:
            rules_list.append("✓ FOREIGN KEY")
        
        if business_rules.get("format"):
            rules_list.append(f"✓ Format: {business_rules['format']}")
        
        if business_rules.get("allowed_values"):
            allowed_vals = business_rules["allowed_values"][:5]
            # Convert all values to strings to avoid type errors in join()
            allowed_vals_str = [str(v) for v in allowed_vals]
            rules_list.append(f"✓ Allowed values: {', '.join(allowed_vals_str)}")
            if len(business_rules["allowed_values"]) > 5:
                rules_list.append(f"  (and {len(business_rules['allowed_values']) - 5} more)")
        
        rules_display = "\n".join(rules_list) if rules_list else "Standard banking rules apply."
        
        # Determine workflow role
        workflow_role = self._determine_workflow_role(concept_match["concept"], concept_def)
        
        # Build business meaning - prioritize .md definition
        if md_definition and md_definition.get("description"):
            business_meaning = md_definition["description"]
            # Add concept context if different
            if concept_match['concept_display'].lower() not in business_meaning.lower():
                business_meaning = f"{business_meaning} This column represents {concept_match['concept_display']} in the banking system."
        else:
            business_meaning = f"This column represents {concept_match['concept_display']} in the banking system. {business_rules.get('reason', '')}"
        
        return {
            "business_meaning": business_meaning,
            "md_description": md_definition.get("description", "") if md_definition else "",
            "md_section": md_definition.get("section", "") if md_definition else "",
            "rules": {
                **business_rules,
                "primary_key": is_pk,
                "foreign_key": is_fk
            },
            "rules_display": rules_display,
            "why_rule_exists": business_rules.get("reason", "Standard banking business rule."),
            "violation_impact": business_rules.get("violation_impact", "Business, financial, and compliance risks."),
            "workflow_role": workflow_role
        }
    
    def _step6_workflow_explanation(self, columns_analysis: List[Dict], df: pd.DataFrame) -> Dict[str, Any]:
        """
        STEP 6: BANKING DATA WORKFLOW EXPLANATION
        
        For each dataset, explain:
        - How customer data is created
        - How it links to account
        - How loans depend on customer & account
        - How transactions depend on account / loan
        - Role of descriptive & contact fields
        """
        # Identify detected concepts
        detected_concepts = {}
        for col_analysis in columns_analysis:
            concept = col_analysis.get("step3_identified_as", "unknown")
            if concept != "unknown":
                if concept not in detected_concepts:
                    detected_concepts[concept] = []
                detected_concepts[concept].append(col_analysis["column_name"])
        
        # Determine dataset type
        has_customer = "customer_id" in detected_concepts
        has_account = "account_number" in detected_concepts
        has_loan = "loan_id" in detected_concepts
        has_transaction = "transaction_id" in detected_concepts
        
        workflow_parts = []
        
        # Customer workflow
        if has_customer:
            workflow_parts.append("CUSTOMER MANAGEMENT: Customer data is created during onboarding with customer_id as primary key. Customer information includes name, contact details, and address for KYC compliance.")
        
        # Account workflow
        if has_account:
            if has_customer:
                workflow_parts.append("ACCOUNT MANAGEMENT: Accounts are created and linked to customers via customer_id (foreign key). Each account_number is globally unique and serves as primary key in account master.")
            else:
                workflow_parts.append("ACCOUNT MANAGEMENT: Accounts are managed with account_number as primary key. Account status and type determine transaction eligibility.")
        
        # Loan workflow
        if has_loan:
            if has_customer and has_account:
                workflow_parts.append("LOAN MANAGEMENT: Loans are created for customers (via customer_id foreign key) and linked to accounts (via account_number foreign key). Loan_id serves as primary key. EMI amounts are deducted from linked accounts.")
            elif has_customer:
                workflow_parts.append("LOAN MANAGEMENT: Loans are created for customers via customer_id. Loan_id is the primary key.")
            else:
                workflow_parts.append("LOAN MANAGEMENT: Loans are managed with loan_id as primary key.")
        
        # Transaction workflow
        if has_transaction:
            if has_account:
                workflow_parts.append("TRANSACTION PROCESSING: Transactions are recorded with transaction_id as primary key. Each transaction links to an account via account_number (foreign key). Debit/Credit columns track money flow, and balance is updated accordingly.")
            else:
                workflow_parts.append("TRANSACTION PROCESSING: Transactions are recorded with transaction_id as primary key. Transaction type, amount, and date are captured for audit and reporting.")
        
        # Descriptive fields
        descriptive_fields = []
        for col_analysis in columns_analysis:
            if col_analysis.get("step2_identifier_eligible", False) == False:
                concept = col_analysis.get("step3_identified_as", "unknown")
                if concept not in ["customer_id", "account_number", "loan_id", "transaction_id", "unknown"]:
                    descriptive_fields.append(col_analysis["column_name"])
        
        if descriptive_fields:
            workflow_parts.append(f"DESCRIPTIVE FIELDS: Columns like {', '.join(descriptive_fields[:5])} provide additional context and metadata but are not identifiers. They support business operations, reporting, and compliance requirements.")
        
        workflow_text = "\n\n".join(workflow_parts) if workflow_parts else "Workflow explanation requires identification of core banking columns."
        
        return {
            "workflow_text": workflow_text,
            "detected_concepts": list(detected_concepts.keys()),
            "dataset_type": self._determine_dataset_type(has_customer, has_account, has_loan, has_transaction),
            "has_customer": has_customer,
            "has_account": has_account,
            "has_loan": has_loan,
            "has_transaction": has_transaction
        }
    
    def _determine_workflow_role(self, concept: str, concept_def: Dict) -> str:
        """Determine the role of this column in banking workflow"""
        domain = concept_def.get("domain", "General")
        
        if concept == "customer_id":
            return "Primary identifier in Customer Master table. Links to all customer-related records (accounts, loans, transactions)."
        elif concept == "account_number":
            return "Primary identifier in Account Master table. Links to transactions and loan accounts. Globally unique across all branches."
        elif concept == "loan_id":
            return "Primary identifier in Loan Master table. Links to customer and account for EMI processing and loan lifecycle management."
        elif concept == "transaction_id":
            return "Primary identifier for each transaction. Ensures unique audit trail and prevents duplicate processing."
        elif domain == "Customer":
            return "Customer information field used for KYC compliance, customer service, and relationship management."
        elif domain == "Account":
            return "Account information field used for account operations, balance management, and transaction routing."
        elif domain == "Loan":
            return "Loan information field used for loan processing, EMI calculation, and portfolio management."
        elif domain == "Transaction":
            return "Transaction information field used for transaction processing, balance updates, and financial reporting."
        else:
            return "Supporting field in banking operations."
    
    def _determine_dataset_type(self, has_customer: bool, has_account: bool, has_loan: bool, has_transaction: bool) -> str:
        """Determine the type of banking dataset"""
        if has_transaction and has_account:
            return "Transaction Processing Dataset"
        elif has_account and has_customer:
            return "Account Management Dataset"
        elif has_loan and (has_customer or has_account):
            return "Loan Management Dataset"
        elif has_customer:
            return "Customer Management Dataset"
        elif has_account:
            return "Account Master Dataset"
        else:
            return "Mixed Banking Dataset"
    
    # Helper methods
    def _extract_keywords(self, column_name: str) -> List[str]:
        """Extract keywords from column name"""
        # Split by underscore, hyphen, or camelCase
        parts = re.split(r'[_\-\s]+|[A-Z]', column_name)
        return [p.lower() for p in parts if p]
    
    def _detect_data_type(self, series: pd.Series) -> str:
        """Detect data type"""
        if pd.api.types.is_datetime64_any_dtype(series):
            return "date"
        elif pd.api.types.is_numeric_dtype(series):
            # Check if it's integer or decimal
            non_null = series.dropna()
            if len(non_null) > 0:
                if (non_null % 1 == 0).all():
                    return "numeric"
                else:
                    return "decimal"
            return "numeric"
        else:
            # Check if alphanumeric
            sample = series.dropna().head(100).astype(str)
            if len(sample) > 0 and all(re.match(r'^[A-Za-z0-9]+$', s) for s in sample):
                return "alphanumeric"
            return "text"
    
    def _detect_date_format(self, series: pd.Series) -> str:
        """Detect date format"""
        sample = series.dropna().head(10).astype(str)
        if len(sample) == 0:
            return "Unknown"
        
        # Try to detect common formats
        first_val = str(sample.iloc[0])
        if '/' in first_val:
            return "DD/MM/YYYY or MM/DD/YYYY"
        elif '-' in first_val:
            return "YYYY-MM-DD"
        else:
            return "Unknown"
    
    def _generate_summary(self, columns_analysis: List[Dict]) -> Dict[str, Any]:
        """Generate summary statistics"""
        total = len(columns_analysis)
        identified = len([c for c in columns_analysis if c.get("step3_identified_as", "unknown") != "unknown"])
        confidence_scores = [c.get("step4_confidence_score", 0) for c in columns_analysis if c.get("step4_confidence_score", 0) > 0]
        avg_confidence = np.mean(confidence_scores) if confidence_scores else 0
        
        domain_counts = {}
        identifier_count = 0
        for col in columns_analysis:
            concept = col.get("step3_identified_as", "unknown")
            if concept != "unknown":
                concept_def = None
                for key, val in self.banking_concepts.items():
                    if key == concept:
                        concept_def = val
                        break
                if concept_def:
                    domain = concept_def.get("domain", "Unknown")
                    domain_counts[domain] = domain_counts.get(domain, 0) + 1
                    if concept_def.get("is_identifier", False) and col.get("step2_identifier_eligible", False):
                        identifier_count += 1
        
        return {
            "total_columns": total,
            "identified_columns": identified,
            "unidentified_columns": total - identified,
            "identification_rate": round((identified / total * 100) if total > 0 else 0, 1),
            "average_confidence": round(float(avg_confidence), 1),
            "domain_distribution": domain_counts,
            "identifier_count": identifier_count
        }
