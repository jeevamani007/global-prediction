"""
Banking Validation Rules Configuration

Comprehensive validation rules for all 23 standard banking columns.
Each rule follows the Definition-Condition-Action format from the banking specification.
"""

import re
from typing import Dict, Any, List, Optional

class BankingValidationRules:
    """Centralized banking validation rules configuration"""
    
    # Comprehensive validation rules for all banking columns (23 core + 30 extended = 53 total)
    RULES = {
        "customer_id": {
            "definition": "A unique identifier assigned to each customer in the banking system. This is the primary key that links all customer accounts, transactions, and services.",
            "mandatory": True,
            "unique": True,
            "sensitive": False,
            "format_regex": r"^(CUST)?[A-Z0-9_-]{8,12}$",
            "format_description": "8-12 alphanumeric characters, optionally starting with CUST",
            "data_type": "text",
            "action_valid": {
                "message": "Customer ID is unique and properly formatted",
                "usage": [
                    "Links all accounts, transactions, and services to this customer",
                    "Used for customer identification in all banking operations",
                    "Required for KYC compliance and audit trails",
                    "Enables personalized banking services"
                ],
                "next_steps": "Customer record is ready for account creation/transaction processing"
            },
            "action_invalid": {
                "missing": "Customer ID is required. This is a unique identifier needed to create your banking profile.",
                "duplicate": "This Customer ID already exists in our system. Each customer must have a unique ID.",
                "format": "Customer ID format is invalid. Please use 8-12 alphanumeric characters (e.g., CUST12345678).",
                "why_required": "Without a valid Customer ID, we cannot uniquely identify you in our banking system",
                "blocked_actions": ["Account creation", "Transaction processing", "Service enrollment"]
            },
            "name_patterns": ["customer_id", "cust_id", "customer_code", "client_id"]
        },
        
        "customer_name": {
            "definition": "The full legal name of the customer as per government-issued identification. Critical for KYC compliance and legal documentation.",
            "mandatory": True,
            "unique": False,
            "sensitive": False,
            "format_regex": r"^[A-Za-z][A-Za-z\s\-\'\.]{1,99}$",
            "format_description": "2-100 characters, letters, spaces, hyphens, apostrophes only",
            "min_length": 2,
            "max_length": 100,
            "data_type": "text",
            "action_valid": {
                "message": "Customer name is complete and properly formatted",
                "usage": [
                    "Appears on all official documents (statements, certificates, cards)",
                    "Used for KYC verification against government IDs",
                    "Required for legal agreements and contracts",
                    "Displayed in customer communication"
                ],
                "next_steps": "Name will be used for all official correspondence"
            },
            "action_invalid": {
                "missing": "Customer Name is required. Please enter your full legal name as per government ID.",
                "too_short": "Name must be at least 2 characters long.",
                "too_long": "Name cannot exceed 100 characters.",
                "invalid_chars": "Name can only contain letters, spaces, hyphens, and apostrophes.",
                "why_required": "Legal name is mandatory for KYC compliance and official banking documents",
                "blocked_actions": ["Account opening", "KYC verification", "Card issuance"]
            },
            "name_patterns": ["customer_name", "name", "full_name", "client_name", "account_holder"]
        },
        
        "age": {
            "definition": "Customer's age in years. Determines account eligibility, product offerings, and regulatory requirements (minor accounts, senior citizen benefits).",
            "mandatory": True,
            "unique": False,
            "sensitive": False,
            "data_type": "numeric",
            "min_value": 0,
            "max_value": 150,
            "format_description": "Numeric value between 0-150",
            "action_valid": {
                "message": "Age is within acceptable range and verified",
                "usage": [
                    "Determines eligible account types (minor/regular/senior)",
                    "Triggers special benefits (senior citizen interest rates)",
                    "Required for risk assessment and insurance products",
                    "Used for regulatory compliance (minors need guardians)"
                ],
                "next_steps": "Age-appropriate products and services will be offered"
            },
            "action_invalid": {
                "missing": "Age is required to determine eligible banking products.",
                "out_of_range": "Age must be between 0 and 150 years.",
                "not_numeric": "Age must be a valid number.",
                "minor_warning": "⚠️ Minor Account: For customers under 18, guardian information is required.",
                "why_required": "Age determines account eligibility and regulatory compliance",
                "blocked_actions": ["Account type selection until age is validated"]
            },
            "name_patterns": ["age", "customer_age", "age_years"]
        },
        
        "phone_number": {
            "definition": "Customer's primary contact number for banking alerts, OTPs, and transaction notifications. Critical security parameter for two-factor authentication.",
            "mandatory": True,
            "unique": True,
            "sensitive": True,
            "masking_pattern": "******{last_4}",
            "format_regex": r"^[6-9][0-9]{9}$|^\+[1-9][0-9]{7,14}$",
            "format_description": "10-digit number starting with 6-9 (India) or international format",
            "data_type": "text",
            "action_valid": {
                "message": "Phone number is correctly formatted and verified via OTP",
                "usage": [
                    "Transaction alerts and notifications (SMS/WhatsApp)",
                    "OTP for secure login and transaction authorization",
                    "Account recovery and password reset",
                    "Fraud detection alerts",
                    "UPI registration"
                ],
                "next_steps": "You'll receive all banking alerts on this number"
            },
            "action_invalid": {
                "missing": "Phone number is required for transaction alerts and security OTPs.",
                "format": "Please enter a valid 10-digit mobile number starting with 6, 7, 8, or 9.",
                "duplicate": "This phone number is already registered with another account.",
                "not_verified": "⚠️ Phone number not verified. Please complete OTP verification.",
                "why_required": "Essential for two-factor authentication and transaction security",
                "blocked_actions": ["Transaction processing", "OTP login", "UPI setup"]
            },
            "name_patterns": ["phone_number", "phone", "mobile", "contact_number", "mobile_number"]
        },
        
        "email": {
            "definition": "Customer's email address for official communications, e-statements, and online banking credentials.",
            "mandatory": True,
            "unique": True,
            "sensitive": True,
            "masking_pattern": "{first}***@{domain}",
            "format_regex": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "format_description": "Valid email format (e.g., name@example.com)",
            "max_length": 254,
            "data_type": "text",
            "action_valid": {
                "message": "Email format is correct and verification link clicked",
                "usage": [
                    "E-statements and account summaries",
                    "Transaction notifications and receipts",
                    "Online banking login credentials",
                    "Important alerts (security, policy changes)",
                    "Digital document delivery"
                ],
                "next_steps": "All official documents will be sent to this email"
            },
            "action_invalid": {
                "missing": "Email address is required for online banking and e-statements.",
                "format": "Please enter a valid email address (e.g., name@example.com).",
                "duplicate": "This email is already registered with another account.",
                "not_verified": "⚠️ Email not verified. Please check your inbox and click the verification link.",
                "why_required": "Needed for e-statements, online banking, and official communications",
                "blocked_actions": ["Online banking setup", "E-statement enrollment"]
            },
            "name_patterns": ["email", "email_address", "email_id"]
        },
        
        "address": {
            "definition": "Customer's residential address for physical document delivery, KYC verification, and legal compliance.",
            "mandatory": True,
            "unique": False,
            "sensitive": False,
            "min_length": 10,
            "max_length": 500,
            "data_type": "text",
            "action_valid": {
                "message": "Address is complete with all required components",
                "usage": [
                    "Physical delivery of cheque books, debit/credit cards",
                    "Address verification for KYC compliance",
                    "Legal correspondence and notices",
                    "Proof of residence validation",
                    "Branch assignment based on location"
                ],
                "next_steps": "This address will be used for all physical deliveries"
            },
            "action_invalid": {
                "missing": "Address is required for KYC compliance and card/cheque book delivery.",
                "too_short": "Please provide a complete address with street, area, city, and pincode.",
                "why_required": "Mandatory for KYC verification and physical document delivery",
                "blocked_actions": ["Card/cheque book issuance", "KYC completion"]
            },
            "name_patterns": ["address", "residential_address", "customer_address", "street_address"]
        },
        
        "account_number": {
            "definition": "A unique identifier for each bank account. Core reference for all transactions, balances, and banking operations.",
            "mandatory": True,
            "unique": True,
            "sensitive": True,
            "masking_pattern": "*****{last_4}",
            "format_regex": r"^[0-9]{10,16}$",
            "format_description": "10-16 digits only",
            "data_type": "text",
            "action_valid": {
                "message": "Account number is unique and active in the system",
                "usage": [
                    "Primary identifier for all transactions (deposit, withdrawal, transfer)",
                    "Used in NEFT/RTGS/IMPS transfers",
                    "Required for salary credits and direct deposits",
                    "Links to debit/credit cards",
                    "Reference for balance inquiries"
                ],
                "next_steps": "Use this account number for all banking transactions"
            },
            "action_invalid": {
                "missing": "Account number is missing. This is auto-generated during account creation.",
                "duplicate": "❌ CRITICAL: Duplicate account number detected. This violates banking integrity rules.",
                "format": "Account number must be 10-16 digits only.",
                "inactive": "This account number exists but is INACTIVE.",
                "why_required": "Account number is the fundamental identifier for all banking operations",
                "blocked_actions": ["ALL transactions blocked until valid account number assigned"]
            },
            "name_patterns": ["account_number", "account_no", "acc_number", "acct_no", "account_id"]
        },
        
        "account_type": {
            "definition": "The category of bank account determining features, benefits, transaction limits, and fees.",
            "mandatory": True,
            "unique": False,
            "sensitive": False,
            "allowed_values": ["SAVINGS", "CURRENT", "SALARY", "FIXED_DEPOSIT", "RECURRING_DEPOSIT", "ZERO_BALANCE", "SENIOR_CITIZEN"],
            "data_type": "text",
            "action_valid": {
                "message": "Account type is recognized and properly configured",
                "usage": [
                    "Determines interest rates",
                    "Sets transaction limits",
                    "Defines minimum balance requirements",
                    "Triggers fees and charges",
                    "Enables type-specific features"
                ],
                "next_steps": "Account rules and benefits applied based on this type"
            },
            "action_invalid": {
                "missing": "Account type is required to set up your account features and benefits.",
                "invalid_value": "Invalid account type. Please select from: Savings, Current, Salary, Fixed Deposit, Recurring Deposit, Zero Balance, Senior Citizen.",
                "why_required": "Determines interest rates, fees, and account features",
                "blocked_actions": ["Interest calculation", "Fee application", "Benefit enrollment"]
            },
            "name_patterns": ["account_type", "acc_type", "account_category", "product_type"]
        },
        
        "account_status": {
            "definition": "Current operational state of the account, indicating whether transactions are allowed, restricted, or blocked.",
            "mandatory": True,
            "unique": False,
            "sensitive": False,
            "allowed_values": ["ACTIVE", "INACTIVE", "DORMANT", "FROZEN", "CLOSED"],
            "data_type": "text",
            "action_valid": {
                "message": "Account status is clearly defined",
                "usage": [
                    "ACTIVE: All transactions allowed",
                    "INACTIVE: Read-only access, reactivation required",
                    "DORMANT: Auto-flagged after 2 years inactivity",
                    "FROZEN: All transactions blocked, legal clearance needed",
                    "CLOSED: No operations allowed"
                ],
                "next_steps": "Transaction rules applied based on status"
            },
            "action_invalid": {
                "missing": "Account status is required to determine operational permissions.",
                "invalid_value": "Invalid status. Please use: ACTIVE, INACTIVE, DORMANT, FROZEN, or CLOSED.",
                "status_conflict": "⚠️ Account status is INACTIVE/FROZEN. Transactions are not allowed.",
                "why_required": "Controls transaction authorization and security",
                "blocked_actions": ["Transactions blocked if not ACTIVE"]
            },
            "name_patterns": ["account_status", "status", "acc_status", "account_state"]
        },
        
        "ifsc_code": {
            "definition": "Indian Financial System Code - unique 11-character code identifying the bank branch for electronic fund transfers (NEFT/RTGS/IMPS).",
            "mandatory": True,
            "unique": False,
            "sensitive": False,
            "format_regex": r"^[A-Z]{4}0[A-Z0-9]{6}$",
            "format_description": "11 characters: 4 letters + '0' + 6 alphanumeric (e.g., SBIN0001234)",
            "data_type": "text",
            "action_valid": {
                "message": "IFSC code is correctly formatted and registered with RBI",
                "usage": [
                    "Required for all NEFT/RTGS/IMPS transfers",
                    "Identifies bank branch for fund routing",
                    "Used in online fund transfers",
                    "Required for salary credits and ECS mandates",
                    "Printed on cheque books"
                ],
                "next_steps": "IFSC will be used for all electronic fund transfers"
            },
            "action_invalid": {
                "missing": "IFSC code is required for electronic fund transfers (NEFT/RTGS/IMPS).",
                "wrong_length": "IFSC code must be exactly 11 characters.",
                "format": "Invalid IFSC format. Should be: 4 letters + '0' + 6 alphanumeric (e.g., SBIN0001234).",
                "why_required": "Mandatory for routing electronic fund transfers to correct branch",
                "blocked_actions": ["NEFT/RTGS/IMPS transfers", "Online beneficiary addition"]
            },
            "name_patterns": ["ifsc_code", "ifsc", "branch_code"]
        },
        
        "balance": {
            "definition": "Current available balance in the account, representing total funds available for transactions.",
            "mandatory": False,
            "unique": False,
            "sensitive": False,
            "data_type": "numeric",
            "min_value": -1000000,
            "max_value": 10000000000,
            "format_description": "Numeric value, can be negative for overdraft accounts",
            "action_valid": {
                "message": "Balance is properly recorded and meets account requirements",
                "usage": [
                    "Determines available funds for withdrawals/transfers",
                    "Checked before transaction authorization",
                    "Used for minimum balance fee calculation",
                    "Interest calculation on average monthly balance",
                    "Overdraft limit monitoring"
                ],
                "next_steps": "Balance available for authorized transactions"
            },
            "action_invalid": {
                "missing": "Balance information is missing.",
                "below_minimum": "⚠️ Balance is below minimum requirement. Minimum balance charges may apply.",
                "negative_non_overdraft": "❌ Insufficient funds. Balance cannot be negative for this account type.",
                "exceeds_overdraft": "❌ Balance exceeds overdraft limit. Please deposit funds immediately.",
                "why_required": "Balance determines transaction authorization and fee calculation",
                "blocked_actions": ["Withdrawals/transfers if balance insufficient"]
            },
            "name_patterns": ["balance", "account_balance", "current_balance", "available_balance"]
        },
        
        "transaction_amount": {
            "definition": "The monetary value of a financial transaction (deposit, withdrawal, transfer).",
            "mandatory": True,
            "unique": False,
            "sensitive": False,
            "data_type": "numeric",
            "min_value": 0.01,
            "max_value": 10000000,
            "format_description": "Positive numeric value, up to 2 decimal places",
            "action_valid": {
                "message": "Transaction amount is within allowed limits",
                "usage": [
                    "Debited/credited to account balance",
                    "Used for transaction fee calculation",
                    "Checked against daily/monthly transaction limits",
                    "Reported to tax authorities if > ₹10 lakh",
                    "Used for fraud detection"
                ],
                "next_steps": "Amount will be processed as per transaction type"
            },
            "action_invalid": {
                "missing": "Transaction amount is required.",
                "zero_or_negative": "Amount must be greater than zero.",
                "exceeds_limit": "❌ Amount exceeds allowed limit for this transaction type.",
                "insufficient_balance": "❌ Insufficient funds. Available balance is less than required amount.",
                "why_required": "Amount determines debit/credit value and regulatory compliance",
                "blocked_actions": ["Transaction will not be processed"]
            },
            "name_patterns": ["transaction_amount", "amount", "txn_amount", "transaction_value"]
        },
        
        "transaction_date": {
            "definition": "The date when the transaction was executed or scheduled. Used for transaction history and reconciliation.",
            "mandatory": True,
            "unique": False,
            "sensitive": False,
            "data_type": "date",
            "format_description": "Valid date format (YYYY-MM-DD or DD-MM-YYYY)",
            "action_valid": {
                "message": "Transaction date is properly formatted and logical",
                "usage": [
                    "Transaction history and statement generation",
                    "Audit trail and compliance reporting",
                    "Interest calculation based on transaction date",
                    "Reconciliation with bank statements",
                    "Tax reporting"
                ],
                "next_steps": "Transaction will be logged with this timestamp"
            },
            "action_invalid": {
                "missing": "Transaction date is required for record-keeping.",
                "invalid_format": "Please enter date in DD-MM-YYYY format.",
                "invalid_date": "Invalid date. Please check day, month, and year.",
                "too_old": "⚠️ Transaction date is older than 7 years.",
                "future_date": "❌ Transaction date cannot be in the future for immediate transactions.",
                "why_required": "Date is mandatory for transaction logging and audit compliance",
                "blocked_actions": ["Transaction will not be logged"]
            },
            "name_patterns": ["transaction_date", "txn_date", "date", "trans_date"]
        },
        
        "transaction_type": {
            "definition": "The category of transaction indicating the nature of the operation - debit (outflow) or credit (inflow).",
            "mandatory": True,
            "unique": False,
            "sensitive": False,
            "allowed_values": ["DEBIT", "DR", "CREDIT", "CR", "WITHDRAWAL", "DEPOSIT", "TRANSFER", "SALARY", "FEE", "INTEREST", "REVERSAL"],
            "data_type": "text",
            "action_valid": {
                "message": "Transaction type is recognized and properly categorized",
                "usage": [
                    "Determines balance impact (debit reduces, credit increases)",
                    "Used in statement categorization",
                    "Triggers specific workflows",
                    "Tax reporting for interest credits",
                    "Transaction limits applied per type"
                ],
                "next_steps": "Transaction processed according to type-specific rules"
            },
            "action_invalid": {
                "missing": "Transaction type is required to process this operation.",
                "invalid_value": "Invalid transaction type. Use: DEBIT, CREDIT, WITHDRAWAL, DEPOSIT, TRANSFER, SALARY, FEE, INTEREST, or REVERSAL.",
                "type_conflict": "⚠️ DEBIT transaction requires sufficient balance.",
                "why_required": "Type determines how transaction affects account balance",
                "blocked_actions": ["Transaction cannot be processed"]
            },
            "name_patterns": ["transaction_type", "txn_type", "trans_type", "type"]
        },
        
        "pan_number": {
            "definition": "Permanent Account Number issued by Income Tax Department. Mandatory for high-value transactions (>₹50,000) and regulatory compliance.",
            "mandatory": False,
            "unique": True,
            "sensitive": True,
            "masking_pattern": "*****{last_5}",
            "format_regex": r"^[A-Z]{5}[0-9]{4}[A-Z]{1}$",
            "format_description": "10 characters: 5 letters + 4 digits + 1 letter (e.g., ABCDE1234F)",
            "data_type": "text",
            "action_valid": {
                "message": "PAN is correctly formatted and verified",
                "usage": [
                    "Linked to all high-value transactions",
                    "TDS deduction and reporting",
                    "Prevents multiple accounts for tax evasion",
                    "Required for loan processing",
                    "Enables higher transaction limits"
                ],
                "next_steps": "PAN verified; full transaction limits enabled"
            },
            "action_invalid": {
                "missing_high_value": "⚠️ PAN is required for transactions above ₹50,000 or FD above ₹5 lakh.",
                "format": "Invalid PAN format. Should be 5 letters + 4 digits + 1 letter (e.g., ABCDE1234F).",
                "lowercase": "PAN must be in UPPERCASE only.",
                "duplicate": "This PAN is already linked to another account.",
                "why_required": "Mandatory for tax compliance and high-value transactions",
                "blocked_actions": ["High-value transactions (>₹50,000)", "Fixed deposit >₹5 lakh", "Higher TDS if PAN not provided"]
            },
            "name_patterns": ["pan_number", "pan", "pan_card", "permanent_account_number"]
        },
        
        "aadhaar_number": {
            "definition": "Unique 12-digit identity number issued by UIDAI. Used for KYC verification and government subsidy credits (DBT).",
            "mandatory": False,
            "unique": True,
            "sensitive": True,
            "masking_pattern": "****-****-{last_4}",
            "format_regex": r"^[0-9]{12}$",
            "format_description": "12 digits only",
            "data_type": "text",
            "action_valid": {
                "message": "Aadhaar is correctly formatted and verified via UIDAI",
                "usage": [
                    "Instant KYC completion (no physical documents needed)",
                    "Government subsidy credits (LPG, pension, scholarships)",
                    "Address updation across banks (centralized)",
                    "Digital authentication for transactions",
                    "Aadhaar Enabled Payment System (AEPS)"
                ],
                "next_steps": "eKYC completed; account fully activated"
            },
            "action_invalid": {
                "invalid_length": "Aadhaar must be exactly 12 digits.",
                "contains_letters": "Aadhaar should contain only numeric digits.",
                "not_verified": "⚠️ Aadhaar not verified. Click 'Verify via OTP' to complete eKYC.",
                "duplicate": "This Aadhaar is already linked to another account.",
                "why_required": "Enables instant eKYC and government subsidy credits",
                "blocked_actions": ["eKYC not possible without Aadhaar", "Subsidy credits may not be received"]
            },
            "name_patterns": ["aadhaar_number", "aadhaar", "aadhar", "aadhaar_no"]
        },
        
        "kyc_status": {
            "definition": "Indicates whether the customer's identity and address documents have been verified and approved. KYC compliance is mandatory per RBI regulations.",
            "mandatory": True,
            "unique": False,
            "sensitive": False,
            "allowed_values": ["VERIFIED", "PENDING", "NOT_VERIFIED", "REJECTED", "EXPIRED"],
            "data_type": "text",
            "action_valid": {
                "message": "Your identity and address documents are approved",
                "usage": [
                    "Enables full banking services (transactions, loans, cards)",
                    "Complies with RBI regulations",
                    "Allows high-value transactions",
                    "Required for credit products",
                    "Enables online banking access"
                ],
                "next_steps": "Full account access granted; no restrictions"
            },
            "action_invalid": {
                "pending": "⚠️ KYC verification is in progress. Expected completion: 2-3 business days.",
                "not_verified": "❌ KYC not completed. Please submit identity and address proof.",
                "rejected": "❌ KYC documents rejected. Please resubmit correct documents.",
                "expired": "⚠️ KYC has expired. Please complete re-KYC to avoid account restrictions.",
                "why_required": "Mandatory for regulatory compliance and account security",
                "blocked_actions": ["High-value transactions (>₹50,000)", "Loan/credit card applications", "Account may be frozen if KYC not completed"]
            },
            "name_patterns": ["kyc_status", "kyc", "verification_status"]
        },
        
        "kyc_verified_date": {
            "definition": "The date when KYC verification was completed. Used to track KYC validity period (typically 10 years).",
            "mandatory": False,
            "unique": False,
            "sensitive": False,
            "data_type": "date",
            "format_description": "Valid date format (YYYY-MM-DD or DD-MM-YYYY)",
            "action_valid": {
                "message": "KYC verification date is recorded",
                "usage": [
                    "Tracks KYC validity period",
                    "Triggers re-KYC alerts (10 years for low-risk)",
                    "Audit trail for compliance reporting",
                    "Used in risk assessment"
                ],
                "next_steps": "KYC valid until [date + 10 years]; re-KYC notification before expiry"
            },
            "action_invalid": {
                "missing_verified": "KYC verified date is missing but status shows VERIFIED.",
                "future_date": "KYC verified date cannot be in the future.",
                "expired_10years": "⚠️ KYC verified on [date]. Re-KYC required.",
                "why_required": "Tracks KYC validity for compliance",
                "blocked_actions": ["Account services may be restricted if KYC expired"]
            },
            "name_patterns": ["kyc_verified_date", "kyc_date", "verification_date"]
        },
        
        "risk_level": {
            "definition": "Customer's risk classification based on transaction patterns, KYC completeness, and regulatory parameters. Used for fraud detection and compliance.",
            "mandatory": False,
            "unique": False,
            "sensitive": False,
            "allowed_values": ["LOW", "MEDIUM", "HIGH"],
            "data_type": "text",
            "action_valid": {
                "message": "Risk level is properly assessed",
                "usage": [
                    "LOW: Normal monitoring, standard transaction limits",
                    "MEDIUM: Enhanced monitoring, occasional verification calls",
                    "HIGH: Strict monitoring, additional approvals required",
                    "Affects loan approval and credit limits",
                    "Used in AML (Anti-Money Laundering) compliance"
                ],
                "next_steps": "Monitoring rules applied as per risk level"
            },
            "action_invalid": {
                "missing": "Risk level assessment pending. Some services may be restricted.",
                "high_risk": "⚠️ Account classified as HIGH RISK. Additional verification required for large transactions.",
                "invalid_value": "Invalid risk level. Must be LOW, MEDIUM, or HIGH.",
                "why_required": "Risk classification is mandatory for regulatory compliance",
                "blocked_actions": ["HIGH risk: Large cash withdrawals may require prior notice"]
            },
            "name_patterns": ["risk_level", "risk", "customer_risk", "risk_category"]
        },
        
        "credit_score": {
            "definition": "A numerical rating (300-900) representing customer's creditworthiness based on credit history and repayment behavior.",
            "mandatory": False,
            "unique": False,
            "sensitive": False,
            "data_type": "numeric",
            "min_value": 300,
            "max_value": 900,
            "format_description": "Numeric value between 300-900",
            "action_valid": {
                "message": "Credit score is available and within valid range",
                "usage": [
                    "750+: Excellent - Best loan rates and high limits",
                    "650-749: Good - Standard loan approval",
                    "550-649: Average - Loan approval with higher interest",
                    "<550: Poor - Loan rejection or very high interest",
                    "Determines credit card approval and limit"
                ],
                "next_steps": "Credit products offered based on score"
            },
            "action_invalid": {
                "null_score": "ℹ️ Credit score not available. May be due to no credit history. Secured loans still available.",
                "out_of_range": "Invalid credit score. Should be between 300-900.",
                "low_score": "⚠️ Credit score is low. Loan approval may be difficult. Consider improving score.",
                "why_required": "Essential for credit product eligibility assessment",
                "blocked_actions": ["Low score: Loan applications may be auto-rejected", "NULL score: Only secured loans available"]
            },
            "name_patterns": ["credit_score", "cibil_score", "score"]
        },
        
        "nominee_name": {
            "definition": "The legal name of the person designated to receive account balance in case of account holder's death. Part of succession planning.",
            "mandatory": False,
            "unique": False,
            "sensitive": False,
            "format_regex": r"^[A-Za-z][A-Za-z\s\-\'\.]{1,99}$",
            "format_description": "2-100 characters, letters, spaces, hyphens, apostrophes only",
            "min_length": 2,
            "max_length": 100,
            "data_type": "text",
            "action_valid": {
                "message": "Nominee details are complete and registered",
                "usage": [
                    "Ensures smooth transfer of balance in case of death",
                    "Avoids legal disputes among family members",
                    "Speeds up claim settlement",
                    "Required for certain account types (PPF, EPF)",
                    "Nominee can claim insurance benefits"
                ],
                "next_steps": "Nominee registered; can be updated anytime"
            },
            "action_invalid": {
                "missing": "ℹ️ No nominee added. We recommend adding a nominee for seamless claim settlement.",
                "format": "Nominee name format is invalid. Please enter alphabetic characters only.",
                "self_nomination": "Nominee cannot be same as account holder.",
                "why_required": "Simplifies claim process for family; avoids legal complications",
                "blocked_actions": ["None (nominee is optional), but claim settlement will be slower"]
            },
            "name_patterns": ["nominee_name", "nominee", "beneficiary_name"]
        },
        
        "loan_amount": {
            "definition": "The principal amount sanctioned for a loan (home, personal, auto, etc.). This is the total loan disbursed, excluding interest.",
            "mandatory": False,
            "unique": False,
            "sensitive": False,
            "data_type": "numeric",
            "min_value": 0.01,
            "max_value": 500000000,
            "format_description": "Positive numeric value",
            "action_valid": {
                "message": "Loan amount is within approved limits",
                "usage": [
                    "Disbursed to borrower's account or vendor",
                    "EMI calculated based on amount, tenure, interest rate",
                    "Interest charged on outstanding principal",
                    "Used for loan-to-value (LTV) ratio calculation",
                    "Reported to credit bureaus"
                ],
                "next_steps": "Loan processing initiated; EMI schedule provided"
            },
            "action_invalid": {
                "zero_or_negative": "Loan amount must be greater than zero.",
                "exceeds_limit": "❌ Requested amount exceeds approved limit. Please reduce amount.",
                "below_minimum": "Minimum loan amount is ₹50,000 for personal loans.",
                "low_credit_score": "⚠️ Loan amount may be reduced due to low credit score.",
                "why_required": "Determines EMI, interest, and repayment schedule",
                "blocked_actions": ["Loan disbursement blocked if amount exceeds limit"]
            },
            "name_patterns": ["loan_amount", "loan", "principal_amount", "sanctioned_amount"]
        },
        
        "emi_amount": {
            "definition": "Equated Monthly Installment - the fixed monthly payment for loan repayment, including both principal and interest.",
            "mandatory": False,
            "unique": False,
            "sensitive": False,
            "data_type": "numeric",
            "min_value": 0.01,
            "max_value": 10000000,
            "format_description": "Positive numeric value",
            "action_valid": {
                "message": "EMI is correctly calculated and affordable",
                "usage": [
                    "Auto-debited from account on EMI due date",
                    "Part goes to principal repayment, part to interest",
                    "Reduces outstanding loan balance each month",
                    "Used for credit score calculation",
                    "Standing instruction set for auto-debit"
                ],
                "next_steps": "EMI schedule created; auto-debit mandate registered"
            },
            "action_invalid": {
                "missing_loan": "EMI amount calculation failed. Please recalculate.",
                "exceeds_income": "❌ EMI exceeds 50% of monthly income. Loan may not be approved.",
                "insufficient_balance": "⚠️ Insufficient balance for EMI debit. Maintain minimum balance by due date.",
                "why_required": "EMI determines loan affordability and repayment tracking",
                "blocked_actions": ["Loan rejected if EMI > 50% of income", "Missed EMI leads to penalty and credit score damage"]
            },
            "name_patterns": ["emi_amount", "emi", "monthly_installment", "installment"]
        },
        
        # ===== BRANCH INFORMATION MODULE (4 columns) =====
        
        "branch_code": {
            "definition": "Equated Monthly Installment - the fixed monthly payment for loan repayment, including both principal and interest.",
            "mandatory": False,
            "unique": False,
            "sensitive": False,
            "data_type": "numeric",
            "min_value": 0.01,
            "max_value": 10000000,
            "format_description": "Positive numeric value",
            "action_valid": {
                "message": "EMI is correctly calculated and affordable",
                "usage": [
                    "Auto-debited from account on EMI due date",
                    "Part goes to principal repayment, part to interest",
                    "Reduces outstanding loan balance each month",
                    "Used for credit score calculation",
                    "Standing instruction set for auto-debit"
                ],
                "next_steps": "EMI schedule created; auto-debit mandate registered"
            },
            "action_invalid": {
                "missing_loan": "EMI amount calculation failed. Please recalculate.",
                "exceeds_income": "❌ EMI exceeds 50% of monthly income. Loan may not be approved.",
                "insufficient_balance": "⚠️ Insufficient balance for EMI debit. Maintain minimum balance by due date.",
                "why_required": "EMI determines loan affordability and repayment tracking",
                "blocked_actions": ["Loan rejected if EMI > 50% of income", "Missed EMI leads to penalty and credit score damage"]
            },
            "name_patterns": ["emi_amount", "emi", "monthly_installment", "installment"]
        }
    }
    
    # Merge extended rules on initialization
    @classmethod
    def _merge_extended_rules(cls):
        """Merge extended banking rules into main RULES"""
        try:
            from extended_banking_validation_rules import EXTENDED_RULES
            cls.RULES.update(EXTENDED_RULES)
        except ImportError:
            pass  # Extended rules file not available
    
    
    @classmethod
    def get_rule(cls, column_name: str) -> Optional[Dict[str, Any]]:
        """
        Get validation rule for a column by matching name patterns.
        
        Args:
            column_name: The column name to find rule for
            
        Returns:
            Rule dictionary if found, None otherwise
        """
        normalized_name = column_name.lower().strip().replace(' ', '_')
        
        # Direct match
        if normalized_name in cls.RULES:
            return cls.RULES[normalized_name]
        
        # Pattern matching
        for rule_key, rule_config in cls.RULES.items():
            name_patterns = rule_config.get('name_patterns', [])
            if normalized_name in name_patterns:
                return rule_config
        
        return None
    
    @classmethod
    def mask_sensitive_data(cls, column_name: str, value: str) -> str:
        """
        Mask sensitive data according to banking rules.
        
        Args:
            column_name: The column name
            value: The value to mask
            
        Returns:
            Masked value
        """
        rule = cls.get_rule(column_name)
        if not rule or not rule.get('sensitive'):
            return value
        
        if not value or str(value).strip() == '':
            return value
        
        value_str = str(value).strip()
        masking_pattern = rule.get('masking_pattern', '')
        
        if '{last_4}' in masking_pattern:
            # Mask all but last 4 characters
            if len(value_str) <= 4:
                return value_str
            return masking_pattern.replace('{last_4}', value_str[-4:])
        
        elif '{last_5}' in masking_pattern:
            # Mask all but last 5 characters
            if len(value_str) <= 5:
                return value_str
            return masking_pattern.replace('{last_5}', value_str[-5:])
        
        elif '{first}' in masking_pattern and '@' in value_str:
            # Email masking
            parts = value_str.split('@')
            if len(parts) == 2:
                return f"{parts[0][0]}***@{parts[1]}"
        
        return value_str
    
    @classmethod
    def get_all_column_types(cls) -> List[str]:
        """Get list of all supported column types"""
        return list(cls.RULES.keys())

# Merge extended rules on module import
BankingValidationRules._merge_extended_rules()

