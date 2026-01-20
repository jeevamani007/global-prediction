"""
Extended Banking Validation Rules - Additional 30 Columns

Comprehensive validation rules for additional banking columns identified in the system.
Each rule follows the Definition-Condition-Action format.
"""

# Additional 30 banking columns validation rules
EXTENDED_RULES = {
    # ===== BRANCH INFORMATION MODULE (4 columns) =====
    
    "branch_code": {
        "definition": "Unique alphanumeric code identifying a specific bank branch. Used for routing transactions and branch-level reporting.",
        "mandatory": True,
        "unique": False,
        "sensitive": False,
        "format_regex": r"^[A-Z0-9]{4,10}$",
        "format_description": "4-10 alphanumeric characters, uppercase",
        "data_type": "text",
        "action_valid": {
            "message": "Branch code is correctly formatted and registered",
            "usage": [
                "Links customer account to specific branch",
                "Used in branch-level transaction reporting",
                "Enables branch-specific services and offers",
                "Required for internal audit and reconciliation",
                "Determines branch jurisdiction for legal matters"
            ],
            "next_steps": "Branch code verified; branch services enabled"
        },
        "action_invalid": {
            "missing": "Branch code is required to identify your home branch.",
            "format": "Invalid branch code format. Should be 4-10 uppercase alphanumeric characters.",
            "not_found": "❌ Branch code not found in our system. Please verify with your branch.",
            "why_required": "Essential for linking account to branch and routing services",
            "blocked_actions": ["Branch-specific services", "Offline transaction processing"]
        },
        "name_patterns": ["branch_code", "branch_id", "home_branch_code"]
    },
    
    "branch_name": {
        "definition": "Full name of the bank branch where the account is maintained. Used for customer reference and correspondence.",
        "mandatory": True,
        "unique": False,
        "sensitive": False,
        "min_length": 3,
        "max_length": 100,
        "data_type": "text",
        "action_valid": {
            "message": "Branch name is complete and verified",
            "usage": [
                "Displayed on account statements and documents",
                "Used for customer identification of home branch",
                "Printed on cheque books and cards",
                "Reference for branch locator services",
                "Required for legal documentation"
            ],
            "next_steps": "Branch name will appear on all official documents"
        },
        "action_invalid": {
            "missing": "Branch name is required for documentation.",
            "too_short": "Branch name must be at least 3 characters.",
            "too_long": "Branch name cannot exceed 100 characters.",
            "why_required": "Required for proper documentation and customer reference",
            "blocked_actions": ["Document generation", "Statement printing"]
        },
        "name_patterns": ["branch_name", "branch", "home_branch"]
    },
    
    "ifsc_verified": {
        "definition": "Boolean flag indicating whether the IFSC code has been verified against RBI's official database.",
        "mandatory": True,
        "unique": False,
        "sensitive": False,
        "allowed_values": ["TRUE", "FALSE", "YES", "NO", "1", "0"],
        "data_type": "text",
        "action_valid": {
            "message": "IFSC code verification status is recorded",
            "usage": [
                "Ensures IFSC is valid and registered with RBI",
                "Prevents transaction failures due to invalid IFSC",
                "Required for enabling online fund transfers",
                "Compliance with NPCI guidelines",
                "Reduces fraud risk from fake IFSC codes"
            ],
            "next_steps": "IFSC verified; online transfers enabled"
        },
        "action_invalid": {
            "missing": "IFSC verification status is missing.",
            "not_verified": "⚠️ IFSC code not verified. Please verify before enabling online transfers.",
            "invalid_value": "IFSC verified must be TRUE/FALSE or YES/NO.",
            "why_required": "Ensures IFSC is valid for electronic fund transfers",
            "blocked_actions": ["Online fund transfers until IFSC verified"]
        },
        "name_patterns": ["ifsc_verified", "ifsc_validation", "ifsc_check"]
    },
    
    "customer_category": {
        "definition": "Classification of customer based on relationship value, demographics, or business criteria (e.g., VIP, Senior Citizen, Student, Standard).",
        "mandatory": True,
        "unique": False,
        "sensitive": False,
        "allowed_values": ["VIP", "SENIOR_CITIZEN", "STUDENT", "STANDARD", "PRIORITY", "BUSINESS", "GOVERNMENT"],
        "data_type": "text",
        "action_valid": {
            "message": "Customer category is properly classified",
            "usage": [
                "Determines service priority and benefits",
                "Triggers category-specific interest rates",
                "Enables special discounts and offers",
                "Sets minimum balance requirements",
                "Defines fee waiver eligibility"
            ],
            "next_steps": "Category-specific benefits applied to account"
        },
        "action_invalid": {
            "missing": "Customer category is required for benefit assignment.",
            "invalid_value": "Invalid category. Please select from: VIP, Senior Citizen, Student, Standard, Priority, Business, Government.",
            "why_required": "Determines service level and applicable benefits",
            "blocked_actions": ["Category-specific benefits", "Special interest rates"]
        },
        "name_patterns": ["customer_category", "customer_type", "account_category", "category"]
    },
    
    # ===== ACCOUNT LIFECYCLE MODULE (2 columns) =====
    
    "account_open_date": {
        "definition": "The date when the account was officially opened and became active. Used for calculating account age and anniversary benefits.",
        "mandatory": True,
        "unique": False,
        "sensitive": False,
        "data_type": "date",
        "format_description": "Valid past date (YYYY-MM-DD or DD-MM-YYYY)",
        "action_valid": {
            "message": "Account opening date is recorded and valid",
            "usage": [
                "Calculates account age for loyalty benefits",
                "Determines eligibility for tenure-based offers",
                "Used in dormancy calculations (no activity for 2 years)",
                "Required for audit and compliance reporting",
                "Triggers anniversary rewards and notifications"
            ],
            "next_steps": "Account age tracked; loyalty benefits enabled"
        },
        "action_invalid": {
            "missing": "Account opening date is required.",
            "future_date": "❌ Account opening date cannot be in the future.",
            "invalid_format": "Please enter date in DD-MM-YYYY format.",
            "why_required": "Essential for tracking account age and eligibility",
            "blocked_actions": ["Loyalty benefits", "Tenure-based offers"]
        },
        "name_patterns": ["account_open_date", "opening_date", "account_opening_date", "open_date"]
    },
    
    "account_closure_date": {
        "definition": "The date when the account was closed. NULL for active accounts. Used for archival and compliance tracking.",
        "mandatory": False,
        "unique": False,
        "sensitive": False,
        "data_type": "date",
        "format_description": "Valid date (YYYY-MM-DD or DD-MM-YYYY), NULL if account active",
        "action_valid": {
            "message": "Account closure date is recorded (account closed)",
            "usage": [
                "Marks account as permanently closed",
                "Triggers final statement generation",
                "Used in archived records retention",
                "Compliance with regulatory record-keeping (7 years)",
                "Prevents reopening of closed accounts"
            ],
            "next_steps": "Account archived; no further transactions allowed"
        },
        "action_invalid": {
            "active_with_closure": "⚠️ Account status is ACTIVE but closure date exists. Please verify.",
            "future_date": "Account closure date cannot be in the future.",
            "before_opening": "❌ Closure date cannot be before account opening date.",
            "why_required": "Required for closed account archival and compliance",
            "blocked_actions": ["ALL transactions blocked on closed accounts"]
        },
        "name_patterns": ["account_closure_date", "closure_date", "account_closed_date", "closed_date"]
    },
    
    # ===== TRANSACTION LIMITS MODULE (3 columns) =====
    
    "daily_transaction_limit": {
        "definition": "Maximum amount that can be transacted per day across all channels. Used for fraud prevention and risk management.",
        "mandatory": True,
        "unique": False,
        "sensitive": False,
        "data_type": "numeric",
        "min_value": 0,
        "max_value": 10000000,
        "format_description": "Positive numeric value (₹)",
        "action_valid": {
            "message": "Daily transaction limit is set and active",
            "usage": [
                "Prevents unauthorized high-value transactions",
                "Fraud detection and prevention",
                "Customizable based on customer profile",
                "Varies by account type and risk classification",
                "Can be temporarily increased for specific needs"
            ],
            "next_steps": "Transactions monitored against daily limit"
        },
        "action_invalid": {
            "missing": "Daily transaction limit is required for security.",
            "zero_or_negative": "Daily limit must be greater than zero.",
            "exceeds_regulatory": "❌ Daily limit exceeds regulatory maximum for this account type.",
            "why_required": "Essential for fraud prevention and account security",
            "blocked_actions": ["High-value transactions exceeding daily limit"]
        },
        "name_patterns": ["daily_transaction_limit", "daily_limit", "per_day_limit"]
    },
    
    "monthly_transaction_limit": {
        "definition": "Maximum amount that can be transacted per month. Used for expense tracking and budget management.",
        "mandatory": True,
        "unique": False,
        "sensitive": False,
        "data_type": "numeric",
        "min_value": 0,
        "max_value": 100000000,
        "format_description": "Positive numeric value (₹)",
        "action_valid": {
            "message": "Monthly transaction limit is configured",
            "usage": [
                "Budget control and expense management",
                "Risk mitigation for account abuse",
                "Supports customer spending goals",
                "Varies by account type and customer category",
                "Alerts sent when approaching limit"
            ],
            "next_steps": "Monthly spending tracked; alerts enabled"
        },
        "action_invalid": {
            "missing": "Monthly transaction limit is required.",
            "less_than_daily": "⚠️ Monthly limit should be greater than or equal to daily limit.",
            "zero_or_negative": "Monthly limit must be greater than zero.",
            "why_required": "Required for budget management and risk control",
            "blocked_actions": ["Transactions exceeding monthly limit"]
        },
        "name_patterns": ["monthly_transaction_limit", "monthly_limit", "per_month_limit"]
    },
    
    "atm_withdrawal_limit": {
        "definition": "Maximum amount that can be withdrawn from ATMs per day. Separate from overall daily transaction limit.",
        "mandatory": True,
        "unique": False,
        "sensitive": False,
        "data_type": "numeric",
        "min_value": 0,
        "max_value": 100000,
        "format_description": "Positive numeric value (₹), typically ₹10,000-₹50,000",
        "action_valid": {
            "message": "ATM withdrawal limit is set",
            "usage": [
                "Limits cash withdrawal from ATMs per day",
                "Prevents card cloning fraud losses",
                "Protects against unauthorized withdrawals",
                "Can be customized via mobile/internet banking",
                "Separate tracking from online/POS transactions"
            ],
            "next_steps": "ATM withdrawals monitored against limit"
        },
        "action_invalid": {
            "missing": "ATM withdrawal limit is required for card security.",
            "exceeds_daily": "⚠️ ATM limit should not exceed overall daily transaction limit.",
            "zero_or_negative": "ATM limit must be greater than zero.",
            "why_required": "Protects against ATM fraud and card cloning",
            "blocked_actions": ["ATM withdrawals exceeding limit"]
        },
        "name_patterns": ["atm_withdrawal_limit", "atm_limit", "cash_withdrawal_limit"]
    },
    
    # ===== SECURITY & AUTHENTICATION MODULE (6 columns) =====
    
    "otp_verified": {
        "definition": "Boolean flag indicating whether the customer's phone number has been verified via OTP (One-Time Password).",
        "mandatory": True,
        "unique": False,
        "sensitive": False,
        "allowed_values": ["TRUE", "FALSE", "YES", "NO", "1", "0"],
        "data_type": "text",
        "action_valid": {
            "message": "OTP verification completed successfully",
            "usage": [
                "Enables two-factor authentication (2FA)",
                "Allows OTP-based transaction authorization",
                "Required for online banking activation",
                "Validates phone number ownership",
                "Mandatory for UPI and mobile banking"
            ],
            "next_steps": "OTP-based services enabled; 2FA active"
        },
        "action_invalid": {
            "not_verified": "❌ OTP not verified. Please complete phone verification.",
            "missing": "OTP verification status is required.",
            "invalid_value": "OTP verified must be TRUE/FALSE or YES/NO.",
            "why_required": "Essential for transaction security and 2FA",
            "blocked_actions": ["Online banking", "UPI", "Mobile banking", "High-value transactions"]
        },
        "name_patterns": ["otp_verified", "otp_validation", "phone_verified"]
    },
    
    "login_attempts": {
        "definition": "Number of consecutive failed login attempts. Used for account security and automatic locking after threshold.",
        "mandatory": True,
        "unique": False,
        "sensitive": False,
        "data_type": "numeric",
        "min_value": 0,
        "max_value": 10,
        "format_description": "Integer value (0-10), auto-locks after threshold",
        "action_valid": {
            "message": "Login attempts tracked within safe threshold",
            "usage": [
                "Monitors failed login attempts",
                "Triggers account lock after 5 failed attempts",
                "Protects against brute-force attacks",
                "Sends security alerts to customer",
                "Resets after successful login"
            ],
            "next_steps": "Login attempts monitored; account secure"
        },
        "action_invalid": {
            "exceeds_threshold": "❌ CRITICAL: Account locked due to multiple failed login attempts. Contact support.",
            "negative": "Login attempts cannot be negative.",
            "why_required": "Protects against unauthorized access attempts",
            "blocked_actions": ["Online banking locked after 5 failed attempts"]
        },
        "name_patterns": ["login_attempts", "failed_logins", "login_count"]
    },
    
    "account_lock_status": {
        "definition": "Indicates whether the account is locked due to security reasons (failed logins, suspicious activity, or manual lock).",
        "mandatory": True,
        "unique": False,
        "sensitive": False,
        "allowed_values": ["UNLOCKED", "LOCKED", "TEMPORARILY_LOCKED", "PERMANENTLY_LOCKED"],
        "data_type": "text",
        "action_valid": {
            "message": "Account lock status is normal (UNLOCKED)",
            "usage": [
                "UNLOCKED: Normal account access",
                "LOCKED: Temporary security lock, can be unlocked",
                "TEMPORARILY_LOCKED: Auto-unlocks after 24 hours",
                "PERMANENTLY_LOCKED: Requires manual intervention"
            ],
            "next_steps": "Account accessible; no lock restrictions"
        },
        "action_invalid": {
            "locked": "❌ Account is LOCKED for security reasons. Please contact support to unlock.",
            "temp_locked": "⚠️ Account temporarily locked. Auto-unlocks in 24 hours or contact support.",
            "perm_locked": "❌ Account PERMANENTLY LOCKED. Please visit branch with ID proof.",
            "why_required": "Critical security control for account protection",
            "blocked_actions": ["ALL online/offline transactions when locked"]
        },
        "name_patterns": ["account_lock_status", "lock_status", "account_locked"]
    },
    
    "last_login_date": {
        "definition": "Date and time of the most recent successful login to internet/mobile banking. Used for security monitoring.",
        "mandatory": False,
        "unique": False,
        "sensitive": False,
        "data_type": "date",
        "format_description": "Datetime (YYYY-MM-DD HH:MM:SS)",
        "action_valid": {
            "message": "Last login date recorded",
            "usage": [
                "Displayed on login for security awareness",
                "Detects unauthorized access patterns",
                "Used in dormancy calculations",
                "Security audit trail",
                "Triggers alerts for unusual login times/locations"
            ],
            "next_steps": "Login history tracked for security"
        },
        "action_invalid": {
            "future_date": "Last login date cannot be in the future.",
            "suspicious": "⚠️ Login from unusual location/time detected. Please verify.",
            "why_required": "Important for security monitoring and fraud detection",
            "blocked_actions": ["None (informational only)"]
        },
        "name_patterns": ["last_login_date", "last_login", "login_timestamp"]
    },
    
    "device_id": {
        "definition": "Unique identifier of the device used for mobile/internet banking. Used for device fingerprinting and fraud detection.",
        "mandatory": False,
        "unique": False,
        "sensitive": True,
        "masking_pattern": "***{last_4}",
        "format_regex": r"^[A-Z0-9\-]{10,50}$",
        "format_description": "10-50 alphanumeric characters with hyphens",
        "data_type": "text",
        "action_valid": {
            "message": "Trusted device registered",
            "usage": [
                "Identifies trusted devices for authentication",
                "Detects login from new/unknown devices",
                "Enables device-based security policies",
                "Fraud prevention through device fingerprinting",
                "Allows multi-device management"
            ],
            "next_steps": "Device trusted; seamless authentication enabled"
        },
        "action_invalid": {
            "new_device": "⚠️ Login from new device detected. OTP sent for verification.",
            "suspicious_device": "❌ Login from suspicious device. Additional verification required.",
            "why_required": "Critical for device-based security and fraud prevention",
            "blocked_actions": ["High-value transactions from unverified devices"]
        },
        "name_patterns": ["device_id", "device_token", "device_identifier"]
    },
    
    "ip_address": {
        "definition": "IP address from which the last banking session was initiated. Used for geo-location tracking and fraud detection.",
        "mandatory": False,
        "unique": False,
        "sensitive": True,
        "masking_pattern": "***.***.{last_segment}",
        "format_regex": r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$",
        "format_description": "Standard IPv4 format (e.g., 192.168.1.1)",
        "data_type": "text",
        "action_valid": {
            "message": "IP address logged for security",
            "usage": [
                "Geo-location based security checks",
                "Detects access from unusual locations",
                "Fraud prevention through IP tracking",
                "Compliance with security logging requirements",
                "Enables geo-fencing policies"
            ],
            "next_steps": "IP logged; location-based security active"
        },
        "action_invalid": {
            "invalid_format": "Invalid IP address format.",
            "suspicious_location": "⚠️ Access from unusual location detected. Additional verification required.",
            "why_required": "Important for security and fraud detection",
            "blocked_actions": ["High-value transactions from suspicious IPs"]
        },
        "name_patterns": ["ip_address", "ip", "login_ip"]
    },
    
    # ===== TRANSACTION PROCESSING MODULE (3 columns) =====
    
    "transaction_channel": {
        "definition": "The channel through which the transaction was initiated (ATM, Internet Banking, Mobile App, Branch, POS).",
        "mandatory": True,
        "unique": False,
        "sensitive": False,
        "allowed_values": ["ATM", "INTERNET_BANKING", "MOBILE_APP", "BRANCH", "POS", "UPI", "NEFT", "RTGS", "IMPS"],
        "data_type": "text",
        "action_valid": {
            "message": "Transaction channel is recognized",
            "usage": [
                "Categorizes transactions by channel",
                "Channel-specific fee application",
                "Fraud detection based on channel patterns",
                "Performance analytics per channel",
                "Enables channel-specific limits"
            ],
            "next_steps": "Transaction processed via specified channel"
        },
        "action_invalid": {
            "missing": "Transaction channel is required.",
            "invalid_value": "Invalid channel. Please select from: ATM, Internet Banking, Mobile App, Branch, POS, UPI, NEFT, RTGS, IMPS.",
            "why_required": "Essential for transaction categorization and fee calculation",
            "blocked_actions": ["Transaction cannot be processed without valid channel"]
        },
        "name_patterns": ["transaction_channel", "channel", "txn_channel"]
    },
    
    "transaction_status": {
        "definition": "Current status of the transaction (Success, Pending, Failed, Reversed, In Process).",
        "mandatory": True,
        "unique": False,
        "sensitive": False,
        "allowed_values": ["SUCCESS", "PENDING", "FAILED", "REVERSED", "IN_PROCESS", "CANCELLED"],
        "data_type": "text",
        "action_valid": {
            "message": "Transaction status is tracked",
            "usage": [
                "Determines transaction completion state",
                "Triggers notifications based on status",
                "Affects balance update timing",
                "Used in reconciliation processes",
                "Enables retry for failed transactions"
            ],
            "next_steps": "Status updated in real-time; notifications sent"
        },
        "action_invalid": {
            "missing": "Transaction status is required.",
            "invalid_value": "Invalid status. Please select from: SUCCESS, PENDING, FAILED, REVERSED, IN_PROCESS, CANCELLED.",
            "why_required": "Critical for transaction tracking and reconciliation",
            "blocked_actions": ["Balance not updated until SUCCESS"]
        },
        "name_patterns": ["transaction_status", "status", "txn_status"]
    },
    
    "reversal_flag": {
        "definition": "Boolean flag indicating whether the transaction is a reversal of a previous transaction (refund/chargeback).",
        "mandatory": True,
        "unique": False,
        "sensitive": False,
        "allowed_values": ["TRUE", "FALSE", "YES", "NO", "1", "0"],
        "data_type": "text",
        "action_valid": {
            "message": "Reversal flag status recorded",
            "usage": [
                "Identifies refund/reversal transactions",
                "Restores balance for failed transactions",
                "Links to original transaction for audit",
                "Compliance with dispute resolution",
                "Separate reporting for reversals"
            ],
            "next_steps": "Reversal processed; balance restored"
        },
        "action_invalid": {
            "missing": "Reversal flag is required.",
            "invalid_value": "Reversal flag must be TRUE/FALSE or YES/NO.",
            "why_required": "Essential for proper transaction categorization",
            "blocked_actions": ["None (affects reporting only)"]
        },
        "name_patterns": ["reversal_flag", "is_reversal", "reversal"]
    },
    
    # ===== CHARGES & TAXATION MODULE (2 columns) =====
    
    "charge_amount": {
        "definition": "Service charges or fees applied to the transaction (processing fee, SMS charges, etc.).",
        "mandatory": False,
        "unique": False,
        "sensitive": False,
        "data_type": "numeric",
        "min_value": 0,
        "max_value": 10000,
        "format_description": "Positive numeric value (₹)",
        "action_valid": {
            "message": "Charge amount calculated and applied",
            "usage": [
                "Applied as per fee schedule",
                "Varies by transaction type and account category",
                "Waived for VIP/Senior Citizen accounts (per policy)",
                "Displayed in transaction breakdown",
                "Used in revenue reporting"
            ],
            "next_steps": "Charges debited separately; shown in statement"
        },
        "action_invalid": {
            "negative": "Charge amount cannot be negative.",
            "excessive": "⚠️ Charge amount seems unusually high. Please verify.",
            "why_required": "Transparency in fee application",
            "blocked_actions": ["None (informational only)"]
        },
        "name_patterns": ["charge_amount", "fee", "service_charge"]
    },
    
    "tax_deducted": {
        "definition": "Tax Deducted at Source (TDS) on interest earnings. Mandatory for interest above ₹40,000 per year.",
        "mandatory": False,
        "unique": False,
        "sensitive": False,
        "data_type": "numeric",
        "min_value": 0,
        "max_value": 1000000,
        "format_description": "Positive numeric value (₹)",
        "action_valid": {
            "message": "TDS calculated and deducted as per IT Act",
            "usage": [
                "Compliance with Income Tax regulations",
                "10% TDS if PAN available, 20% if not",
                "Quarterly TDS certificate generated",
                "Reflected in Form 26AS",
                "Claimable as tax credit in ITR"
            ],
            "next_steps": "TDS deducted; certificate issued quarterly"
        },
        "action_invalid": {
            "negative": "TDS amount cannot be negative.",
            "missing_pan": "⚠️ Higher TDS (20%) deducted due to missing PAN. Please update PAN to reduce TDS to 10%.",
            "why_required": "Mandatory tax compliance for interest income",
            "blocked_actions": ["None (regulatory requirement)"]
        },
        "name_patterns": ["tax_deducted", "tds", "tax_amount"]
    },
    
    # ===== INTEREST & RECURRING SERVICES MODULE (3 columns) =====
    
    "interest_rate": {
        "definition": "Annual interest rate applicable to the account (savings interest or loan interest).",
        "mandatory": False,
        "unique": False,
        "sensitive": False,
        "data_type": "numeric",
        "min_value": 0,
        "max_value": 50,
        "format_description": "Percentage value (e.g., 3.5 for 3.5%)",
        "action_valid": {
            "message": "Interest rate applied as per account type",
            "usage": [
                "Determines interest earnings (savings) or cost (loan)",
                "Varies by account type and balance slab",
                "Senior citizens get additional 0.5%",
                "Reviewed periodically (quarterly/annually)",
                "Displayed in account summary"
            ],
            "next_steps": "Interest calculated monthly; credited quarterly"
        },
        "action_invalid": {
            "negative": "Interest rate cannot be negative.",
            "exceeds_max": "⚠️ Interest rate exceeds maximum permissible rate.",
            "zero_for_savings": "⚠️ Interest rate is zero for savings account. Please verify.",
            "why_required": "Determines interest earnings/cost",
            "blocked_actions": ["None (informational only)"]
        },
        "name_patterns": ["interest_rate", "rate", "roi"]
    },
    
    "interest_credit_date": {
        "definition": "Date on which interest is credited to the account (typically quarterly for savings accounts).",
        "mandatory": False,
        "unique": False,
        "sensitive": False,
        "data_type": "date",
        "format_description": "Valid date (YYYY-MM-DD or DD-MM-YYYY)",
        "action_valid": {
            "message": "Interest credit date scheduled",
            "usage": [
                "Interest credited quarterly (end of Mar/Jun/Sep/Dec)",
                "Reflects in account balance immediately",
                "Used for tax calculation (when earned)",
                "Notification sent after credit",
                "Audit trail for interest payments"
            ],
            "next_steps": "Interest will be credited on scheduled date"
        },
        "action_invalid": {
            "future_beyond_quarter": "Interest credit date should not be beyond next quarter end.",
            "invalid_format": "Please enter date in DD-MM-YYYY format.",
            "why_required": "Tracks interest payment schedule",
            "blocked_actions": ["None (informational only)"]
        },
        "name_patterns": ["interest_credit_date", "interest_date", "credit_date"]
    },
    
    "standing_instruction_flag": {
        "definition": "Boolean flag indicating whether standing instructions (auto-pay, recurring transfers) are set up on the account.",
        "mandatory": False,
        "unique": False,
        "sensitive": False,
        "allowed_values": ["TRUE", "FALSE", "YES", "NO", "1", "0"],
        "data_type": "text",
        "action_valid": {
            "message": "Standing instruction status recorded",
            "usage": [
                "Auto-debit for EMI, SIP, bill payments",
                "Recurring fund transfers",
                "Automatic balance maintenance",
                "Requires sufficient balance on due date",
                "Can be modified/cancelled anytime"
            ],
            "next_steps": "Standing instructions will execute on scheduled dates"
        },
        "action_invalid": {
            "missing": "Standing instruction status is missing.",
            "invalid_value": "Standing instruction flag must be TRUE/FALSE or YES/NO.",
            "insufficient_balance": "⚠️ Insufficient balance for standing instruction execution.",
            "why_required": "Enables automated payments and transfers",
            "blocked_actions": ["None (service feature)"]
        },
        "name_patterns": ["standing_instruction_flag", "si_flag", "auto_debit"]
    },
    
    # ===== NOMINEE & COMPLIANCE MODULE (2 columns) =====
    
    "nominee_relation": {
        "definition": "Relationship of the nominee to the account holder (Spouse, Son, Daughter, Father, Mother, etc.).",
        "mandatory": False,
        "unique": False,
        "sensitive": False,
        "allowed_values": ["SPOUSE", "SON", "DAUGHTER", "FATHER", "MOTHER", "BROTHER", "SISTER", "GUARDIAN", "OTHER"],
        "data_type": "text",
        "action_valid": {
            "message": "Nominee relationship recorded",
            "usage": [
                "Validates nominee eligibility",
                "Required for claim processing",
                "Legal documentation",
                "Succession planning",
                "Insurance claim settlement"
            ],
            "next_steps": "Nominee relationship verified"
        },
        "action_invalid": {
            "missing_with_nominee": "Nominee relationship is required when nominee is registered.",
            "invalid_value": "Invalid relationship. Please select from: Spouse, Son, Daughter, Father, Mother, Brother, Sister, Guardian, Other.",
            "why_required": "Required for legal succession planning",
            "blocked_actions": ["None (nominee registration)"]
        },
        "name_patterns": ["nominee_relation", "relation", "relationship"]
    },
    
    "freeze_reason": {
        "definition": "Reason for account freeze/block (if applicable). NULL for active accounts. Examples: Court Order, Suspicious Activity, Customer Request.",
        "mandatory": False,
        "unique": False,
        "sensitive": False,
        "allowed_values": ["COURT_ORDER", "SUSPICIOUS_ACTIVITY", "CUSTOMER_REQUEST", "REGULATORY_COMPLIANCE", "DECEASED", "UNDER_INVESTIGATION", None],
        "data_type": "text",
        "action_valid": {
            "message": "Freeze reason recorded (account frozen)",
            "usage": [
                "Documents reason for account freeze",
                "Compliance with legal/regulatory requirements",
                "Customer protection (suspicious activity)",
                "Audit trail for frozen accounts",
                "Determines unfreeze process"
            ],
            "next_steps": "Account frozen; specific process required to unfreeze"
        },
        "action_invalid": {
            "active_with_freeze": "⚠️ Account status is ACTIVE but freeze reason exists. Please verify.",
            "invalid_value": "Invalid freeze reason. Please select from: Court Order, Suspicious Activity, Customer Request, Regulatory Compliance, Deceased, Under Investigation.",
            "why_required": "Legal documentation for account freeze",
            "blocked_actions": ["ALL transactions blocked when account frozen"]
        },
        "name_patterns": ["freeze_reason", "block_reason", "hold_reason"]
    },
    
    # ===== AML & FRAUD DETECTION MODULE (3 columns) =====
    
    "aml_alert_flag": {
        "definition": "Anti-Money Laundering alert flag. TRUE if suspicious patterns detected requiring manual review.",
        "mandatory": True,
        "unique": False,
        "sensitive": False,
        "allowed_values": ["TRUE", "FALSE", "YES", "NO", "1", "0"],
        "data_type": "text",
        "action_valid": {
            "message": "AML monitoring status tracked",
            "usage": [
                "Flags suspicious transaction patterns",
                "Triggers manual compliance review",
                "Regulatory reporting (STR/CTR)",
                "Prevents financial crimes",
                "Customer due diligence (CDD/EDD)"
            ],
            "next_steps": "AML monitoring active; alerts generated for suspicious activity"
        },
        "action_invalid": {
            "missing": "AML alert flag is required for compliance.",
            "flagged": "⚠️ AML ALERT: This account requires compliance review. High-value transactions may be delayed.",
            "invalid_value": "AML alert flag must be TRUE/FALSE or YES/NO.",
            "why_required": "Mandatory for anti-money laundering compliance",
            "blocked_actions": ["Large cash transactions pending AML review"]
        },
        "name_patterns": ["aml_alert_flag", "aml_flag", "suspicious_flag"]
    },
    
    "suspicious_txn_score": {
        "definition": "Risk score (0-100) indicating likelihood of suspicious activity. Calculated by fraud detection algorithms.",
        "mandatory": False,
        "unique": False,
        "sensitive": False,
        "data_type": "numeric",
        "min_value": 0,
        "max_value": 100,
        "format_description": "Integer value 0-100 (0=safe, 100=highly suspicious)",
        "action_valid": {
            "message": "Suspicious transaction score calculated",
            "usage": [
                "0-30: Low risk (normal processing)",
                "31-60: Medium risk (enhanced monitoring)",
                "61-100: High risk (manual review required)",
                "Machine learning based scoring",
                "Updated with each transaction"
            ],
            "next_steps": "Score monitored; alerts triggered for high scores"
        },
        "action_invalid": {
            "negative": "Score cannot be negative.",
            "exceeds_max": "Score must be between 0-100.",
            "high_score": "⚠️ HIGH RISK: Suspicious score > 60. Transaction requires manual approval.",
            "why_required": "Critical for fraud detection",
            "blocked_actions": ["High-value transactions when score > 60"]
        },
        "name_patterns": ["suspicious_txn_score", "fraud_score", "risk_score"]
    },
    
    "customer_consent": {
        "definition": "Boolean flag indicating customer's consent for data sharing, marketing communications, and third-party services.",
        "mandatory": True,
        "unique": False,
        "sensitive": False,
        "allowed_values": ["TRUE", "FALSE", "YES", "NO", "1", "0"],
        "data_type": "text",
        "action_valid": {
            "message": "Customer consent recorded",
            "usage": [
                "Compliance with data privacy regulations (GDPR, RBI guidelines)",
                "Controls marketing communications",
                "Third-party data sharing permissions",
                "Can be withdrawn anytime",
                "Affects service offerings and promotions"
            ],
            "next_steps": "Consent preferences applied to account"
        },
        "action_invalid": {
            "missing": "Customer consent status is required for data privacy compliance.",
            "invalid_value": "Consent must be TRUE/FALSE or YES/NO.",
            "not_consented": "ℹ️ Customer has not consented to marketing communications.",
            "why_required": "Mandatory for data privacy and regulatory compliance",
            "blocked_actions": ["Marketing communications if not consented"]
        },
        "name_patterns": ["customer_consent", "consent", "data_consent"]
    },
    
    # ===== REPORTING & NOTIFICATIONS MODULE (2 columns) =====
    
    "statement_cycle": {
        "definition": "Frequency of account statement generation and delivery (Monthly, Quarterly, Annually).",
        "mandatory": True,
        "unique": False,
        "sensitive": False,
        "allowed_values": ["MONTHLY", "QUARTERLY", "HALF_YEARLY", "ANNUALLY"],
        "data_type": "text",
        "action_valid": {
            "message": "Statement cycle preference set",
            "usage": [
                "Determines statement generation frequency",
                "Email/physical delivery as per preference",
                "Available for download anytime",
                "Compliance with regulatory requirements",
                "Can be changed via internet banking"
            ],
            "next_steps": "Statements generated as per selected cycle"
        },
        "action_invalid": {
            "missing": "Statement cycle is required.",
            "invalid_value": "Invalid cycle. Please select from: Monthly, Quarterly, Half-Yearly, Annually.",
            "why_required": "Determines statement delivery schedule",
            "blocked_actions": ["None (affects statement delivery only)"]
        },
        "name_patterns": ["statement_cycle", "statement_frequency", "cycle"]
    },
    
    "notification_preference": {
        "definition": "Customer's preferred channel for notifications (SMS, Email, Push, WhatsApp, All).",
        "mandatory": True,
        "unique": False,
        "sensitive": False,
        "allowed_values": ["SMS", "EMAIL", "PUSH", "WHATSAPP", "ALL", "NONE"],
        "data_type": "text",
        "action_valid": {
            "message": "Notification preference set",
            "usage": [
                "Sends alerts via preferred channel",
                "Transaction notifications",
                "Balance alerts",
                "Security alerts (cannot be disabled)",
                "Promotional messages (if consented)"
            ],
            "next_steps": "Notifications sent via preferred channel"
        },
        "action_invalid": {
            "missing": "Notification preference is required.",
            "invalid_value": "Invalid preference. Please select from: SMS, Email, Push, WhatsApp, All, None.",
            "security_override": "ℹ️ Security alerts cannot be disabled regardless of preference.",
            "why_required": "Ensures timely alerts and notifications",
            "blocked_actions": ["None (notification delivery only)"]
        },
        "name_patterns": ["notification_preference", "notification_channel", "alert_preference"]
    }
}
