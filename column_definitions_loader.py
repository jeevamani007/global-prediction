"""
Column Definitions Loader
Loads column definitions from the .md file content
"""

# Column definitions extracted from .md file
COLUMN_DEFINITIONS = {
    "customer_id": {
        "description": "Unique identifier for each customer in the system, used as primary key for customer records",
        "section": "Customer & Account"
    },
    "account_number": {
        "description": "Unique account identifier assigned to customer for all banking transactions and operations",
        "section": "Customer & Account"
    },
    "account_type": {
        "description": "Defines account category (Savings/Current/Fixed Deposit/Recurring) determining features and interest rates",
        "section": "Customer & Account"
    },
    "first_name": {
        "description": "Customer's legal first name as per government-issued identification documents",
        "section": "Customer & Account"
    },
    "last_name": {
        "description": "Customer's legal surname/family name for official correspondence and legal purposes",
        "section": "Customer & Account"
    },
    "middle_name": {
        "description": "Customer's middle name for complete identification and name matching with official records",
        "section": "Customer & Account"
    },
    "date_of_birth": {
        "description": "Customer's birth date used for age verification, eligibility checks, and senior citizen benefits",
        "section": "Customer & Account"
    },
    "gender": {
        "description": "Customer's gender for demographic analysis, targeted products, and regulatory reporting",
        "section": "Customer & Account"
    },
    "email_address": {
        "description": "Primary email for account statements, notifications, alerts, and digital communication",
        "section": "Customer & Account"
    },
    "phone_number": {
        "description": "Landline number for customer contact, verification calls, and official communication",
        "section": "Customer & Account"
    },
    "mobile_number": {
        "description": "Primary mobile for OTP authentication, SMS alerts, and mobile banking access",
        "section": "Customer & Account"
    },
    "pan_number": {
        "description": "Permanent Account Number for tax compliance, high-value transactions, and KYC verification",
        "section": "Customer & Account"
    },
    "aadhar_number": {
        "description": "Unique identification number for Indian citizens used for eKYC and government verification",
        "section": "Customer & Account"
    },
    "passport_number": {
        "description": "International identification for NRI customers and foreign exchange transactions",
        "section": "Customer & Account"
    },
    "nationality": {
        "description": "Customer's citizenship determining account type eligibility and foreign exchange regulations",
        "section": "Customer & Account"
    },
    "marital_status": {
        "description": "Marital status for joint account eligibility, nominee relationships, and customer profiling",
        "section": "Customer & Account"
    },
    "occupation": {
        "description": "Employment details for credit assessment, risk profiling, and income verification",
        "section": "Customer & Account"
    },
    "annual_income": {
        "description": "Yearly income for loan eligibility, credit limit decisions, and customer segmentation",
        "section": "Customer & Account"
    },
    "address_line1": {
        "description": "Primary residential address for correspondence, verification, and regulatory compliance",
        "section": "Customer & Account"
    },
    "address_line2": {
        "description": "Additional address details (apartment/suite number) for accurate delivery and location",
        "section": "Customer & Account"
    },
    "city": {
        "description": "Customer's city of residence for regional marketing, branch allocation, and demographic analysis",
        "section": "Location & Identity"
    },
    "state": {
        "description": "State/province for regulatory compliance, tax calculations, and regional product offerings",
        "section": "Location & Identity"
    },
    "country": {
        "description": "Country of residence determining tax laws, regulations, and international banking rules",
        "section": "Location & Identity"
    },
    "postal_code": {
        "description": "ZIP/PIN code for address verification, location-based services, and delivery logistics",
        "section": "Location & Identity"
    },
    "landmark": {
        "description": "Nearby reference point for easy address location during field verification visits",
        "section": "Location & Identity"
    },
    "residence_type": {
        "description": "Owned/Rented status affecting loan eligibility, stability assessment, and address proof requirements",
        "section": "Location & Identity"
    },
    "kyc_status": {
        "description": "Know Your Customer verification status (Pending/Verified/Rejected) for regulatory compliance",
        "section": "Location & Identity"
    },
    "kyc_verification_date": {
        "description": "Date when KYC documents were verified for tracking renewal and compliance audits",
        "section": "Location & Identity"
    },
    "customer_status": {
        "description": "Current account holder status (Active/Inactive/Suspended/Closed) controlling access and operations",
        "section": "Location & Identity"
    },
    "customer_category": {
        "description": "Customer classification (Retail/Corporate/Premium/NRI) determining services and fee structures",
        "section": "Location & Identity"
    },
    "risk_rating": {
        "description": "Credit and fraud risk score (Low/Medium/High) for transaction monitoring and lending decisions",
        "section": "Location & Identity"
    },
    "customer_segment": {
        "description": "Marketing segment (Mass/Affluent/HNI) for targeted product offerings and relationship management",
        "section": "Location & Identity"
    },
    "branch_code": {
        "description": "Home branch identifier where account was opened for service allocation and reporting",
        "section": "Location & Identity"
    },
    "branch_name": {
        "description": "Branch name for customer reference, physical service location, and regional management",
        "section": "Location & Identity"
    },
    "relationship_manager_id": {
        "description": "Dedicated banker assigned to high-value customers for personalized service and portfolio management",
        "section": "Location & Identity"
    },
    "account_status": {
        "description": "Current operational status (Active/Dormant/Frozen/Closed) controlling transaction permissions",
        "section": "Account & Balance"
    },
    "account_opening_date": {
        "description": "Date account was created for tenure calculation, anniversary offers, and historical tracking",
        "section": "Account & Balance"
    },
    "account_closing_date": {
        "description": "Date account was terminated for final settlement, retention analysis, and record archival",
        "section": "Account & Balance"
    },
    "available_balance": {
        "description": "Funds available for immediate withdrawal after deducting holds and pending transactions",
        "section": "Account & Balance"
    },
    "current_balance": {
        "description": "Total account balance including holds and uncleared funds for ledger reconciliation",
        "section": "Account & Balance"
    },
    "minimum_balance": {
        "description": "Required balance threshold to avoid penalty charges and maintain account benefits",
        "section": "Account & Balance"
    },
    "overdraft_limit": {
        "description": "Maximum negative balance permitted for current accounts in emergency situations",
        "section": "Account & Balance"
    },
    "interest_rate": {
        "description": "Annual percentage rate applied to account balance for interest calculation and accrual",
        "section": "Account & Balance"
    },
    "maturity_date": {
        "description": "End date for fixed deposits when principal and interest become payable",
        "section": "Account & Balance"
    },
    "nominee_name": {
        "description": "Legal beneficiary who receives account proceeds in case of account holder's death",
        "section": "Account & Balance"
    },
    "nominee_relationship": {
        "description": "Relation between account holder and nominee (Spouse/Child/Parent) for legal validation",
        "section": "Account & Balance"
    },
    "joint_account_holder": {
        "description": "Secondary owner with equal rights for joint accounts requiring dual authorization",
        "section": "Account & Balance"
    },
    "account_currency": {
        "description": "Base currency (USD/INR/EUR) for multi-currency accounts and foreign exchange operations",
        "section": "Account & Balance"
    },
    "last_transaction_date": {
        "description": "Most recent transaction date for dormancy identification and inactive account processing",
        "section": "Account & Balance"
    },
    "dormant_flag": {
        "description": "Indicator showing account has no transactions for extended period requiring reactivation process",
        "section": "Account & Balance"
    },
    "transaction_id": {
        "description": "Unique identifier for each transaction ensuring traceability and preventing duplicate processing",
        "section": "Transaction"
    },
    "transaction_date": {
        "description": "Calendar date when transaction occurred for daily reconciliation and financial reporting",
        "section": "Transaction"
    },
    "transaction_time": {
        "description": "Exact timestamp for transaction sequencing, cutoff time validation, and audit trails",
        "section": "Transaction"
    },
    "transaction_type": {
        "description": "Category of transaction (Deposit/Withdrawal/Transfer/Payment) for processing rules and fee application",
        "section": "Transaction"
    },
    "transaction_amount": {
        "description": "Monetary value of transaction for balance updates, limit checks, and financial reporting",
        "section": "Transaction"
    },
    "transaction_currency": {
        "description": "Currency code for foreign exchange transactions and multi-currency account operations",
        "section": "Transaction"
    },
    "debit_credit_flag": {
        "description": "Indicator showing money movement direction (Debit reduces balance, Credit increases balance)",
        "section": "Transaction"
    },
    "transaction_status": {
        "description": "Processing state (Pending/Success/Failed/Reversed) for reconciliation and customer notification",
        "section": "Transaction"
    },
    "payment_mode": {
        "description": "Method used (Cash/Cheque/NEFT/RTGS/UPI) determining processing time and fees",
        "section": "Transaction"
    },
    "reference_number": {
        "description": "External reference like UTR/ARN for tracking inter-bank transactions and dispute resolution",
        "section": "Transaction"
    },
    "cheque_number": {
        "description": "Physical cheque number for matching deposits/withdrawals with issued cheque books",
        "section": "Transaction"
    },
    "beneficiary_account": {
        "description": "Recipient's account number for fund transfers ensuring money reaches correct destination",
        "section": "Transaction"
    },
    "beneficiary_name": {
        "description": "Payee name for verification, name matching, and preventing fraud in transfers",
        "section": "Transaction"
    },
    "beneficiary_ifsc": {
        "description": "Bank identifier code for routing inter-bank transfers to correct branch and account",
        "section": "Transaction"
    },
    "transaction_description": {
        "description": "Purpose/remarks explaining transaction reason for customer reference and audit purposes",
        "section": "Transaction"
    },
    "created_date": {
        "description": "Timestamp when record was first inserted for data lifecycle tracking and audit compliance",
        "section": "Audit & System"
    },
    "created_by": {
        "description": "User/system ID who created record establishing accountability for data entry actions",
        "section": "Audit & System"
    },
    "modified_date": {
        "description": "Latest timestamp when record was updated for change tracking and version control",
        "section": "Audit & System"
    },
    "modified_by": {
        "description": "User ID who last modified record for audit trails and unauthorized change detection",
        "section": "Audit & System"
    },
    "approved_date": {
        "description": "When transaction/change was authorized completing maker-checker workflow process",
        "section": "Audit & System"
    },
    "approved_by": {
        "description": "Approver's user ID validating dual control for high-risk operations and compliance",
        "section": "Audit & System"
    },
    "rejected_date": {
        "description": "When transaction/application was declined for quality control and rejection analysis",
        "section": "Audit & System"
    },
    "rejected_by": {
        "description": "User who rejected request with authority establishing accountability for denial decisions",
        "section": "Audit & System"
    },
    "maker_id": {
        "description": "User who initiated transaction in dual control system preventing single-person fraud",
        "section": "Audit & System"
    },
    "checker_id": {
        "description": "User who verified/approved transaction completing segregation of duties for critical operations",
        "section": "Audit & System"
    },
    "authorization_status": {
        "description": "Approval workflow state (Pending/Approved/Rejected) for transaction processing control",
        "section": "Audit & System"
    },
    "ip_address": {
        "description": "Network address of transaction origin for fraud detection and geographic access monitoring",
        "section": "Audit & System"
    },
    "session_id": {
        "description": "Unique session identifier linking related actions and detecting unauthorized access attempts",
        "section": "Audit & System"
    },
    "is_active": {
        "description": "Soft delete flag showing record validity without physical deletion for data retention compliance",
        "section": "Audit & System"
    },
    "is_deleted": {
        "description": "Logical deletion marker preserving data for audit while hiding from active operations",
        "section": "Audit & System"
    }
}
