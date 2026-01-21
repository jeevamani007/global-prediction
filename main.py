from fastapi import FastAPI, File, UploadFile, HTTPException, Request
from typing import List, Dict, Any, Optional
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from financial import FinancialDomainDetector
import human_resource
from insurence import InsuranceDomainDetector
import os
from government import GovernmentDomainDetector
from bank import BankingDomainDetector  # your banking domain detector class
from banking_dataset_validator import BankingDatasetValidator  # Banking Dataset Validator
from core_banking_validator import CoreBankingValidator  # Core Banking Validation Engine
from database import engine
from sqlalchemy import text
from health_care import HealthcareDomainDetector
from retail import RetailDomainDetector
from space import SpaceDomainDetector
from human_resource import HRDomainDetector
from file_converter import FileConverter
from data_validation_engine import DataValidationEngine
from complete_banking_validator import CompleteBankingValidator
from multi_file_processor import MultiFileProcessor
from application_structure import BankingApplicationStructureGenerator


app = FastAPI(title="Domain Detection API")


def _normalize_column_name(col_name: str) -> str:
    """Normalize column name for matching in business rule mapping."""
    return str(col_name).lower().strip().replace(" ", "_").replace("-", "_")


def _map_column_to_business_rule_template(col_name: str) -> Dict[str, Any]:
    """
    Map a raw column name to a standard banking business rule template.

    Each template follows the user-required format:
    - Definition
    - Condition
    - Action (valid)
    - Action (invalid)
    """
    n = _normalize_column_name(col_name)

    # Helper predicates
    def has(*parts: str) -> bool:
        return all(p in n for p in parts)

    def pts(*items: str) -> List[str]:
        """Convenience: return up to 2 short bullet points."""
        out = [i.strip() for i in items if i and i.strip()]
        return out[:2]

    def one_line(s: str) -> str:
        return " ".join(str(s).split())

    # Core templates
    templates: List[Dict[str, Any]] = [
        # ===== Security / Access / Device / Consent (NEW) =====
        {
            "match": lambda: n == "otp_verified" or ("otp" in n and "verified" in n),
            "column_name": "otp_verified",
            "display_name": "OTP Verified",
            "is_sensitive": False,
            "definition": "Shows whether the customer completed OTP verification, protecting accounts from unauthorized access.",
            "condition": "Must be a boolean/flag value (true/false, Y/N, 1/0). Required for digital journeys where OTP is used.",
            "action_valid_points": pts(
                "OTP checks are satisfied, so protected flows can continue.",
                "Used for login security and high-risk action approval."
            ),
            "action_invalid_points": pts(
                "OTP flag is missing/invalid, so verification cannot be trusted.",
                "Fix the value to enable OTP-protected actions (do not block other fields)."
            ),
            "ui_issue_message": "OTP verification flag is missing/invalid. Verify OTP to proceed with protected actions.",
        },
        {
            "match": lambda: n == "login_attempts" or ("login" in n and "attempt" in n),
            "column_name": "login_attempts",
            "display_name": "Login Attempts",
            "is_sensitive": False,
            "definition": "Counts recent failed login attempts to detect brute-force attacks and protect the account.",
            "condition": "Must be a non-negative integer (0–20 typical). If present, cannot be negative or non-numeric.",
            "action_valid_points": pts(
                "Value is consistent, so login security controls can work correctly.",
                "Used for throttling, OTP/captcha triggers, and security alerts."
            ),
            "action_invalid_points": pts(
                "Value must be a non-negative number (0,1,2...).",
                "Correct this to enable accurate lock/throttle logic; other fields stay usable."
            ),
            "ui_issue_message": "Login attempts must be a non‑negative number.",
        },
        {
            "match": lambda: n == "account_lock_status" or (("account" in n or "acct" in n) and "lock" in n),
            "column_name": "account_lock_status",
            "display_name": "Account Lock Status",
            "is_sensitive": False,
            "definition": "Indicates whether the account is locked due to security risk, compliance action, or repeated failures.",
            "condition": "Must be a valid status flag (LOCKED/UNLOCKED or true/false). If LOCKED, sensitive actions must be restricted.",
            "action_valid_points": pts(
                "Lock status is clear, so access controls can be enforced correctly.",
                "Used to block/allow transactions and logins safely."
            ),
            "action_invalid_points": pts(
                "Value must be LOCKED/UNLOCKED (or an equivalent flag).",
                "Fix this so only lock-dependent actions are enforced correctly."
            ),
            "ui_issue_message": "Account lock status is invalid. Use LOCKED/UNLOCKED (or true/false).",
        },
        {
            "match": lambda: n == "last_login_date" or ("last" in n and "login" in n and "date" in n),
            "column_name": "last_login_date",
            "display_name": "Last Login Date",
            "is_sensitive": False,
            "definition": "Tracks when the customer last authenticated to help detect unusual activity and support audits.",
            "condition": "Must be a valid date/time, not in the far future, and reasonably recent for active users.",
            "action_valid_points": pts(
                "Timestamp is valid, so security/audit history is reliable.",
                "Used for fraud monitoring and inactivity checks."
            ),
            "action_invalid_points": pts(
                "Provide a valid timestamp (not in the future).",
                "Until fixed, login-audit analytics may be skipped for this record only."
            ),
            "ui_issue_message": "Last login date is invalid. Provide a valid timestamp (not in the future).",
        },
        {
            "match": lambda: n == "device_id" or ("device" in n and "id" in n),
            "column_name": "device_id",
            "display_name": "Device ID",
            "is_sensitive": True,
            "definition": "Identifies the customer device used for login/transactions, enabling device trust and fraud prevention.",
            "condition": "Optional but recommended. If present, must be non-empty, consistent, and within 6–128 characters (alphanumeric/hyphen/underscore).",
            "action_valid_points": pts(
                "Device identifier is usable for security checks (masked in UI).",
                "Used for device trust, risk scoring, and anomaly detection."
            ),
            "action_invalid_points": pts(
                "Device ID is missing/invalid, so device-based checks are weakened.",
                "Fix it to enable device trust; do not block unrelated workflows."
            ),
            "ui_issue_message": "Device ID is missing/invalid. Provide a valid device identifier.",
        },
        {
            "match": lambda: n == "ip_address" or (n.endswith("ip") or ("ip" in n and "address" in n)),
            "column_name": "ip_address",
            "display_name": "IP Address",
            "is_sensitive": True,
            "definition": "Captures the network source of a login or transaction, used for security monitoring and geolocation risk checks.",
            "condition": "Optional but recommended. If present, must be a valid IPv4 or IPv6 address.",
            "action_valid_points": pts(
                "IP is valid (masked in UI), so security/audit trails remain reliable.",
                "Used for geo/IP rules and suspicious access detection."
            ),
            "action_invalid_points": pts(
                "Provide a valid IPv4/IPv6 address.",
                "Until fixed, IP-based risk checks may be skipped for this record only."
            ),
            "ui_issue_message": "IP address is invalid. Provide a valid IPv4/IPv6 value.",
        },
        {
            "match": lambda: n == "customer_consent" or ("consent" in n),
            "column_name": "customer_consent",
            "display_name": "Customer Consent",
            "is_sensitive": False,
            "definition": "Records customer permission for data usage (marketing, notifications, sharing), required for privacy compliance.",
            "condition": "Must be a valid consent flag (true/false, Y/N) and must be TRUE where consent is legally required for processing.",
            "action_valid_points": pts(
                "Consent is recorded, so processing/communication is compliant.",
                "Used to control notifications and privacy-sensitive operations."
            ),
            "action_invalid_points": pts(
                "Consent is missing/invalid, so privacy-controlled actions must be paused.",
                "Fix consent to enable marketing/optional communications (other fields stay OK)."
            ),
            "ui_issue_message": "Customer consent is missing/invalid. Confirm consent to proceed with privacy-controlled actions.",
        },

        # ===== Branch / Account metadata (NEW) =====
        {
            "match": lambda: n == "branch_code" or ("branch" in n and "code" in n),
            "column_name": "branch_code",
            "display_name": "Branch Code",
            "is_sensitive": False,
            "definition": "Uniquely identifies the servicing branch for an account, used for routing, reporting, and branch-level controls.",
            "condition": "Should be non-empty and match bank format (often alphanumeric 3–15). If IFSC is used, branch code should align with it.",
            "action_valid_points": pts(
                "Branch mapping is reliable for routing and ownership.",
                "Used for branch-wise reporting and transfer processing."
            ),
            "action_invalid_points": pts(
                "Branch code is missing/invalid, so routing/reporting may be wrong.",
                "Enter the correct branch code to enable branch-level controls."
            ),
            "ui_issue_message": "Branch code is missing/invalid. Enter the correct branch code.",
        },
        {
            "match": lambda: n == "branch_name" or ("branch" in n and "name" in n),
            "column_name": "branch_name",
            "display_name": "Branch Name",
            "is_sensitive": False,
            "definition": "Human-readable branch name used for customer communication and operational reports.",
            "condition": "Optional but recommended. If present, should be 3–100 characters and not purely numeric.",
            "action_valid_points": pts(
                "Branch display is correct for statements and reports.",
                "Used in customer communication and internal dashboards."
            ),
            "action_invalid_points": pts(
                "Branch name looks invalid or empty.",
                "Provide a proper name to avoid confusion in reports (no full-file failure)."
            ),
            "ui_issue_message": "Branch name looks invalid. Provide a proper branch name.",
        },
        {
            "match": lambda: n == "ifsc_verified" or ("ifsc" in n and "verified" in n),
            "column_name": "ifsc_verified",
            "display_name": "IFSC Verified",
            "is_sensitive": False,
            "definition": "Shows whether the IFSC/branch routing code has been verified to prevent misrouted transfers.",
            "condition": "Must be a boolean/flag value (true/false, Y/N, 1/0). Required for payments that rely on IFSC.",
            "action_valid_points": pts(
                "Routing verification is confirmed, reducing transfer failures.",
                "Used to safely allow IFSC-based transfers."
            ),
            "action_invalid_points": pts(
                "IFSC verification flag is missing/invalid.",
                "Verify routing code to enable IFSC-dependent transfers only."
            ),
            "ui_issue_message": "IFSC verification flag is missing/invalid. Verify routing code before transfers.",
        },
        {
            "match": lambda: n == "customer_category" or ("customer" in n and "category" in n),
            "column_name": "customer_category",
            "display_name": "Customer Category",
            "is_sensitive": False,
            "definition": "Classifies customers (e.g., Retail, Corporate, Staff, NRI) to apply correct risk, pricing, and service rules.",
            "condition": "Must be one of the bank’s allowed categories; cannot be empty for active customers.",
            "action_valid_points": pts(
                "Category is recognized, enabling correct pricing and limits.",
                "Used for segmentation, service eligibility, and compliance rules."
            ),
            "action_invalid_points": pts(
                "Customer category is missing/invalid.",
                "Choose an allowed category to apply correct pricing/limits (no full-file failure)."
            ),
            "ui_issue_message": "Customer category is missing/invalid. Choose an allowed category.",
        },
        {
            "match": lambda: n == "account_open_date" or (("account" in n or "acct" in n) and "open" in n and "date" in n),
            "column_name": "account_open_date",
            "display_name": "Account Open Date",
            "is_sensitive": False,
            "definition": "Date the account was opened, used for lifecycle tracking, interest accrual, and compliance history.",
            "condition": "Must be a valid date, not in the future. Required for account lifecycle reporting.",
            "action_valid_points": pts(
                "Account lifecycle date is valid for audits and reporting.",
                "Used for account age, interest eligibility, and compliance history."
            ),
            "action_invalid_points": pts(
                "Open date is missing/invalid (or in the future).",
                "Provide a valid date; lifecycle reporting may be affected until fixed."
            ),
            "ui_issue_message": "Account open date is invalid/missing. Provide a valid date (not in the future).",
        },
        {
            "match": lambda: n == "account_closure_date" or (("account" in n or "acct" in n) and ("closure" in n or "close" in n) and "date" in n),
            "column_name": "account_closure_date",
            "display_name": "Account Closure Date",
            "is_sensitive": False,
            "definition": "Records when an account was closed, required for audits, regulatory retention, and preventing post-closure activity.",
            "condition": "Optional for active accounts. If present, must be a valid date on/after account_open_date.",
            "action_valid_points": pts(
                "Closure date is consistent with the account lifecycle.",
                "Used to prevent post-closure activity and support audits."
            ),
            "action_invalid_points": pts(
                "Closure date is invalid or before open date.",
                "Fix it to ensure correct closure enforcement (do not block other fields)."
            ),
            "ui_issue_message": "Account closure date is invalid or before account open date.",
        },

        # ===== Limits / Controls (NEW) =====
        {
            "match": lambda: n == "daily_transaction_limit" or ("daily" in n and "transaction" in n and "limit" in n),
            "column_name": "daily_transaction_limit",
            "display_name": "Daily Transaction Limit",
            "is_sensitive": True,
            "definition": "Maximum total transaction value allowed per day to control risk and prevent abuse.",
            "condition": "Must be numeric and >= 0. If present, should be consistent with customer category and account type policies.",
            "action_valid_points": pts(
                "Daily cap is valid, enabling limit enforcement and risk control.",
                "Used to block only excess transactions and trigger alerts."
            ),
            "action_invalid_points": pts(
                "Limit must be a non-negative number.",
                "Fix this to enforce daily caps; do not block unrelated processing."
            ),
            "ui_issue_message": "Daily transaction limit must be a non‑negative number.",
        },
        {
            "match": lambda: n == "monthly_transaction_limit" or ("monthly" in n and "transaction" in n and "limit" in n),
            "column_name": "monthly_transaction_limit",
            "display_name": "Monthly Transaction Limit",
            "is_sensitive": True,
            "definition": "Maximum total transaction value allowed per month for risk control and compliance.",
            "condition": "Must be numeric and >= 0. Should be >= daily_transaction_limit in normal setups.",
            "action_valid_points": pts(
                "Monthly cap is valid for long-term risk control.",
                "Used for limit enforcement and AML/risk monitoring."
            ),
            "action_invalid_points": pts(
                "Limit must be a non-negative number (and usually >= daily limit).",
                "Fix it to enforce monthly caps; other fields remain usable."
            ),
            "ui_issue_message": "Monthly transaction limit must be a non‑negative number (and usually ≥ daily limit).",
        },
        {
            "match": lambda: n == "atm_withdrawal_limit" or ("atm" in n and ("withdraw" in n or "withdrawal" in n) and "limit" in n),
            "column_name": "atm_withdrawal_limit",
            "display_name": "ATM Withdrawal Limit",
            "is_sensitive": True,
            "definition": "Caps cash withdrawals to prevent fraud and reduce cash-out risk.",
            "condition": "Must be numeric and >= 0. Typically aligns with account type and customer risk level.",
            "action_valid_points": pts(
                "ATM limit is valid, enabling cash-out risk control.",
                "Used to enforce caps and raise alerts for unusual withdrawals."
            ),
            "action_invalid_points": pts(
                "ATM limit must be a non-negative number.",
                "Fix it to enforce ATM caps; do not block other operations."
            ),
            "ui_issue_message": "ATM withdrawal limit must be a non‑negative number.",
        },
        {
            "match": lambda: n == "account_lock_status" or ("lock" in n and "status" in n),
            "column_name": "account_lock_status",
            "display_name": "Account Lock Status",
            "is_sensitive": False,
            "definition": "Indicates whether access is temporarily restricted due to security or policy.",
            "condition": "Must be LOCKED/UNLOCKED or equivalent flag. If LOCKED, transactions and logins must be restricted.",
            "action_valid": "Marked as VALID. Used to enforce security restrictions and reduce fraud.",
            "action_invalid": "Highlight this field only. Message: “Account lock status is invalid. Use LOCKED/UNLOCKED.” Lock enforcement may not work until corrected.",
        },

        # ===== Transactions (NEW) =====
        {
            "match": lambda: n == "transaction_channel" or ("transaction" in n and "channel" in n),
            "column_name": "transaction_channel",
            "display_name": "Transaction Channel",
            "is_sensitive": False,
            "definition": "Specifies where the transaction occurred (ATM, UPI, NEFT, IMPS, POS, ONLINE), used for risk scoring and reporting.",
            "condition": "Must be one of the bank’s allowed channels; cannot be empty for transaction datasets.",
            "action_valid_points": pts(
                "Channel is recognized, enabling channel-based controls.",
                "Used for fraud checks, limits, and reporting."
            ),
            "action_invalid_points": pts(
                "Transaction channel is missing/invalid.",
                "Select a valid channel to apply channel rules (no full-file failure)."
            ),
            "ui_issue_message": "Transaction channel is missing/invalid. Select a valid channel (ATM/UPI/NEFT/IMPS/POS/ONLINE).",
        },
        {
            "match": lambda: n == "transaction_status" or ("transaction" in n and "status" in n),
            "column_name": "transaction_status",
            "display_name": "Transaction Status",
            "is_sensitive": False,
            "definition": "Indicates whether a transaction is successful, pending, failed, or reversed—critical for reconciliation and customer communication.",
            "condition": "Must be one of the allowed statuses (SUCCESS, PENDING, FAILED, REVERSED).",
            "action_valid_points": pts(
                "Status is valid, so statements and reconciliation are accurate.",
                "Used for settlement and customer notifications."
            ),
            "action_invalid_points": pts(
                "Status must be SUCCESS/PENDING/FAILED/REVERSED.",
                "Fix it to reconcile this transaction; do not block the whole dataset."
            ),
            "ui_issue_message": "Transaction status is missing/invalid. Use SUCCESS/PENDING/FAILED/REVERSED.",
        },
        {
            "match": lambda: n == "transaction_status" or ("status" in n and "transaction" in n),
            "column_name": "transaction_status",
            "display_name": "Transaction Status",
            "is_sensitive": False,
            "definition": "Tracks transaction completion state for reconciliation and customer updates.",
            "condition": "Allowed values typically include SUCCESS, PENDING, FAILED, REVERSED.",
            "action_valid": "Marked as VALID. The system will use it for settlement, dispute handling, and statement generation.",
            "action_invalid": "Highlight this field only. Message: “Transaction status is invalid. Provide a valid status.” Statement/reconciliation for this transaction may be impacted until corrected.",
        },
        {
            "match": lambda: n == "reversal_flag" or ("reversal" in n and ("flag" in n or "is" in n)),
            "column_name": "reversal_flag",
            "display_name": "Reversal Flag",
            "is_sensitive": False,
            "definition": "Marks whether the transaction was reversed, preventing double counting in balances and reports.",
            "condition": "Must be boolean/flag (true/false, Y/N, 1/0). If true, transaction should not be treated as settled.",
            "action_valid_points": pts(
                "Reversal state is clear, preventing double counting.",
                "Used for correct balance and settlement reporting."
            ),
            "action_invalid_points": pts(
                "Reversal flag is missing/invalid (true/false expected).",
                "Fix it so reversal handling is correct for this transaction only."
            ),
            "ui_issue_message": "Reversal flag is missing/invalid. Use true/false (or Y/N, 1/0).",
        },
        {
            "match": lambda: n == "transaction_channel" or ("channel" in n and "txn" in n),
            "column_name": "transaction_channel",
            "display_name": "Transaction Channel",
            "is_sensitive": False,
            "definition": "Captures the source channel of the transaction to apply channel-specific risk and limits.",
            "condition": "Must match allowed channel values (ATM/UPI/NEFT/IMPS/POS/ONLINE/etc.).",
            "action_valid": "Marked as VALID. Enables channel-specific controls and analytics.",
            "action_invalid": "Highlight this field only. Message: “Transaction channel is invalid. Choose a valid channel.” Channel rules cannot be applied until corrected.",
        },

        # ===== Charges / Taxes / Interest (NEW) =====
        {
            "match": lambda: n == "charge_amount" or ("charge" in n and "amount" in n),
            "column_name": "charge_amount",
            "display_name": "Charge Amount",
            "is_sensitive": True,
            "definition": "Fee charged by the bank for a service (transaction fee, maintenance fee, etc.), used in billing and statements.",
            "condition": "Must be numeric and >= 0. Should be 0 when no charge applies.",
            "action_valid_points": pts(
                "Charge is valid, enabling correct billing and statements.",
                "Used for fee posting and accounting (masked in UI)."
            ),
            "action_invalid_points": pts(
                "Charge amount must be a non-negative number.",
                "Fix it to post fees correctly; do not block other fields."
            ),
            "ui_issue_message": "Charge amount must be a non‑negative number.",
        },
        {
            "match": lambda: n == "tax_deducted" or ("tax" in n and ("deduct" in n or "deducted" in n)),
            "column_name": "tax_deducted",
            "display_name": "Tax Deducted",
            "is_sensitive": True,
            "definition": "Tax withheld/deducted (e.g., TDS) for compliance reporting and customer tax statements.",
            "condition": "Must be numeric and >= 0. If present, should not exceed the related taxable amount.",
            "action_valid_points": pts(
                "Tax value is valid for compliant reporting.",
                "Used in tax statements and regulatory summaries (masked in UI)."
            ),
            "action_invalid_points": pts(
                "Tax must be non-negative and not exceed the taxable amount.",
                "Fix it to keep tax reporting correct; do not fail the whole file."
            ),
            "ui_issue_message": "Tax deducted must be non‑negative and cannot exceed the taxable amount.",
        },
        {
            "match": lambda: n == "interest_rate" or ("interest" in n and "rate" in n),
            "column_name": "interest_rate",
            "display_name": "Interest Rate",
            "is_sensitive": False,
            "definition": "Rate applied to balances or loans to calculate interest credited/charged, affecting customer earnings and bank revenue.",
            "condition": "Must be numeric; typical range 0–100. Must follow product policy (e.g., 3.5 for 3.5%).",
            "action_valid_points": pts(
                "Rate is valid, enabling accurate interest calculation.",
                "Used for pricing, disclosures, and accruals."
            ),
            "action_invalid_points": pts(
                "Interest rate must be a valid numeric percentage.",
                "Fix it to calculate interest correctly; other fields remain usable."
            ),
            "ui_issue_message": "Interest rate is invalid. Provide a numeric percentage within allowed range.",
        },
        {
            "match": lambda: n == "interest_credit_date" or ("interest" in n and "credit" in n and "date" in n),
            "column_name": "interest_credit_date",
            "display_name": "Interest Credit Date",
            "is_sensitive": False,
            "definition": "Date when interest is credited to the account, used for statement accuracy and audit trails.",
            "condition": "Must be a valid date, usually not in the far future, and aligned with statement cycle policy.",
            "action_valid_points": pts(
                "Credit date is valid, keeping statements and accruals consistent.",
                "Used to schedule/record interest posting."
            ),
            "action_invalid_points": pts(
                "Provide a valid interest credit date (not in the far future).",
                "Fix it to avoid posting/statement timing issues for this account."
            ),
            "ui_issue_message": "Interest credit date is invalid. Provide a valid date (not in the far future).",
        },

        # ===== Instructions / Nominee / Freeze / AML (NEW) =====
        {
            "match": lambda: n == "standing_instruction_flag" or ("standing" in n and "instruction" in n),
            "column_name": "standing_instruction_flag",
            "display_name": "Standing Instruction Flag",
            "is_sensitive": False,
            "definition": "Indicates whether automatic scheduled payments/transfers are enabled for the account.",
            "condition": "Must be boolean/flag (true/false, Y/N, 1/0). If true, schedule details must exist elsewhere.",
            "action_valid_points": pts(
                "Flag is valid, so recurring payments can be controlled correctly.",
                "Used for automated debits/credits and bill payments."
            ),
            "action_invalid_points": pts(
                "Standing instruction flag must be true/false.",
                "Fix it to enable/disable automation correctly; other fields stay OK."
            ),
            "ui_issue_message": "Standing instruction flag is invalid. Use true/false (or Y/N, 1/0).",
        },
        {
            "match": lambda: n == "nominee_relation" or ("nominee" in n and ("relation" in n or "relationship" in n)),
            "column_name": "nominee_relation",
            "display_name": "Nominee Relation",
            "is_sensitive": False,
            "definition": "Relationship of nominee to account holder (Spouse/Parent/Child/etc.) required for legal and claims processing.",
            "condition": "Optional but recommended. Must be a valid relationship label from bank’s allowed list; not empty if nominee_name is present.",
            "action_valid_points": pts(
                "Nominee relationship is clear for legal processing.",
                "Used for nomination verification and claim workflows."
            ),
            "action_invalid_points": pts(
                "Nominee relation is missing/invalid.",
                "Provide a valid relationship (e.g., Spouse/Child/Parent) to complete nomination."
            ),
            "ui_issue_message": "Nominee relation is missing/invalid. Provide a valid relationship (Spouse/Child/Parent/etc.).",
        },
        {
            "match": lambda: n == "freeze_reason" or ("freeze" in n and "reason" in n),
            "column_name": "freeze_reason",
            "display_name": "Freeze Reason",
            "is_sensitive": True,
            "definition": "Explains why the account is frozen (compliance, legal order, suspected fraud), used for audit and controlled handling.",
            "condition": "Required if account is frozen/locked by policy. Must be a meaningful text (min 5 chars).",
            "action_valid_points": pts(
                "Reason is recorded for compliant freeze handling (masked in UI).",
                "Used for audits and controlled operations on frozen accounts."
            ),
            "action_invalid_points": pts(
                "Freeze reason is required when an account is frozen/locked.",
                "Add a clear reason to satisfy audit/compliance requirements."
            ),
            "ui_issue_message": "Freeze reason is required when the account is frozen/locked. Provide a clear reason.",
        },
        {
            "match": lambda: n == "aml_alert_flag" or ("aml" in n and ("alert" in n or "flag" in n)),
            "column_name": "aml_alert_flag",
            "display_name": "AML Alert Flag",
            "is_sensitive": False,
            "definition": "Indicates whether AML monitoring triggered an alert for this customer/transaction, supporting regulatory compliance.",
            "condition": "Must be boolean/flag (true/false, Y/N, 1/0). If true, supporting alert details should exist in monitoring systems.",
            "action_valid_points": pts(
                "Flag is valid, enabling correct AML case routing.",
                "Used to trigger compliance review and enhanced due diligence."
            ),
            "action_invalid_points": pts(
                "AML alert flag must be true/false.",
                "Fix it so AML routing is accurate; do not block other processing."
            ),
            "ui_issue_message": "AML alert flag is invalid. Use true/false (or Y/N, 1/0).",
        },
        {
            "match": lambda: n == "suspicious_txn_score" or ("suspicious" in n and ("score" in n or "txn" in n)),
            "column_name": "suspicious_txn_score",
            "display_name": "Suspicious Transaction Score",
            "is_sensitive": True,
            "definition": "Risk score indicating likelihood of suspicious activity, used for AML/fraud prioritization.",
            "condition": "Must be numeric within an allowed range (commonly 0–100). Higher score implies higher risk.",
            "action_valid_points": pts(
                "Score is valid (masked in UI), enabling accurate risk prioritization.",
                "Used for AML/fraud investigation queues and thresholds."
            ),
            "action_invalid_points": pts(
                "Score must be numeric within the allowed range (e.g., 0–100).",
                "Fix it to enable proper risk scoring; do not fail the whole file."
            ),
            "ui_issue_message": "Suspicious transaction score is invalid. Provide a numeric value within allowed range (e.g., 0–100).",
        },

        # ===== Statements / Notifications (NEW) =====
        {
            "match": lambda: n == "statement_cycle" or ("statement" in n and "cycle" in n),
            "column_name": "statement_cycle",
            "display_name": "Statement Cycle",
            "is_sensitive": False,
            "definition": "Defines how often statements are generated (Monthly/Quarterly) and which date range they cover.",
            "condition": "Must match allowed cycles (e.g., MONTHLY, QUARTERLY) and/or a valid day-of-month rule depending on bank policy.",
            "action_valid_points": pts(
                "Cycle is valid, so statement schedules are reliable.",
                "Used for statement generation and interest posting timelines."
            ),
            "action_invalid_points": pts(
                "Statement cycle is invalid or missing.",
                "Choose a supported cycle to generate statements correctly."
            ),
            "ui_issue_message": "Statement cycle is invalid. Choose a supported cycle (e.g., MONTHLY/QUARTERLY).",
        },
        {
            "match": lambda: n == "notification_preference" or ("notification" in n and ("preference" in n or "pref" in n)),
            "column_name": "notification_preference",
            "display_name": "Notification Preference",
            "is_sensitive": False,
            "definition": "Specifies how the customer wants to receive alerts (SMS/EMAIL/PUSH/NONE), supporting communication and consent.",
            "condition": "Must be one of the allowed preferences (SMS/EMAIL/PUSH/NONE) and should align with available verified contact info.",
            "action_valid_points": pts(
                "Preference is valid, enabling compliant notifications.",
                "Used for alerts, OTP delivery choices, and statement notifications."
            ),
            "action_invalid_points": pts(
                "Preference must be SMS/EMAIL/PUSH/NONE.",
                "Fix it to route notifications correctly; do not block other fields."
            ),
            "ui_issue_message": "Notification preference is invalid. Choose SMS/EMAIL/PUSH/NONE.",
        },
        {
            "match": lambda: has("customer") and has("id"),
            "column_name": "customer_id",
            "display_name": "Customer ID",
            "is_sensitive": False,
            "definition": "Unique internal identifier for each customer, used to link all accounts, loans, and transactions to the right person.",
            "condition": "Must be unique, non‑empty, 3–20 characters, typically alphanumeric (e.g., CUST001, C12345).",
            "action_valid": (
                "Marked as VALID. The system can safely use this ID to join customer master data with "
                "accounts, loans, and transactions for reporting, risk checks, and compliance."
            ),
            "action_invalid": (
                "Only this field is highlighted. Ask the user to enter a unique, non‑empty ID in the expected format; "
                "customer‑level reports and linking will be blocked until it is corrected, but other fields remain usable."
            ),
        },
        {
            "match": lambda: has("customer") and ("name" in n or "nm" in n),
            "column_name": "customer_name",
            "display_name": "Customer Name",
            "is_sensitive": False,
            "definition": "Full legal name of the customer, used on statements, KYC records, and regulatory reports.",
            "condition": "Mandatory for active customers. Must contain alphabetic characters, length 3–100, no only-numeric values.",
            "action_valid": (
                "Marked as VALID. The name can be safely printed on statements, used in KYC checks, and shown in the UI "
                "for relationship managers and support teams."
            ),
            "action_invalid": (
                "Only the name field is highlighted. Explain that a proper full name is required for compliance and "
                "communication; update or correct the value before KYC completion or account activation."
            ),
        },
        {
            "match": lambda: n == "age" or has("age"),
            "column_name": "age",
            "display_name": "Age",
            "is_sensitive": False,
            "definition": "Customer age, used to check eligibility for products and apply age-based risk rules.",
            "condition": "Optional but recommended. If present, must be numeric and typically between 18 and 120 for individual customers.",
            "action_valid": (
                "Marked as VALID. The age can be used to auto‑check product eligibility (e.g., senior citizen schemes) "
                "and to apply age‑based risk or marketing rules."
            ),
            "action_invalid": (
                "Only the age field is highlighted. Clarify that the value must be a realistic number; age-based offers "
                "and checks will be skipped until a valid value is provided, but the rest of the record remains usable."
            ),
        },
        {
            "match": lambda: ("phone" in n or "mobile" in n or "contact" in n) and "country" not in n,
            "column_name": "phone_number",
            "display_name": "Phone Number",
            "is_sensitive": True,
            "definition": "Primary contact number used for OTPs, alerts, and communication with the customer.",
            "condition": "Mandatory for digital banking. Must contain 8–15 digits after removing spaces and symbols; no alphabetic characters.",
            "action_valid": (
                "Marked as VALID. The number can be used (in masked form, e.g., ****1234) for SMS alerts, OTP delivery, "
                "and support contact without exposing full details in the UI."
            ),
            "action_invalid": (
                "Only the phone field is highlighted. Show a clear message that the format is invalid or missing; "
                "disable OTP/alert‑related flows for this customer until it is corrected, but allow non‑dependent views."
            ),
        },
        {
            "match": lambda: "email" in n,
            "column_name": "email",
            "display_name": "Email",
            "is_sensitive": True,
            "definition": "Customer email address used for e‑statements, alerts, and digital communication.",
            "condition": "Optional but recommended. Must follow standard email format (local@domain), length up to 254 characters.",
            "action_valid": (
                "Marked as VALID. The email can be safely used for e‑statements and notifications; only a masked version "
                "is shown in the UI (e.g., f***@example.com)."
            ),
            "action_invalid": (
                "Only the email field is highlighted. Explain that the address looks invalid and cannot be used for "
                "notifications; all email‑based workflows are disabled until corrected, but other operations continue normally."
            ),
        },
        {
            "match": lambda: "address" in n,
            "column_name": "address",
            "display_name": "Address",
            "is_sensitive": True,
            "definition": "Residential or mailing address, used for KYC, dispatch, and legal communication.",
            "condition": "Recommended for KYC. Should be at least 10 characters and contain street/area information, not only numbers.",
            "action_valid": (
                "Marked as VALID. The address can be used for communication, KYC checks, and risk assessment; only a "
                "trimmed or partial view is shown in UI to reduce exposure."
            ),
            "action_invalid": (
                "Only the address field is highlighted. Inform the user that the address appears incomplete or invalid; "
                "KYC completion and physical dispatch will be blocked until corrected, but viewing other data is allowed."
            ),
        },
        {
            "match": lambda: has("account") and ("number" in n or n.endswith("no") or "accno" in n),
            "column_name": "account_number",
            "display_name": "Account Number",
            "is_sensitive": True,
            "definition": "Unique bank account identifier used to post transactions and retrieve balances.",
            "condition": "Mandatory for active accounts. Must be 6–18 digits, mostly numeric, and unique within the dataset.",
            "action_valid": (
                "Marked as VALID. The system can safely use this number to post debits/credits, show balances, and link "
                "to statements. In the UI it is always masked (e.g., ****5678) except in strictly controlled views."
            ),
            "action_invalid": (
                "Only the account number field is highlighted. Show a message that the format or uniqueness is incorrect; "
                "posting of transactions and account‑level operations are blocked for this record until fixed, but other "
                "unrelated records remain unaffected."
            ),
        },
        {
            "match": lambda: has("account") and "type" in n,
            "column_name": "account_type",
            "display_name": "Account Type",
            "is_sensitive": False,
            "definition": "Describes the product category of the account such as Savings, Current, or Salary.",
            "condition": "Mandatory for new accounts. Must be one of the allowed values (e.g., SAVINGS, CURRENT, SALARY, STUDENT, PENSION).",
            "action_valid": (
                "Marked as VALID. Correct account type allows proper interest calculation, fee logic, and eligibility rules "
                "to run automatically for this account."
            ),
            "action_invalid": (
                "Only the account type field is highlighted. Explain that the value is not a recognized product; "
                "interest and fee rules will not be applied until a valid type is selected, but the rest of the row remains visible."
            ),
        },
        {
            "match": lambda: has("account") and ("status" in n or "state" in n),
            "column_name": "account_status",
            "display_name": "Account Status",
            "is_sensitive": False,
            "definition": "Indicates whether the account is Active or Deactive/Closed for operations.",
            "condition": "Mandatory. Typically allowed values are ACTIVE or DEACTIVE/CLOSED (case‑insensitive).",
            "action_valid": (
                "Marked as VALID. The system uses this field to allow or block transactions and to filter accounts in reports "
                "(e.g., only ACTIVE accounts for payments)."
            ),
            "action_invalid": (
                "Only the status field is highlighted. Inform the user that the status is not recognized; "
                "until corrected, new transactions may be blocked to avoid acting on an improperly classified account."
            ),
        },
        {
            "match": lambda: "ifsc" in n or ("branch" in n and "code" in n),
            "column_name": "ifsc_code",
            "display_name": "IFSC / Branch Code",
            "is_sensitive": False,
            "definition": "Routing code that uniquely identifies the bank branch for transfers and compliance reporting.",
            "condition": "Optional for internal analysis but mandatory for inter‑bank transfers. Must follow bank’s code format (e.g., 4 letters + 7 digits in India).",
            "action_valid": (
                "Marked as VALID. The code can be used to route payments, identify the correct branch, and generate regulatory reports.",
            ),
            "action_invalid": (
                "Only the IFSC/branch code field is highlighted. Show that the format is invalid or missing; "
                "outgoing transfers requiring this code will be blocked for this record, but internal analysis can still proceed."
            ),
        },
        {
            "match": lambda: "balance" in n and "opening" not in n and "closing" not in n,
            "column_name": "balance",
            "display_name": "Balance",
            "is_sensitive": True,
            "definition": "Current monetary balance of the account used for risk checks, interest, and limit validations.",
            "condition": "Must be numeric, usually >= 0 except for allowed overdraft products.",
            "action_valid": (
                "Marked as VALID. The balance can be safely used (in masked or aggregated form) to check available funds, "
                "apply interest, and trigger low‑balance alerts without exposing the raw amount to all UI users."
            ),
            "action_invalid": (
                "Only the balance field is highlighted. Explain that non‑numeric or inconsistent values cannot be used; "
                "balance‑dependent checks (e.g., insufficient funds) are skipped until corrected, but the record remains visible."
            ),
        },
        {
            "match": lambda: ("transaction" in n or "txn" in n) and ("amount" in n or "amt" in n),
            "column_name": "transaction_amount",
            "display_name": "Transaction Amount",
            "is_sensitive": True,
            "definition": "Monetary value of each debit or credit posted to an account.",
            "condition": "Mandatory for transaction rows. Must be numeric and non‑zero; usually positive value, sign indicated by transaction type (DEBIT/CREDIT).",
            "action_valid": (
                "Marked as VALID. The amount can be used to update balances, compute fees, and generate transaction‑level reports; "
                "large values can feed risk and fraud monitoring rules."
            ),
            "action_invalid": (
                "Only the amount field is highlighted. Inform the user that the value is missing or not numeric; "
                "this transaction will not be considered in balance or reporting until corrected, but other transactions remain unaffected."
            ),
        },
        {
            "match": lambda: ("transaction" in n or "txn" in n) and ("date" in n or "dt" in n),
            "column_name": "transaction_date",
            "display_name": "Transaction Date",
            "is_sensitive": False,
            "definition": "Date on which the transaction is considered effective for balance updates and statements.",
            "condition": "Mandatory for transaction datasets. Must be a valid calendar date, not in the far future, in a consistent format (e.g., YYYY‑MM‑DD).",
            "action_valid": (
                "Marked as VALID. The date can be used to build statements, calculate interest periods, and perform daily reconciliation.",
            ),
            "action_invalid": (
                "Only the date field is highlighted. Show that the date is missing or invalid; "
                "this transaction will be excluded from date‑based reports and interest calculations until fixed."
            ),
        },
        {
            "match": lambda: ("transaction" in n or "txn" in n) and ("type" in n or "category" in n),
            "column_name": "transaction_type",
            "display_name": "Transaction Type",
            "is_sensitive": False,
            "definition": "Indicates whether the transaction is a Debit or Credit, and sometimes the channel or purpose.",
            "condition": "Mandatory for transaction lines. Allowed core values are DEBIT or CREDIT; additional values may be mapped to these.",
            "action_valid": (
                "Marked as VALID. The system can correctly interpret direction (money in vs money out), update balances, and classify transactions.",
            ),
            "action_invalid": (
                "Only the transaction type field is highlighted. Explain that the value is not recognized; "
                "this row will not be used in balance direction checks or analytics until the type is corrected."
            ),
        },
        {
            "match": lambda: "pan" in n,
            "column_name": "pan_number",
            "display_name": "PAN Number",
            "is_sensitive": True,
            "definition": "Tax identification number (such as PAN) used for regulatory reporting and high‑value transaction tracking.",
            "condition": "Highly sensitive. Must match the country’s official PAN format; usually alphanumeric with fixed length.",
            "action_valid": (
                "Marked as VALID. The PAN can be used (in encrypted form) for compliance checks and regulatory reports, "
                "but is always heavily masked in the UI (e.g., XXXXX1234)."
            ),
            "action_invalid": (
                "Only the PAN field is highlighted. Inform the user that the ID appears invalid or incomplete; "
                "tax‑related reporting and some high‑value operations will be restricted until corrected, but the rest of the record is still accessible."
            ),
        },
        {
            "match": lambda: "aadhaar" in n or "aadhar" in n,
            "column_name": "aadhaar_number",
            "display_name": "Aadhaar Number",
            "is_sensitive": True,
            "definition": "Government‑issued identity number used only where legally permitted for KYC.",
            "condition": "Extremely sensitive. Must strictly follow the official format; storage and display must comply with local privacy laws.",
            "action_valid": (
                "Marked as VALID and treated under strict masking (e.g., XXXX‑XXXX‑1234). It can be used only for compliant KYC checks, "
                "never fully shown in the UI."
            ),
            "action_invalid": (
                "Only the Aadhaar field is highlighted. Explain that the number cannot be verified; "
                "Aadhaar‑based KYC flows are blocked until a correct value is supplied, but other non‑dependent operations are not blocked."
            ),
        },
        {
            "match": lambda: "kyc" in n and "status" in n,
            "column_name": "kyc_status",
            "display_name": "KYC Status",
            "is_sensitive": False,
            "definition": "Indicates whether the customer's identity verification (KYC) is complete or pending.",
            "condition": "Allowed values typically include PENDING, IN_PROGRESS, and COMPLETED (case‑insensitive).",
            "action_valid": (
                "Marked as VALID. The system can use this status to allow or restrict high‑risk operations (e.g., cash withdrawals, remittances).",
            ),
            "action_invalid": (
                "Only the KYC status field is highlighted. Explain that the value is not recognized; "
                "KYC‑dependent activities may be blocked or require manual review, while low‑risk views remain available."
            ),
        },
        {
            "match": lambda: "kyc" in n and "date" in n,
            "column_name": "kyc_verified_date",
            "display_name": "KYC Verified Date",
            "is_sensitive": False,
            "definition": "Date on which KYC verification was completed, used to track expiry and re‑KYC cycles.",
            "condition": "Must be a valid past date and follow a consistent date format.",
            "action_valid": (
                "Marked as VALID. The system can use this date to determine when re‑KYC is due and trigger reminders.",
            ),
            "action_invalid": (
                "Only this date field is highlighted. Inform the user that the verification date looks invalid; "
                "automatic re‑KYC tracking may be disabled for this customer until corrected."
            ),
        },
        {
            "match": lambda: "risk" in n and ("level" in n or "score" in n),
            "column_name": "risk_level",
            "display_name": "Risk Level",
            "is_sensitive": False,
            "definition": "Categorizes the customer or account as Low, Medium, or High risk for compliance and monitoring.",
            "condition": "Optional but recommended. Allowed values are LOW, MEDIUM, or HIGH (or mapped equivalents).",
            "action_valid": (
                "Marked as VALID. Risk level can be used by monitoring systems to prioritize alerts and apply enhanced checks.",
            ),
            "action_invalid": (
                "Only the risk level field is highlighted. Explain that the value is not one of the allowed categories; "
                "risk‑based prioritization will not work correctly until this is fixed."
            ),
        },
        {
            "match": lambda: "nominee" in n and ("name" in n or "nm" in n),
            "column_name": "nominee_name",
            "display_name": "Nominee Name",
            "is_sensitive": False,
            "definition": "Name of the nominee who can claim funds in case of the account holder’s death, as per bank policy.",
            "condition": "Optional but recommended. Should look like a person’s name (alphabetic, 3–100 characters).",
            "action_valid": (
                "Marked as VALID. The nominee can be recorded for legal and operational use, and displayed to authorized users.",
            ),
            "action_invalid": (
                "Only the nominee field is highlighted. Clarify that the name appears invalid; "
                "nomination details will be treated as incomplete until corrected, but the account remains operational."
            ),
        },
        {
            "match": lambda: "loan" in n and "amount" in n,
            "column_name": "loan_amount",
            "display_name": "Loan Amount",
            "is_sensitive": True,
            "definition": "Principal amount sanctioned or outstanding on a loan facility.",
            "condition": "Must be numeric and greater than zero; typically within product‑specific min/max limits.",
            "action_valid": (
                "Marked as VALID. The amount can be used to compute EMIs, interest, and exposure; "
                "only masked or aggregated values should be shown to general UI users."
            ),
            "action_invalid": (
                "Only the loan amount field is highlighted. Explain that the value is missing or non‑numeric; "
                "loan calculations and schedules will be skipped for this record until corrected."
            ),
        },
        {
            "match": lambda: "emi" in n,
            "column_name": "emi_amount",
            "display_name": "EMI Amount",
            "is_sensitive": True,
            "definition": "Fixed installment amount the customer pays periodically towards the loan.",
            "condition": "Must be numeric and greater than zero; typically derived from loan amount, tenure, and interest rate.",
            "action_valid": (
                "Marked as VALID. EMI can be used to generate repayment schedules and track missed payments, "
                "while showing only necessary details in the UI."
            ),
            "action_invalid": (
                "Only the EMI field is highlighted. Inform that the installment value is invalid; "
                "repayment schedule logic will be disabled for this record until corrected."
            ),
        },
        {
            "match": lambda: ("credit" in n or "cibil" in n) and "score" in n,
            "column_name": "credit_score",
            "display_name": "Credit Score",
            "is_sensitive": True,
            "definition": "Numeric score indicating the customer’s creditworthiness, used for loan eligibility and pricing.",
            "condition": "Optional, but if present must be numeric, typically between 300 and 900 (or scheme specific).",
            "action_valid": (
                "Marked as VALID. The score can be used (often in masked or bucketed form) to automate approvals and "
                "interest rate decisions."
            ),
            "action_invalid": (
                "Only the credit score field is highlighted. Explain that the value is missing or outside an acceptable range; "
                "score‑based automation will not run for this customer until corrected, but manual review is still possible."
            ),
        },
    ]

    for tpl in templates:
        try:
            if tpl["match"]():
                # Backward compatibility: also provide long-form text fields
                action_valid_points = tpl.get("action_valid_points") or []
                action_invalid_points = tpl.get("action_invalid_points") or []
                ui_issue_message = tpl.get("ui_issue_message")
                return {
                    "column_name": tpl["column_name"],
                    "display_name": tpl["display_name"],
                    "is_sensitive": tpl["is_sensitive"],
                    "definition": one_line(tpl["definition"]),
                    "condition": one_line(tpl["condition"]),
                    "action_valid": pts_to_string(tpl.get("action_valid_points"), tpl.get("action_valid")),
                    "action_invalid": pts_to_string(tpl.get("action_invalid_points"), tpl.get("action_invalid")),
                    "ui_issue_message": tpl.get("ui_issue_message"),
                }
        except Exception:
            continue

    # Fallback generic template
    pretty_name = col_name.replace("_", " ").title()
    return {
        "column_name": _normalize_column_name(col_name),
        "display_name": pretty_name,
        "is_sensitive": False,
        "definition": f"{pretty_name} is a banking data field.",
        "condition": "Should follow a consistent format.",
        "action_valid": "This field meets banking standards.",
        "action_invalid": "This field is missing or invalid. Please correct it.",
        "ui_issue_message": "This field is missing/invalid. Please correct it and re-submit.",
    }

def pts_to_string(pts_list, fallback):
    if pts_list and isinstance(pts_list, list):
        return pts_list[0] if pts_list else fallback
    return fallback or ""



def build_standard_business_rules(
    banking_validator_result: Optional[Dict[str, Any]],
    fallback_columns: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Build a banking‑grade, UI‑ready business rule set per detected column.

    Output (JSON‑serializable):
    {
        "columns": [
            {
                "column_name": "account_number",
                "display_name": "Account Number",
                "is_sensitive": true,
                "definition": "...",
                "condition": "...",
                "action_valid": "...",
                "action_invalid": "...",
                "status": "VALID" | "WARNING" | "INVALID",
                "issues": [...],
            },
            ...
        ]
    }
    """
    if not banking_validator_result or not isinstance(banking_validator_result, dict):
        columns_meta = None
    else:
        columns_meta = banking_validator_result.get("columns") or banking_validator_result.get("column_results")

    std_columns: List[Dict[str, Any]] = []

    # Case 1: we have detailed validator results
    if columns_meta and isinstance(columns_meta, list):
        for col in columns_meta:
            raw_name = col.get("name") or col.get("column_name") or "Unknown"
            tmpl = _map_column_to_business_rule_template(raw_name)

            # Map validation status into a normalized status for the UI
            raw_status = (col.get("status") or col.get("validation_result") or "").upper()
            if raw_status in {"PASS", "MATCH", "VALID"}:
                status = "VALID"
            elif raw_status in {"WARNING", "WARN"}:
                status = "WARNING"
            elif raw_status:
                status = "INVALID"
            else:
                status = "UNKNOWN"

            issues = col.get("failures") or col.get("reasons") or col.get("issues") or []
            if isinstance(issues, str):
                issues = [issues]

            std_columns.append(
                {
                    **tmpl,
                    "original_column_name": raw_name,
                    "status": status,
                    "issues": issues,
                    "confidence": col.get("confidence"),
                }
            )
    # Case 2: no validator result, but we know column names from other engines
    elif fallback_columns:
        seen: set = set()
        for raw_name in fallback_columns:
            if not raw_name:
                continue
            key = _normalize_column_name(raw_name)
            if key in seen:
                continue
            seen.add(key)
            tmpl = _map_column_to_business_rule_template(raw_name)
            std_columns.append(
                {
                    **tmpl,
                    "original_column_name": raw_name,
                    "status": "UNKNOWN",
                    "issues": [],
                    "confidence": None,
                }
            )

    if not std_columns:
        return None
    if not std_columns:
        return None

    failed = [c for c in std_columns if c.get("status") in ("INVALID", "WARNING")]
    all_valid = len(std_columns) > 0 and len(failed) == 0

    return {
        "columns": std_columns,
        "summary": {
            "all_valid": all_valid,
            "failed_columns": [
                {
                    "column_name": c.get("column_name"),
                    "original_column_name": c.get("original_column_name"),
                    "display_name": c.get("display_name"),
                    "status": c.get("status"),
                    "issues": c.get("issues") or [],
                    "ui_issue_message": c.get("ui_issue_message"),
                    "action_invalid_points": c.get("action_invalid_points") or [],
                    "action_valid_points": c.get("action_valid_points") or [],
                }
                for c in failed
            ],
        },
    }

# Verify database connection on startup (read-only check)
@app.on_event("startup")
async def startup_event():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM banking_keywords"))
            count = result.scalar()
        print(f"Database connected successfully. Found {count} banking keywords in table.")
    except Exception as e:
        print(f"Warning: Database connection error: {e}")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Upload directory
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".sql"}

# Templates folder
templates = Jinja2Templates(directory="templates")  # make a 'templates' folder and put index.html inside

def is_allowed_file(filename: str):
    return any(filename.lower().endswith(ext) for ext in ALLOWED_EXTENSIONS)


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="No file uploaded")

        if not is_allowed_file(file.filename):
            raise HTTPException(status_code=400, detail="Invalid file type")

        content = await file.read()
        if len(content) == 0:
            raise HTTPException(status_code=400, detail="Empty file")

        # Save the uploaded file
        file_path = os.path.join(UPLOAD_DIR, file.filename)
        with open(file_path, "wb") as f:
            f.write(content)

        # Convert file to CSV format if needed (handles SQL, Excel, etc.)
        file_converter = None
        original_file_path = file_path
        file_ext = os.path.splitext(file.filename)[1].lower()
        sql_schema_info = None  # Store SQL schema info for relationship prediction
        
        if file_ext in ['.sql', '.xlsx', '.xls']:
            try:
                file_converter = FileConverter()
                # Convert to CSV
                file_path = file_converter.convert_to_csv(file_path)
                print(f"File converted to CSV: {file_path}")
                
                # 🔥 Extract SQL schema info (relationships, PKs, FKs) for SQL files
                if file_ext == '.sql':
                    sql_schema_info = file_converter.get_sql_schema_info()
                    if sql_schema_info:
                        print(f"SQL Schema Info Extracted: {len(sql_schema_info.get('foreign_keys', []))} foreign keys, "
                              f"{len(sql_schema_info.get('primary_keys', {}))} primary keys")
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Error processing file: {str(e)}")

        # 🔥 STEP 0: CORE BANKING BUSINESS RULES ENGINE (RUNS FIRST)
        core_banking_rules_result = None
        try:
            from core_banking_business_rules_engine import CoreBankingBusinessRulesEngine
            import pandas as pd
            core_rules_engine = CoreBankingBusinessRulesEngine()
            df_for_rules = pd.read_csv(file_path)
            core_banking_rules_result = core_rules_engine.analyze_dataset(file_path, df_for_rules)
            print(f"Core Banking Business Rules Engine: Analyzed {core_banking_rules_result.get('total_columns', 0)} columns")
        except Exception as e:
            print(f"Warning: Core Banking Business Rules Engine error: {str(e)}")
            import traceback
            traceback.print_exc()
            core_banking_rules_result = {"error": str(e)}
        
        # 🔥 ENHANCED BANKING DATA VALIDATION ENGINE (COMPREHENSIVE 23-COLUMN VALIDATION)
        banking_data_validation_result = None
        enhanced_banking_validation_result = None
        try:
            # Use enhanced validation engine for comprehensive validation
            from enhanced_banking_validation import EnhancedBankingValidationEngine
            import pandas as pd
            df = pd.read_csv(file_path)
            enhanced_validation_engine = EnhancedBankingValidationEngine()
            enhanced_banking_validation_result = enhanced_validation_engine.validate_file(file_path, df)
            print(f"Enhanced Banking Validation Engine: Validated file with {enhanced_banking_validation_result.get('summary', {}).get('total_columns', 0)} columns")
            
            # Keep legacy format for backward compatibility
            banking_data_validation_result = enhanced_banking_validation_result
        except Exception as e:
            print(f"Warning: Enhanced Banking Validation Engine error: {str(e)}")
            import traceback
            traceback.print_exc()
            # Fallback to basic validation
            try:
                from banking_data_validation_engine import BankingDataValidationEngine
                validation_engine = BankingDataValidationEngine()
                banking_data_validation_result = validation_engine.validate_file(file_path)
            except Exception as fallback_e:
                print(f"Warning: Fallback validation also failed: {str(fallback_e)}")
                banking_data_validation_result = {"error": str(e)}
            enhanced_banking_validation_result = banking_data_validation_result

        # 🔥 DYNAMIC BUSINESS RULES VALIDATOR WITH APPLICATION TYPE PREDICTION
        dynamic_business_rules_result = None
        application_type_prediction = None
        try:
            from dynamic_business_rules_validator import DynamicBusinessRulesValidator
            dynamic_validator = DynamicBusinessRulesValidator()
            dynamic_business_rules_result = dynamic_validator.validate(file_path)
            application_type_prediction = dynamic_business_rules_result.get("application_type_prediction")
            print(f"Dynamic Business Rules Validator: Application type predicted: {application_type_prediction.get('application_type', 'Unknown') if application_type_prediction else 'None'}")
        except Exception as e:
            print(f"Warning: Dynamic Business Rules Validator error: {str(e)}")
            import traceback
            traceback.print_exc()
            dynamic_business_rules_result = {"error": str(e)}

        # Run domain detection – banking first, then others as needed
        try:
            # Banking domain detection
            banking_detector = BankingDomainDetector()
            banking_result = banking_detector.predict(file_path)
            
            # Check if result contains an error
            if isinstance(banking_result, dict) and "error" in banking_result:
                raise HTTPException(status_code=500, detail=f"Banking analysis error: {banking_result['error']}")
            
            # Banking Dataset Validator - run validation on banking datasets
            banking_validator_result = None
            if banking_result and isinstance(banking_result, dict) and banking_result.get("decision") not in (None, "UNKNOWN"):
                try:
                    # Use our new complete banking validator
                    complete_validator = CompleteBankingValidator()
                    banking_validator_result = complete_validator.validate_dataset(file_path)
                    
                    # Map the results to match the expected format for UI
                    if "error" not in banking_validator_result:
                        # Extract column-level validation results
                        column_validation_results = banking_validator_result.get("column_wise_validation", [])
                        
                        columns_result = []
                        for col in column_validation_results:
                            status = col.get("validation_result", "FAIL").upper()
                            
                            columns_result.append({
                                "name": col.get("column_name", "Unknown"),
                                "meaning": col.get("standard_name", "Unknown"),
                                "status": status,
                                "confidence": col.get("confidence_percentage", 0),  # Already in percentage
                                "rules_passed": 1,  # Placeholder - would need actual count
                                "rules_total": 1,   # Placeholder - would need actual count
                                "failures": col.get("detected_issue", []),
                                "applied_rules": [col.get("business_rule", "General Rule")],
                                "reasons": col.get("detected_issue", [])
                            })
                        
                        # Calculate overall dataset confidence
                        summary = banking_validator_result.get("summary", {})
                        avg_confidence = summary.get("overall_confidence", 0)
                        
                        # Determine final decision based on overall confidence
                        overall_confidence = summary.get("overall_confidence", 0)
                        if overall_confidence >= 95:
                            final_decision = "PASS"
                        elif overall_confidence >= 80:
                            final_decision = "PASS WITH WARNINGS"
                        else:
                            final_decision = "FAIL"
                        
                        banking_validator_result = {
                            "final_decision": final_decision,
                            "dataset_confidence": round(avg_confidence, 1),
                            "explanation": f"Banking validation completed. {summary.get('total_columns_analyzed', 0)} columns analyzed, {summary.get('total_passed', 0)} passed, {summary.get('total_failed', 0)} failed.",
                            "columns": columns_result,
                            "relationships": banking_validator_result.get("cross_column_validations", []),
                            "total_records": summary.get("total_records", 0)
                        }
                except Exception as e:
                    print(f"Warning: Complete banking validator error: {str(e)}")
                    # Fall back to original validator
                    try:
                        validator = BankingDatasetValidator()
                        banking_validator_result = validator.validate(file_path)
                    except Exception as fallback_e:
                        print(f"Warning: Fallback banking validator error: {str(fallback_e)}")
                        banking_validator_result = {"error": str(e)}
            
            # Core Banking Validation Engine - run comprehensive validation
            core_banking_validator_result = None
            if banking_result and isinstance(banking_result, dict) and banking_result.get("decision") not in (None, "UNKNOWN"):
                try:
                    core_validator = CoreBankingValidator()
                    core_banking_validator_result = core_validator.validate(file_path)
                except Exception as e:
                    print(f"Warning: Core banking validator error: {str(e)}")
                    core_banking_validator_result = {"error": str(e)}

            # Financial domain detection (only if banking is NOT clearly detected)
            financial_result = None
            banking_decision = None
            if isinstance(banking_result, dict):
                banking_decision = banking_result.get("decision")

            if banking_decision in (None, "UNKNOWN"):
                financial_detector = FinancialDomainDetector()
                financial_result = financial_detector.predict(file_path)

                if isinstance(financial_result, dict) and "error" in financial_result:
                    raise HTTPException(status_code=500, detail=f"Financial analysis error: {financial_result['error']}")
            else:
                # Explicit marker so UI / logs know financial was skipped intentionally
                financial_result = {
                    "domain": "Financial",
                    "decision": "SKIPPED",
                    "reason": "Skipped because Banking domain was already detected for this file.",
                    "confidence_percentage": 0.0,
                    "confidence_out_of_10": 0.0,
                    "qualitative": "Not evaluated"
                }

            insurance_detector = InsuranceDomainDetector()
            insurance_result = insurance_detector.predict(file_path)
            if isinstance(insurance_result, dict) and "error" in insurance_result:
                raise HTTPException(status_code=500, detail=f"Insurance analysis error: {insurance_result['error']}")
            
            government_detector = GovernmentDomainDetector()
            government_result = government_detector.predict(file_path)
            if isinstance(government_result, dict) and "error" in government_result:
                raise HTTPException(
                    status_code=500,
                    detail=f"Government analysis error: {government_result['error']}"
                )
            healthcare_detector = HealthcareDomainDetector()
            healthcare_result = healthcare_detector.predict(file_path)
            if isinstance(healthcare_result, dict) and "error" in healthcare_result:
                raise HTTPException(
                    status_code=500,
                    detail=f"Healthcare analysis error: {healthcare_result['error']}"
                )

            retail_detector = RetailDomainDetector()
            retail_result = retail_detector.predict(file_path)
            if isinstance(retail_result, dict) and "error" in retail_result:
                raise HTTPException(
                    status_code=500,
                    detail=f"Retail analysis error: {retail_result['error']}"
                )

            space_detector = SpaceDomainDetector()
            space_result = space_detector.predict(file_path)
            if isinstance(space_result, dict) and "error" in space_result:
                raise HTTPException(
                    status_code=500,
                    detail=f"Space analysis error: {space_result['error']}"
                )

            human_resource_detect =HRDomainDetector()
            human_resource_result =human_resource_detect.predict(file_path)

            if isinstance(human_resource_result,dict) and  "error"  in human_resource_result:
                raise HTTPException(
                    status_code=500,
                    detail=f'hr analysis error : {human_resource_result["error"]}'
                )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error during analysis: {str(e)}")

        # NOTE: Don't clean up temp files yet - we need them for blueprint and structure analysis
        
        # 🔥 BANKING BLUEPRINT ANALYSIS (UNIFIED)
        banking_blueprint = None
        try:
            from banking_blueprint_engine import BankingBlueprintEngine
            import pandas as pd
            blueprint_engine = BankingBlueprintEngine()
            # For single file, use analyze_file method
            try:
                df = pd.read_csv(file_path)
                blueprint_result = blueprint_engine.analyze_file(file_path, df)
                banking_blueprint = blueprint_result
            except Exception as e:
                print(f"Warning: Could not analyze file for blueprint: {str(e)}")
                # Fallback: try with file path as dict key
                try:
                    df = pd.read_csv(file_path)
                    blueprint_result = blueprint_engine.analyze_multiple_files({file.filename: df})
                    banking_blueprint = blueprint_result
                except Exception as e2:
                    print(f"Warning: Multi-file fallback also failed: {str(e2)}")
                    raise e
        except Exception as e:
            print(f"Warning: Banking blueprint analysis error: {str(e)}")
            import traceback
            traceback.print_exc()
            # Generate minimal blueprint with business rules only
            try:
                import pandas as pd
                from banking_blueprint_engine import BankingBlueprintEngine
                df = pd.read_csv(file_path)
                blueprint_engine = BankingBlueprintEngine()
                # At minimum, generate business rules
                columns = list(df.columns)
                business_rules = blueprint_engine.apply_business_rules(columns, df)
                banking_blueprint = {
                    "domain": "Banking",
                    "confidence_percentage": 50,
                    "application": "Unknown",
                    "application_confidence": 0,
                    "business_rules": business_rules,
                    "error": f"Partial analysis due to: {str(e)}"
                }
            except Exception as fallback_e:
                print(f"Warning: Fallback blueprint generation also failed: {str(fallback_e)}")
                banking_blueprint = {
                    "domain": "Banking",
                    "confidence_percentage": 0,
                    "application": "Unknown",
                    "error": f"Blueprint analysis failed: {str(fallback_e)}"
                }

        # 🔥 APPLICATION STRUCTURE GENERATOR (NEW FEATURE)
        application_structure = None
        try:
            structure_generator = BankingApplicationStructureGenerator()
            application_structure = structure_generator.generate_structure([file_path])
        except Exception as e:
            print(f"Warning: Application structure generation error: {str(e)}")
            import traceback
            traceback.print_exc()
            application_structure = {"error": str(e)}

        # 🔥 BUILD COLUMN RELATIONSHIP ANALYSIS (For SQL files, use extracted relationships; for CSV, generate basic analysis)
        column_relationship_analysis = {}
        
        # For SQL files: use extracted relationships
        if sql_schema_info and sql_schema_info.get("relationships"):
            # Convert SQL relationships to the format expected by UI
            sql_relationships_list = sql_schema_info.get("relationships", [])
            sql_tables = sql_schema_info.get("tables", {})
            sql_primary_keys = sql_schema_info.get("primary_keys", {})
            
            # Build file_domains (one entry per table)
            file_domains = []
            for table_name, table_info in sql_tables.items():
                columns = table_info.get("columns", [])
                file_domains.append({
                    "file_name": f"{table_name}.sql",
                    "total_columns": len(columns),
                    "total_rows": 10,  # Placeholder since we don't know exact count
                    "primary_domain": "Banking",
                    "domain_icon": "🏦",
                    "domain_confidence": 85,
                    "domain_description": f"Table '{table_name}' from SQL schema with {len(columns)} columns",
                    "matched_columns": columns
                })
            
            # Build column_relationships in the format expected by UI
            column_relationships = []
            for rel in sql_relationships_list:
                child_table = rel.get("child_table", "")
                child_col = rel.get("child_column", "")
                parent_table = rel.get("parent_table", "")
                parent_col = rel.get("parent_column", "")
                
                column_relationships.append({
                    "file1": f"{child_table}.sql",
                    "file1_column": child_col,
                    "file1_domain": "Banking",
                    "file2": f"{parent_table}.sql",
                    "file2_column": parent_col,
                    "file2_domain": "Banking",
                    "relationship_type": "FOREIGN KEY Reference",
                    "connection_strength": "Strong",
                    "explanation": rel.get("explanation", f"The '{child_col}' column in '{child_table}' references '{parent_col}' in '{parent_table}'. This is a foreign key relationship that links records between these tables."),
                    "business_value": rel.get("business_rule", f"Every record in {child_table} must have a corresponding record in {parent_table}. This ensures data integrity and enables joins between these tables."),
                    "overlap_info": {
                        "overlap_count": 10,  # Placeholder
                        "overlap_percentage": 100,  # FK should have 100% match
                        "total_unique_file1": 10,
                        "total_unique_file2": 10
                    }
                })
            
            column_relationship_analysis = {
                "file_domains": file_domains,
                "column_relationships": column_relationships,
                "total_relationships": len(column_relationships),
                "primary_keys": sql_primary_keys,
                "foreign_keys": sql_schema_info.get("foreign_keys", []),
                "source": "SQL Schema Analysis"
            }
            print(f"Column Relationship Analysis: {len(column_relationships)} relationships found from SQL schema")
        else:
            # For CSV/Excel files: generate basic file domain analysis
            try:
                from column_relationship_analyzer import ColumnRelationshipAnalyzer
                import pandas as pd
                analyzer = ColumnRelationshipAnalyzer()
                df = pd.read_csv(file_path)
                file_name = os.path.basename(file_path)
                file_dataframes = {file_name: df}
                
                # Generate file domain analysis (even for single file)
                domain_info = analyzer.analyze_file_banking_domain(df, file_name)
                column_relationship_analysis = {
                    "file_domains": [domain_info],
                    "column_relationships": [],  # No relationships for single file
                    "total_relationships": 0,
                    "source": "CSV File Analysis"
                }
                print(f"Column Relationship Analysis: File domain detected for {file_name}")
            except Exception as e:
                print(f"Warning: Could not generate column relationship analysis for CSV: {str(e)}")
                column_relationship_analysis = {
                    "file_domains": [],
                    "column_relationships": [],
                    "total_relationships": 0,
                    "source": "Error",
                    "error": str(e)
                }

        # 🔥 STANDARD BUSINESS RULES (NEW UI‑READY FORMAT)
        # Build fallback column list if validator results are not available
        fallback_cols: List[str] = []
        if isinstance(banking_result, dict):
            matched = banking_result.get("matched_columns") or []
            fallback_cols.extend(matched)
            mapping = banking_result.get("banking_column_mapping") or {}
            if isinstance(mapping, dict):
                fallback_cols.extend(list(mapping.keys()))
            core_detected = banking_result.get("core_detected_columns") or []
            if isinstance(core_detected, list):
                for c in core_detected:
                    if isinstance(c, dict):
                        name = c.get("column_name")
                        if name:
                            fallback_cols.append(name)
        # De‑duplicate while preserving order
        seen_fc: set = set()
        dedup_fallback_cols: List[str] = []
        for c in fallback_cols:
            key = _normalize_column_name(c)
            if key in seen_fc:
                continue
            seen_fc.add(key)
            dedup_fallback_cols.append(c)

        # 🔥 DYNAMIC BUSINESS RULES FROM OBSERVED DATA (PRIMARY SOURCE - NO HARDCODED RULES)
        standard_business_rules = None
        business_rules_summary = None
        try:
            from dynamic_business_rules_from_data import generate_dynamic_business_rules
            dynamic_rules = generate_dynamic_business_rules(file_path)
            
            if dynamic_rules and dynamic_rules.get('columns'):
                # Use dynamic rules as PRIMARY source - 100% data-driven, no hardcoded templates
                standard_business_rules = dynamic_rules
                print(f"✅ Generated {len(dynamic_rules.get('columns', []))} dynamic business rules from observed data")
                
                # 🔥 GENERATE HIGH-LEVEL BUSINESS RULES SUMMARY (FOR EXECUTIVES/STAKEHOLDERS)
                try:
                    from banking_business_rules_summarizer import summarize_banking_business_rules
                    business_rules_summary = summarize_banking_business_rules(dynamic_rules)
                    print(f"✅ Generated {business_rules_summary.get('summary', {}).get('total_rules', 0)} high-level business rules across {business_rules_summary.get('summary', {}).get('themes_covered', 0)} themes")
                except Exception as summary_e:
                    print(f"Warning: Could not generate business rules summary: {str(summary_e)}")
                    import traceback
                    traceback.print_exc()
            else:
                print("Warning: Dynamic rules generation returned empty result")
        except Exception as e:
            print(f"Error: Could not generate dynamic business rules from observed data: {str(e)}")
            import traceback
            traceback.print_exc()
        
        # FALLBACK: Only use standard/hardcoded rules if dynamic generation completely failed
        if not standard_business_rules or not standard_business_rules.get('columns'):
            print("⚠️ Falling back to standard business rules (hardcoded templates)")
            standard_business_rules = build_standard_business_rules(
                banking_validator_result,
                fallback_columns=dedup_fallback_cols or None,
            )

        # Clean up temporary files AFTER all analysis is done
        if file_converter:
            try:
                file_converter.cleanup_temp_files()
            except Exception as e:
                print(f"Warning: Could not clean up temp files: {e}")

        return JSONResponse(
            content={
                "message": "File analyzed successfully",
                "filename": file.filename,
                "file_type": "SQL" if file.filename.lower().endswith('.sql') else "CSV/Excel",
                # 🔥 BANKING DATA VALIDATION ENGINE (UI-FRIENDLY FORMAT)
                "banking_data_validation": banking_data_validation_result,
                # 🔥 ENHANCED BANKING VALIDATION (COMPREHENSIVE 23 COLUMNS)
                "enhanced_banking_validation": enhanced_banking_validation_result,
                "banking": banking_result,
                "banking_account_validation": banking_result.get("account_number_validation"),
                "banking_account_check": banking_result.get("account_number_check"),
                "banking_account_status": banking_result.get("account_status"),
                "banking_missing_columns": banking_result.get("missing_columns_check"),
                "banking_balance_analysis": banking_result.get("balance_analysis"),
                "banking_opening_debit_credit_detection": banking_result.get("opening_debit_credit_detection"),
                # KYC, PAN, Branch Code, Fraud Detection REMOVED as per specification
                "banking_customer_id_validation": banking_result.get("customer_id_validation"),
                "banking_transaction_validation": banking_result.get("transaction_validation"),
                "banking_transaction_type_validation": banking_result.get("transaction_type_validation"),
                "banking_debit_credit_validation": banking_result.get("debit_credit_validation"),
                "banking_purpose_detection": banking_result.get("purpose_detection"),
                "banking_purpose_report": banking_result.get("purpose_detection"), # Standardize key
                "banking_probability_explanations": banking_result.get("probability_explanations"),
                "banking_transaction_rules": banking_result.get("banking_transaction_rules"),
                "banking_column_purpose_report": banking_result.get("column_purpose_report"),
                "banking_column_mapping": banking_result.get("banking_column_mapping"),
                
                # 🔥 CORE BANKING BUSINESS RULES ENGINE (PRIMARY - RUNS FIRST)
                "core_banking_business_rules": core_banking_rules_result,
                
                # 🔥 DYNAMIC BUSINESS RULES VALIDATOR WITH APPLICATION TYPE PREDICTION
                "dynamic_business_rules": dynamic_business_rules_result,
                "application_type_prediction": application_type_prediction,
                
                # 🔥 BANKING BLUEPRINT (NEW UNIFIED ANALYSIS)
                "banking_blueprint": banking_blueprint,
                
                # 🔥 APPLICATION STRUCTURE GENERATOR (NEW FEATURE)
                "application_structure": application_structure,
                
                # 🔥 SQL SCHEMA INFO (RELATIONSHIPS, PKs, FKs) - NEW FOR SQL FILES
                "sql_schema_info": sql_schema_info,
                "sql_relationships": sql_schema_info.get("relationships", []) if sql_schema_info else None,
                "sql_primary_keys": sql_schema_info.get("primary_keys", {}) if sql_schema_info else None,
                "sql_foreign_keys": sql_schema_info.get("foreign_keys", []) if sql_schema_info else None,
                
                # 🔥 COLUMN RELATIONSHIP ANALYSIS (For UI display)
                "column_relationship_analysis": column_relationship_analysis,
                "relationships": sql_schema_info.get("relationships", []) if sql_schema_info else [],
                "primary_keys": sql_schema_info.get("primary_keys", {}) if sql_schema_info else {},
                "foreign_keys": sql_schema_info.get("foreign_keys", []) if sql_schema_info else [],
                
                # 🔥 CORE BANKING ENGINE RESULTS (PRIMARY OUTPUT - KYC REMOVED)
                "banking_core_analysis": banking_result.get("core_banking_analysis"),
                "banking_core_detected_columns": banking_result.get("core_detected_columns"),
                "banking_core_column_validations": banking_result.get("core_column_validations"),
                "banking_core_cross_validations": banking_result.get("core_cross_validations"),
                "banking_core_validation_summary": banking_result.get("core_validation_summary"),
                
                # 🔥 BANKING DATASET VALIDATOR RESULTS
                "banking_dataset_validator": banking_validator_result,
                
                # 🔥 CORE BANKING VALIDATION ENGINE RESULTS
                "core_banking_validator": core_banking_validator_result,

                # 🔥 STANDARDIZED BUSINESS RULES FOR UI (Definition / Condition / Action)
                "standard_business_rules": standard_business_rules,
                
                # 🔥 HIGH-LEVEL BUSINESS RULES SUMMARY (FOR EXECUTIVES/STAKEHOLDERS)
                "business_rules_summary": business_rules_summary,
                
                # 🔥 BANKING APPLICATION TYPE PREDICTION (NEW)
                "banking_application_type": banking_result.get("banking_application_type"),
                
                "financial": financial_result,
                "insurance": insurance_result,
                "government": government_result,
                "healthcare": healthcare_result,
                "retail": retail_result,
                "space":space_result,
                "human_resource":human_resource_result
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")


@app.post("/upload-multiple")
async def upload_multiple_files(files: List[UploadFile] = File(...)):
    """Multi-file upload endpoint"""
    try:
        if not files or len(files) == 0:
            raise HTTPException(status_code=400, detail="No files uploaded")
        
        # Validate all files
        file_paths = []
        file_converters = []
        
        for file in files:
            if not file.filename:
                continue
            
            if not is_allowed_file(file.filename):
                raise HTTPException(status_code=400, detail=f"Invalid file type: {file.filename}")
            
            content = await file.read()
            if len(content) == 0:
                continue
            
            # Save the uploaded file
            file_path = os.path.join(UPLOAD_DIR, file.filename)
            with open(file_path, "wb") as f:
                f.write(content)
            
            # Convert file to CSV format if needed
            file_ext = os.path.splitext(file.filename)[1].lower()
            file_converter = None
            
            if file_ext in ['.sql', '.xlsx', '.xls']:
                try:
                    file_converter = FileConverter()
                    file_path = file_converter.convert_to_csv(file_path)
                    file_converters.append(file_converter)
                    print(f"File converted to CSV: {file_path}")
                except Exception as e:
                    raise HTTPException(status_code=400, detail=f"Error processing file {file.filename}: {str(e)}")
            
            file_paths.append(file_path)
        
        if not file_paths:
            raise HTTPException(status_code=400, detail="No valid files to process")
        
        # 🔥 STEP 0: CORE BANKING BUSINESS RULES ENGINE (RUNS FIRST FOR EACH FILE)
        core_banking_rules_results = {}
        try:
            from core_banking_business_rules_engine import CoreBankingBusinessRulesEngine
            import pandas as pd
            core_rules_engine = CoreBankingBusinessRulesEngine()
            
            for file_path in file_paths:
                try:
                    df = pd.read_csv(file_path)
                    file_name = os.path.basename(file_path)
                    core_rules_result = core_rules_engine.analyze_dataset(file_path, df)
                    core_banking_rules_results[file_name] = core_rules_result
                except Exception as e:
                    print(f"Warning: Core Banking Rules analysis failed for {file_path}: {str(e)}")
                    core_banking_rules_results[os.path.basename(file_path)] = {"error": str(e)}
        except Exception as e:
            print(f"Warning: Core Banking Business Rules Engine initialization error: {str(e)}")
        
        # 🔥 ENHANCED BANKING DATA VALIDATION ENGINE (MULTI-FILE) - COMPREHENSIVE 23-COLUMN VALIDATION
        banking_data_validation_results = {}
        enhanced_banking_validation_results = {}
        try:
            from enhanced_banking_validation import EnhancedBankingValidationEngine
            import pandas as pd
            enhanced_validation_engine = EnhancedBankingValidationEngine()
            
            for file_path in file_paths:
                try:
                    file_name = os.path.basename(file_path)
                    df = pd.read_csv(file_path)
                    enhanced_validation_result = enhanced_validation_engine.validate_file(file_path, df)
                    enhanced_banking_validation_results[file_name] = enhanced_validation_result
                    # Keep legacy format for backward compatibility
                    banking_data_validation_results[file_name] = enhanced_validation_result
                except Exception as e:
                    print(f"Warning: Enhanced Banking Validation failed for {file_path}: {str(e)}")
                    file_name = os.path.basename(file_path)
                    banking_data_validation_results[file_name] = {"error": str(e)}
                    enhanced_banking_validation_results[file_name] = {"error": str(e)}
        except Exception as e:
            print(f"Warning: Banking Data Validation Engine initialization error: {str(e)}")
        
        # Process multiple files using multi-file processor
        try:
            multi_file_processor = MultiFileProcessor()
            result = multi_file_processor.process_files(file_paths)
            
            # Clean up temporary files
            for converter in file_converters:
                try:
                    converter.cleanup_temp_files()
                except Exception as e:
                    print(f"Warning: Could not clean up temp files: {e}")
            
            # Format response to match single-file format (for UI compatibility)
            if result.get("multi_file_mode"):
                # Extract primary banking result for backward compatibility
                banking_result = result.get("banking", {})
                banking_validator_result = result.get("banking_dataset_validator")
                core_banking_validator_result = result.get("core_banking_validator")
                
                # 🔥 STANDARD BUSINESS RULES (NEW UI‑READY FORMAT) - MULTI-FILE PER FILE
                standard_business_rules_files: Dict[str, Any] = {}
                try:
                    validator = BankingDatasetValidator()
                    for fp in file_paths:
                        file_key = os.path.basename(fp)
                        try:
                            per_file_validator = validator.validate(fp)
                            standard_business_rules_files[file_key] = build_standard_business_rules(per_file_validator) or {"columns": [], "summary": {"all_valid": False, "failed_columns": []}}
                        except Exception as per_file_e:
                            standard_business_rules_files[file_key] = {
                                "columns": [],
                                "summary": {"all_valid": False, "failed_columns": []},
                                "error": str(per_file_e),
                            }
                except Exception as e:
                    # If per-file validation fails, fall back to aggregated validator result
                    standard_business_rules_files = {}

                # Aggregate summary across files
                all_files_valid = True
                failed_columns_all: List[Dict[str, Any]] = []
                for fname, rules in standard_business_rules_files.items():
                    s = (rules or {}).get("summary") or {}
                    if s.get("all_valid") is not True:
                        all_files_valid = False
                    for fc in (s.get("failed_columns") or []):
                        failed_columns_all.append({**fc, "source_file": fname})

                # 🔥 DYNAMIC BUSINESS RULES FROM OBSERVED DATA (MULTI-FILE - PRIMARY SOURCE)
                business_rules_summaries = {}
                try:
                    from dynamic_business_rules_from_data import generate_dynamic_business_rules
                    from banking_business_rules_summarizer import summarize_banking_business_rules
                    
                    for file_path in file_paths:
                        try:
                            file_key = os.path.basename(file_path)
                            dynamic_rules = generate_dynamic_business_rules(file_path)
                            
                            if dynamic_rules and dynamic_rules.get('columns'):
                                # Use dynamic rules as PRIMARY source - 100% data-driven
                                standard_business_rules_files[file_key] = dynamic_rules
                                print(f"✅ Generated {len(dynamic_rules.get('columns', []))} dynamic rules for {file_key}")
                                
                                # Generate high-level business rules summary for this file
                                try:
                                    file_summary = summarize_banking_business_rules(dynamic_rules)
                                    business_rules_summaries[file_key] = file_summary
                                    print(f"✅ Generated {file_summary.get('summary', {}).get('total_rules', 0)} business rules for {file_key}")
                                except Exception as summary_e:
                                    print(f"Warning: Could not generate summary for {file_key}: {str(summary_e)}")
                            else:
                                # Fallback to standard rules only if dynamic generation failed
                                if file_key not in standard_business_rules_files:
                                    print(f"⚠️ Using fallback rules for {file_key}")
                        except Exception as file_e:
                            print(f"Error: Could not generate dynamic rules for {file_path}: {str(file_e)}")
                            import traceback
                            traceback.print_exc()
                            # Keep existing standard rules for this file if available
                except Exception as e:
                    print(f"Error: Dynamic business rules generation failed for multi-file: {str(e)}")
                    import traceback
                    traceback.print_exc()

                standard_business_rules = {
                    "multi_file": True,
                    "files": standard_business_rules_files,
                    "summary": {
                        "all_valid": bool(all_files_valid) if standard_business_rules_files else False,
                        "failed_columns": failed_columns_all,
                    },
                }
                
                # Build response maintaining same structure as single-file
                response = {
                    "message": "Files analyzed successfully",
                    "multi_file_mode": True,
                    "total_files": result.get("total_files", len(file_paths)),
                    # 🔥 BANKING DATA VALIDATION ENGINE (UI-FRIENDLY FORMAT)
                    "banking_data_validation": banking_data_validation_results,
                    # 🔥 ENHANCED BANKING VALIDATION (COMPREHENSIVE 23 COLUMNS)
                    "enhanced_banking_validation": enhanced_banking_validation_results,
                    # 🔥 CORE BANKING BUSINESS RULES ENGINE (PRIMARY - RUNS FIRST)
                    "core_banking_business_rules": core_banking_rules_results,
                    "banking": banking_result,
                    "banking_dataset_validator": banking_validator_result,
                    "core_banking_validator": core_banking_validator_result,
                    # 🔥 STANDARDIZED BUSINESS RULES FOR UI (Definition / Condition / Action)
                    "standard_business_rules": standard_business_rules,
                    
                    # 🔥 HIGH-LEVEL BUSINESS RULES SUMMARY (FOR EXECUTIVES/STAKEHOLDERS) - MULTI-FILE
                    "business_rules_summary": business_rules_summaries if business_rules_summaries else None,
                    "banking_blueprint": result.get("banking_blueprint"),
                    "application_structure": result.get("application_structure"),  # New: Application structure
                    "domain_detection": result.get("domain_detection"),
                    "primary_keys": result.get("primary_keys"),
                    "foreign_keys": result.get("foreign_keys"),
                    "relationships": result.get("relationships"),
                    "file_relationships": result.get("file_relationships", []),  # File-to-file relationships with explanations
                    "column_relationship_analysis": result.get("column_relationship_analysis", {}),  # Column relationships and domains
                    "overall_verdict": result.get("overall_verdict"),
                    "overall_confidence": result.get("overall_confidence"),
                    "business_explanation": result.get("business_explanation"),
                    "table_results": result.get("table_results", [])
                }
                
                # Add all domain results from first table (for UI compatibility)
                if result.get("table_results") and len(result["table_results"]) > 0:
                    first_table = result["table_results"][0]
                    if first_table.get("status") == "SUCCESS":
                        # Set default domain results (they'll be empty for multi-file)
                        response["financial"] = None
                        response["insurance"] = None
                        response["government"] = None
                        response["healthcare"] = None
                        response["retail"] = None
                        response["space"] = None
                        response["human_resource"] = None
                
                return JSONResponse(content=response)
            else:
                return JSONResponse(content=result)
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error during multi-file analysis: {str(e)}")
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return JSONResponse(content={"status": "ok", "message": "Server is running"})

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """Return the index.html page"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/account", response_class=HTMLResponse)
async def account_page(request: Request):
    """Return the account.html page for account number detection results and application structure"""
    return templates.TemplateResponse("account.html", {"request": request})

@app.get("/account.html", response_class=HTMLResponse)
async def account_page_html(request: Request):
    """Return the account.html page (html suffix)"""
    return templates.TemplateResponse("account.html", {"request": request})

@app.get("/banking_validation", response_class=HTMLResponse)
async def banking_validation_page(request: Request):
    """Return the banking_validation.html page for detailed field-level validation results"""
    return templates.TemplateResponse("banking_validation.html", {"request": request})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
