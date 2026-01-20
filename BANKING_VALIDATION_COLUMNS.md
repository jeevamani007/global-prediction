# Banking Validation Rules - Complete Summary

## Total Columns Supported: 53

This document provides a complete reference of all banking columns validated by the system.

---

## Core Banking Columns (23)

### Customer Information Module (6 columns)
1. **customer_id** - Unique customer identifier
2. **customer_name** - Legal name per government ID
3. **age** - Customer age (determines account type eligibility)
4. **phone_number** - Primary contact (OTP, alerts) [SENSITIVE]
5. **email** - Email for e-statements and online banking [SENSITIVE]
6. **address** - Residential address for KYC

### Account Information Module (5 columns)
7. **account_number** - Unique account identifier [SENSITIVE]
8. **account_type** - Account category (SAVINGS, CURRENT, SALARY, etc.)
9. **account_status** - Operational state (ACTIVE, INACTIVE, DORMANT, FROZEN, CLOSED)
10. **ifsc_code** - 11-character IFSC for electronic transfers
11. **balance** - Current account balance

### Transaction Module (3 columns)
12. **transaction_amount** - Transaction value
13. **transaction_date** - Transaction timestamp
14. **transaction_type** - Type (DEBIT, CREDIT, WITHDRAWAL, DEPOSIT, etc.)

### KYC & Compliance Module (4 columns)
15. **pan_number** - PAN for tax compliance [SENSITIVE]
16. **aadhaar_number** - 12-digit Aadhaar for eKYC [SENSITIVE]
17. **kyc_status** - KYC verification status (VERIFIED, PENDING, REJECTED, EXPIRED)
18. **kyc_verified_date** - Date of KYC completion

### Risk & Scoring Module (2 columns)
19. **risk_level** - Customer risk classification (LOW, MEDIUM, HIGH)
20. **credit_score** - CIBIL score (300-900)

### Nominee & Additional Module (3 columns)
21. **nominee_name** - Nominee for succession planning
22. **loan_amount** - Principal loan amount
23. **emi_amount** - Monthly installment

---

## Extended Banking Columns (30)

### Branch Information Module (4 columns)
24. **branch_code** - Unique branch identifier
25. **branch_name** - Full branch name
26. **ifsc_verified** - IFSC verification status (TRUE/FALSE)
27. **customer_category** - Customer classification (VIP, SENIOR_CITIZEN, STUDENT, etc.)

### Account Lifecycle Module (2 columns)
28. **account_open_date** - Account opening date
29. **account_closure_date** - Account closure date (NULL if active)

### Transaction Limits Module (3 columns)
30. **daily_transaction_limit** - Maximum daily transaction amount
31. **monthly_transaction_limit** - Maximum monthly transaction amount
32. **atm_withdrawal_limit** - Maximum ATM withdrawal per day

### Security & Authentication Module (6 columns)
33. **otp_verified** - OTP verification status (TRUE/FALSE)  
34. **login_attempts** - Count of failed login attempts
35. **account_lock_status** - Account lock state (UNLOCKED, LOCKED, TEMPORARILY_LOCKED, PERMANENTLY_LOCKED)
36. **last_login_date** - Timestamp of last login
37. **device_id** - Device identifier for trusted devices [SENSITIVE]
38. **ip_address** - IP address of last session [SENSITIVE]

### Transaction Processing Module (3 columns)
39. **transaction_channel** - Channel (ATM, INTERNET_BANKING, MOBILE_APP, BRANCH, POS, UPI, etc.)
40. **transaction_status** - Status (SUCCESS, PENDING, FAILED, REVERSED, IN_PROCESS, CANCELLED)
41. **reversal_flag** - Is reversal transaction (TRUE/FALSE)

### Charges & Taxation Module (2 columns)
42. **charge_amount** - Service charges/fees
43. **tax_deducted** - TDS on interest earnings

### Interest & Recurring Services Module (3 columns)
44. **interest_rate** - Annual interest rate (%)
45. **interest_credit_date** - Date of interest credit
46. **standing_instruction_flag** - Auto-pay/recurring transfer (TRUE/FALSE)

### Nominee & Compliance Module (2 columns)
47. **nominee_relation** - Relationship to account holder (SPOUSE, SON, DAUGHTER, etc.)
48. **freeze_reason** - Reason for account freeze (COURT_ORDER, SUSPICIOUS_ACTIVITY, etc.)

### AML & Fraud Detection Module (3 columns)
49. **aml_alert_flag** - AML monitoring alert (TRUE/FALSE)
50. **suspicious_txn_score** - Fraud risk score (0-100)
51. **customer_consent** - Data sharing consent (TRUE/FALSE)

### Reporting & Notifications Module (2 columns)
52. **statement_cycle** - Statement frequency (MONTHLY, QUARTERLY, HALF_YEARLY, ANNUALLY)
53. **notification_preference** - Alert channel (SMS, EMAIL, PUSH, WHATSAPP, ALL, NONE)

---

## Validation Features

### Field-Level Validation
- ✅ **Never fails entire form** - Each column validated independently
- ✅ **Specific error messages** - Clear guidance for each field
- ✅ **Definition-Condition-Action** - Complete business context

### Sensitive Data Protection
- ✅ **Automatic Masking** for:
  - Account Number: `*****6789`
  - Phone: `******1234`
  - PAN: `*****1234  F`
  - Aadhaar: `****-****-1234`
  - Email: `j***@gmail.com`
  - Device ID: `***4567`
  - IP Address: `***.***. 123.45`

### Validation Status
- ✅ **VALID** - Meets all requirements
- ⚠️ **WARNING** - Minor issues, can proceed
- ❌ **INVALID** - Must be corrected

### UI Logic
- **All Valid:** Show single global success message
- **Any Invalid:** Highlight only failed columns with specific issues
- **No Global Errors:** Only field-specific messages shown

---

## Usage Example

```python
from enhanced_banking_validation import EnhancedBankingValidationEngine

engine = EnhancedBankingValidationEngine()
result = engine.validate_file("banking_data.csv")

# Result structure
{
    "status": "VALID|WARNING|INVALID",
    "columns": [
        {
            "column_name": "customer_id",
            "status": "VALID",
            "definition": "A unique identifier assigned to each customer...",
            "condition": "8-12 alphanumeric characters...",
            "action": { ... },
            "violations": [],
            "masked_sample": None  # Only for sensitive fields
        }
    ],
    "summary": {
        "total_columns": 53,
        "valid_count": 50,
        "warning_count": 2,
        "invalid_count": 1,
        "message": "..."
    },
    "can_proceed": true
}
```

---

## API Integration

The validation engine is integrated into main.py:
- Single file endpoint: `/upload`
- Multi-file endpoint: `/upload_multiple`
- Results available in: `response.enhanced_banking_validation`

## Frontend Display

View validation results at: `http://localhost:8000/banking_validation`

Features:
- Color-coded status cards
- Filterable columns (All/Valid/Warning/Invalid)
- Comprehensive explanations
- Sensitive data masking
- Responsive design
