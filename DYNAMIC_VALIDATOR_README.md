# Dynamic Business Rules Validator

## Overview

The `DynamicBusinessRulesValidator` applies business rules dynamically based on what columns are present in user-uploaded data. Missing columns are skipped gracefully - only business rules for existing columns are applied.

## Key Features

1. **Dynamic Column Detection**: Automatically detects which columns exist in the uploaded data
2. **Missing Columns Skipped**: No failures for missing columns - only validates what's present
3. **Business Rules Only**: Validates business logic, not static format validation
4. **Cross-Column Validation**: Validates relationships between columns (e.g., balance formula, debit/credit exclusivity)

## Supported Columns

The validator applies business rules to these columns (only if they exist):

- `account_number` - Account number validation
- `customer_id` - Customer ID validation
- `customer_name` - Customer name validation
- `account_type` - Account type validation
- `account_status` - Account status validation
- `branch_code` - Branch code validation
- `ifsc_code` - IFSC code validation
- `transaction_id` - Transaction ID validation
- `transaction_date` - Transaction date validation
- `transaction_type` - Transaction type validation
- `debit` - Debit amount validation
- `credit` - Credit amount validation
- `opening_balance` - Opening balance validation
- `closing_balance` - Closing balance validation
- `phone` - Phone number validation
- `pan` - PAN format validation (5 letters + 4 digits + 1 letter)

## Usage

```python
from dynamic_business_rules_validator import DynamicBusinessRulesValidator

# Initialize validator
validator = DynamicBusinessRulesValidator()

# Validate CSV file
results = validator.validate("bank.csv")

# Check results
print(f"Detected columns: {results['detected_columns']}")
print(f"Rules applied: {results['summary']['rules_applied']}")
print(f"Rules passed: {results['summary']['rules_passed']}")
print(f"Rules failed: {results['summary']['rules_failed']}")
```

## Business Rules Applied

### Account Number
- Must be digits only
- Length between 6-18 characters
- Can repeat across rows (multiple transactions per account)

### Customer ID
- Must be alphanumeric

### Customer Name
- Must be letters and spaces only
- Minimum 3 characters

### Account Type
- Must be from allowed values: Savings or Current only

### Account Status
- Must be from allowed values: Active or Deactive only

### Branch Code
- Must be alphanumeric (spaces allowed for branch names)

### IFSC Code
- Must be alphanumeric
- Length between 3-15 characters

### Transaction ID
- Must be alphanumeric
- Should be unique per transaction (warning if not)

### Transaction Date
- Must be valid date format (parseable by pandas)

### Transaction Type
- Must be from allowed values: Debit or Credit only

### Debit
- Must be numeric and >= 0
- Mutually exclusive with credit (warning if both > 0)

### Credit
- Must be numeric and >= 0
- Mutually exclusive with debit (warning if both > 0)

### Opening Balance
- Must be numeric and >= 0
- Validates balance formula: Closing = Opening + Credit - Debit (if all columns present)

### Closing Balance
- Must be numeric and >= 0
- Validates balance formula: Closing = Opening + Credit - Debit (if all columns present)

### Phone
- Must be numeric (after removing separators)
- Must be 10 digits

### PAN
- Must match pattern: 5 letters + 4 digits + 1 letter (case-insensitive)

## Example Output

```
DETECTED COLUMNS:
  account_number                 -> account_number
  customer_id                    -> customer_id
  transaction_id                 -> transaction_id
  debit                          -> debit
  credit                         -> credit

BUSINESS RULES VALIDATION:
[PASS] Account Number Business Rule             [account_number]
[PASS] Customer ID Business Rule                [customer_id]
[PASS] Transaction ID Business Rule             [transaction_id]
[PASS] Debit Business Rule                      [debit]
[PASS] Credit Business Rule                     [credit]

SUMMARY:
  Total columns in file:     5
  Columns detected:          5
  Business rules applied:    5
  Rules passed:              5
  Rules failed:               0
  Rules skipped (empty):     0

NOTE: The following columns are not present in the data:
  (This is OK - business rules are only applied to existing columns)
    - account_status
    - transaction_date
    - closing_balance
    - phone
    ...
```

## Testing

Run the test script to see the validator in action:

```bash
python test_dynamic_validator.py
```

## Important Notes

1. **No Static Validation**: This validator focuses on business rules, not format validation
2. **Missing Columns**: If a column is missing, its business rules are simply not applied - no error
3. **Flexible Matching**: Column names are matched flexibly (e.g., "txn_date" matches "transaction_date")
4. **Cross-Column Rules**: Some rules require multiple columns (e.g., balance formula needs opening, closing, debit, credit)
