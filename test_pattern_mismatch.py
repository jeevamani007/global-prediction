"""
Test script to demonstrate pattern mismatch detection.

This shows how the validator detects when column name matches but data pattern doesn't.
"""

import pandas as pd
from dynamic_business_rules_validator import DynamicBusinessRulesValidator

# Create a test CSV with a column that has matching name but wrong data pattern
test_data = {
    "account_number": ["123456789", "987654321", "111222333"],  # Valid pattern
    "account_type": ["SAVINGS", "CURRENT", "LOAN"],  # Valid pattern
    "customer_id": ["12345", "67890", "11111"],  # Name matches but data is numeric only (should be alphanumeric)
    "transaction_id": ["ABC123", "DEF456", "GHI789"],  # Valid pattern
    "debit": [100, 200, 300],  # Valid pattern
    "credit": [0, 0, 0]  # Valid pattern
}

df = pd.DataFrame(test_data)
df.to_csv("test_pattern_mismatch.csv", index=False)

# Test the validator
validator = DynamicBusinessRulesValidator()
results = validator.validate("test_pattern_mismatch.csv")

print("=" * 80)
print("PATTERN MISMATCH TEST")
print("=" * 80)
print()
print("Test Data:")
print("- account_number: Valid (digits, 6-18 chars)")
print("- account_type: Valid (allowed values)")
print("- customer_id: Name matches BUT data is numeric only (should be alphanumeric)")
print("- transaction_id: Valid (alphanumeric)")
print("- debit: Valid (numeric)")
print("- credit: Valid (numeric)")
print()

print("DETECTED COLUMNS:")
print("-" * 80)
for col_name, role in results["detected_columns"].items():
    print(f"  {col_name:30} -> {role}")
print()

print("ALL BUSINESS RULES:")
print("-" * 80)
for rule in results["business_rules"]:
    status = rule["status"]
    col_name = rule.get('column_name', 'N/A')
    
    if status == "PASS":
        print(f"[PASS]     {rule['rule_name']:40} [{col_name}]")
    elif status == "PATTERN_MISMATCH":
        print(f"[PATTERN MISMATCH] {rule['rule_name']:40} [{col_name}]")
        print(f"    Reason: {rule.get('reason', 'N/A')}")
    elif status == "NOT_APPLICABLE":
        print(f"[NOT APPLICABLE] {rule['rule_name']:40} [{col_name}]")
        print(f"    Reason: {rule.get('reason', 'N/A')}")
    elif status == "FAIL":
        print(f"[FAIL]     {rule['rule_name']:40} [{col_name}]")
        for violation in rule.get("violations", []):
            print(f"    - {violation}")

print()
print("=" * 80)
print("KEY POINT:")
print("customer_id column name matches, but data pattern (numeric only)")
print("does not match expected pattern (alphanumeric), so rule is NOT APPLIED")
print("=" * 80)
