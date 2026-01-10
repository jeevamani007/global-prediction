"""Test script for Core Banking Engine"""
from banking_core_engine import CoreBankingEngine
import pandas as pd
import json

# Test with bank.csv
df = pd.read_csv('bank.csv')
engine = CoreBankingEngine()
result = engine.analyze_banking_dataset(df)

print("=" * 60)
print("CORE BANKING ENGINE TEST RESULTS")
print("=" * 60)
print(f"\nFinal Decision: {result.get('final_decision', {}).get('decision', 'N/A')}")
print(f"Reason: {result.get('final_decision', {}).get('reason', 'N/A')}")

print("\nDetected Columns (Role | Confidence):")
for col_info in result.get('detected_columns', []):
    col_name = col_info.get('column_name', 'N/A')
    role = col_info.get('role', 'UNKNOWN')
    conf = col_info.get('confidence', 0)
    print(f"  {col_name} -> {role} ({conf}%)")

print("\nColumn Validations:")
for col, validation in result.get('column_validations', {}).items():
    role = validation.get('role', 'UNKNOWN')
    is_valid = validation.get('is_valid')
    failed_count = len(validation.get('rules_failed', []))
    if failed_count > 0:
        print(f"  {col} ({role}): {failed_count} rule(s) failed")
        for failed_rule in validation.get('rules_failed', [])[:2]:
            print(f"    - {failed_rule.get('rule', 'N/A')}: {failed_rule.get('reason', 'N/A')}")

print("\nValidation Summary:")
summary = result.get('validation_summary', {})
print(f"  Rules Applied: {len(summary.get('rules_applied', []))}")
print(f"  Rules Passed: {len(summary.get('rules_passed', []))}")
print(f"  Rules Failed: {len(summary.get('rules_failed', []))}")
print(f"  Rules Skipped: {len(summary.get('rules_skipped', []))}")

# Test JSON serialization
try:
    json_str = json.dumps(result, default=str, indent=2)
    print("\n✓ JSON Serialization: SUCCESS")
except Exception as e:
    print(f"\n✗ JSON Serialization: FAILED - {e}")
