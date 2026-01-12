"""
Test script for Dynamic Business Rules Validator

Demonstrates how the validator works with different column combinations.
"""

from dynamic_business_rules_validator import DynamicBusinessRulesValidator
import json

def test_validator(csv_path: str):
    """Test the dynamic business rules validator."""
    validator = DynamicBusinessRulesValidator()
    results = validator.validate(csv_path)
    
    print("=" * 80)
    print("DYNAMIC BUSINESS RULES VALIDATION RESULTS")
    print("=" * 80)
    print()
    
    if "error" in results:
        print(f"ERROR: {results['error']}")
        return
    
    # Display detected columns
    print("DETECTED COLUMNS:")
    print("-" * 80)
    if results["detected_columns"]:
        for col_name, role in results["detected_columns"].items():
            print(f"  {col_name:30} -> {role}")
    else:
        print("  No columns detected")
    print()
    
    # Display business rules results
    print("ALL BUSINESS RULES VALIDATION:")
    print("-" * 80)
    for rule in results["business_rules"]:
        if rule["status"] == "PASS":
            status_symbol = "[PASS]"
        elif rule["status"] == "FAIL":
            status_symbol = "[FAIL]"
        elif rule["status"] == "SKIPPED":
            status_symbol = "[SKIP]"
        elif rule["status"] == "PATTERN_MISMATCH":
            status_symbol = "[PATTERN MISMATCH]"
        elif rule["status"] == "NOT_APPLICABLE":
            status_symbol = "[NOT APPLICABLE]"
        else:
            status_symbol = "[UNKNOWN]"
        
        col_name = rule.get('column_name', 'N/A')
        print(f"{status_symbol} {rule['rule_name']:40} [{col_name}]")
        
        if rule["status"] == "SKIPPED":
            print(f"    Reason: {rule.get('reason', 'N/A')}")
        elif rule["status"] == "PATTERN_MISMATCH":
            print(f"    Reason: {rule.get('reason', 'N/A')}")
        elif rule["status"] == "NOT_APPLICABLE":
            print(f"    Reason: {rule.get('reason', 'N/A')}")
        elif rule["violations"]:
            for violation in rule["violations"]:
                print(f"    - {violation}")
    print()
    
    # Display summary
    print("SUMMARY:")
    print("-" * 80)
    summary = results["summary"]
    print(f"  Total columns in file:           {summary['total_columns']}")
    print(f"  Columns detected (name+pattern): {summary['detected_columns']}")
    print(f"  Total business rules defined:    {summary['total_business_rules']}")
    print(f"  Rules applied:                   {summary['rules_applied']}")
    print(f"    - Rules passed:                 {summary['rules_passed']}")
    print(f"    - Rules failed:                 {summary['rules_failed']}")
    print(f"    - Rules skipped (empty):        {summary['rules_skipped']}")
    print(f"    - Pattern mismatch:             {summary['rules_pattern_mismatch']}")
    print(f"    - Not applicable (missing):     {summary['rules_not_applicable']}")
    print()
    
    # Show breakdown by status
    print("RULE STATUS BREAKDOWN:")
    print("-" * 80)
    status_counts = {}
    for rule in results["business_rules"]:
        status = rule["status"]
        status_counts[status] = status_counts.get(status, 0) + 1
    
    for status, count in sorted(status_counts.items()):
        print(f"  {status:20} {count:3} rules")
    print()
    
    print("=" * 80)

if __name__ == "__main__":
    # Test with the bank.csv file
    test_validator("bank.csv")
    
    print("\n\n")
    print("KEY FEATURES:")
    print("=" * 80)
    print("1. Dynamic Column Detection: Only validates columns that exist")
    print("2. Missing Columns Skipped: No failures for missing columns")
    print("3. Business Rules Only: Validates business logic, not static formats")
    print("4. Cross-Column Validation: Validates relationships (e.g., balance formula)")
    print("=" * 80)
