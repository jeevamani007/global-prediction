"""
Test script for Banking Dataset Validator
"""

import json
from banking_dataset_validator import BankingDatasetValidator

def test_validator(csv_path):
    """Test the validator with a CSV file."""
    validator = BankingDatasetValidator()
    result = validator.validate(csv_path)
    
    print("=" * 80)
    print("BANKING DATASET VALIDATION RESULTS")
    print("=" * 80)
    print(f"\nFile: {csv_path}")
    print(f"Final Decision: {result['final_decision']}")
    print(f"Dataset Confidence: {result['dataset_confidence']}%")
    print(f"\nExplanation: {result['explanation']}")
    
    print("\n" + "-" * 80)
    print("COLUMN VALIDATION RESULTS")
    print("-" * 80)
    for col in result['columns']:
        status_symbol = "[PASS]" if col['status'] == "MATCH" else "[WARN]" if col['status'] == "WARNING" else "[FAIL]"
        print(f"\n{status_symbol} {col['name']}")
        print(f"   Meaning: {col['meaning']}")
        print(f"   Confidence: {col['confidence']}%")
        print(f"   Rules: {col['rules_passed']}/{col['rules_total']} passed")
        print(f"   Status: {col['status']}")
        if col.get('failures'):
            print(f"   Issues: {', '.join(col['failures'][:3])}")
    
    print("\n" + "-" * 80)
    print("RELATIONSHIP VALIDATION RESULTS")
    print("-" * 80)
    for rel in result['relationships']:
        for key, value in rel.items():
            status_symbol = "[PASS]" if value == "PASS" else "[FAIL]" if value == "FAIL" else "[SKIP]"
            # Replace Unicode arrow with ASCII
            key_ascii = key.replace('\u2192', '->')
            print(f"{status_symbol} {key_ascii}: {value}")
    
    print("\n" + "=" * 80)
    print("\nFull JSON Output:")
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    test_validator("bank.csv")