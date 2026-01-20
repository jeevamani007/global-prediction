"""
Quick test script for enhanced banking validation engine
"""

from enhanced_banking_validation import EnhancedBankingValidationEngine
import pandas as pd
import json

def test_validation():
    # Test file path
    test_file = "test_banking_validation_data.csv"
    
    print("=" * 80)
    print("TESTING ENHANCED BANKING VALIDATION ENGINE")
    print("=" * 80)
    
    # Create validation engine
    engine = EnhancedBankingValidationEngine()
    
    # Load test file
    print(f"\n📁 Loading test file: {test_file}")
    df = pd.read_csv(test_file)
    print(f"✓ Loaded {len(df)} rows, {len(df.columns)} columns")
    print(f"  Columns: {', '.join(df.columns)}")
    
    # Run validation
    print(f"\n🔍 Running validation...")
    result = engine.validate_file(test_file, df)
    
    # Print summary
    print(f"\n📊 VALIDATION SUMMARY")
    print("=" * 80)
    summary = result.get('summary', {})
    print(f"File: {result.get('file_name', 'Unknown')}")
    print(f"Overall Status: {result.get('status', 'UNKNOWN')}")
    print(f"Can Proceed: {result.get('can_proceed', False)}")
    print(f"\nColumn Statistics:")
    print(f"  Total Columns: {summary.get('total_columns', 0)}")
    print(f"  ✅ Valid: {summary.get('valid_count', 0)}")
    print(f"  ⚠️  Warnings: {summary.get('warning_count', 0)}")
    print(f"  ❌ Invalid: {summary.get('invalid_count', 0)}")
    print(f"\n{summary.get('message', '')}")
    
    # Print column details
    print(f"\n📋 COLUMN VALIDATION DETAILS")
    print("=" * 80)
    
    columns = result.get('columns', [])
    for i, col in enumerate(columns, 1):
        status = col.get('status', 'UNKNOWN')
        icon = '✅' if status == 'VALID' else ('⚠️' if status == 'WARNING' else '❌')
        
        print(f"\n{i}. {icon} {col.get('column_name', 'Unknown')} - {status}")
        print(f"   Definition: {col.get('definition', 'N/A')[:100]}...")
        
        if col.get('violations'):
            print(f"   Issues:")
            for violation in col.get('violations', []):
                print(f"     - {violation}")
        
        if col.get('masked_sample'):
            print(f"   🔒 Sensitive data masked: {col.get('masked_sample')}")
    
    # Save full result to JSON
    output_file = "test_validation_result.json"
    with open(output_file, 'w') as f:
        json.dump(result, f, indent=2)
    print(f"\n💾 Full validation result saved to: {output_file}")
    
    print("\n" + "=" * 80)
    print("TEST COMPLETED")
    print("=" * 80)

if __name__ == "__main__":
    test_validation()
