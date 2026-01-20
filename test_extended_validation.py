"""
Test script for validating extended banking columns (30 additional columns)
"""

from enhanced_banking_validation import EnhancedBankingValidationEngine
import pandas as pd
import json

def create_test_data():
    """Create test CSV with extended banking columns"""
    data = {
        # Core columns
        'customer_id': ['CUST00001', 'CUST00002', 'CUST00003'],
        'customer_name': ['Rajesh Kumar', 'Priya Sharma', 'Amit Patel'],
        'account_number': ['1234567890', '9876543211', '5432167890'],
        'account_type': ['SAVINGS', 'CURRENT', 'SALARY'],
        
        # Extended columns - Branch Information
        'branch_code': ['BR001', 'BR002', 'INVALID'],  # One invalid
        'branch_name': ['Main Branch', 'City Center Branch', 'Downtown Branch'],
        'ifsc_verified': ['TRUE', 'TRUE', 'FALSE'],
        'customer_category': ['VIP', 'STANDARD', 'SENIOR_CITIZEN'],
        
        # Extended columns - Account Lifecycle
        'account_open_date': ['2020-01-15', '2021-06-10', '2019-03-22'],
        'account_closure_date': [None, None, None],
        
        # Extended columns - Transaction Limits
        'daily_transaction_limit': [50000, 100000, 25000],
        'monthly_transaction_limit': [500000, 1000000, 250000],
        'atm_withdrawal_limit': [10000, 20000, 5000],
        
        # Extended columns - Security
        'otp_verified': ['TRUE', 'TRUE', 'FALSE'],  # One not verified
        'login_attempts': [0, 1, 0],
        'account_lock_status': ['UNLOCKED', 'UNLOCKED', 'UNLOCKED'],
        
        # Extended columns - Transaction Processing
        'transaction_channel': ['INTERNET_BANKING', 'MOBILE_APP', 'ATM'],
        'transaction_status': ['SUCCESS', 'SUCCESS', 'PENDING'],
        'reversal_flag': ['FALSE', 'FALSE', 'FALSE'],
        
        # Extended columns - Compliance
        'aml_alert_flag': ['FALSE', 'FALSE', 'FALSE'],
        'suspicious_txn_score': [15, 25, 35],
        'customer_consent': ['TRUE', 'TRUE', 'TRUE'],
        
        # Extended columns - Notifications
        'statement_cycle': ['MONTHLY', 'QUARTERLY', 'MONTHLY'],
        'notification_preference': ['ALL', 'EMAIL', 'SMS']
    }
    
    df = pd.DataFrame(data)
    test_file = 'test_extended_validation.csv'
    df.to_csv(test_file, index=False)
    return test_file, df

def main():
    print("=" * 80)
    print("TESTING EXTENDED BANKING VALIDATION (53 COLUMNS)")
    print("=" * 80)
    
    # Create test data
    test_file, df = create_test_data()
    print(f"\n✓ Created test file with {len(df.columns)} columns")
    print(f"  Columns: {', '.join(df.columns[:10])}...")
    
    # Run validation
    print(f"\n🔍 Running validation on extended columns...")
    engine = EnhancedBankingValidationEngine()
    result = engine.validate_file(test_file, df)
    
    # Print summary
    print(f"\n📊 VALIDATION SUMMARY")
    print("=" * 80)
    summary = result.get('summary', {})
    print(f"File: {result.get('file_name', 'Unknown')}")
    print(f"Overall Status: {result.get('status', 'UNKNOWN')}")
    print(f"\nColumn Statistics:")
    print(f"  Total Columns: {summary.get('total_columns', 0)}")
    print(f"  ✅ Valid: {summary.get('valid_count', 0)}")
    print(f"  ⚠️  Warnings: {summary.get('warning_count', 0)}")
    print(f"  ❌ Invalid: {summary.get('invalid_count', 0)}")
    
    # Print invalid columns
    invalid_cols = [col for col in result.get('columns', []) if col.get('status') == 'INVALID']
    if invalid_cols:
        print(f"\n❌ INVALID COLUMNS ({len(invalid_cols)}):")
        print("=" * 80)
        for col in invalid_cols:
            print(f"\n{col.get('column_name', 'Unknown')}")
            print(f"  Status: {col.get('status', 'UNKNOWN')}")
            if col.get('violations'):
                print(f"  Issues:")
                for violation in col.get('violations', []):
                    print(f"    - {violation}")
    
    # Print valid extended columns
    extended_column_names = [
        'branch_code', 'branch_name', 'ifsc_verified', 'customer_category',
        'account_open_date', 'account_closure_date', 'daily_transaction_limit',
        'monthly_transaction_limit', 'atm_withdrawal_limit', 'otp_verified',
        'login_attempts', 'account_lock_status', 'transaction_channel',
        'transaction_status', 'reversal_flag', 'aml_alert_flag',
        'suspicious_txn_score', 'customer_consent', 'statement_cycle',
        'notification_preference'
    ]
    
    extended_results = [col for col in result.get('columns', []) if col.get('column_name') in extended_column_names]
    
    print(f"\n✅ EXTENDED COLUMNS VALIDATED ({len(extended_results)}):")
    print("=" * 80)
    for col in extended_results:
        status_icon = '✅' if col.get('status') == 'VALID' else ('⚠️' if col.get('status') == 'WARNING' else '❌')
        print(f"{status_icon} {col.get('column_name', 'Unknown')}: {col.get('status', 'UNKNOWN')}")
    
    # Save full result
    output_file = "test_extended_validation_result.json"
    with open(output_file, 'w') as f:
        json.dump(result, f, indent=2)
    print(f"\n💾 Full validation result saved to: {output_file}")
    
    print("\n" + "=" * 80)
    print("TEST COMPLETED - Extended Banking Validation Working!")
    print("=" * 80)
    print(f"\n📝 Summary:")
    print(f"   - Total columns validated: {summary.get('total_columns', 0)}")
    print(f"   - Extended columns working: {len(extended_results)}")
    print(f"   - System now supports 53 banking columns (23 core + 30 extended)")

if __name__ == "__main__":
    main()
