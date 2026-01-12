"""
Demo script for the Complete Banking Validator
This script demonstrates how to use the complete banking validator with a sample dataset.
"""

import pandas as pd
import numpy as np
from complete_banking_validator import CompleteBankingValidator
import os

def create_sample_banking_data():
    """Create a sample banking dataset for demonstration."""
    np.random.seed(42)
    
    # Generate sample data
    data = {
        'account_number': [f'{np.random.randint(100000, 999999999)}' for _ in range(100)],
        'customer_id': [f'CUST{np.random.randint(1000, 99999)}' for _ in range(100)],
        'customer_name': [f'Customer {i}' for i in range(1, 101)],
        'account_type': np.random.choice(['Savings', 'Current', 'Salary'], 100),
        'branch': ['Main Branch', 'City Center', 'Suburban'] * 33 + ['Main Branch'],
        'ifsc_code': ['ABC00123456', 'XYZ00789012', 'DEF00345678'] * 33 + ['ABC00123456'],
        'transaction_id': [f'TXN{i:06d}' for i in range(1, 101)],
        'txn_date': pd.date_range(start='2023-01-01', periods=100, freq='D').strftime('%Y-%m-%d'),
        'transaction_type': np.random.choice(['DEBIT', 'CREDIT'], 100),
        'debit': [np.random.uniform(0, 10000) if np.random.choice([True, False]) else 0 for _ in range(100)],
        'credit': [np.random.uniform(0, 10000) if not (np.random.choice([True, False])) else 0 for _ in range(100)],
        'opening_balance': [np.random.uniform(1000, 50000) for _ in range(100)],
        'closing_balance': [np.random.uniform(1000, 50000) for _ in range(100)],
        'currency': ['USD', 'EUR', 'GBP'] * 33 + ['USD'],
        'country': ['USA', 'UK', 'Germany'] * 33 + ['USA'],
        'phone': [f'{np.random.randint(1000000000, 2147483647)}' for _ in range(100)]
    }
    
    # Introduce some intentional errors to demonstrate validation
    # Add some invalid account numbers (too short)
    data['account_number'][0] = '123'  # Too short
    data['account_number'][1] = '12345'  # Too short
    
    # Add some invalid customer names (with numbers)
    data['customer_name'][2] = 'Customer123'
    data['customer_name'][3] = 'John2023'
    
    # Add some invalid account types
    data['account_type'][4] = 'Fixed Deposit'  # Not allowed
    data['account_type'][5] = 'Recurring'      # Not allowed
    
    # Add some invalid transaction types
    data['transaction_type'][6] = 'TRANSFER'  # Not allowed
    data['transaction_type'][7] = 'WITHDRAW'  # Not allowed
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Save to CSV
    csv_path = 'sample_banking_data.csv'
    df.to_csv(csv_path, index=False)
    print(f"Sample banking data saved to {csv_path}")
    
    return csv_path

def run_validation_demo():
    """Run a complete validation demo."""
    print("=" * 80)
    print("BANKING DATA VALIDATOR DEMONSTRATION")
    print("=" * 80)
    
    # Create sample data
    print("\n1. Creating sample banking dataset with intentional errors...")
    sample_file = create_sample_banking_data()
    
    # Initialize validator
    print("\n2. Initializing Complete Banking Validator...")
    validator = CompleteBankingValidator()
    
    # Run validation
    print(f"\n3. Running validation on {sample_file}...")
    results = validator.validate_dataset(sample_file)
    
    # Generate report
    print("\n4. Generating validation report...")
    report = validator.generate_structured_report(results)
    print(report)
    
    # Detailed breakdown
    print("\n5. DETAILED VALIDATION BREAKDOWN:")
    print("-" * 50)
    
    for result in results['column_wise_validation']:
        print(f"\nColumn: {result['column_name']} ({result['standard_name']})")
        print(f"  Rule: {result['business_rule']}")
        print(f"  Result: {result['validation_result']}")
        print(f"  Confidence: {result['confidence_percentage']}%")
        if result['detected_issue']:
            print(f"  Issues: {result['detected_issue']}")
        else:
            print("  Issues: None")
    
    print("\n6. SUMMARY STATISTICS:")
    summary = results['summary']
    print(f"  Total columns analyzed: {summary['total_columns_analyzed']}")
    print(f"  Passed: {summary['total_passed']}")
    print(f"  Failed: {summary['total_failed']}")
    print(f"  Overall confidence: {summary['overall_confidence']}%")
    print(f"  Total records: {summary['total_records']}")
    
    # Cleanup
    if os.path.exists(sample_file):
        os.remove(sample_file)
        print(f"\n7. Cleaned up sample file: {sample_file}")

def explain_features():
    """Explain key features of the banking validator."""
    print("\n" + "=" * 80)
    print("KEY FEATURES OF THE COMPLETE BANKING VALIDATOR")
    print("=" * 80)
    
    features = [
        "Column Detection and Mapping",
        "Standard Banking Column Validation",
        "Business Rule Enforcement",
        "Cross-Column Validation",
        "Comprehensive Issue Detection",
        "Confidence Scoring",
        "Structured Reporting",
        "Flexible Input Support (CSV/SQL)"
    ]
    
    for i, feature in enumerate(features, 1):
        print(f"{i}. {feature}")
    
    print("\nBUSINESS RULES COVERED:")
    print("-" * 30)
    rules = [
        "account_number: Numeric, 6-18 digits, unique",
        "customer_id: Alphanumeric, unique per customer", 
        "customer_name: Letters & spaces only, minimum 3 chars",
        "account_type: Only 'Savings' or 'Current'",
        "account_status: Only 'Active' or 'Deactive'",
        "branch: Alphanumeric",
        "ifsc_code: Alphanumeric, 3-15 characters (flexible format)",
        "transaction_id: Unique, alphanumeric",
        "txn_date: YYYY-MM-DD format",
        "transaction_type: Only 'Debit' or 'Credit'",
        "debit/credit: Numeric, positive, mutually exclusive",
        "opening_balance/closing_balance: Numeric, positive",
        "currency/country: Letters only",
        "phone: Numeric, unique",
        "kyc_status: Letters & spaces only"
    ]
    
    for rule in rules:
        print(f"  • {rule}")

if __name__ == "__main__":
    explain_features()
    run_validation_demo()
    print("\n" + "=" * 80)
    print("VALIDATION COMPLETED SUCCESSFULLY!")
    print("=" * 80)