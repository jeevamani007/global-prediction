import pandas as pd
import os
from banking_dataset_validator_full import BankingDatasetValidator


def create_sample_banking_data():
    """Create a sample banking dataset for testing."""
    data = {
        'account_number': ['123456789012', '234567890123', '345678901234', '456789012345'],
        'customer_id': ['CUST001', 'CUST002', 'CUST003', 'CUST004'],
        'customer_name': ['John Doe', 'Jane Smith', 'Robert Johnson', 'Emily Davis'],
        'account_type': ['Savings', 'Current', 'Salary', 'Savings'],
        'branch': ['MUMBAI', 'DELHI', 'CHENNAI', 'BANGALORE'],
        'ifsc_code': ['SBIN0002499', 'HDFC0000123', 'ICIC0001999', 'AXIS0000102'],
        'transaction_id': ['TXN001', 'TXN002', 'TXN003', 'TXN004'],
        'txn_date': ['2023-01-15', '2023-01-16', '2023-01-17', '2023-01-18'],
        'transaction_type': ['DEBIT', 'CREDIT', 'DEBIT', 'CREDIT'],
        'amount': [5000.00, 10000.00, 2500.00, 7500.00]
    }
    
    df = pd.DataFrame(data)
    df.to_csv('sample_banking_data.csv', index=False)
    return 'sample_banking_data.csv'


def create_sample_banking_data_with_issues():
    """Create a sample banking dataset with intentional issues for testing."""
    data = {
        'account_number': ['12345', '234567890123', 'ABCDEF', '456789012345'],  # Some invalid entries
        'customer_id': ['CUST001', 'CUST002', '', 'CUST004'],  # Empty value
        'customer_name': ['John', 'Jane Smith', 'Robert Johnson', 'Emily Davis'],  # Short name
        'account_type': ['Savings', 'Current', 'Loan', 'Savings'],  # Invalid type
        'branch': ['MUMBAI', 'DELHI', 'CHENNAI', 'BANGALORE'],
        'ifsc_code': ['SBIN0002499', 'SHORT', 'ICIC0001999', 'AXIS0000102'],  # Wrong length
        'transaction_id': ['TXN001', 'TXN002', 'TXN003', 'TXN004'],
        'txn_date': ['2023-01-15', 'INVALID_DATE', '2023-01-17', '2023-01-18'],  # Invalid date
        'transaction_type': ['DEBIT', 'CREDIT', 'WITHDRAWAL', 'CREDIT'],  # Invalid type
        'amount': [-500.00, 10000.00, 2500.00, 7500.00]  # Negative amount
    }
    
    df = pd.DataFrame(data)
    df.to_csv('sample_banking_data_issues.csv', index=False)
    return 'sample_banking_data_issues.csv'


def test_banking_validator():
    """Test the banking dataset validator."""
    print("Testing Banking Dataset Validator...")
    
    # Test with good data
    print("\n1. Testing with valid banking data:")
    good_file = create_sample_banking_data()
    validator = BankingDatasetValidator()
    results = validator.validate_dataset(good_file)
    
    if "error" not in results:
        summary = results["validation_report"]["summary"]
        print(f"   Final Decision: {summary['final_decision']}")
        print(f"   Overall Compliance: {summary['overall_compliance_percentage']}%")
        print(f"   Total Columns: {summary['total_columns']}")
        print(f"   Matched Columns: {summary['matched_columns']}")
        print(f"   Failed Columns: {summary['failed_columns']}")
        
        print("\n   Column Details:")
        for col in results["validation_report"]["columns"]:
            print(f"     {col['column_name']}: {col['status']} (Conf: {col['confidence']:.2f})")
    else:
        print(f"   Error: {results['error']}")
    
    # Test with data containing issues
    print("\n2. Testing with data containing issues:")
    bad_file = create_sample_banking_data_with_issues()
    results = validator.validate_dataset(bad_file)
    
    if "error" not in results:
        summary = results["validation_report"]["summary"]
        print(f"   Final Decision: {summary['final_decision']}")
        print(f"   Overall Compliance: {summary['overall_compliance_percentage']}%")
        print(f"   Total Columns: {summary['total_columns']}")
        print(f"   Matched Columns: {summary['matched_columns']}")
        print(f"   Failed Columns: {summary['failed_columns']}")
        
        print("\n   Issues Found:")
        for col in results["validation_report"]["columns"]:
            if col["issues"]:
                print(f"     {col['column_name']}: {col['issues']}")
    else:
        print(f"   Error: {results['error']}")
    
    # Cleanup
    if os.path.exists(good_file):
        os.remove(good_file)
    if os.path.exists(bad_file):
        os.remove(bad_file)
    
    print("\nTest completed!")


if __name__ == "__main__":
    test_banking_validator()