"""
Test script for the Data Validation Engine
"""

from data_validation_engine import DataValidationEngine, print_validation_report
import pandas as pd
import io

def test_basic_functionality():
    """Test the basic functionality of the validation engine."""
    print("Testing Data Validation Engine...")
    print()
    
    validator = DataValidationEngine()
    
    # Test with the existing bank.csv file
    print("Analyzing bank.csv file:")
    results = validator.validate_dataset("bank.csv")
    print_validation_report(results)
    
    print("\n" + "="*100)
    print("TEST COMPLETED")
    print("="*100)

def create_sample_csv():
    """Create a sample CSV file for testing."""
    sample_data = """account_number,customer_id,customer_name,account_type,branch,ifsc_code,transaction_id,txn_date,transaction_type,amount
100000001,CUST001,Karthik,Savings,Main Branch,IFSC001,TXN20260101001,2026-01-01,DEBIT,500
100000002,CUST002,Anitha,Current,Main Branch,IFSC001,TXN20260101002,2026-01-01,CREDIT,2000
100000003,CUST003,Mani,Savings,West Branch,IFSC002,TXN20260102003,2026-01-02,DEBIT,1500
100000004,CUST004,Priya,Current,East Branch,IFSC003,TXN20260102004,2026-01-02,CREDIT,5000
100000005,CUST005,Suresh,Savings,South Branch,IFSC004,TXN20260103005,2026-01-03,DEBIT,2000"""
    
    with open("sample_test.csv", "w") as f:
        f.write(sample_data)
    
    print("Created sample_test.csv for testing")

if __name__ == "__main__":
    # Create sample file if needed
    create_sample_csv()
    
    # Run the test
    test_basic_functionality()