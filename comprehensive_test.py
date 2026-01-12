"""
Comprehensive test for the Data Validation Engine with various scenarios
"""

from data_validation_engine import DataValidationEngine, print_validation_report
import pandas as pd
import numpy as np

def create_test_datasets():
    """Create various test datasets to showcase different validation scenarios."""
    
    # 1. Banking dataset with some issues
    banking_data_with_issues = """account_number,customer_id,customer_name,account_type,transaction_date,transaction_type,debit,credit,amount
100000001,CUST001,John Doe,SAVINGS,2023-01-01,DEBIT,500,0,500
100000002,CUST002,Jane Smith,CURRENT,2023-01-01,CREDIT,0,1000,1000
100000003,CUST003,Bob Johnson,LOAN,2023-01-02,DEBIT,200,0,200
100000004,CUST004,Alice Brown,SAVINGS,2023-01-02,CREDIT,0,500,500
,MISSING_ID,Invalid Acc,INVALID_TYPE,2023-01-03,INVALID_TYPE,100,50,150
100000006,CUST006,Valid User,SAVINGS,2023-01-04,DEBIT,300,200,500  # Both debit and credit > 0
"""
    
    with open("banking_with_issues.csv", "w") as f:
        f.write(banking_data_with_issues)
    
    # 2. Sales dataset
    sales_data = """product_name,customer_id,customer_name,quantity,unit_price,total_amount,tax_amount,net_amount,transaction_date
Laptop,CUST001,John Doe,1,1200,1200,180,1380,2023-01-05
Mouse,CUST002,Jane Smith,2,25,50,7.5,57.5,2023-01-05
Keyboard,CUST003,Bob Johnson,1,75,75,11.25,86.25,2023-01-06
Monitor,CUST004,Alice Brown,1,300,300,45,345,2023-01-06
"""
    
    with open("sales_data.csv", "w") as f:
        f.write(sales_data)
    
    # 3. HR dataset
    hr_data = """employee_id,employee_name,department,designation,salary,joining_date,phone,email
EMP001,John Doe,IT,Software Engineer,75000,2022-01-15,9876543210,john.doe@company.com
EMP002,Jane Smith,HR,HR Manager,85000,2021-05-20,9876543211,jane.smith@company.com
EMP003,Bob Johnson,Finance,Accountant,65000,2020-03-10,9876543212,bob.johnson@company.com
"""
    
    with open("hr_data.csv", "w") as f:
        f.write(hr_data)
    
    print("Created test datasets:")
    print("- banking_with_issues.csv (with validation issues)")
    print("- sales_data.csv (sales domain)")
    print("- hr_data.csv (HR domain)")

def run_comprehensive_tests():
    """Run comprehensive tests on different datasets."""
    validator = DataValidationEngine()
    
    test_files = [
        ("bank.csv", "Original banking dataset"),
        ("banking_with_issues.csv", "Banking dataset with issues"),
        ("sales_data.csv", "Sales dataset"),
        ("hr_data.csv", "HR dataset")
    ]
    
    for filename, description in test_files:
        print(f"\n{'='*80}")
        print(f"TESTING: {description}")
        print(f"FILE: {filename}")
        print('='*80)
        
        try:
            results = validator.validate_dataset(filename)
            print_validation_report(results)
        except Exception as e:
            print(f"ERROR processing {filename}: {str(e)}")

if __name__ == "__main__":
    create_test_datasets()
    run_comprehensive_tests()