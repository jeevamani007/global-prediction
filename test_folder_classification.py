"""
Test script for folder-based application classification system.
Creates sample CSV files in test folders and tests the analyzer.
"""

import os
import pandas as pd
from folder_based_application_analyzer import FolderBasedApplicationAnalyzer
from application_type_detector import ApplicationTypeDetector


def create_test_folders():
    """Create test data folders with sample CSV files."""
    
    # Create test_data directory
    base_dir = "test_data"
    os.makedirs(base_dir, exist_ok=True)
    
    # Folder 1: Core Banking Application
    banking_folder = os.path.join(base_dir, "core_banking")
    os.makedirs(banking_folder, exist_ok=True)
    
    # accounts.csv - Master Data
    accounts_df = pd.DataFrame({
        'account_number': ['ACC001', 'ACC002', 'ACC003', 'ACC004', 'ACC005'],
        'customer_id': ['CUST001', 'CUST002', 'CUST003', 'CUST001', 'CUST004'],
        'account_type': ['SAVINGS', 'CURRENT', 'SAVINGS', 'SAVINGS', 'CURRENT'],
        'branch_code': ['BR001', 'BR002', 'BR001', 'BR003', 'BR002'],
        'opening_balance': [10000, 25000, 5000, 15000, 50000],
        'current_balance': [12500, 30000, 7500, 18000, 55000],
        'account_status': ['ACTIVE', 'ACTIVE', 'ACTIVE', 'ACTIVE', 'INACTIVE'],
        'opening_date': ['2023-01-15', '2023-02-20', '2023-03-10', '2023-04-05', '2023-05-12']
    })
    accounts_df.to_csv(os.path.join(banking_folder, 'accounts.csv'), index=False)
    
    # transactions.csv - Transaction Data
    transactions_df = pd.DataFrame({
        'transaction_id': [f'TXN{str(i).zfill(3)}' for i in range(1, 21)],
        'account_number': ['ACC001', 'ACC002', 'ACC001', 'ACC003', 'ACC002'] * 4,
        'transaction_date': ['2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18', '2024-01-19'] * 4,
        'transaction_type': ['DEPOSIT', 'WITHDRAWAL', 'DEPOSIT', 'WITHDRAWAL', 'TRANSFER'] * 4,
        'transaction_amount': [1000, 500, 2000, 300, 1500] * 4,
        'debit_credit': ['CR', 'DR', 'CR', 'DR', 'DR'] * 4,
        'balance_after': [11000, 24500, 13000, 7200, 28500] * 4
    })
    transactions_df.to_csv(os.path.join(banking_folder, 'transactions.csv'), index=False)
    
    # customers.csv - Reference Data
    customers_df = pd.DataFrame({
        'customer_id': ['CUST001', 'CUST002', 'CUST003', 'CUST004'],
        'customer_name': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Williams'],
        'email': ['john@example.com', 'jane@example.com', 'bob@example.com', 'alice@example.com'],
        'phone': ['1234567890', '2345678901', '3456789012', '4567890123'],
        'address': ['123 Main St', '456 Oak Ave', '789 Pine Rd', '321 Elm St']
    })
    customers_df.to_csv(os.path.join(banking_folder, 'customers.csv'), index=False)
    
    print(f"✅ Created Core Banking test folder with 3 CSV files")
    
    # Folder 2: Loan Management Application
    loan_folder = os.path.join(base_dir, "loan_management")
    os.makedirs(loan_folder, exist_ok=True)
    
    # loans.csv - Master Data
    loans_df = pd.DataFrame({
        'loan_id': ['LOAN001', 'LOAN002', 'LOAN003', 'LOAN004'],
        'customer_id': ['CUST001', 'CUST002', 'CUST003', 'CUST004'],
        'loan_amount': [500000, 1000000, 750000, 300000],
        'interest_rate': [8.5, 9.0, 8.75, 8.25],
        'tenure': [60, 120, 84, 36],
        'loan_type': ['HOME', 'HOME', 'PERSONAL', 'CAR'],
        'sanction_date': ['2023-01-10', '2023-02-15', '2023-03-20', '2023-04-25'],
        'maturity_date': ['2028-01-10', '2033-02-15', '2030-03-20', '2026-04-25'],
        'outstanding_amount': [450000, 980000, 700000, 250000],
        'loan_status': ['ACTIVE', 'ACTIVE', 'ACTIVE', 'ACTIVE']
    })
    loans_df.to_csv(os.path.join(loan_folder, 'loans.csv'), index=False)
    
    # emi_schedule.csv - Transaction Data
    emi_df = pd.DataFrame({
        'emi_id': [f'EMI{str(i).zfill(3)}' for i in range(1, 17)],
        'loan_id': ['LOAN001', 'LOAN002', 'LOAN003', 'LOAN004'] * 4,
        'emi_number': [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4],
        'emi_date': ['2023-02-10', '2023-03-15', '2023-04-20', '2023-05-25'] * 4,
        'emi_amount': [10000, 15000, 12000, 9000] * 4,
        'principal': [7000, 10000, 8500, 6500] * 4,
        'interest': [3000, 5000, 3500, 2500] * 4,
        'payment_status': ['PAID', 'PAID', 'PAID', 'PAID'] * 4
    })
    emi_df.to_csv(os.path.join(loan_folder, 'emi_schedule.csv'), index=False)
    
    print(f"✅ Created Loan Management test folder with 2 CSV files")
    
    # Folder 3: Payments Application
    payments_folder = os.path.join(base_dir, "payments")
    os.makedirs(payments_folder, exist_ok=True)
    
    # payments.csv - Transaction Data
    payments_df = pd.DataFrame({
        'payment_id': [f'PAY{str(i).zfill(4)}' for i in range(1, 11)],
        'payment_reference': [f'REF{str(i).zfill(6)}' for i in range(1, 11)],
        'payer_account': [f'ACC{str(i).zfill(3)}' for i in range(1, 11)],
        'beneficiary_account': [f'BEN{str(i).zfill(3)}' for i in range(1, 11)],
        'payment_amount': [5000, 10000, 2500, 7500, 15000, 3000, 8000, 12000, 6000, 9000],
        'payment_date': ['2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18', '2024-01-19',
                         '2024-01-20', '2024-01-21', '2024-01-22', '2024-01-23', '2024-01-24'],
        'payment_mode': ['NEFT', 'RTGS', 'IMPS', 'UPI', 'NEFT', 'RTGS', 'IMPS', 'UPI', 'NEFT', 'RTGS'],
        'payment_status': ['SUCCESS', 'SUCCESS', 'SUCCESS', 'PENDING', 'SUCCESS', 
                          'SUCCESS', 'FAILED', 'SUCCESS', 'SUCCESS', 'SUCCESS'],
        'utr_number': [f'UTR{str(i).zfill(12)}' for i in range(1, 11)]
    })
    payments_df.to_csv(os.path.join(payments_folder, 'payments.csv'), index=False)
    
    print(f"✅ Created Payments test folder with 1 CSV file")
    
    return base_dir


def test_folder_analyzer():
    """Test the folder-based analyzer on test data."""
    
    print("\n" + "="*80)
    print("Testing Folder-Based Application Classification System")
    print("="*80 + "\n")
    
    # Create test folders
    base_dir = create_test_folders()
    
    # Initialize analyzers
    folder_analyzer = FolderBasedApplicationAnalyzer()
    app_type_detector = ApplicationTypeDetector()
    
    # Test each folder
    folders = ["core_banking", "loan_management", "payments"]
    
    for folder_name in folders:
        folder_path = os.path.join(base_dir, folder_name)
        
        print(f"\n{'─'*80}")
        print(f"📁 Analyzing Folder: {folder_name}")
        print(f"{'─'*80}\n")
        
        # Analyze folder
        folder_analysis = folder_analyzer.analyze_folder(folder_path)
        
        if 'error' in folder_analysis:
            print(f"❌ Error: {folder_analysis['error']}")
            continue
        
        # Display folder stats
        print(f"📊 Folder Statistics:")
        print(f"   • Total CSV Files: {folder_analysis['total_files']}")
        print(f"   • Total Rows: {folder_analysis['total_rows']}")
        print(f"   • Total Columns: {folder_analysis['total_columns']}")
        print(f"   • Unique Columns: {folder_analysis['unique_column_count']}\n")
        
        # Display file roles
        print(f"🏷️  File Roles:")
        for filename, role in folder_analysis['file_roles'].items():
            print(f"   • {filename}: {role}")
        
        # Display relationships
        print(f"\n🔗 Cross-File Relationships: {len(folder_analysis['cross_file_relationships'])}")
        for rel in folder_analysis['cross_file_relationships']:
            print(f"   • {rel['file1']} ↔ {rel['file2']}")
            print(f"     Column: {rel['column']} | Type: {rel['relationship_type']}")
            print(f"     Overlap: {rel['overlap_percentage']}%")
        
        # Detect application type
        csv_files_data = folder_analysis.get('csv_files_data', {})
        app_type_result = app_type_detector.detect_type(
            csv_files_data,
            relationships=folder_analysis['cross_file_relationships']
        )
        
        # Display application type
        print(f"\n🎯 Application Type Detection:")
        print(f"   • Type: {app_type_result['application_type']}")
        print(f"   • Confidence: {app_type_result['confidence']}% ({app_type_result['confidence_level']})")
        print(f"   • Patterns Detected: {', '.join(app_type_result['patterns_detected'][:3])}")
        
        if app_type_result['alternative_types']:
            print(f"\n   📋 Alternative Types:")
            for alt in app_type_result['alternative_types'][:2]:
                print(f"      • {alt['type']}: {alt['confidence']}%")
        
        print(f"\n   💡 {app_type_result['explanation']}")
    
    print(f"\n{'='*80}")
    print("✅ Testing Complete!")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    test_folder_analyzer()
