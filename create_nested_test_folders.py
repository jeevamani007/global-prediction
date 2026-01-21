"""
Create nested folder structure for testing multi-application upload
Each subfolder will be analyzed as a separate application
"""
import os
import shutil

# Define nested folder structure
# Parent folder contains multiple app subfolders
nested_structure = {
    'banking_system': {
        'customers.csv': 'customer_id,name,email,phone\n1,John Doe,john@bank.com,1234567890\n2,Jane Smith,jane@bank.com,0987654321',
        'accounts.csv': 'account_number,customer_id,account_type,balance\nACC001,1,Savings,50000\nACC002,2,Current,75000',
        'transactions.csv': 'transaction_id,account_number,amount,transaction_date\nTX001,ACC001,5000,2024-01-01\nTX002,ACC002,10000,2024-01-02'
    },
    'loan_management': {
        'loans.csv': 'loan_id,customer_id,loan_amount,interest_rate,tenure\nL001,1,500000,8.5,60\nL002,2,1000000,7.5,120',
        'emi_schedule.csv': 'loan_id,emi_number,emi_amount,due_date\nL001,1,10000,2024-02-01\nL001,2,10000,2024-03-01',
        'disbursements.csv': 'loan_id,disbursement_date,disbursement_amount\nL001,2024-01-15,500000\nL002,2024-01-20,1000000'
    },
    'hr_payroll': {
        'employees.csv': 'employee_id,name,designation,salary,department\nE001,Alice,Manager,80000,IT\nE002,Bob,Developer,60000,IT',
        'attendance.csv': 'employee_id,date,status,hours_worked\nE001,2024-01-01,Present,8\nE002,2024-01-01,Present,8',
        'salaries.csv': 'employee_id,month,basic_salary,allowances,deductions,net_salary\nE001,2024-01,80000,10000,5000,85000\nE002,2024-01,60000,8000,3000,65000'
    },
    'card_management': {
        'credit_cards.csv': 'card_number,customer_id,card_type,credit_limit,card_status\nCC001,1,Platinum,200000,Active\nCC002,2,Gold,150000,Active',
        'card_transactions.csv': 'transaction_id,card_number,merchant,amount,transaction_date\nCT001,CC001,Amazon,5000,2024-01-01\nCT002,CC002,Flipkart,3000,2024-01-02',
        'statements.csv': 'card_number,statement_date,total_dues,minimum_due,due_date\nCC001,2024-01-31,50000,5000,2024-02-10\nCC002,2024-01-31,30000,3000,2024-02-10'
    }
}

# Create the nested structure
parent_folder = 'test_nested_apps'

# Remove existing folder if it exists
if os.path.exists(parent_folder):
    shutil.rmtree(parent_folder)

# Create parent folder
os.makedirs(parent_folder, exist_ok=True)

# Create each subfolder with its CSV files
for subfolder_name, files_dict in nested_structure.items():
    subfolder_path = os.path.join(parent_folder, subfolder_name)
    os.makedirs(subfolder_path, exist_ok=True)
    
    for filename, content in files_dict.items():
        file_path = os.path.join(subfolder_path, filename)
        with open(file_path, 'w') as f:
            f.write(content)

print("✅ Created nested folder structure for testing!")
print("\n📁 Structure:")
print(f"   {parent_folder}/")
for subfolder_name, files_dict in nested_structure.items():
    print(f"   ├── {subfolder_name}/")
    for filename in files_dict.keys():
        print(f"   │   ├── {filename}")
    print(f"   │   └── ({len(files_dict)} files)")

print(f"\n🎯 Expected Behavior:")
print(f"   When you upload the '{parent_folder}' folder:")
print(f"   ✅ 4 Applications will be detected (not 1)")
print(f"   ✅ Each subfolder analyzed separately:")
print(f"      • banking_system → Core Banking")
print(f"      • loan_management → Loan Management")  
print(f"      • hr_payroll → HR/Payroll")
print(f"      • card_management → Cards")

print(f"\n📤 How to Upload:")
print(f"   1. Go to http://localhost:8000")
print(f"   2. Click '📁 Upload Folders'")
print(f"   3. Select the '{parent_folder}' folder")
print(f"      (Browser will include all subfolders automatically)")
print(f"   4. Click '🚀 Analyze Files'")

print(f"\n✨ Result on /multi-analysis page:")
print(f"   - 4 Applications Analyzed")
print(f"   - 12 Total CSV Files (3 per app)")
print(f"   - Multiple Relationships Found")
print(f"   - Each app shown in separate section with:")
print(f"     ✓ Unique application type")
print(f"     ✓ Separate business rules")
print(f"     ✓ Separate column relationships")
print(f"     ✓ Individual confidence percentage")
