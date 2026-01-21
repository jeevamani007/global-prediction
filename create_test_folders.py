"""
Test script for multiple folder upload
Shows how each folder is analyzed separately with its own application type
"""
import os
import shutil

# Create test folders with different application types
test_folders = {
    'banking_app': [
        ('customers.csv', 'customer_id,name,account_number\n1,John,ACC001\n2,Jane,ACC002'),
        ('transactions.csv', 'transaction_id,account_number,amount\nTX1,ACC001,100\nTX2,ACC002,200'),
    ],
    'loan_app': [
        ('loans.csv', 'loan_id,customer_id,loan_amount,interest_rate\nL1,1,50000,8.5\nL2,2,100000,7.5'),
        ('emi_schedule.csv', 'loan_id,emi_number,emi_amount\nL1,1,5000\nL1,2,5000'),
    ],
    'hr_app': [
        ('employees.csv', 'employee_id,name,salary,designation\nE1,Alice,50000,Manager\nE2,Bob,40000,Developer'),
        ('attendance.csv', 'employee_id,date,status\nE1,2024-01-01,Present\nE2,2024-01-01,Present'),
    ]
}

# Create test folder structure
base_path = 'test_multi_folders'
os.makedirs(base_path, exist_ok=True)

for folder_name, files in test_folders.items():
    folder_path = os.path.join(base_path, folder_name)
    os.makedirs(folder_path, exist_ok=True)
    
    for filename, content in files:
        file_path = os.path.join(folder_path, filename)
        with open(file_path, 'w') as f:
            f.write(content)

print(f"✅ Created {len(test_folders)} test folders in '{base_path}/':")
for folder_name in test_folders.keys():
    files = test_folders[folder_name]
    print(f"  📁 {folder_name}/ ({len(files)} files)")
    for filename, _ in files:
        print(f"     - {filename}")

print(f"\n📤 To test multiple folder upload:")
print(f"1. Go to http://localhost:8000")
print(f"2. Click '📁 Upload Folders'")
print(f"3. Select all 3 folders: banking_app, loan_app, hr_app")
print(f"4. Click '🚀 Analyze Files'")
print(f"\n✨ Expected result on /multi-analysis page:")
print(f"   - 3 Applications Analyzed (not 1)")
print(f"   - Each folder shown in separate section:")
print(f"     • banking_app → Core Banking (60-80% confidence)")
print(f"     • loan_app → Loan Management (70-85% confidence)")
print(f"     • hr_app → HR/Payroll (60-80% confidence)")
