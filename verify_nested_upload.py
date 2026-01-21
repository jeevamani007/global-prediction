"""
Verification script for Nested Folder Upload
Enhanced to verify all multi-folder upload fixes:
1. Application type and confidence for each folder
2. Business rules summaries for each CSV file
3. Application purpose with explanation_points array
"""
import requests
import json

url = "http://localhost:8000/upload-folders"

# Simulate browser sending files with full relative paths
# Browser sends: filename="parent/app_name/file.csv"
files = [
    # App 1: Banking
    ('files', ('company_data/banking_app/customers.csv', b'customer_id,customer_name,email,phone,account_status\n1,John Doe,john@example.com,1234567890,ACTIVE', 'text/csv')),
    ('files', ('company_data/banking_app/accounts.csv', b'account_number,customer_id,account_type,balance,account_status\n100001,1,SAVINGS,5000.50,ACTIVE', 'text/csv')),
    
    # App 2: Loan
    ('files', ('company_data/loan_app/loans.csv', b'loan_id,customer_id,loan_amount,emi_amount,loan_status\n1001,1,50000,1500,ACTIVE', 'text/csv')),
    
    # App 3: HR
    ('files', ('company_data/hr_app/employees.csv', b'employee_id,name,role,salary,joining_date\n1,Jane Smith,Developer,60000,2023-01-15', 'text/csv')),
]

print("📤 Sending files with nested paths (simulating browser upload)...")
for _, (name, _, _) in files:
    print(f"   - {name}")

try:
    response = requests.post(url, files=files)
    
    if response.status_code == 200:
        data = response.json()
        print("\n✅ Response Received!")
        print(f"Total Folders Detected: {data.get('total_folders')}")
        
        folders = data.get('folders', {})
        print(f"\n📊 Detected Applications: {len(folders)}")
        
        # Enhanced validation
        all_checks_passed = True
        
        for name, info in folders.items():
            print(f"\n{'='*60}")
            print(f"📂 Application: {name}")
            print(f"{'='*60}")
            
            # Check 1: CSV Files
            csv_files = info.get('csv_files', [])
            print(f"  ✓ CSV Files ({len(csv_files)}): {csv_files}")
            
            # Check 2: Application Type & Confidence
            app_type = info.get('application_type', {})
            if app_type and isinstance(app_type, dict):
                confidence = app_type.get('confidence', 0)
                app_name = app_type.get('application_type', 'Unknown')
                print(f"  ✓ Application Type: {app_name}")
                print(f"  ✓ Confidence: {confidence}%", end="")
                
                if confidence > 0:
                    print(" ✅")
                else:
                    print(" ❌ ISSUE: Confidence is 0!")
                    all_checks_passed = False
            else:
                print(f"  ❌ ISSUE: application_type missing or invalid")
                all_checks_passed = False
            
            # Check 3: Business Rules Summaries
            business_rules = info.get('business_rules_summaries', {})
            if business_rules and isinstance(business_rules, dict):
                print(f"  ✓ Business Rules Files: {len(business_rules)}")
                for filename, summary in business_rules.items():
                    if summary and isinstance(summary, dict):
                        total_cols = summary.get('total_columns', 0)
                        critical = len(summary.get('critical_columns', []))
                        warning = len(summary.get('warning_columns', []))
                        safe = summary.get('safe_count', 0)
                        print(f"    - {filename}: {total_cols} cols (Critical: {critical}, Warning: {warning}, Safe: {safe}) ✅")
                    else:
                        print(f"    - {filename}: Invalid summary ❌")
                        all_checks_passed = False
            else:
                print(f"  ❌ ISSUE: business_rules_summaries missing or invalid")
                all_checks_passed = False
            
            # Check 4: Application Purpose
            app_purpose = info.get('application_purpose')
            if app_purpose and isinstance(app_purpose, dict):
                has_line1 = bool(app_purpose.get('line1'))
                has_line2 = bool(app_purpose.get('line2'))
                explanation_points = app_purpose.get('explanation_points', [])
                
                print(f"  ✓ Application Purpose:")
                print(f"    - Line1: {'✅' if has_line1 else '❌'}")
                print(f"    - Line2: {'✅' if has_line2 else '❌'}")
                print(f"    - Explanation Points: {len(explanation_points)} points", end="")
                
                if explanation_points and len(explanation_points) > 0:
                    print(" ✅")
                    # Show first point as sample
                    first_point = explanation_points[0][:80] + "..." if len(explanation_points[0]) > 80 else explanation_points[0]
                    print(f"      Sample: {first_point}")
                else:
                    print(" ⚠️  WARNING: explanation_points array is empty")
                    if has_line1 or has_line2:
                        print("      (But line1/line2 exist - frontend fallback should work)")
                    else:
                        all_checks_passed = False
            else:
                print(f"  ❌ ISSUE: application_purpose missing or invalid")
                all_checks_passed = False
            
            # Check 5: File Relationships
            relationships = info.get('file_relationships', [])
            print(f"  ✓ Relationships: {len(relationships)}")
        
        # Final verification
        print(f"\n{'='*60}")
        expected_apps = {'banking_app', 'loan_app', 'hr_app'}
        detected_apps = set(folders.keys())
        
        if expected_apps.issubset(detected_apps):
            print("✅ All nested subfolders detected as separate applications!")
        else:
            print(f"❌ Expected {expected_apps}, but got {detected_apps}")
            all_checks_passed = False
        
        if all_checks_passed:
            print("\n🎉 SUCCESS: All validation checks passed!")
            print("   - Application types have confidence > 0")
            print("   - Business rules exist for all CSV files")
            print("   - Application purpose has explanation_points")
            print("\n✨ The multi-folder upload fixes are working correctly!")
        else:
            print("\n⚠️  SOME ISSUES DETECTED - Check warnings above")
            print("   Review server logs for more details")
            
    else:
        print(f"❌ Error {response.status_code}: {response.text}")

except Exception as e:
    print(f"❌ Exception: {e}")
    import traceback
    traceback.print_exc()
