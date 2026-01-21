"""
Diagnostic script to check the actual API response from /upload-folders
This will help us see exactly what data structure is being returned
"""
import requests
import json
from pathlib import Path

url = "http://localhost:8000/upload-folders"

# Use the actual test_nested_apps folder
test_folder = Path("test_nested_apps")

if not test_folder.exists():
    print(f"❌ Test folder not found: {test_folder}")
    print("Please create test_nested_apps folder with subfolders first")
    exit(1)

# Collect all CSV files from the test folder
files_to_upload = []
for csv_file in test_folder.rglob("*.csv"):
    # Get relative path from test_nested_apps
    relative_path = csv_file.relative_to(test_folder.parent)
    print(f"Found: {relative_path}")
    
    with open(csv_file, 'rb') as f:
        content = f.read()
        # Send with the full relative path (parent/subfolder/file.csv)
        files_to_upload.append(('files', (str(relative_path).replace('\\', '/'), content, 'text/csv')))

if not files_to_upload:
    print("❌ No CSV files found in test_nested_apps")
    exit(1)

print(f"\n📤 Uploading {len(files_to_upload)} files...")
print("="*60)

try:
    response = requests.post(url, files=files_to_upload)
    
    if response.status_code == 200:
        data = response.json()
        
        print("\n✅ API Response Received!")
        print("="*60)
        
        # Save full response to file for inspection
        with open("api_response_debug.json", "w") as f:
            json.dump(data, f, indent=2)
        print("📝 Full response saved to: api_response_debug.json\n")
        
        # Analyze the response structure
        print("📊 Response Structure Analysis:")
        print(f"  - multi_folder_mode: {data.get('multi_folder_mode')}")
        print(f"  - total_folders: {data.get('total_folders')}")
        print(f"  - Has 'folders' key: {('folders' in data)}")
        
        if 'folders' in data:
            folders = data['folders']
            print(f"  - Type of 'folders': {type(folders)}")
            print(f"  - Number of folders: {len(folders) if isinstance(folders, dict) else 'N/A'}")
            
            if isinstance(folders, dict):
                print(f"\n📂 Folder Keys: {list(folders.keys())}")
                print("\n" + "="*60)
                
                for idx, (folder_name, folder_data) in enumerate(folders.items(), 1):
                    print(f"\nFolder {idx}: {folder_name}")
                    print("-" * 40)
                    
                    # Check critical fields
                    has_app_type = 'application_type' in folder_data
                    has_csv_files = 'csv_files' in folder_data
                    has_business_rules = 'business_rules_summaries' in folder_data
                    has_app_purpose = 'application_purpose' in folder_data
                    
                    print(f"  ✓ application_type: {has_app_type}")
                    if has_app_type:
                        app_type = folder_data['application_type']
                        print(f"    - Type: {app_type.get('application_type', 'N/A')}")
                        print(f"    - Confidence: {app_type.get('confidence', 'N/A')}%")
                    
                    print(f"  ✓ csv_files: {has_csv_files}")
                    if has_csv_files:
                        print(f"    - Count: {len(folder_data['csv_files'])}")
                        print(f"    - Files: {folder_data['csv_files']}")
                    
                    print(f"  ✓ business_rules_summaries: {has_business_rules}")
                    if has_business_rules:
                        print(f"    - Files with rules: {len(folder_data['business_rules_summaries'])}")
                    
                    print(f"  ✓ application_purpose: {has_app_purpose}")
                    if has_app_purpose:
                        app_purpose = folder_data['application_purpose']
                        has_points = 'explanation_points' in app_purpose
                        print(f"    - Has explanation_points: {has_points}")
                        if has_points:
                            print(f"    - Points count: {len(app_purpose['explanation_points'])}")
            else:
                print(f"\n❌ ERROR: 'folders' is not a dict! Type: {type(folders)}")
        else:
            print("\n❌ ERROR: No 'folders' key in response!")
            print(f"Available keys: {list(data.keys())}")
        
        print("\n" + "="*60)
        print("💡 Next Steps:")
        print("  1. Check api_response_debug.json for full response")
        print("  2. Verify folder structure matches expected format")
        print("  3. Check server terminal logs for backend validation output")
        
    else:
        print(f"❌ API Error {response.status_code}")
        print(f"Response: {response.text[:500]}")

except Exception as e:
    print(f"❌ Exception: {e}")
    import traceback
    traceback.print_exc()
