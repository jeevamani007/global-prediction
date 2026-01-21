"""
Test script to check folder upload API response structure
"""
import requests
import json
from pathlib import Path

# Test the /upload-folders endpoint with the bank folder
url = "http://localhost:8000/upload-folders"

# Get files from bank folder
bank_folder = Path("uploads/bank")
csv_files = list(bank_folder.glob("*.csv"))[:5]  # Test with first 5 files

print(f"Testing with {len(csv_files)} files from bank folder...")

# Prepare files for upload
files = []
for csv_file in csv_files:
    folder_name = "bank"
    filename = csv_file.name
    # Format: "folder_name/filename"
    with open(csv_file, 'rb') as f:
        content = f.read()
    # Create file with folder structure in name
    files.append(('files', (f"{folder_name}/{filename}", content, 'text/csv')))

print("\nSending request...")
response = requests.post(url, files=files)

if response.status_code == 200:
    print("✅ Success!\n")
    data = response.json()
    
    print(f"Response keys: {list(data.keys())}\n")
    
    if 'folders' in data:
        for folder_name, folder_data in data['folders'].items():
            print(f"\n📁 Folder: {folder_name}")
            print(f"   Keys: {list(folder_data.keys())}")
            
            if 'application_type' in folder_data:
                app_type = folder_data['application_type']
                print(f"\n   Application Type Data:")
                print(f"     Type: {app_type.get('application_type', 'N/A')}")
                print(f"     Confidence: {app_type.get('confidence', 'N/A')}")
                print(f"     Confidence Type: {type(app_type.get('confidence'))}")
                print(f"     All keys: {list(app_type.keys())}")
            
            if 'business_rules_summaries' in folder_data:
                print(f"\n   Business Rules Summaries: {list(folder_data['business_rules_summaries'].keys())}")
            
            if 'application_purpose' in folder_data:
                print(f"\n   Application Purpose: {folder_data['application_purpose'] is not None}")
    
    # Save full response for inspection
    with open('test_response.json', 'w') as f:
        json.dump(data, f, indent=2)
    print(f"\n💾 Full response saved to test_response.json")
    
else:
    print(f"❌ Error {response.status_code}")
    print(response.text)
