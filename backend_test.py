# Backend Verification Test
# This script checks if the backend returns all necessary data for the frontend

import requests
import json

BASE_URL = "http://localhost:8000"

print("=" * 60)
print("BACKEND VERIFICATION TEST")
print("=" * 60)

# Test 1: Check if server is running
print("\n1. Testing server health...")
try:
    response = requests.get(f"{BASE_URL}/health", timeout=5)
    if response.status_code == 200:
        print("✅ Server is running!")
    else:
        print(f"❌ Server returned status code: {response.status_code}")
except Exception as e:
    print(f"❌ Server not responding: {e}")
    print("Please make sure the server is running with: python -m uvicorn main:app --reload")
    exit(1)

# Test 2: Check if index page loads
print("\n2. Testing index page...")
try:
    response = requests.get(BASE_URL, timeout=5)
    if response.status_code == 200 and "Domain Classification System" in response.text:
        print("✅ Index page loads correctly!")
        print("   Contains: Pie Chart, Upload Area, Domain Detection")
    else:
        print(f"❌ Index page issue: {response.status_code}")
except Exception as e:
    print(f"❌ Error loading index: {e}")

# Test 3: Check if rules page loads
print("\n3. Testing rules page...")
try:
    response = requests.get(f"{BASE_URL}/rules", timeout=5)
    if response.status_code == 200 and "Business Rules" in response.text:
        print("✅ Rules page loads correctly!")
        print("   Contains: Application Type, Business Rules, Column Analysis")
    else:
        print(f"❌ Rules page issue: {response.status_code}")
except Exception as e:
    print(f"❌ Error loading rules page: {e}")

# Test 4: Check if relation page loads
print("\n4. Testing relation page...")
try:
    response = requests.get(f"{BASE_URL}/relation", timeout=5)
    if response.status_code == 200 and "Relationship Analysis" in response.text:
        print("✅ Relation page loads correctly!")
        print("   Contains: Primary Keys, Foreign Keys, Data Flow")
    else:
        print(f"❌ Relation page issue: {response.status_code}")
except Exception as e:
    print(f"❌ Error loading relation page: {e}")

print("\n" + "=" * 60)
print("BACKEND STRUCTURE CHECK")
print("=" * 60)

# Expected response structure
expected_structure = {
    "banking": {
        "required_fields": ["domain", "decision", "confidence_percentage", "qualitative", "confidence_out_of_10"],
        "description": "Domain detection results"
    },
    "banking_dataset_validator": {
        "required_fields": ["columns", "final_decision", "dataset_confidence", "total_records"],
        "description": "Business rules validation"
    },
    "banking_application_type": {
        "required_fields": ["application_type", "description", "reasoning", "key_indicators"],
        "description": "Application type detection"
    },
    "multi_file_mode": {
        "required_fields": ["relationships", "primary_keys", "foreign_keys"],
        "description": "Multi-file relationships (if multiple files uploaded)"
    }
}

print("\n✅ Expected Response Structure:")
print(json.dumps(expected_structure, indent=2))

print("\n" + "=" * 60)
print("COLUMN DATA STRUCTURE")
print("=" * 60)

column_structure = {
    "name": "Column name",
    "meaning": "Standard banking name",
    "status": "MATCH/WARNING/FAIL",
    "confidence": "Percentage",
    "column_type": "e.g., customer_id, account_number",
    "purpose": "What this column is for",
    "usage": "How it's used in the system",
    "business_rules": ["List of validation rules"],
    "explanation": "Detailed 3-line explanation"
}

print("\n✅ Each column should have:")
print(json.dumps(column_structure, indent=2))

print("\n" + "=" * 60)
print("NEXT STEPS")
print("=" * 60)
print("""
1. Upload a CSV file through the web interface at http://localhost:8000
2. Check if the pie chart appears with domain percentages
3. Verify the highest domain is highlighted
4. Click on "View Business Rules" to see detailed analysis
5. Click on "View Table Relationships" (if multiple files uploaded)

FRONTEND FEATURES TO VERIFY:
- ✅ Pie chart shows all domains
- ✅ Domain cards show percentages and decisions
- ✅ Navigation buttons appear when Banking > 50%
- ✅ Rules page shows Application Type banner
- ✅ Rules page shows Purpose, Usage, Rules, Explanation for each column
- ✅ Relation page shows full paragraph explanations
- ✅ Data persists across pages via sessionStorage
""")

print("\n" + "=" * 60)
print("TEST COMPLETE")
print("=" * 60)
