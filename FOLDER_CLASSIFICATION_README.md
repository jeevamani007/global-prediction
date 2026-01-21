# Folder-Based Application Classification System

## Quick Start Guide

### 1. Start the Server

```bash
cd C:\Users\jeeva\llm_work\global-prediction
python main.py
```

Server will start on `http://localhost:8000`

### 2. Upload Folders via API

**File Naming Convention**: Use folder structure in filenames: `folder_name/file.csv`

**Example with cURL**:
```bash
curl -X POST http://localhost:8000/upload-folders \
  -F "files=@core_banking/accounts.csv" \
  -F "files=@core_banking/transactions.csv" \
  -F "files=@core_banking/customers.csv" \
  -F "files=@loan_app/loans.csv" \
  -F "files=@loan_app/emi_schedule.csv"
```

**Example with Python**:
```python
import requests

files = [
    ('files', ('core_banking/accounts.csv', open('path/to/accounts.csv', 'rb'))),
    ('files', ('core_banking/transactions.csv', open('path/to/transactions.csv', 'rb'))),
    ('files', ('loan_app/loans.csv', open('path/to/loans.csv', 'rb')))
]

response = requests.post('http://localhost:8000/upload-folders', files=files)
print(response.json())
```

### 3. View Results

**Option A: View JSON Response**
```json
{
  "multi_folder_mode": true,
  "total_folders": 2,
  "folders": {
    "core_banking": {
      "application_type": {
        "application_type": "Core Banking",
        "confidence": 92
      },
      "csv_files": [...],
      "file_roles": {...},
      "file_relationships": [...],
      "business_rules": {...}
    }
  }
}
```

**Option B: View in UI**
1. Open `http://localhost:8000/templates/multi_application.html`
2. In browser console, paste the API response:
   ```javascript
   sessionStorage.setItem('multi_folder_analysis', JSON.stringify(response));
   location.reload();
   ```

### 4. Run Demo

**Quick Demo** (creates sample data and analyzes):
```bash
python demo_folder_classification.py
```

This will:
- Create 3 test folders (Core Banking, Loan Management, Payments)
- Analyze each folder independently
- Detect application types
- Show file roles and relationships
- Generate `sample_multi_folder_response.json`

## What Gets Detected

### Application Types (10+)
- ✅ Core Banking
- ✅ Loan Management
- ✅ Payments
- ✅ Deposits
- ✅ Cards
- ✅ KYC
- ✅ CRM
- ✅ Insurance
- ✅ Trading
- ✅ HR/Payroll

### File Roles
- **Master Data**: customer.csv, accounts.csv, products.csv
- **Transaction Data**: transactions.csv, payments.csv, orders.csv
- **Reference Data**: branch_codes.csv, status_lookup.csv
- **Mapping Data**: account_customer_map.csv
- **History Data**: account_history.csv, audit_log.csv

### Relationships
- One-to-Many (Master-Detail)
- Many-to-One (Detail-Master)
- Many-to-Many
- Shared Reference

## Response Structure

```json
{
  "message": "Folders analyzed successfully",
  "multi_folder_mode": true,
  "total_folders": 2,
  "folders": {
    "folder_name": {
      "folder_name": "core_banking",
      "application_type": {
        "application_type": "Core Banking",
        "confidence": 92,
        "confidence_level": "High",
        "patterns_detected": ["Account Number", "Customer Id", "Balance"],
        "evidence": {
          "column_matches": 15,
          "relationship_matches": 3
        }
      },
      "csv_files": ["accounts.csv", "transactions.csv"],
      "file_roles": {
        "accounts.csv": "Master Data",
        "transactions.csv": "Transaction Data"
      },
      "file_relationships": [
        {
          "file1": "accounts.csv",
          "file2": "transactions.csv",
          "column": "account_number",
          "relationship_type": "One-to-Many",
          "overlap_percentage": 100
        }
      ],
      "business_rules": { /* existing framework */ },
      "application_purpose": { /* generated explanation */ },
      "total_files": 2,
      "total_rows": 150,
      "total_columns": 15,
      "issues": [],
      "warnings": [],
      "status": "success"
    }
  }
}
```

## Key Files

| File | Purpose |
|------|---------|
| `application_type_detector.py` | Detects application types with confidence scoring |
| `folder_based_application_analyzer.py` | Analyzes folder structure, files, and relationships |
| `main.py` | New `/upload-folders` endpoint |
| `templates/multi_application.html` | UI for displaying results |
| `demo_folder_classification.py` | Demo script |
| `test_folder_classification.py` | Test script |

## Examples

See `sample_multi_folder_response.json` for a complete API response example.

Run `python demo_folder_classification.py` to see live analysis output.
