"""
Test column normalization and matching for churn dataset
"""
from core_banking_business_rules_engine import CoreBankingBusinessRulesEngine
import pandas as pd

# Test column normalization
engine = CoreBankingBusinessRulesEngine()

test_columns = [
    "CustomerId",
    "CreditScore",  
    "NumOfProducts",
    "HasCrCard",
    "IsActiveMember",
    "EstimatedSalary",
    "Exited",
    "RowNumber"
]

print("=== COLUMN NORMALIZATION TEST ===\n")
for col in test_columns:
    normalized = engine._normalize_column_name(col)
    in_concepts = normalized in engine.banking_concepts
    print(f"{col:20} → {normalized:25} {'✅ FOUND' if in_concepts else '❌ MISSING'}")

# Load and analyze actual dataset
print("\n=== ANALYZING TEST CHURN DATA ===\n")
df = pd.read_csv('test_churn_data.csv')
result = engine.analyze_dataset('test_churn_data.csv', df)

print(f"Total columns analyzed: {result.get('total_columns', 0)}")
print(f"Total rows: {result.get('total_rows', 0)}\n")

print("=== COLUMN MATCHING RESULTS ===\n")
for col_analysis in result.get('columns_analysis', []):
    col_name = col_analysis.get('column_name')
    identified_as = col_analysis.get('step3_identified_as')
    confidence = col_analysis.get('step4_confidence_score', 0)
    
    # Check if identified correctly
    status = "✅" if identified_as and identified_as != "unknown" else "❌"
    
    print(f"{status} {col_name:20} → {identified_as or 'NOT IDENTIFIED':25} ({confidence:.0f}% confidence)")

print("\n✅ Test completed!")
