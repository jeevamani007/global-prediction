"""
Test script to verify churn prediction business rules implementation
"""
from core_banking_business_rules_engine import CoreBankingBusinessRulesEngine
import pandas as pd

# Load test data
df = pd.read_csv('test_churn_data.csv')
print(f"Loaded test data: {len(df)} rows, {len(df.columns)} columns")
print(f"Columns: {list(df.columns)}")

# Create engine and analyze
engine = CoreBankingBusinessRulesEngine()
result = engine.analyze_dataset('test_churn_data.csv', df)

# Print results
print(f"\n=== ANALYSIS RESULTS ===")
print(f"Total columns analyzed: {result.get('total_columns', 0)}")
print(f"Total rows: {result.get('total_rows', 0)}")

print(f"\n=== COLUMN ANALYSIS ===")
for col_analysis in result.get('columns_analysis', [])[:5]:  # Show first 5
    col_name = col_analysis.get('column_name')
    identified_as = col_analysis.get('step3_identified_as')
    confidence = col_analysis.get('step4_confidence_score', 0)
    business_meaning = col_analysis.get('step5_business_meaning', 'N/A')
    
    print(f"\nColumn: {col_name}")
    print(f"  Identified as: {identified_as}")
    print(f"  Confidence: {confidence}%")
    print(f"  Business Meaning: {business_meaning[:100]}...")

print(f"\n✅ Test completed successfully!")
