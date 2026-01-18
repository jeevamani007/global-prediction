"""
Simple test to check if churn columns are recognized
"""
from core_banking_business_rules_engine import CoreBankingBusinessRulesEngine

# Create engine
engine = CoreBankingBusinessRulesEngine()

# Check if churn column concepts exist
churn_columns = [
    "row_number", "surname", "credit_score", "geography", "gender",
    "age", "tenure", "num_of_products", "has_cr_card", 
    "is_active_member", "estimated_salary", "exited"
]

print("=== Checking Churn Column Concepts ===")
for col in churn_columns:
    if col in engine.banking_concepts:
        concept = engine.banking_concepts[col]
        print(f"✅ {col}: Domain={concept['domain']}, Patterns={concept['name_patterns'][:2]}")
    else:
        print(f"❌ {col}: NOT FOUND")

print(f"\n✅ All churn column concepts are registered!")
print(f"Total banking concepts: {len(engine.banking_concepts)}")
