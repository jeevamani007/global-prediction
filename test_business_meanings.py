"""
Simple test to check what business meanings are defined for churn columns
"""
from core_banking_business_rules_engine import CoreBankingBusinessRulesEngine

engine = CoreBankingBusinessRulesEngine()

# Check what's defined for each churn column
churn_mappings = {
    "customer_id": "CustomerId",
    "surname": "Surname", 
    "credit_score": "CreditScore",
    "geography": "Geography",
    "gender": "Gender",
    "age": "Age",
    "tenure": "Tenure",
    "current_balance": "Balance",
    "num_of_products": "NumOfProducts",
    "has_cr_card": "HasCrCard",
    "is_active_member": "IsActiveMember",
    "estimated_salary": "EstimatedSalary",
    "exited": "Exited"
}

print("CHECKING BUSINESS RULES FOR CHURN COLUMNS\n")
for concept_key, csv_column in churn_mappings.items():
    if concept_key in engine.banking_concepts:
        concept = engine.banking_concepts[concept_key]
        business_meaning = concept.get("business_rules", {}).get("reason", "NOT DEFINED")
        domain = concept.get("domain", "UNKNOWN")
        print(f"{csv_column:20} -> {concept_key:25} | Domain: {domain:20} | {business_meaning[:80]}")
    else:
        print(f"{csv_column:20} -> {concept_key:25} | NOT FOUND IN CONCEPTS")

print("\nDone!")
