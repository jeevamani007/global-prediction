-- Query: Monthly Transaction Summary per Account
-- Purpose: Aggregate transaction amounts by type for each account.
-- Relationships: accounts -> transactions

SELECT 
    a.account_number,
    t.transaction_type,
    COUNT(*) as transaction_count,
    SUM(t.amount) as total_amount
FROM 
    accounts a
JOIN 
    transactions t ON a.account_number = t.account_number
GROUP BY 
    a.account_number, t.transaction_type
HAVING 
    COUNT(*) > 1;
