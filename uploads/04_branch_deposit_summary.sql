-- Query: Aggregate account balances by Branch
-- Purpose: Analyze total deposits per branch.
-- Relationships: branches.branch_id -> accounts.branch_id

SELECT 
    b.branch_name,
    b.city,
    COUNT(a.account_number) as total_accounts,
    SUM(a.balance) as total_deposits
FROM 
    branches b
JOIN 
    accounts a ON b.branch_id = a.branch_id
GROUP BY 
    b.branch_name, b.city
ORDER BY 
    total_deposits DESC;
