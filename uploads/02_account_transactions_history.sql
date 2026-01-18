-- Query: Retrieve transaction history for accounts
-- Purpose: Link transactions to their respective accounts.
-- Relationships: accounts.account_number -> transactions.account_number

SELECT 
    a.account_number,
    a.account_type,
    t.transaction_id,
    t.transaction_type,
    t.amount,
    t.transaction_date,
    t.description
FROM 
    accounts a
JOIN 
    transactions t ON a.account_number = t.account_number
WHERE 
    a.status = 'Active'
ORDER BY 
    t.transaction_date DESC;
