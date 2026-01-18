-- Query: Find patterns of high-value transactions for customers
-- Purpose: Link Customers -> Accounts -> Transactions to find big spenders/depositors.
-- Relationships: customers.customer_id -> accounts.customer_id -> transactions.account_number

SELECT 
    c.first_name,
    c.last_name,
    t.transaction_type,
    t.amount,
    t.description,
    t.transaction_date
FROM 
    customers c
JOIN 
    accounts a ON c.customer_id = a.customer_id
JOIN 
    transactions t ON a.account_number = t.account_number
WHERE 
    t.amount > 5000.00
ORDER BY 
    t.amount DESC;
