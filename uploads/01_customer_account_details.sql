-- Query: Get all accounts for each customer using the Foreign Key relationship (customer_id)
-- Purpose: List full customer details alongside their account information.
-- Relationships: customers.customer_id -> accounts.customer_id

SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    a.account_number,
    a.account_type,
    a.balance,
    a.status
FROM 
    customers c
JOIN 
    accounts a ON c.customer_id = a.customer_id
ORDER BY 
    c.customer_id;
