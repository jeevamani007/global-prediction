-- Query: Find customers with no active accounts (Potential Churn)
-- Purpose: Identify customers who exist in the system but have no accounts or only closed accounts.
-- Relationships: customers.customer_id -> accounts.customer_id (LEFT JOIN with IS NULL check)

SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.created_at
FROM 
    customers c
LEFT JOIN 
    accounts a ON c.customer_id = a.customer_id AND a.status = 'Active'
WHERE 
    a.account_number IS NULL;
