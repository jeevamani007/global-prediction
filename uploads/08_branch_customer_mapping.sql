-- Query: Branch Performance Report
-- Purpose: List all branches and the customers associated with them via accounts.
-- Relationships: branches -> accounts -> customers

SELECT 
    b.branch_name,
    b.state,
    c.first_name || ' ' || c.last_name as customer_name,
    a.account_type,
    a.opening_date
FROM 
    branches b
JOIN 
    accounts a ON b.branch_id = a.branch_id
JOIN 
    customers c ON a.customer_id = c.customer_id
ORDER BY 
    b.branch_name, a.opening_date;
