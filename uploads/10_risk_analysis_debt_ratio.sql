-- Query: Risk Analysis - Customers with Low Balance but High Loan Debt
-- Purpose: Identify risky customers who owe more than they have in deposit.
-- Relationships: customers -> accounts, customers -> loans

SELECT 
    c.customer_id,
    c.last_name,
    SUM(a.balance) as total_deposits,
    SUM(l.outstanding_balance) as total_debt,
    (SUM(l.outstanding_balance) - SUM(a.balance)) as net_deficit
FROM 
    customers c
JOIN 
    accounts a ON c.customer_id = a.customer_id
JOIN 
    loans l ON c.customer_id = l.customer_id
GROUP BY 
    c.customer_id, c.last_name
HAVING 
    total_debt > total_deposits;
