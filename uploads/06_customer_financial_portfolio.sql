-- Query: Customer 360 View (Accounts + Loans)
-- Purpose: Combine data from multiple tables to get a full financial picture of a customer.
-- Relationships: customers -> accounts, customers -> loans

SELECT 
    c.customer_id,
    c.first_name,
    COUNT(DISTINCT a.account_number) as num_accounts,
    SUM(DISTINCT a.balance) as total_account_balance,
    COUNT(DISTINCT l.loan_id) as num_loans,
    SUM(DISTINCT l.outstanding_balance) as total_loan_debt
FROM 
    customers c
LEFT JOIN 
    accounts a ON c.customer_id = a.customer_id
LEFT JOIN 
    loans l ON c.customer_id = l.customer_id
GROUP BY 
    c.customer_id, c.first_name;
