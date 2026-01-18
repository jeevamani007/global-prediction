-- Query: List all loans associated with customers
-- Purpose: View active loan details for customers.
-- Relationships: customers.customer_id -> loans.customer_id

SELECT 
    c.first_name,
    c.last_name,
    c.kyc_status,
    l.loan_id,
    l.loan_type,
    l.principal_amount,
    l.outstanding_balance,
    l.end_date
FROM 
    customers c
JOIN 
    loans l ON c.customer_id = l.customer_id
WHERE 
    l.status = 'Active';
