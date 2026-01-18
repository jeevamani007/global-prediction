-- Table structure for table `loans`
CREATE TABLE IF NOT EXISTS loans (
    loan_id VARCHAR(20) PRIMARY KEY,
    customer_id INT,
    loan_type VARCHAR(50), -- Home Loan, Personal Loan, Auto Loan, Education Loan
    principal_amount DECIMAL(15, 2),
    interest_rate DECIMAL(5, 2),
    term_months INT,
    start_date DATE,
    end_date DATE,
    status VARCHAR(20), -- Active, Closed, Defaulted
    outstanding_balance DECIMAL(15, 2),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Dumping data for table `loans`
INSERT INTO loans (loan_id, customer_id, loan_type, principal_amount, interest_rate, term_months, start_date, end_date, status, outstanding_balance) VALUES
('LN20001', 1001, 'Home Loan', 250000.00, 3.50, 360, '2022-06-01', '2052-06-01', 'Active', 245000.00),
('LN20002', 1002, 'Auto Loan', 30000.00, 5.25, 60, '2023-02-15', '2028-02-15', 'Active', 28500.00),
('LN20003', 1004, 'Personal Loan', 10000.00, 8.90, 24, '2023-03-01', '2025-03-01', 'Active', 8500.00),
('LN20004', 1005, 'Education Loan', 40000.00, 4.00, 120, '2021-09-01', '2031-09-01', 'Active', 35000.00),
('LN20005', 1006, 'Home Loan', 500000.00, 3.25, 360, '2020-01-10', '2050-01-10', 'Active', 480000.00),
('LN20006', 1008, 'Personal Loan', 5000.00, 9.50, 12, '2022-12-01', '2023-12-01', 'Active', 2000.00),
('LN20007', 1010, 'Auto Loan', 45000.00, 5.00, 60, '2023-05-25', '2028-05-25', 'Active', 45000.00);
