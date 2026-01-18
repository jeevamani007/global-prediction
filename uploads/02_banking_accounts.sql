-- Table structure for table `accounts`
CREATE TABLE IF NOT EXISTS accounts (
    account_number VARCHAR(20) PRIMARY KEY,
    customer_id INT,
    account_type VARCHAR(20), -- Savings, Checking, Salary
    currency VARCHAR(3) DEFAULT 'USD',
    balance DECIMAL(15, 2),
    status VARCHAR(20), -- Active, Dormant, Closed
    branch_id VARCHAR(10),
    opening_date DATE,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Dumping data for table `accounts`
INSERT INTO accounts (account_number, customer_id, account_type, currency, balance, status, branch_id, opening_date) VALUES
('ACC10001', 1001, 'Savings', 'USD', 5000.50, 'Active', 'BR001', '2023-01-12'),
('ACC10002', 1001, 'Checking', 'USD', 1250.00, 'Active', 'BR001', '2023-01-15'),
('ACC10003', 1002, 'Savings', 'USD', 8900.75, 'Active', 'BR002', '2023-01-12'),
('ACC10004', 1003, 'Salary', 'USD', 0.00, 'Pending', 'BR003', '2023-02-02'),
('ACC10005', 1004, 'Savings', 'USD', 12000.00, 'Active', 'BR001', '2023-02-16'),
('ACC10006', 1005, 'Checking', 'USD', 450.25, 'Active', 'BR004', '2023-03-06'),
('ACC10007', 1006, 'Savings', 'USD', 6700.00, 'Active', 'BR002', '2023-03-22'),
('ACC10008', 1008, 'Savings', 'USD', 3400.00, 'Active', 'BR005', '2023-04-14'),
('ACC10009', 1009, 'Checking', 'USD', 900.50, 'Active', 'BR003', '2023-05-06'),
('ACC10010', 1010, 'Savings', 'USD', 15000.00, 'Active', 'BR004', '2023-05-21'),
('ACC10011', 1002, 'Checking', 'USD', 200.00, 'Dormant', 'BR002', '2023-06-01'),
('ACC10012', 1005, 'Savings', 'EUR', 1000.00, 'Active', 'BR004', '2023-06-10');
