-- Table structure for table `transactions`
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(36) PRIMARY KEY,
    account_number VARCHAR(20),
    transaction_type VARCHAR(20), -- Deposit, Withdrawal, Transfer, Payment
    amount DECIMAL(15, 2),
    transaction_date TIMESTAMP,
    description VARCHAR(255),
    reference_id VARCHAR(50),
    FOREIGN KEY (account_number) REFERENCES accounts(account_number)
);

-- Dumping data for table `transactions`
INSERT INTO transactions (transaction_id, account_number, transaction_type, amount, transaction_date, description, reference_id) VALUES
('TXN1000001', 'ACC10001', 'Deposit', 1000.00, '2023-01-13 10:00:00', 'Initial Deposit', 'REF001'),
('TXN1000002', 'ACC10002', 'Deposit', 2000.00, '2023-01-16 09:30:00', 'Paycheck', 'REF002'),
('TXN1000003', 'ACC10001', 'Withdrawal', 200.00, '2023-01-20 14:15:00', 'ATM Withdrawal', 'REF003'),
('TXN1000004', 'ACC10003', 'Deposit', 5000.00, '2023-01-13 11:00:00', 'Transfer from external', 'REF004'),
('TXN1000005', 'ACC10002', 'Payment', 150.00, '2023-01-25 18:00:00', 'Utility Bill Payment', 'REF005'),
('TXN1000006', 'ACC10005', 'Deposit', 12000.00, '2023-02-17 09:00:00', 'Bonus Payout', 'REF006'),
('TXN1000007', 'ACC10004', 'Deposit', 10.00, '2023-02-03 10:00:00', 'Account Opening', 'REF007'),
('TXN1000008', 'ACC10001', 'Transfer', 500.00, '2023-02-10 12:30:00', 'Transfer to ACC10002', 'REF008'),
('TXN1000009', 'ACC10002', 'Deposit', 500.00, '2023-02-10 12:30:00', 'Transfer from ACC10001', 'REF008'),
('TXN1000010', 'ACC10006', 'Deposit', 500.00, '2023-03-07 10:00:00', 'Cash Deposit', 'REF009'),
('TXN1000011', 'ACC10007', 'Deposit', 7000.00, '2023-03-23 11:45:00', 'Investment Dividend', 'REF010'),
('TXN1000012', 'ACC10007', 'Withdrawal', 300.00, '2023-03-25 16:20:00', 'Grocery shopping', 'REF011'),
('TXN1000013', 'ACC10008', 'Deposit', 3400.00, '2023-04-15 09:15:00', 'Salary', 'REF012'),
('TXN1000014', 'ACC10009', 'Deposit', 1000.00, '2023-05-07 10:10:00', 'Part-time payment', 'REF013'),
('TXN1000015', 'ACC10010', 'Deposit', 15000.00, '2023-05-22 09:00:00', 'Inheritance', 'REF014'),
('TXN1000016', 'ACC10001', 'Withdrawal', 50.00, '2023-06-01 19:00:00', 'Dinner', 'REF015'),
('TXN1000017', 'ACC10002', 'Payment', 500.00, '2023-06-05 10:00:00', 'Rent Payment', 'REF016'),
('TXN1000018', 'ACC10005', 'Withdrawal', 1000.00, '2023-06-15 14:00:00', 'Vacation Fund', 'REF017'),
('TXN1000019', 'ACC10012', 'Deposit', 1000.00, '2023-06-10 11:00:00', 'Euro Deposit', 'REF018'),
('TXN1000020', 'ACC10003', 'Transfer', 100.00, '2023-06-20 15:30:00', 'Gift to friend', 'REF019');
