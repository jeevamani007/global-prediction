-- CREATE TABLE
CREATE TABLE banking_transactions (
    account_number VARCHAR(16) NOT NULL,
    customer_id VARCHAR(10) NOT NULL,
    customer_name VARCHAR(50),
    account_type VARCHAR(20),
    account_status VARCHAR(10),
    branch_code VARCHAR(10),
    ifsc_code VARCHAR(11),
    transaction_id VARCHAR(15) NOT NULL,
    transaction_date DATE,
    transaction_type VARCHAR(10),
    debit DECIMAL(10,2),
    credit DECIMAL(10,2),
    opening_balance DECIMAL(10,2),
    closing_balance DECIMAL(10,2),
    phone VARCHAR(15)
);

-- INSERT SAMPLE DATA (50 rows)
INSERT INTO banking_transactions 
(account_number, customer_id, customer_name, account_type, account_status, branch_code, ifsc_code, transaction_id, transaction_date, transaction_type, debit, credit, opening_balance, closing_balance, phone)
VALUES
('123456789001','CUST1001','Alice Kumar','Savings','ACTIVE','BR001','IFSC0000001','TXN0001','2024-01-01','DEBIT',500.00,0.00,10000.00,9500.00,'9876543210'),
('123456789001','CUST1001','Alice Kumar','Savings','ACTIVE','BR001','IFSC0000001','TXN0002','2024-01-05','CREDIT',0.00,2000.00,9500.00,11500.00,'9876543210'),
('123456789002','CUST1002','Bob Sharma','Current','ACTIVE','BR002','IFSC0000002','TXN0003','2024-01-02','DEBIT',300.00,0.00,5000.00,4700.00,'9876543211'),
('123456789002','CUST1002','Bob Sharma','Current','ACTIVE','BR002','IFSC0000002','TXN0004','2024-01-06','CREDIT',0.00,1000.00,4700.00,5700.00,'9876543211'),
('123456789003','CUST1003','Carol Singh','Savings','ACTIVE','BR003','IFSC0000003','TXN0005','2024-01-03','DEBIT',200.00,0.00,8000.00,7800.00,'9876543212'),
('123456789003','CUST1003','Carol Singh','Savings','ACTIVE','BR003','IFSC0000003','TXN0006','2024-01-07','CREDIT',0.00,500.00,7800.00,8300.00,'9876543212'),
('123456789004','CUST1004','David Patel','Current','ACTIVE','BR004','IFSC0000004','TXN0007','2024-01-04','DEBIT',1000.00,0.00,12000.00,11000.00,'9876543213'),
('123456789004','CUST1004','David Patel','Current','ACTIVE','BR004','IFSC0000004','TXN0008','2024-01-08','CREDIT',0.00,1500.00,11000.00,12500.00,'9876543213'),
('123456789005','CUST1005','Eve Reddy','Savings','ACTIVE','BR005','IFSC0000005','TXN0009','2024-01-05','DEBIT',400.00,0.00,7000.00,6600.00,'9876543214'),
('123456789005','CUST1005','Eve Reddy','Savings','ACTIVE','BR005','IFSC0000005','TXN0010','2024-01-09','CREDIT',0.00,1000.00,6600.00,7600.00,'9876543214');

-- Repeat similar INSERT statements till 50 rows
