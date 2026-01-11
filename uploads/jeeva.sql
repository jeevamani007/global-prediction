CREATE TABLE bank_transactions (
    account_number      VARCHAR(16),
    customer_id         VARCHAR(20),
    customer_name       VARCHAR(100),
    transaction_id      VARCHAR(30),
    transaction_type    VARCHAR(10), -- DEBIT / CREDIT
    debit               DECIMAL(12,2),
    credit              DECIMAL(12,2),
    opening_balance     DECIMAL(12,2),
    closing_balance     DECIMAL(12,2),
    transaction_date    DATE,
    branch_code         VARCHAR(10),
    account_type        VARCHAR(20),
    account_status      VARCHAR(15)
);


INSERT INTO bank_transactions VALUES
('123456789012','CUST1001','Ravi Kumar','TXN001','CREDIT',0,5000,10000,15000,'2025-01-01','BR001','Savings','ACTIVE'),
('123456789012','CUST1001','Ravi Kumar','TXN002','DEBIT',2000,0,15000,13000,'2025-01-02','BR001','Savings','ACTIVE'),
('123456789012','CUST1001','Ravi Kumar','TXN003','DEBIT',3000,0,13000,10000,'2025-01-03','BR001','Savings','ACTIVE'),

('987654321098','CUST1002','Anitha Raj','TXN004','CREDIT',0,10000,20000,30000,'2025-01-01','BR002','Current','ACTIVE'),
('987654321098','CUST1002','Anitha Raj','TXN005','DEBIT',5000,0,30000,25000,'2025-01-02','BR002','Current','ACTIVE'),

('555666777888','CUST1003','John Peter','TXN006','CREDIT',0,7000,5000,12000,'2025-01-01','BR003','Savings','ACTIVE'),
('555666777888','CUST1003','John Peter','TXN007','DEBIT',2000,0,12000,10000,'2025-01-02','BR003','Savings','ACTIVE'),

('444333222111','CUST1004','Sneha Devi','TXN008','CREDIT',0,15000,10000,25000,'2025-01-01','BR001','Salary','ACTIVE'),
('444333222111','CUST1004','Sneha Devi','TXN009','DEBIT',5000,0,25000,20000,'2025-01-02','BR001','Salary','ACTIVE'),
('444333222111','CUST1004','Sneha Devi','TXN010','DEBIT',3000,0,20000,17000,'2025-01-03','BR001','Salary','ACTIVE');
