CREATE TABLE customers (
    customer_id VARCHAR(10) PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    phone VARCHAR(15) UNIQUE,
    email VARCHAR(100),
    city VARCHAR(50)
);

INSERT INTO customers VALUES
('CUST001','Ravi Kumar','9876543210','ravi@gmail.com','Chennai'),
('CUST002','Anitha Rao','9876543211','anitha@gmail.com','Bangalore'),
('CUST003','John Paul','9876543212','john@gmail.com','Coimbatore'),
('CUST004','Priya Sharma','9876543213','priya@gmail.com','Delhi'),
('CUST005','Arun Mehta','9876543214','arun@gmail.com','Mumbai'),
('CUST006','Kavya Iyer','9876543215','kavya@gmail.com','Chennai'),
('CUST007','Manoj Singh','9876543216','manoj@gmail.com','Patna'),
('CUST008','Divya Nair','9876543217','divya@gmail.com','Kochi'),
('CUST009','Suresh Babu','9876543218','suresh@gmail.com','Madurai'),
('CUST010','Neha Gupta','9876543219','neha@gmail.com','Jaipur');



CREATE TABLE transactions (
    transaction_id VARCHAR(10) PRIMARY KEY,
    account_number VARCHAR(10),
    customer_id VARCHAR(10),
    transaction_date DATE,
    transaction_type VARCHAR(10),
    debit DECIMAL(10,2),
    credit DECIMAL(10,2),

    FOREIGN KEY (account_number)
    REFERENCES accounts(account_number),

    FOREIGN KEY (customer_id)
    REFERENCES customers(customer_id)
);

INSERT INTO transactions VALUES
('TXN001','ACC1001','CUST001','2026-01-01','Debit',500,0),
('TXN002','ACC1002','CUST002','2026-01-02','Credit',0,2000),
('TXN003','ACC1003','CUST003','2026-01-03','Debit',1000,0),
('TXN004','ACC1004','CUST004','2026-01-04','Credit',0,500),
('TXN005','ACC1005','CUST005','2026-01-05','Debit',200,0),
('TXN006','ACC1006','CUST006','2026-01-06','Credit',0,1000),
('TXN007','ACC1007','CUST007','2026-01-07','Debit',1500,0),
('TXN008','ACC1008','CUST008','2026-01-08','Credit',0,200),
('TXN009','ACC1009','CUST009','2026-01-09','Debit',300,0),
('TXN010','ACC1010','CUST010','2026-01-10','Credit',0,700);




CREATE TABLE accounts (
    account_number VARCHAR(10) PRIMARY KEY,
    customer_id VARCHAR(10),
    account_type VARCHAR(20),
    account_status VARCHAR(20),
    opening_balance DECIMAL(10,2),

    FOREIGN KEY (customer_id)
    REFERENCES customers(customer_id)
);

INSERT INTO accounts VALUES
('ACC1001','CUST001','Savings','Active',10000),
('ACC1002','CUST002','Current','Active',5000),
('ACC1003','CUST003','Savings','Active',15000),
('ACC1004','CUST004','Current','Inactive',3000),
('ACC1005','CUST005','Savings','Active',8000),
('ACC1006','CUST006','Savings','Active',12000),
('ACC1007','CUST007','Current','Active',6000),
('ACC1008','CUST008','Savings','Inactive',4000),
('ACC1009','CUST009','Savings','Active',9000),
('ACC1010','CUST010','Current','Active',7000);
