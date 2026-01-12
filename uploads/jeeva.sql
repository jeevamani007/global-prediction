CREATE TABLE accounts (
    account_id SERIAL PRIMARY KEY,
    account_number VARCHAR(12) UNIQUE,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(15),
    account_type VARCHAR(20),
    branch VARCHAR(50),
    opening_date DATE,
    balance DECIMAL(12,2),
    status VARCHAR(20)
);

INSERT INTO accounts 
(account_number, customer_name, email, phone, account_type, branch, opening_date, balance, status)
VALUES
('123456789001','Ravi Kumar','ravi@gmail.com','9876543210','SAVINGS','Chennai','2022-01-10',25000,'ACTIVE'),
('123456789002','Anitha S','anitha@gmail.com','9876543211','CURRENT','Bangalore','2021-11-05',120000,'ACTIVE'),
('123456789003','Karthik R','karthik@gmail.com','9876543212','SAVINGS','Coimbatore','2023-03-15',18000,'ACTIVE'),
('123456789004','Divya M','divya@gmail.com','9876543213','SAVINGS','Madurai','2022-07-20',45000,'ACTIVE'),
('123456789005','Suresh P','suresh@gmail.com','9876543214','CURRENT','Trichy','2020-09-12',300000,'BLOCKED');
