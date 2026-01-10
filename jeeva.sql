CREATE TABLE credit_cards (
    card_id INT PRIMARY KEY,
    customer_id INT,
    card_number VARCHAR(20),
    card_type VARCHAR(20),
    credit_limit DECIMAL(10,2),
    outstanding_amount DECIMAL(10,2),
    card_status VARCHAR(20)
);

INSERT INTO credit_cards VALUES
(301, 1, '4111-xxxx-1001', 'Visa', 100000.00, 25000.00, 'Active'),
(302, 2, '4111-xxxx-1002', 'MasterCard', 150000.00, 40000.00, 'Active'),
(303, 3, '4111-xxxx-1003', 'Visa', 50000.00, 10000.00, 'Blocked'),
(304, 4, '4111-xxxx-1004', 'RuPay', 75000.00, 0.00, 'Active'),
(305, 5, '4111-xxxx-1005', 'Visa', 200000.00, 180000.00, 'Overdue');
