
CREATE TABLE customers (
    customer_id VARCHAR(10) PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL
);

CREATE TABLE transactions (
    transaction_id VARCHAR(10) PRIMARY KEY,
    customer_id VARCHAR(10),
    amount DECIMAL(10,2),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
