-- Table structure for table `customers`
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone_number VARCHAR(20),
    address TEXT,
    date_of_birth DATE,
    kyc_status VARCHAR(20), -- Verified, Pending, Rejected
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dumping data for table `customers`
INSERT INTO customers (customer_id, first_name, last_name, email, phone_number, address, date_of_birth, kyc_status, created_at) VALUES
(1001, 'John', 'Doe', 'john.doe@example.com', '555-0101', '123 Elm St, New York, NY', '1985-06-15', 'Verified', '2023-01-10 09:00:00'),
(1002, 'Jane', 'Smith', 'jane.smith@example.com', '555-0102', '456 Oak St, Los Angeles, CA', '1990-08-22', 'Verified', '2023-01-11 10:30:00'),
(1003, 'Robert', 'Brown', 'robert.b@example.com', '555-0103', '789 Pine St, Chicago, IL', '1978-03-12', 'Pending', '2023-02-01 11:15:00'),
(1004, 'Emily', 'Davis', 'emily.d@example.com', '555-0104', '321 Maple Ave, Houston, TX', '1995-11-30', 'Verified', '2023-02-15 14:20:00'),
(1005, 'Michael', 'Wilson', 'm.wilson@example.com', '555-0105', '654 Cedar Ln, Miami, FL', '1982-01-05', 'Verified', '2023-03-05 09:45:00'),
(1006, 'Sarah', 'Taylor', 'sarah.t@example.com', '555-0106', '987 Birch Rd, Seattle, WA', '1989-07-19', 'Verified', '2023-03-20 16:00:00'),
(1007, 'David', 'Anderson', 'david.a@example.com', '555-0107', '159 Spruce Blvd, Boston, MA', '1975-09-25', 'Rejected', '2023-04-01 10:00:00'),
(1008, 'Jennifer', 'Thomas', 'jen.thomas@example.com', '555-0108', '753 Willow Way, Denver, CO', '1992-05-14', 'Verified', '2023-04-12 13:30:00'),
(1009, 'James', 'Jackson', 'james.j@example.com', '555-0109', '951 Cherry Ct, Atlanta, GA', '1980-12-08', 'Verified', '2023-05-05 15:45:00'),
(1010, 'Linda', 'White', 'linda.white@example.com', '555-0110', '357 Aspen Dr, Phoenix, AZ', '1968-04-20', 'Verified', '2023-05-20 08:30:00');
