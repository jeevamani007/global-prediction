-- Table structure for table `branches`
CREATE TABLE IF NOT EXISTS branches (
    branch_id VARCHAR(10) PRIMARY KEY,
    branch_name VARCHAR(100),
    branch_code VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    zip_code VARCHAR(20),
    country VARCHAR(50),
    manager_name VARCHAR(100),
    contact_number VARCHAR(20)
);

-- Dumping data for table `branches`
INSERT INTO branches (branch_id, branch_name, branch_code, address, city, state, zip_code, country, manager_name, contact_number) VALUES
('BR001', 'Downtown Main', 'BNK-DT-001', '100 Main St', 'New York', 'NY', '10001', 'USA', 'Alice Williams', '212-555-0100'),
('BR002', 'Westside', 'BNK-WS-002', '200 Market St', 'Los Angeles', 'CA', '90001', 'USA', 'Bob Johnson', '310-555-0200'),
('BR003', 'Lakeshore', 'BNK-LS-003', '300 Lake Dr', 'Chicago', 'IL', '60601', 'USA', 'Charlie Brown', '312-555-0300'),
('BR004', 'Southern Hub', 'BNK-SH-004', '400 River Rd', 'Houston', 'TX', '77001', 'USA', 'Diana Prince', '713-555-0400'),
('BR005', 'Mountain View', 'BNK-MV-005', '500 High St', 'Denver', 'CO', '80201', 'USA', 'Evan Wright', '303-555-0500');
