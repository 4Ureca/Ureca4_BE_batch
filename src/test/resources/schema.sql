CREATE TABLE customers (
    identification_num VARCHAR(36) PRIMARY KEY,
    name VARCHAR(100),
    customer_type VARCHAR(20),
    gender VARCHAR(2),
    birth_date DATE,
    grade_code VARCHAR(20),
    preferred_contact VARCHAR(20),
    email VARCHAR(100),
    phone VARCHAR(20),
    created_at TIMESTAMP
);