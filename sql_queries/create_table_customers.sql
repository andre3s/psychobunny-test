DROP TABLE IF EXISTS psychobunny_db.customers;
CREATE TABLE IF NOT EXISTS psychobunny_db.customers (
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    company_name VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(255),
    county VARCHAR(255) NULL,
    state VARCHAR(255) NULL,
    postal VARCHAR(50) NULL,  -- Unified column for zip/postal/post
    phone1 VARCHAR(255),
    phone2 VARCHAR(255),
    email VARCHAR(255),
    web VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);