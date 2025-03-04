CREATE SCHEMA IF NOT EXISTS psychobunny_db;
USE psychobunny_db;

CREATE ROLE admin;
CREATE ROLE data_engineer;
CREATE ROLE analyst;

-- Grant permissions on customers table
GRANT ALL PRIVILEGES ON psychobunny_db.customers TO admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON psychobunny_db.customers TO data_engineer;
GRANT SELECT ON psychobunny_db.customers TO analyst;

-- Grant permissions on transactions table
GRANT ALL PRIVILEGES ON psychobunny_db.transactions TO admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON psychobunny_db.transactions TO data_engineer;
GRANT SELECT ON psychobunny_db.transactions TO analyst;