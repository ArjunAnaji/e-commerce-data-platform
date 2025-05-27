-- PostgreSQL: Query for operational data store schema creation in the postgres database
CREATE SCHEMA IF NOT EXISTS operational_data_store;

-- PostgreSQL: Query for customer table creation in the operational data store schema of postgres database
CREATE TABLE IF NOT EXISTS operational_data_store.customer (
    id INTEGER PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL CHECK (email ~* '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'),
    gender VARCHAR(10) CHECK (gender IN ('Male', 'Female')),
    billing_address VARCHAR(255),
    shipping_address VARCHAR(255) NOT NULL,
	extracted_at TIMESTAMP
);

-- PostgreSQL: Query for salesorder table creation in the operational data store schema of postgres database
CREATE TABLE IF NOT EXISTS operational_data_store.salesorder (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES operational_data_store.customer(id),
    order_number VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL,
    modified_at TIMESTAMP NOT NULL,
    order_total DECIMAL(10,2) NOT NULL,
    total_qty_ordered INTEGER NOT NULL,
	extracted_at TIMESTAMP
);

-- PostgreSQL: Query for product table creation in the operational data store schema of postgres database
CREATE TABLE IF NOT EXISTS operational_data_store.product (
    product_id INTEGER PRIMARY KEY,
    product_sku VARCHAR(100) NOT NULL,
    product_name VARCHAR(255)
);

-- PostgreSQL: Query for salesorderitem table creation in the operational data store schema of postgres database
CREATE TABLE IF NOT EXISTS operational_data_store.salesorderitem (
    item_id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES operational_data_store.salesorder(id),
    product_id INTEGER NOT NULL REFERENCES operational_data_store.product(product_id),
    qty_ordered INTEGER NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    line_total DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    modified_at TIMESTAMP NOT NULL,
    extracted_at TIMESTAMP NOT NULL
);


