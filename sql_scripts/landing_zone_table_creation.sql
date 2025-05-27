-- PostgreSQL: Query for landing zone schema creation in the postgres database
CREATE SCHEMA IF NOT EXISTS landing_zone;

-- PostgreSQL: Query for customer table creation in the landing zone schema of postgres database
CREATE TABLE IF NOT EXISTS landing_zone.customer (
    id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    gender CHAR(20),
    billing_address VARCHAR(255),
    shipping_address VARCHAR(255),
	extracted_at TIMESTAMP,
);

-- PostgreSQL: Query for salesorder table creation in the landing zone schema of postgres database
CREATE TABLE IF NOT EXISTS landing_zone.salesorder (
    id INT,
    customer_id INT,
    order_number VARCHAR(100),
    created_at VARCHAR(50),
    modified_at VARCHAR(50),
    order_total DECIMAL(10, 2),
    total_qty_ordered INT,
	extracted_at TIMESTAMP,
);

-- PostgreSQL: Query for salesorderitem table creation in the landing zone schema of postgres database
CREATE TABLE IF NOT EXISTS landing_zone.salesorderitem (
    item_id INT,
    order_id INT,
    product_id INT,
    product_sku VARCHAR(100),
    product_name VARCHAR(255),
    qty_ordered INT,
    price DECIMAL(10, 2),
    line_total DECIMAL(10, 2),
    created_at VARCHAR(50),
    modified_at VARCHAR(50),
	extracted_at TIMESTAMP
);
