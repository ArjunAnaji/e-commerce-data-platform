-- PostgreSQL: Query for data mart schema creation in the postgres database
CREATE SCHEMA IF NOT EXISTS data_mart;

-- PostgreSQL: Query for sales_order_item_flat table creation in the data mart schema of postgres database
CREATE TABLE IF NOT EXISTS data_mart.sales_order_item_flat (
    item_id INT NOT NULL,
    order_id INT NOT NULL,
    order_number VARCHAR(100) NOT NULL,
    order_created_at TIMESTAMP NOT NULL,
    order_total DOUBLE PRECISION NOT NULL,
    total_qty_ordered INT NOT NULL,
    customer_id INT NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    customer_gender VARCHAR(10),
    customer_email VARCHAR(100) NOT NULL,
    product_id INT NOT NULL,
    product_sku VARCHAR(100) NOT NULL,
    product_name VARCHAR(255),
    item_price DOUBLE PRECISION NOT NULL,
    item_qty_order INT NOT NULL,
    item_unit_total DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (order_id, item_id)
);