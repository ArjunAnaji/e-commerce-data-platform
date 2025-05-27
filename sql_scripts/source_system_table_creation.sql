-- MySQL: Switch to db database if not in this database already
USE db;

-- MySQL:  Query for customer table creation in the db database
CREATE TABLE customer (
    id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    gender CHAR(20),
    billing_address VARCHAR(255),
    shipping_address VARCHAR(255)
);

-- MySQL:  Query for salesorder table creation in the db database
CREATE TABLE salesorder (
    id INT,
    customer_id INT,
    order_number VARCHAR(100),
    created_at VARCHAR(50),
    modified_at VARCHAR(50),
    order_total DECIMAL(10, 2),
    total_qty_ordered INT
);

-- MySQL:  Query for salesorderitem table creation in the db database
CREATE TABLE salesorderitem (
    item_id INT,
    order_id INT,
    product_id INT,
    product_sku VARCHAR(100),
    product_name VARCHAR(255),
    qty_ordered INT,
    price DECIMAL(10, 2),
    line_total DECIMAL(10, 2),
    created_at VARCHAR(50),
    modified_at VARCHAR(50)
);


-- For customer.id
CREATE INDEX idx_customer_id ON customer(id);

-- For salesorder.id
CREATE INDEX idx_salesorder_id ON salesorder(id);

-- For salesorderitem.item_id
CREATE INDEX idx_salesorderitem_item_id ON salesorderitem(item_id);
