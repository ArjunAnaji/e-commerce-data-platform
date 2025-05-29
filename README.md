# E-Commerce Sales Data Management Platform
End-to-End ETL Pipelines for E-Commerce Sales Data using Python, Airflow, MySQL PostgreSQL and Docker

This project demonstrates the design and implementation of a complete data platform for your-luxury-goods.com, an e-commerce company. The solution extracts transactional sales data from a MySQL OLTP system, loads it into a PostgreSQL-based Data Warehouse, and performs data quality checks, cleansing, normalization, and transformation to power analytics-ready Data Marts.

The project is structured across three layers:

Landing Zone: Incremental extraction of raw data from MySQL

Operational Data Store (ODS): Cleansed, validated, and normalized data

Data Mart: Flattened, business-ready dataset for reporting and analytics

Data Analysis: Sales, product, customer and pricing data analytics on the flattened table

<img width="962" alt="e-commerce-data-flow-architecture" src="https://github.com/user-attachments/assets/7315ede0-948e-4fe8-b7e8-14395c66b704" />

Tools & Tech: Airflow (v1.10.9), MySQL, PostgreSQL, Docker, Pandas, SQLAlchemy
