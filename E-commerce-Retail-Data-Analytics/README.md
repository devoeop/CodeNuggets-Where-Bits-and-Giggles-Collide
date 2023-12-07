# E-commerce-Retail-Data-Analytics
- End-to-end design and implementation of a comprehensive data platform for retail analytics in an e-commerce organization utilizing technologies such as MySQL for OLTP databases, PostgreSQL for staging data warehousing, and Db2 on Cloud for production data warehousing.
- Designed and deployed a star schema data warehouse named "softcart," incorporating dimension tables (e.g., softcartDimDate, softcartDimCategory, softcartDimCountry) and a fact table (softcartFactSales), enabling seamless reporting and analytics on yearly, monthly, weekly, and daily bases.
- Developed ETL pipelines to automate the extraction, transformation, and loading of incremental data from the OLTP database to the staging data warehouse, ensuring data accuracy and freshness for reporting purposes.
- Implemented a Business Intelligence dashboard using Cognos, featuring dynamic visualizations such as quarterly sales bar charts, electronic goods sales pie charts by category, and monthly total sales line charts for enhanced business insights.

- Databases: MySQL (OLTP), PostgreSQL (Staging), Db2 on Cloud (Production)
- ETL Tools: Shell scripting for data extraction, transformation, and loading; PgAdmin for schema design; Cron jobs for task scheduling
- Business Intelligence: Cognos for creating dynamic and interactive dashboards
- Data Warehousing: Designed star schema, implemented staging environment, and loaded transformed data into the data warehouse using CSV files.
