
# 🛒 Retail Sales Data Engineering Project

## 📌 Project Overview
This project demonstrates an end-to-end data engineering workflow using retail sales data. The goal is to clean raw data, validate records, transform datasets, and prepare reliable data for business analysis.

## 🎯 Objectives
- Ingest raw retail sales data.
- Clean and validate data using Python and PySpark.
- Implement Medallion Architecture.
- Build analytical datasets using SQL.
- Perform data quality checks.

## 🏗️ Architecture

**Bronze → Silver → Gold**

- **Bronze Layer:** Store raw sales data.
- **Silver Layer:** Clean data, handle null values, and remove duplicates.
- **Gold Layer:** Create aggregated sales tables for reporting.

## 🛠️ Technologies
- Python
- SQL
- PySpark
- Databricks
- Delta Lake
- Medallion Architecture

## 🔄 Project Workflow
1. Load retail sales CSV data.
2. Validate the schema and inspect records.
3. Clean and transform the data.
4. Store processed data in Silver tables.
5. Create Gold tables for sales analysis.
6. Run data quality checks and SQL queries.

## 📊 Planned Business Analysis
- Total sales revenue
- Sales by product category
- Monthly sales trends
- Top-performing products
- Regional sales performance

## 📁 Project Structure

```text
retail-sales-data-engineering/
├── README.md
├── data/
├── notebooks/
└── sql/

## 🛠️ Technologies Used

- Python
- PySpark
- SQL
- Azure Databricks
- Delta Lake
- Power BI
- GitHub

## 📂 Project Structure

retail-sales-data-engineering/
│
├── notebooks/
│   ├── 01_Bronze_Ingestion.py
│   ├── 02_Silver_Transformation.py
│   └── 03_Gold_Analytics.py
│
├── data/
├── README.md
└── requirements.txt

## ⚙️ ETL Workflow

1. Extract raw retail sales data.
2. Load data into Bronze Layer.
3. Clean and transform data in Silver Layer.
4. Create aggregated datasets in Gold Layer.
5. Connect Gold data to Power BI for reporting.

## 🔍 Data Quality Checks

- Null value validation
- Duplicate record checks
- Schema validation
- Data consistency checks

## 📊 Business Insights

- Total Sales
- Product-wise Sales
- Customer-wise Sales
- Sales Trends
- Category-wise Performance

## 👨‍💻 Author
Dhanraj Jadhav

Aspiring Data Engineer | Python | SQL | PySpark | Databricks

GitHub: https://github.com/Dhanraj8767

