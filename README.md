# Project 2: Course Capstone
#### Due: Fri May 8, 2026 11:59pm

# AdventureWorks Retail Sales Data Lakehouse: Medallion Architecture

## Project Overview
Building upon the traditional batch ETL data mart designed in Project 1, this Capstone project modernizes the "Retail Sales" business process by migrating it to a distributed **Data Lakehouse** utilizing the **Medallion Architecture (Bronze, Silver, Gold)**. 

The goal of this project is to demonstrate the integration of static "cold-path" reference data with real-time "hot-path" streaming data using Apache Spark (PySpark) Structured Streaming.

This project was completed individually in accordance with the UVA Honor Policy.

## Architecture & Source Systems
To satisfy the multi-source data integration requirements, data was extracted from four distinct environments:
1. **Relational Database (MySQL):** Product catalog reference data (`dim_products_vw`) was extracted from the local transactional database.
2. **NoSQL Database (MongoDB Atlas):** Customer demographic profiles were fetched from a cloud-hosted document cluster.
3. **Local File System (CSV & JSON):** Temporal data (`dim_date.csv`) was used for the date dimension. To simulate a real-time event stream, the transactional fact data was partitioned into three distinct JSON files and fed into the pipeline chronologically.
4. **PySpark JVM:** A junk dimension (`dim_order_status`) was natively generated using Spark SQL to satisfy schema dimensionality requirements and optimize the streaming joins.

## Destination System & Deployment Strategy
* **Data Lakehouse:** The destination system is a PySpark-managed Data Lakehouse (`adventureworks_dlh`) running on a local Spark worker environment. 
* **Schema Design:** The system maintains a dimensional Star Schema, centered around a streaming Fact table.
* **Deployment:** Python (via Jupyter Notebooks) orchestrates the entire pipeline, utilizing `findspark` and `pyspark.sql` to manage the distributed compute clusters, stream processing, and DataFrame transformations.

## The Streaming Pipeline (Medallion Architecture)
The `Project 2 Capstone.ipynb` script executes the following continuous pipeline:

1. **Ingest Reference Data:** Batch dimension tables (Customers, Products, Dates, Statuses) are extracted, cleaned, assigned Surrogate Keys via Spark SQL Windowing functions, and saved as managed tables in the Lakehouse.
2. **Bronze Layer (Raw Ingestion):** The partitioned JSON files simulating the live sales stream are ingested via PySpark `readStream`. Traceability columns (`receipt_time` and `source_file`) are appended, and the raw data is written continuously to Parquet files.
3. **Silver Layer (Cleansing & Integration):** The Bronze stream is read and structurally joined with the static batch Dimension tables. Currency strings are cast to DoubleTypes using `regexp_replace`, and the cleansed, unified stream is output to the Silver directory.
4. **Gold Layer (Business Aggregation):** The Silver stream is consumed into memory. Using the HyperLogLog algorithm (`approx_count_distinct`) for safe, continuous memory execution, the data is aggregated to calculate the Total Orders and Total Revenue grouped by Customer City and Year. 

## Validation 
The final Gold table results successfully mirror the static batch results generated in Project 1. The minor accepted variance ( ~1% error margin) in the `TotalOrders` column effectively demonstrates the mathematical tradeoffs (HyperLogLog) required when executing continuous distinct counts over infinite streams in Big Data architectures.

## Repository Contents
* `Project 2 Capstone.ipynb`: The primary PySpark script handling all streaming, transformation, and Lakehouse management.
* `dim_date.csv`: The temporal dimension data.
* `adventureworks/streaming/sales_orders/`: The partitioned JSON files used to simulate the live sales transaction stream.
* `README.md`: Project documentation and architecture summary.
