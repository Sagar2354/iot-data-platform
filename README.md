# IoT Data Engineering Pipeline (Azure Databricks)

## Overview

This project implements an end-to-end data engineering pipeline to process and analyze IoT sensor data using Azure Databricks and Delta Lake. The pipeline follows the Medallion Architecture (Bronze → Silver → Gold) to ensure scalable, reliable, and high-quality data processing.

The system is designed to handle messy, real-world data and transform it into analytics-ready datasets for downstream reporting and visualization.

---

## Problem Statement

Modern IoT systems generate large volumes of semi-structured data (JSON) that often contain:

* Missing values
* Incorrect data types
* Duplicate records
* Outliers and inconsistent readings
* Schema drift

These issues impact data quality and can break downstream analytics pipelines.

This project addresses these challenges by building a robust pipeline that ingests, cleans, validates, and transforms raw data into structured, reliable datasets suitable for analysis and visualization.

---

## Architecture



The pipeline is built using a Medallion Architecture:

* **Bronze Layer**: Raw data ingestion from JSON source
* **Silver Layer**: Data cleaning, validation, deduplication, and transformation
* **Gold Layer**: Aggregated business metrics and analytics-ready tables

architecture/architecture.jpeg

---

## Technologies Used

* Azure Databricks
* PySpark
* Delta Lake
* SQL

---

## Data Pipeline Flow

Raw JSON → Bronze → Silver → Gold → Dashboard

---

## Implementation Details

### Bronze Layer

* Ingested raw IoT JSON data (~120K records)
* Preserved original structure for traceability
* Included malformed data (nulls, wrong types, duplicates)

### Silver Layer

* Applied data cleaning and validation
* Handled incorrect data types using safe casting
* Removed duplicate records using composite keys (device_id, timestamp)
* Filtered out null and invalid values
* Removed outliers from sensor readings
* Implemented incremental processing using Delta MERGE (UPSERT)

### Gold Layer

* Created aggregated datasets for analytics
* Computed device-level metrics:

  * Average temperature
  * Total readings
  * Active vs inactive counts
* Generated time-series data for trend analysis

---

## Key Features

* Processed over 120K IoT records
* Reduced malformed data significantly through validation and cleaning
* Implemented incremental data processing using Delta Lake
* Handled schema inconsistencies using schema overwrite techniques
* Built analytics-ready datasets for dashboarding

---

## Results

* Improved overall data quality through cleaning and validation
* Eliminated duplicate records
* Enabled reliable incremental data ingestion
* Produced structured datasets for reporting and analysis

---

## Dashboard & Outputs

### Gold Layer Aggregation

screenshots/gold_table.jpeg

### Active vs Inactive Devices

screenshots/Sensor_ActiveInactive_Pie.png

### Temperature Trends

screenshots/Sensor_ActiveInactive_Pie.png

---

## Learnings

* Handling schema drift and type inconsistencies in semi-structured data
* Designing scalable data pipelines using Medallion Architecture
* Implementing incremental data processing with Delta Lake
* Managing data quality issues in real-world datasets

---

## Future Improvements

* Add real-time streaming ingestion (Kafka or Event Hub)
* Integrate orchestration using Airflow or Azure Data Factory
* Connect to BI tools such as Power BI for advanced dashboards

---
