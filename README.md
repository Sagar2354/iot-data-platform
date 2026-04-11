🚀 IoT Data Engineering Pipeline (Azure Databricks)

---

📌 Overview

This project implements an end-to-end data engineering pipeline to process and analyze IoT sensor data using Azure Databricks + Delta Lake.

It follows the Medallion Architecture (Bronze → Silver → Gold) to transform messy, real-world data into analytics-ready datasets.

---

🧠 Problem Statement

Modern IoT systems generate large volumes of semi-structured JSON data that contain:

❌ Missing values
❌ Incorrect data types
❌ Duplicate records
❌ Outliers and inconsistent readings
❌ Schema drift

👉 These issues break downstream analytics and reporting pipelines.

---

🏗️ Architecture

![architecture](architecture.jpeg)

The pipeline is built using a layered Medallion Architecture:

IoT JSON Data
      ↓
Bronze Layer (Raw)
      ↓
Silver Layer (Cleaned & Validated)
      ↓
Gold Layer (Aggregated KPIs)
      ↓
Dashboard (Insights)

"Architecture Diagram" (architecture/architecture.jpeg)

---

⚙️ Technologies Used

- Azure Databricks
- PySpark
- Delta Lake
- SQL

---

🔄 Data Pipeline Flow

Raw JSON → Bronze → Silver → Gold → Dashboard

---

⚡ Implementation Details

🟤 Bronze Layer (Raw Ingestion)

- Ingested ~120K IoT records
- Preserved raw structure for traceability
- Included:
  - Null values
  - Wrong data types
  - Duplicate records

---

⚪ Silver Layer (Data Cleaning & Processing)

- Applied data validation and cleaning
- Handled incorrect types using safe casting ("try_cast")
- Removed duplicates using:
  device_id + timestamp
- Filtered null and invalid records
- Removed outliers
- Implemented incremental pipeline using Delta MERGE (UPSERT)

---

🟢 Gold Layer (Business Aggregation)

Created analytics-ready datasets:

- 📊 Average temperature per device
- 📈 Total readings
- ⚡ Active vs inactive sensors
- ⏱️ Time-series trends

---

🚀 Key Features

✔ Processed 120K+ IoT records
✔ Reduced data quality issues significantly
✔ Implemented incremental data pipeline (MERGE)
✔ Handled schema evolution using "overwriteSchema"
✔ Built datasets ready for visualization

---

📈 Results

- Improved overall data quality through cleaning and validation
- Eliminated duplicate records
- Enabled reliable incremental data ingestion
- Generated structured datasets for analytics and dashboards

---

📊 Dashboard & Outputs

🟡 Gold Layer Aggregation

![gold_table](gold_table.jpeg)

---

🔵 Active vs Inactive Devices

![Sensor_ActiveInactive](Sensor_ActiveInactive_Pie-1.png)

---

🟢 Temperature Trends

![Sensor_AvgTemp](Sensor_AvgTemp_LineDotted-1.png)

---

🧠 Learnings

- Handling schema drift in semi-structured data
- Designing scalable pipelines using Medallion Architecture
- Implementing incremental processing with Delta Lake
- Managing real-world data quality issues

---

🔮 Future Improvements

- Add real-time streaming (Kafka / Event Hub)
- Integrate orchestration (Airflow / Azure Data Factory)
- Connect to BI tools (Power BI / Tableau)

---

⭐ Final Outcome

Messy IoT Data → Clean Pipeline → Business Insights

---

🔗 Author

VS Vidyasagar
Associate Data Engineer