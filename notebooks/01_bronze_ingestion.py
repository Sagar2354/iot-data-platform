# Databricks notebook source
# MAGIC %md
# MAGIC BRONZE_LAYER_Ingestion

# COMMAND ----------

df = spark.read.json("file:/Workspace/Users/vidyasagar2354@gmail.com/iot_data.json")

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("iot_data_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC The ingestion failed since the column humidity has inconsistent datatypes (string, double)
# MAGIC
# MAGIC FIX:
# MAGIC Cast properly

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.read.json("file:/Workspace/Users/vidyasagar2354@gmail.com/iot_data.json")

df = df.withColumn("humidity", col("humidity").cast("double")) \
       .withColumn("temperature", col("temperature").cast("double")) \
       .withColumn("pressure", col("pressure").cast("double"))

df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("iot_data_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🚨 Issue: Data Type Inconsistency Causing Pipeline Failure
# MAGIC
# MAGIC While loading raw IoT JSON data into the Bronze layer, the pipeline failed due to inconsistent data types in the `humidity` column.
# MAGIC
# MAGIC #### 🔍 Root Cause
# MAGIC
# MAGIC The dataset contained mixed data types:
# MAGIC
# MAGIC * Numeric values (e.g., `45.2`)
# MAGIC * String values (e.g., `"high"`)
# MAGIC
# MAGIC When attempting to cast the column to `DOUBLE` using `.cast("double")`, Spark (running in ANSI mode) threw a `CAST_INVALID_INPUT` error for malformed values like `"high"`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛠️ Resolution: Safe Type Casting with `try_cast`
# MAGIC
# MAGIC To handle malformed data without breaking the pipeline, `try_cast` was used instead of `cast`.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import expr
# MAGIC
# MAGIC df = df.withColumn("humidity", expr("try_cast(humidity as double)"))
# MAGIC ```
# MAGIC
# MAGIC #### ✅ Behavior of `try_cast`:
# MAGIC
# MAGIC * Valid numeric values → Successfully cast to `DOUBLE`
# MAGIC * Invalid values (e.g., `"high"`) → Converted to `NULL` instead of causing failure
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📈 Impact
# MAGIC
# MAGIC * Prevented pipeline failure due to bad data
# MAGIC * Enabled ingestion of semi-structured, inconsistent IoT data
# MAGIC * Preserved data integrity while allowing downstream cleaning in Silver layer
# MAGIC * Allowed tracking of malformed records via NULL counts for data quality metrics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧠 Key Learning
# MAGIC
# MAGIC In production pipelines, always use **safe casting mechanisms** (like `try_cast`) when dealing with semi-structured or untrusted data sources to ensure robustness and fault tolerance.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import expr

df = spark.read.json("file:/Workspace/Users/vidyasagar2354@gmail.com/iot_data.json")

#Safe casting
df = df.withColumn("humidity", expr("try_cast(humidity as double)")) \
       .withColumn("temperature", expr("try_cast(temperature as double)")) \
       .withColumn("pressure", expr("try_cast(pressure as double)"))

df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("iot_data_bronze")

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.sql("SELECT * FROM iot_data_bronze")

# COMMAND ----------

total = df.count()
print("Total records:", total)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we know there are underlying issues. 
# MAGIC Lets identify data issues

# COMMAND ----------

bad_humidity = df.filter(col("humidity").isNull()).count()
print("Bad humidity:", bad_humidity)


# COMMAND ----------

bad_temp = df.filter(col("temperature").isNull()).count()
print("Bad temperature:", bad_temp)

# COMMAND ----------

outliers = df.filter((col("temperature") > 150) | (col("temperature") < -50)).count()
print("Outliers:", outliers)

# COMMAND ----------

duplicates = df.count() - df.dropDuplicates(["device_id", "timestamp"]).count()
print("Duplicates:", duplicates)

# COMMAND ----------

bad_total = bad_humidity + bad_temp + outliers + duplicates

# COMMAND ----------

bad_percentage = (bad_total / total) * 100
print("Bad data %:", bad_percentage)

# COMMAND ----------

from pyspark.sql.functions import col

bad_df = df.filter(
    (col("humidity").isNull()) |
    (col("temperature").isNull()) |
    (col("temperature") > 150) |
    (col("temperature") < -50)
)

# COMMAND ----------

bad_unique = bad_df.dropDuplicates(["device_id", "timestamp"]).count()
print("Unique bad records:", bad_unique)

# COMMAND ----------

total = df.dropDuplicates(["device_id", "timestamp"]).count()

# COMMAND ----------

bad_percentage = (bad_unique / total) * 100
print("Actual bad data %:", bad_percentage)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Data Quality Measurement Correction
# MAGIC
# MAGIC #### 🚨 Initial Observation
# MAGIC
# MAGIC Initial calculation showed **~93% bad data**, which appeared unrealistically high.
# MAGIC
# MAGIC #### 🔍 Issue in Calculation
# MAGIC
# MAGIC The original approach summed individual issue counts:
# MAGIC
# MAGIC ```python
# MAGIC bad_total = bad_humidity + bad_temp + outliers + duplicates
# MAGIC ```
# MAGIC
# MAGIC This led to **double counting**, because the same record could fall into multiple categories:
# MAGIC
# MAGIC * A record with `"humidity": "high"` → becomes NULL after casting
# MAGIC * The same record could also have:
# MAGIC
# MAGIC   * NULL temperature
# MAGIC   * Outlier temperature
# MAGIC   * Duplicate entry
# MAGIC
# MAGIC 👉 Result: **One record counted multiple times**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧠 Root Cause
# MAGIC
# MAGIC The metric was inflated due to:
# MAGIC
# MAGIC * Overlapping data quality issues
# MAGIC * Lack of deduplication before aggregation
# MAGIC * Counting issues instead of **unique affected records**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛠️ Corrected Approach
# MAGIC
# MAGIC To fix this, we:
# MAGIC
# MAGIC 1. Identified bad records using combined conditions:
# MAGIC
# MAGIC ```python
# MAGIC bad_df = df.filter(
# MAGIC     (col("humidity").isNull()) |
# MAGIC     (col("temperature").isNull()) |
# MAGIC     (col("temperature") > 150) |
# MAGIC     (col("temperature") < -50)
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC 2. Counted **unique bad records**:
# MAGIC
# MAGIC ```python
# MAGIC bad_unique = bad_df.dropDuplicates(["device_id", "timestamp"]).count()
# MAGIC ```
# MAGIC
# MAGIC 3. Calculated total unique records:
# MAGIC
# MAGIC ```python
# MAGIC total = df.dropDuplicates(["device_id", "timestamp"]).count()
# MAGIC ```
# MAGIC
# MAGIC 4. Computed actual percentage:
# MAGIC
# MAGIC ```python
# MAGIC bad_percentage = (bad_unique / total) * 100
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📈 Final Result
# MAGIC
# MAGIC * Initial (incorrect): **~93% bad data**
# MAGIC * Corrected (accurate): **~40% bad data**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧠 Key Learning
# MAGIC
# MAGIC When measuring data quality:
# MAGIC
# MAGIC * Always account for **overlapping conditions**
# MAGIC * Use **deduplication** to avoid double counting
# MAGIC * Measure **unique impacted records**, not total issue occurrences
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Insight
# MAGIC
# MAGIC Accurate metric calculation is critical in data engineering, as incorrect aggregation can significantly misrepresent system health and lead to poor decision-making.
# MAGIC

# COMMAND ----------

