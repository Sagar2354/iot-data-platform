# Databricks notebook source
df = spark.sql("SELECT * FROM iot_data_bronze")

# COMMAND ----------

from pyspark.sql.functions import col, lower, when

df_silver = df

# 1. Normalize status
df_silver = df_silver.withColumn("status", lower(col("status")))

# 2. Remove NULL critical values
df_silver = df_silver.filter(
    col("humidity").isNotNull() &
    col("temperature").isNotNull()
)

# 3. Remove outliers
df_silver = df_silver.filter(
    (col("temperature") >= -50) &
    (col("temperature") <= 150)
)

# 4. Deduplicate
df_silver = df_silver.dropDuplicates(["device_id", "timestamp"])

# COMMAND ----------

df_silver = df_silver.withColumn(
    "temp_category",
    when(col("temperature") < 30, "LOW")
    .when((col("temperature") >= 30) & (col("temperature") < 70), "MEDIUM")
    .otherwise("HIGH")
)

# COMMAND ----------

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("iot_data_silver")

# COMMAND ----------

df_bronze = spark.sql("SELECT * FROM iot_data_bronze")
total_before = df_bronze.dropDuplicates(["device_id", "timestamp"]).count()

# COMMAND ----------

total_after = df_silver.count()

# COMMAND ----------

cleaned_percentage = ((total_before - total_after) / total_before) * 100

print("Records before:", total_before)
print("Records after:", total_after)
print("Data cleaned %:", cleaned_percentage)

# COMMAND ----------

spark.sql("SHOW TABLES").show()

# COMMAND ----------

spark.sql("DESCRIBE HISTORY iot_data_bronze").show(truncate=False)

# COMMAND ----------

spark.sql("DESCRIBE HISTORY iot_data_gold").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC We now evaluate the performance

# COMMAND ----------

import time

start = time.time()

df_test = spark.sql("SELECT * FROM iot_data_bronze")
df_test.groupBy("device_id").count().show()

end = time.time()

baseline_time = end - start
print("Baseline time:", baseline_time)

# COMMAND ----------

import time

start = time.time()

df_test = spark.sql("SELECT * FROM iot_data_bronze")
df_test.groupBy("device_id").count().show()

end = time.time()

baseline_time = end - start
print("Baseline time:", baseline_time)

# COMMAND ----------

import time

start = time.time()

df_opt = spark.sql("SELECT * FROM iot_data_bronze") \
              .repartition(8, "device_id")

df_opt.groupBy("device_id").count().show()

end = time.time()

optimized_time = end - start
print("Optimized time:", optimized_time)

# COMMAND ----------

import time

start = time.time()

df_opt = spark.sql("SELECT * FROM iot_data_bronze") \
              .repartition(8, "device_id")

df_opt.groupBy("device_id").count().show()

end = time.time()

optimized_time = end - start
print("Optimized time:", optimized_time)

# COMMAND ----------

import time

start = time.time()

df_test = spark.sql("SELECT * FROM iot_data_bronze")

df_test.groupBy("device_id", "status") \
       .agg({"temperature": "avg", "humidity": "avg"}) \
       .show()

end = time.time()

baseline_time = end - start
print("Baseline time:", baseline_time)

# COMMAND ----------

import time

start = time.time()

df_test = spark.sql("SELECT * FROM iot_data_bronze")

df_join = df_test.alias("a").join(
    df_test.alias("b"),
    on="device_id"
)

df_join.groupBy("device_id").count().show()

end = time.time()

baseline_join_time = end - start
print("Baseline JOIN time:", baseline_join_time)

# COMMAND ----------

import time

start = time.time()

df_test = spark.sql("SELECT * FROM iot_data_bronze") \
              .repartition(8, "device_id")

df_join = df_test.alias("a").join(
    df_test.alias("b"),
    on="device_id"
)

df_join.groupBy("device_id").count().show()

end = time.time()

optimized_join_time = end - start
print("Optimized JOIN time:", optimized_join_time)

# COMMAND ----------

improvement = ((baseline_join_time - optimized_join_time) / baseline_join_time) * 100
print("Improvement %:", improvement)

# COMMAND ----------

# MAGIC %md
# MAGIC We are now merging the new batch into silver

# COMMAND ----------

df_source = spark.sql("SELECT * FROM iot_data_bronze")

df_source.createOrReplaceTempView("iot_source")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO iot_data_silver AS target
# MAGIC USING iot_source AS source
# MAGIC ON target.device_id = source.device_id 
# MAGIC AND target.timestamp = source.timestamp
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   target.device_id = source.device_id,
# MAGIC   target.timestamp = source.timestamp,
# MAGIC   target.temperature = source.temperature,
# MAGIC   target.humidity = source.humidity,
# MAGIC   target.pressure = source.pressure,
# MAGIC   target.status = source.status
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   device_id, timestamp, temperature, humidity, pressure, status
# MAGIC )
# MAGIC VALUES (
# MAGIC   source.device_id, source.timestamp, source.temperature,
# MAGIC   source.humidity, source.pressure, source.status
# MAGIC )

# COMMAND ----------

df_source = spark.sql("SELECT * FROM iot_data_bronze")

df_source_clean = df_source.dropDuplicates(["device_id", "timestamp"])

df_source_clean.createOrReplaceTempView("iot_source_clean")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO iot_data_silver AS target
# MAGIC USING iot_source_clean AS source
# MAGIC ON target.device_id = source.device_id 
# MAGIC AND target.timestamp = source.timestamp
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   target.device_id = source.device_id,
# MAGIC   target.timestamp = source.timestamp,
# MAGIC   target.temperature = source.temperature,
# MAGIC   target.humidity = source.humidity,
# MAGIC   target.pressure = source.pressure,
# MAGIC   target.status = source.status
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   device_id, timestamp, temperature, humidity, pressure, status
# MAGIC )
# MAGIC VALUES (
# MAGIC   source.device_id, source.timestamp, source.temperature,
# MAGIC   source.humidity, source.pressure, source.status
# MAGIC )

# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM iot_data_silver").show()

# COMMAND ----------

spark.sql("""
SELECT device_id, timestamp, COUNT(*) 
FROM iot_data_silver
GROUP BY device_id, timestamp
HAVING COUNT(*) > 1
""").show()

# COMMAND ----------

