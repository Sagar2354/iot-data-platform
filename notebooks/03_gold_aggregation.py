# Databricks notebook source
df_gold = spark.sql("""
SELECT 
    device_id,

    COUNT(*) as total_readings,

    ROUND(AVG(temperature), 2) as avg_temperature,
    MAX(temperature) as max_temperature,
    MIN(temperature) as min_temperature,

    ROUND(AVG(humidity), 2) as avg_humidity,

    SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) as active_count,
    SUM(CASE WHEN status = 'inactive' THEN 1 ELSE 0 END) as inactive_count,

    ROUND(
        (SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) * 100.0) / COUNT(*),
        2
    ) as active_percentage

FROM iot_data_silver
GROUP BY device_id
""")

# COMMAND ----------

df_gold.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("iot_data_gold")

# COMMAND ----------

spark.sql("SELECT * FROM iot_data_gold").show()

# COMMAND ----------

# MAGIC %md
# MAGIC time based - aggreation

# COMMAND ----------

df_gold_time = spark.sql("""
SELECT 
    DATE(timestamp) as date,
    device_id,
    COUNT(*) as readings,
    ROUND(AVG(temperature), 2) as avg_temp
FROM iot_data_silver
GROUP BY DATE(timestamp), device_id
""")

df_gold_time.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("iot_data_gold_time")

# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM iot_data_gold").show()
spark.sql("SELECT COUNT(*) FROM iot_data_gold_time").show()

# COMMAND ----------

spark.sql("SELECT DISTINCT DATE(timestamp) FROM iot_data_silver").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Lets perform some visualizations

# COMMAND ----------

df = spark.sql("""
SELECT device_id, avg_temperature
FROM iot_data_gold
ORDER BY avg_temperature DESC
""")

display(df)

# COMMAND ----------

spark.sql("DESCRIBE iot_data_gold").show()

# COMMAND ----------

df_gold = spark.sql("""
SELECT 
    device_id,

    COUNT(*) as total_readings,

    ROUND(AVG(temperature), 2) as avg_temperature,
    MAX(temperature) as max_temperature,
    MIN(temperature) as min_temperature,

    ROUND(AVG(humidity), 2) as avg_humidity,

    SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) as active_count,
    SUM(CASE WHEN status = 'inactive' THEN 1 ELSE 0 END) as inactive_count,

    ROUND(
        (SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) * 100.0) / COUNT(*),
        2
    ) as active_percentage

FROM iot_data_silver
GROUP BY device_id
""")

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("iot_data_gold")

# COMMAND ----------

spark.sql("DESCRIBE iot_data_gold").show()

# COMMAND ----------

df = spark.sql("""
SELECT 'active' as status, SUM(active_count) as count
FROM iot_data_gold

UNION ALL

SELECT 'inactive' as status, SUM(inactive_count) as count
FROM iot_data_gold
""")

display(df)

# COMMAND ----------

df = spark.sql("""
SELECT 'active' as status, SUM(active_count) as count
FROM iot_data_gold

UNION ALL

SELECT 'inactive' as status, SUM(inactive_count) as count
FROM iot_data_gold
""")

display(df)

# COMMAND ----------

df = spark.sql("""
SELECT date, device_id, avg_temp
FROM iot_data_gold_time
""")
display(df)

# COMMAND ----------

