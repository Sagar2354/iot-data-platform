# Databricks notebook source
df_new = spark.read.json("file:/Workspace/Users/vidyasagar2354@gmail.com/iot_data.json")

df_new.count()

# COMMAND ----------

from pyspark.sql.functions import expr

df_new = spark.read.json("file:/Workspace/Users/vidyasagar2354@gmail.com/iot_data.json")

df_new = df_new.withColumn("humidity", expr("try_cast(humidity as double)")) \
               .withColumn("temperature", expr("try_cast(temperature as double)")) \
               .withColumn("pressure", expr("try_cast(pressure as double)"))

# COMMAND ----------

df_new.write.format("delta") \
    .mode("append") \
    .saveAsTable("iot_data_bronze")

# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM iot_data_bronze").show()

# COMMAND ----------

