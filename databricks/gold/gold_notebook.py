# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Zone**

# COMMAND ----------

def read_data(path):
    return (spark.read.format("parquet")
                     .option("header", "true")
                     .option("inferSchema", "true")
                     .load(path))

# COMMAND ----------

df_zone=read_data("abfss://silver@nyctaxidatalakesk.dfs.core.windows.net/trip_zone")
display(df_zone)

# COMMAND ----------

(
    df_zone.write.format("delta").option("path","abfss://gold@nyctaxidatalakesk.dfs.core.windows.net/trip_zone")
                  .mode("append").saveAsTable("nyc_taxi.gold.gold_trip_zone")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Type**

# COMMAND ----------

df_trip_type=read_data("abfss://silver@nyctaxidatalakesk.dfs.core.windows.net/trip_type")
display(df_trip_type)

# COMMAND ----------

(
    df_trip_type.write.format("delta").option("path","abfss://gold@nyctaxidatalakesk.dfs.core.windows.net/trip_type")
                  .mode("append").saveAsTable("nyc_taxi.gold.gold_trip_type")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Trips Data**

# COMMAND ----------

df_trips_data=read_data("abfss://silver@nyctaxidatalakesk.dfs.core.windows.net/trips2023data")
display(df_trips_data)

# COMMAND ----------

(
    df_trips_data.write.format("delta").option("path","abfss://gold@nyctaxidatalakesk.dfs.core.windows.net/trips_data")
                  .mode("append").saveAsTable("nyc_taxi.gold.gold_trips_data")
)