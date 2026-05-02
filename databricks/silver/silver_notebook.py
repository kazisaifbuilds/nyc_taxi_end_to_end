# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Reading

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing Libraries
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Reading CSV data**

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Function for reading the data

# COMMAND ----------

def read_data(path):
    return (spark.read.format("csv")
                     .option("header", "true")
                     .option("inferSchema", "true")
                     .load(path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trip Type Data

# COMMAND ----------

df_trip_type_data = read_data("abfss://bronze@nyctaxidatalakesk.dfs.core.windows.net/trip_type")
display(df_trip_type_data)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Trip Zone
# MAGIC

# COMMAND ----------

df_trip_zone = read_data("abfss://bronze@nyctaxidatalakesk.dfs.core.windows.net/trip_zone")
display(df_trip_zone)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trip Data

# COMMAND ----------

myschema = '''
VendorID BIGINT,
lpep_pickup_datetime TIMESTAMP,
lpep_dropoff_datetime TIMESTAMP,
store_and_fwd_flag STRING,
RatecodeID BIGINT,
PULocationID BIGINT,
DOLocationID BIGINT,
passenger_count BIGINT,
trip_distance DOUBLE,
fare_amount DOUBLE,
extra DOUBLE,
mta_tax DOUBLE,
tip_amount DOUBLE,
tolls_amount DOUBLE,
ehail_fee DOUBLE,
improvement_surcharge DOUBLE,
total_amount DOUBLE,
payment_type BIGINT,
trip_type BIGINT,
congestion_surcharge DOUBLE
'''

# COMMAND ----------

df_trip_data =(
               spark.read.format("parquet")
                          .schema(myschema)
                          .option("recursiveFileLookup", "true")
                          .load("abfss://bronze@nyctaxidatalakesk.dfs.core.windows.net/trips2023data")
)
display(df_trip_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC **Taxi Trip Type**

# COMMAND ----------

df_trip_type_data =df_trip_type_data.withColumnRenamed("description","trip_type_desc")
display(df_trip_type_data)

# COMMAND ----------

(df_trip_type_data.write.format("parquet")
                        .mode("append")
                        .option("path","abfss://silver@nyctaxidatalakesk.dfs.core.windows.net/trip_type")
                        .save())

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Zone**

# COMMAND ----------

df_trip_zone = (
    df_trip_zone
    .withColumn("Zone1", expr("CASE WHEN Zone LIKE '%/%' THEN split(Zone, '/')[0] ELSE Zone END"))
    .withColumn("Zone2", expr("CASE WHEN Zone LIKE '%/%' THEN split(Zone, '/')[1] ELSE NULL END"))
)

display(df_trip_zone)



# COMMAND ----------

(df_trip_zone.write.format("parquet")
                        .mode("append")
                        .option("path","abfss://silver@nyctaxidatalakesk.dfs.core.windows.net/trip_zone")
                        .save())

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Data**

# COMMAND ----------

df_trip_data.display()


# COMMAND ----------

df_trip_data = df_trip_data.withColumn("trip_date",to_date(col("lpep_pickup_datetime")))\
                           .withColumn("trip_year",year(col("lpep_pickup_datetime")))\
                           .withColumn("trip_month",month(col("lpep_pickup_datetime")))


# COMMAND ----------

df_trip_data.display()

# COMMAND ----------

df_trip_data=df_trip_data.select("vendorid","pulocationid","dolocationid","fare_amount","total_amount")

# COMMAND ----------

df_trip_data.limit(10).display()

# COMMAND ----------

(df_trip_data.write.format("parquet")
                        .mode("append")
                        .option("path","abfss://silver@nyctaxidatalakesk.dfs.core.windows.net/trips2023data")
                        .save())