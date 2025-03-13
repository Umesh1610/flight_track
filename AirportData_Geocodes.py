# Databricks notebook source
# MAGIC %fs
# MAGIC ls mnt/flighttrack
# MAGIC

# COMMAND ----------


# NECESSARY LIBRARIES
import requests
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Define the schema
schema = StructType([
    StructField("icao", StringType(), True),
    StructField("iata", StringType(), True),
    StructField("airport", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("latitude_degrees", IntegerType(), True),
    StructField("latitude_minutes", IntegerType(), True),
    StructField("latitude_seconds", IntegerType(), True),
    StructField("latitude_direction", StringType(), True),
    StructField("longitude_degrees", IntegerType(), True),
    StructField("longitude_minutes", IntegerType(), True),
    StructField("longitude_seconds", IntegerType(), True),
    StructField("longitude_direction", StringType(), True),
    StructField("altitude", IntegerType(), True),
    StructField("latitude_decimal_degrees", DoubleType(), True),
    StructField("longitude_decimal_degrees", DoubleType(), True)
])

# Print schema
print(schema)

# COMMAND ----------

df = spark.read.option("delimiter", ":").csv("/mnt/flighttrack/geocode", schema=schema)


# COMMAND ----------

display(df)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS airport_db_raw")
spark.sql("CREATE DATABASE IF NOT EXISTS airport_db_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS airport_db_gold")


# COMMAND ----------

spark.sql("SHOW DATABASES").show()


# COMMAND ----------

# MAGIC %md
# MAGIC Bronze Table Creation

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("airport_db_raw.geocode_raw")

# COMMAND ----------

#df.write.format("delta").mode("overwrite").saveAsTable("airport_db_raw.geocode_raw")


# COMMAND ----------

# MAGIC %md
# MAGIC Silver Table creation

# COMMAND ----------

df_geo_raw = spark.sql("SELECT * FROM airport_db_raw.geocode_raw")

# COMMAND ----------

df_geo_raw = df_geo_raw.select(
    "icao",
    "iata",
    "airport",
    "city",
    "country",
    "latitude_decimal_degrees",
    "longitude_decimal_degrees"
    )
# CORRECT ENGLAND MISSPELLING
df_geo_raw = df_geo_raw.withColumn("country", when(col("country") == "ENGALND", "ENGLAND").otherwise(df_geo_raw.country))

# WRITE TO SILVER TABLE
df_geo_raw.write.mode("overwrite").saveAsTable("airport_db_silver.geocode_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC Check Silver table
# MAGIC
# MAGIC THIS CHECKS THE SILVER TABLE BEFORE PUSHING IT TO THE GOLD LEVEL TABLE BY PERFORMING UNIT TESTS. THIS ENSURES BAD DATA WILL BE KEPT OUT OF PRODUCTION AND ANY DATA VISUALIZATIONS.
# MAGIC

# COMMAND ----------

df_geo_silver = spark.sql("SELECT * FROM airport_db_silver.geocode_silver")

# CHECK THAT EVERY COLUMN IS THERE
airport_columns = ["icao", "iata", "airport", "city", "country", "latitude_decimal_degrees", "longitude_decimal_degrees"]

for i in airport_columns:
    if i in df_geo_silver.columns:
        print(f"Column '{i}' exists in DataFrame")
    else:
        raise ValueError(f"Missing column: {i}")


# COMMAND ----------

# CHECK THE DATA ISN'T EMPTY
if df_geo_silver.count() > 1:
    print("Data found")
else:
    raise ValueError("There is no data!")

# COMMAND ----------

# CHECK FOR NULL DATA
columns_to_check = ["icao", "airport", "city", "country", "latitude_decimal_degrees", "longitude_decimal_degrees" ]

# LOOP THROUGH THE COLUMNS TO SEE WHICH ONE HAS NULL DATA
for col_name in columns_to_check:
    if df_geo_silver.filter(col(col_name).isNull()).limit(1).count() > 0:
        raise ValueError(f"There is a null in the {col_name} column!")

print("No nulls found in the dataset")

display(df_geo_silver)
     

# COMMAND ----------

# MAGIC %md
# MAGIC **PUSH TO GOLD TABLE**
# MAGIC
# MAGIC THIS CREATES THE GOLD TABLE. WITH THE UNIT TESTS ABOVE, THE GOLD TABLE SHOULD ONLY INCLUDE READY TO USE DATA.
# MAGIC

# COMMAND ----------

df_geo_silver.write.mode("overwrite").saveAsTable("airport_db_gold.geocode_gold")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from airport_db_gold.geocode_gold

# COMMAND ----------

