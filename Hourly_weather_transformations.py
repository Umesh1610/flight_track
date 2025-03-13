# Databricks notebook source
# MAGIC %md
# MAGIC **NATIONAL WEATHER SERVICE HOURLY SILVER TABLE**
# MAGIC
# MAGIC This Python script relies on the NWS Bronze task to run. Once that runs, this script cleans the data and performs a unit test. If the unit test is passed, then the table is written to the gold table.

# COMMAND ----------


# IMPORT REQUIRED LIBRARIES
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **LET'S CLEAN THE DATA**

# COMMAND ----------


# CREATE THE DATAFRAME FROM THE BRONZE TABLE
hourly_bronze = spark.sql("SELECT * FROM airport_db_raw.weather_hourly_raw")
display(hourly_bronze)

# COMMAND ----------


# CHANGE THE DATE TYPE TO TIMESTAMP
hourly_bronze = hourly_bronze.withColumn("startTime", col("startTime").cast(TimestampType())).withColumn("endTime", col("endTime").cast(TimestampType()))

# CHANGE COLUMN FORMATTING TO UPPERCASE
hourly_bronze = hourly_bronze.withColumn("isDaytime", upper(col("isDaytime"))).withColumn("windSpeed", upper(col("windSpeed"))).withColumn("shortForecast", upper(col("shortForecast")))

# RENAME COLUMNS
hourly_bronze = hourly_bronze \
                .select(
                    hourly_bronze['startTime'].alias('start_time'),
                    hourly_bronze['endTime'].alias('end_time'),
                    hourly_bronze['isDaytime'].alias('is_daytime'),
                    hourly_bronze['temperature'],
                    hourly_bronze['temperatureUnit'].alias('temperature_unit'),
                    hourly_bronze['windSpeed'].alias('wind_speed'),
                    hourly_bronze['windDirection'].alias('wind_direction'),
                    hourly_bronze['shortForecast'].alias('short_forecast'),
                    hourly_bronze['uploaded_timestamp']
                )

# CREATE THE SILVER TABLE
hourly_bronze.write.mode("overwrite").saveAsTable("airport_db_silver.weather_hourly_silver")

display(hourly_bronze)
hourly_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **UNIT TESTS**

# COMMAND ----------


# QUERY THE TABLE
hourly_silver = spark.sql("SELECT * FROM airport_db_silver.weather_hourly_silver")

# PRE-DEFINED COLUMN NAMES
hourly_columns = ["start_time", "end_time", "is_daytime", "temperature", "temperature_unit", "wind_speed", "wind_direction", "short_forecast", "uploaded_timestamp"]

# CHECK THAT EVERY COLUMN EXISTS
for i in hourly_columns:
    if i in hourly_silver.columns:
        print(f"Column - {i} - exists in DataFrame")
    else:
        raise ValueError("There is a missing column!")

# CHECK THAT DATA IS FOUND
if hourly_silver.count() > 1:
    print("Data found")
else:
    raise ValueError("There is no data!")

# ENSURE THAT NO NULLS EXIST
if hourly_silver.filter(col("start_time").isNull()).limit(1).count() > 0:  
    raise ValueError("There is a null in the start_time column!")
elif hourly_silver.filter(col("end_time").isNull()).limit(1).count() > 0:  
    raise ValueError("There is a null in the end_time column!")
elif hourly_silver.filter(col("is_daytime").isNull()).limit(1).count() > 0:  
    raise ValueError("There is a null in the is_daytime column!")
elif hourly_silver.filter(col("temperature").isNull()).limit(1).count() > 0:  
    raise ValueError("There is a null in the temperature column!")
elif hourly_silver.filter(col("temperature_unit").isNull()).limit(1).count() > 0:  
    raise ValueError("There is a null in the temperature_unit column!")
elif hourly_silver.filter(col("wind_speed").isNull()).limit(1).count() > 0:  
    raise ValueError("There is a null in the wind_speed column!")
elif hourly_silver.filter(col("wind_direction").isNull()).limit(1).count() > 0:  
    raise ValueError("There is a null in the wind_direction column!")
elif hourly_silver.filter(col("short_forecast").isNull()).limit(1).count() > 0:  
    raise ValueError("There is a null in the short_forecast column!")
elif hourly_silver.filter(col("uploaded_timestamp").isNull()).limit(1).count() > 0:  
    raise ValueError("There is a null in the uploaded_timestamp column!")
else:
    print("No nulls found in the dataset")

# COMMAND ----------

# MAGIC %md
# MAGIC **GOLD TABLES**

# COMMAND ----------

hourly_silver.write.mode("overwrite").saveAsTable("airport_db_gold.weather_hourly_gold")


# COMMAND ----------

