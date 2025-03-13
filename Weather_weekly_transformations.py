# Databricks notebook source
# MAGIC %md
# MAGIC **NATIONAL WEATHER SERVICE WEEKLY SILVER TABLE**
# MAGIC
# MAGIC This Python script relies on the NWS Bronze task to run. Once that runs, this script cleans the data and performs a unit test. If the unit test is passed, then the table is written to the gold table.

# COMMAND ----------


# IMPORT NECESSARY LIBRARIES
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# QUERY THE WEEKLY BRONZE TABLE
weekly_bronze = spark.sql("SELECT * FROM airport_db_raw.weather_raw")

# CHANGE DATA TYPE
weekly_bronze = weekly_bronze.withColumn("startTime", col("startTime").cast(TimestampType())).withColumn("endTime", col("endTime").cast(TimestampType()))

# UPDATE FORMATTING SO COLUMNS ARE UPPERCASE
weekly_bronze = weekly_bronze.withColumn("name", upper(col("name"))).withColumn("isDaytime", upper(col("isDaytime"))).withColumn("windSpeed", upper(col("windSpeed"))).withColumn("shortForecast", upper(col("shortForecast"))).withColumn("detailedForecast", upper(col("detailedForecast")))

# RENAME COLUMN NAMES
weekly_bronze = weekly_bronze \
                .select(
                    weekly_bronze['name'],
                    weekly_bronze['startTime'].alias('start_time'),
                    weekly_bronze['endTime'].alias('end_time'),
                    weekly_bronze['isDaytime'].alias('is_daytime'),
                    weekly_bronze['temperature'],
                    weekly_bronze['temperatureUnit'].alias('temperature_unit'),
                    weekly_bronze['windSpeed'].alias('wind_speed'),
                    weekly_bronze['windDirection'].alias('wind_direction'),
                    weekly_bronze['shortForecast'].alias('short_forecast'),
                    weekly_bronze['detailedForecast'].alias('detailed_forecast')
                )

# COMMAND ----------

# CREATE THE SILVER TABLE
weekly_bronze.write.mode("overwrite").saveAsTable("airport_db_silver.weather_silver")

display(weekly_bronze)
weekly_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **LET'S PERFORM THE UNIT TESTS**
# MAGIC
# MAGIC BY PERFORMING UNIT TESTS, END USERS CAN BE SURE OF THE QUALITY OF THE DATA. THIS WILL AVOID PUTTING INCORRECT OR WRONG DATA INTO PRODUCTION.

# COMMAND ----------

# QUERY THE SILVER TABLE
weekly_silver = spark.sql("SELECT * FROM airport_db_silver.weather_silver")

# THESE ARE THE PRE-DETERMINED COLUMN NAMES
weekly_columns = ["name", "start_time", "end_time", "is_daytime", "temperature", "temperature_unit", "wind_speed", "wind_direction", "short_forecast", "detailed_forecast"]

# CHECK THAT THE REQUIRED COLUMNS EXIST
for i in weekly_columns:
    if i in weekly_silver.columns:
        print(f"Column - {i} - exists in DataFrame")
    else:
        raise ValueError("There is a missing column!")

# CHECK THAT THERE IS DATA
if weekly_silver.count() > 1:
    print("Data found")
else:
    raise ValueError("There is no data!")

# CHECK THAT THE COLUMNS HAVE DATA
if weekly_silver.filter(col("name").isNull()).limit(1).count() > 0: 
    raise ValueError("There is a null in the name column!")
elif weekly_silver.filter(col("start_time").isNull()).limit(1).count() > 0:  
    raise ValueError("There is a null in the start_time column!")
elif weekly_silver.filter(col("end_time").isNull()).limit(1).count() > 0:  
    raise ValueError("There is a null in the end_time column!")
elif weekly_silver.filter(col("is_daytime").isNull()).limit(1).count() > 0:  
    raise ValueError("There is a null in the is_daytime column!")
elif weekly_silver.filter(col("temperature").isNull()).limit(1).count() > 0:  
    raise ValueError("There is a null in the temperature column!")
elif weekly_silver.filter(col("temperature_unit").isNull()).limit(1).count() > 0:  
    raise ValueError("There is a null in the temperature_unit column!")
elif weekly_silver.filter(col("wind_speed").isNull()).limit(1).count() > 0:  
    raise ValueError("There is a null in the wind_speed column!")
elif weekly_silver.filter(col("wind_direction").isNull()).limit(1).count() > 0: 
    raise ValueError("There is a null in the wind_direction column!")
elif weekly_silver.filter(col("short_forecast").isNull()).limit(1).count() > 0:  
    raise ValueError("There is a null in the short_forecast column!")
elif weekly_silver.filter(col("detailed_forecast").isNull()).limit(1).count() > 0: 
    raise ValueError("There is a null in the detailed_forecast column!")
else:
    print("No nulls found in the dataset")

# COMMAND ----------

# MAGIC %md
# MAGIC **GOLD TABLES**
# MAGIC
# MAGIC IF THE DATA PASSES THE UNIT TESTS, THEN THE DATA CAN BE WRITTEN INTO THE GOLD TABLE.

# COMMAND ----------

weekly_silver.write.mode("overwrite").saveAsTable("airport_db_gold.weather_gold")


# COMMAND ----------

