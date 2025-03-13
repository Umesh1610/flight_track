# Databricks notebook source
# MAGIC %md
# MAGIC **Real-time DFW Flight Data Using AviationStack API**
# MAGIC This Python script retrieves real-time arrival and departure flight information for Dallas/Fort Worth International Airport (DFW) from the AviationStack API. The data is updated every minute and undergoes unit testing before being stored in the gold table.
# MAGIC
# MAGIC **Fetch Data** → The script sends a GET request to the AviationStack API, filtering for today’s DFW arrivals and departures.
# MAGIC
# MAGIC **Process & Structure** → The response is flattened into a structured PySpark DataFrame.
# MAGIC
# MAGIC **Perform Unit Tests** → Before pushing the data, unit tests check for missing values, data integrity, and consistency.
# MAGIC
# MAGIC **Store in Gold Table** → After validation, the clean data is written to the gold table, ensuring high-quality flight tracking information.
# MAGIC
# MAGIC This automated pipeline ensures accurate, real-time flight data, supporting operational efficiency at DFW. 

# COMMAND ----------

# MAGIC %md
# MAGIC create hash key
# MAGIC and upsert into deltatables based on hashkey for incremental uploads
# MAGIC
# MAGIC
# MAGIC Change Architecture to managed tables and Use upsert logic and change API keys(Expired _Exhustaed), and delete all external tables and data
# MAGIC

# COMMAND ----------


# IMPORT NECESSARY LIBRARIES
import uuid
import json
import logging
import requests
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import broadcast

# COMMAND ----------


# SET UP VARIABLES
API_KEY = "f007c23540ca92eea5844d899bd0beae"
BASE_URL = "http://api.aviationstack.com/v1/flights"

# PULL IN GEOCODE DATA
airport_geocode_gold = spark.sql("SELECT * FROM airport_db_gold.geocode_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC Fetching API data and storing it in a List

# COMMAND ----------

airline_iata_codes = ["AA", "AC", "AF", "AM", "AS", "AZ", "B6", "BA", "DL", "EK", "F9", "IB", "KL", "LH", "LX", "NK", "QR", "SQ", "TK", "UA", "WS"]  


all_flights_data = []

# LOOP OVER EACH AIRLINE USING THE GIVEN PARAMETERS
for airline in airline_iata_codes:
    params = {
        "access_key": API_KEY,
        "dep_iata": "DFW",  # SET DFW AS DEPARTURE AIRPORT
        "airline_iata": airline,  # LOOPS THROUGH LIST OF AIRPORTS ABOVE
    }

    response = requests.get(BASE_URL, params=params)

    if response.status_code == 200:
        data = response.json().get("data", [])
        
        if data:
            
            all_flights_data.extend(data)
        else:
            print(f"No flight data available for airline {airline}.")
    else:
        print(f"Error fetching data for airline {airline}: {response.status_code}, {response.text}")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType

# Define Schema
schema = StructType([
    StructField("flight_date", StringType(), True),
    StructField("flight_status", StringType(), True),

    # Departure Schema
    StructField("departure", StructType([
        StructField("airport", StringType(), True),
        StructField("timezone", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("icao", StringType(), True),
        StructField("terminal", StringType(), True),
        StructField("gate", StringType(), True),
        StructField("delay", IntegerType(), True),
        StructField("scheduled", StringType(), True),
        StructField("estimated", StringType(), True),
        StructField("actual", StringType(), True),
        StructField("estimated_runway", StringType(), True),
        StructField("actual_runway", StringType(), True)
    ]), True),

    # Arrival Schema
    StructField("arrival", StructType([
        StructField("airport", StringType(), True),
        StructField("timezone", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("icao", StringType(), True),
        StructField("terminal", StringType(), True),
        StructField("gate", StringType(), True),
        StructField("baggage", StringType(), True),
        StructField("delay", IntegerType(), True),
        StructField("scheduled", StringType(), True),
        StructField("estimated", StringType(), True),
        StructField("actual", StringType(), True),
        StructField("estimated_runway", StringType(), True),
        StructField("actual_runway", StringType(), True)
    ]), True),

    # Airline Schema
    StructField("airline", StructType([
        StructField("name", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("icao", StringType(), True)
    ]), True),

    # Flight Schema
    StructField("flight", StructType([
        StructField("number", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("icao", StringType(), True),
        StructField("codeshared", StringType(), True)
    ]), True),

    # Aircraft Schema (Newly Added)
    StructField("aircraft", StructType([
        StructField("registration", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("icao", StringType(), True),
        StructField("icao24", StringType(), True)
    ]), True),

    # Live Data Schema (Newly Added)
    StructField("live", StructType([
        StructField("updated", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("altitude", StringType(), True),
        StructField("direction", StringType(), True),
        StructField("speed_horizontal", StringType(), True),
        StructField("speed_vertical", StringType(), True),
        StructField("is_ground", BooleanType(), True)
    ]), True)
])

df = spark.createDataFrame(all_flights_data, schema=schema)

# COMMAND ----------

df_flattened = df.select(
    col("flight_date"),
    col("flight_status"),
    # Departure
    col("departure.airport").alias("departure_airport"),
    col("departure.timezone").alias("departure_timezone"),
    col("departure.iata").alias("departure_iata"),
    col("departure.icao").alias("departure_icao"),
    col("departure.terminal").alias("departure_terminal"),
    col("departure.gate").alias("departure_gate"),
    col("departure.delay").alias("departure_delay"),
    col("departure.scheduled").alias("departure_scheduled"),
    col("departure.estimated").alias("departure_estimated"),
    col("departure.actual").alias("departure_actual"),
    col("departure.estimated_runway").alias("departure_estimated_runway"),
    col("departure.actual_runway").alias("departure_actual_runway"),
    # Arrival
    col("arrival.airport").alias("arrival_airport"),
    col("arrival.timezone").alias("arrival_timezone"),
    col("arrival.iata").alias("arrival_iata"),
    col("arrival.icao").alias("arrival_icao"),
    col("arrival.terminal").alias("arrival_terminal"),
    col("arrival.gate").alias("arrival_gate"),
    col("arrival.baggage").alias("arrival_baggage"),
    col("arrival.delay").alias("arrival_delay"),
    col("arrival.scheduled").alias("arrival_scheduled"),
    col("arrival.estimated").alias("arrival_estimated"),
    col("arrival.actual").alias("arrival_actual"),
    col("arrival.estimated_runway").alias("arrival_estimated_runway"),
    col("arrival.actual_runway").alias("arrival_actual_runway"),
# Airline
    col("airline.name").alias("airline_name"),
    col("airline.iata").alias("airline_iata"),
    col("airline.icao").alias("airline_icao"),
# Flight
    col("flight.number").alias("flight_number"),
    col("flight.iata").alias("flight_iata"),
    col("flight.icao").alias("flight_icao"),
    col("flight.codeshared").alias("flight_codeshared"),
# Aircraft
    col("aircraft.registration").alias("aircraft_registration"),
    col("aircraft.iata").alias("aircraft_iata"),
    col("aircraft.icao").alias("aircraft_icao"),
    col("aircraft.icao24").alias("aircraft_icao24"),
# Live
    col("live.updated").alias("live_updated"),
    col("live.latitude").alias("live_latitude"),
    col("live.longitude").alias("live_longitude"),
    col("live.altitude").alias("live_altitude"),
    col("live.direction").alias("live_direction"),
    col("live.speed_horizontal").alias("live_speed_horizontal"),
    col("live.speed_vertical").alias("live_speed_vertical"),
    col("live.is_ground").alias("live_is_ground")
)


# COMMAND ----------

display(df_flattened)

# COMMAND ----------

def generate_uuid5(flight_iata, flight_date, flight_status):
    input_string = f"{flight_iata}-{flight_date}-{flight_status}"  # Combine three column values
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, input_string))

# Register UDF
uuid5_udf = udf(generate_uuid5, StringType())

# Apply UDF to DataFrame
df_updated = df_flattened.withColumn("uuid", uuid5_udf(df_flattened["flight_iata"], df_flattened["flight_date"], df_flattened["flight_status"]))\
    .withColumn("uploaded_timestamp", current_timestamp())

# COMMAND ----------

df_updated.write.mode("overwrite").saveAsTable("airport_db_raw.aviation_raw")


# COMMAND ----------

# MAGIC %md
# MAGIC **LET'S CLEAN THE DEPARTURE BRONZE DATA**
# MAGIC
# MAGIC TO GET THE DFW DEPARTURE DATA INTO A USEABLE FORMAT, THE DATA MUST BE IMPROVED BY CHANGING THE DATA TYPES AND THE COLUMN NAMES. THIS STEP WILL EXCLUDE ANY UNNEEDED DATA.

# COMMAND ----------

depart_silver=spark.sql('select * from airport_db_raw.aviation_raw')
depart_silver = depart_silver.drop('departure', 'arrival', 'airline', 'flight', 'aircraft', 'live', '_rescued_data', 'flight_codeshared')

# COMMAND ----------

# DEFINE COLUMN TYPES
type_conversions = {
    "flight_date": DateType(),
    "arrival_actual": TimestampType(),
    "arrival_actual_runway": TimestampType(),
    "arrival_delay": IntegerType(),
    "arrival_estimated": TimestampType(),
    "arrival_estimated_runway": TimestampType(),
    "arrival_scheduled": TimestampType(),
    "arrival_terminal": IntegerType(),
    "departure_actual": TimestampType(),
    "departure_actual_runway": TimestampType(),
    "departure_delay": IntegerType(),
    "departure_estimated": TimestampType(),
    "departure_estimated_runway": TimestampType(),
    "departure_scheduled": TimestampType(),
    "uploaded_timestamp": TimestampType()
}

# CAST COLUMNS AS NEW TYPES
for col_name, new_type in type_conversions.items():
    depart_silver = depart_silver.withColumn(col_name, col(col_name).cast(new_type))

# SELECT CERTAIN COLUMNS
depart_silver = depart_silver.select(
    "airline_iata",
    "airline_name",
    "arrival_iata",
    "departure_actual",
    "departure_delay",
    "departure_estimated",
    "departure_estimated_runway",
    "departure_iata",
    "departure_scheduled",
    "flight_iata",
    "flight_date",
    "flight_status",
    "uploaded_timestamp",
    'uuid'
    )

# COMMAND ----------

display(depart_silver)

# COMMAND ----------


depart_geocode = depart_silver \
                .join(broadcast(airport_geocode_gold), depart_silver.arrival_iata == airport_geocode_gold.iata, "left") \
                .select(
                    depart_silver['*'], 
                    airport_geocode_gold['city'].alias('arrival_city'),
                    airport_geocode_gold['country'].alias('arrival_country'),
                    airport_geocode_gold['latitude_decimal_degrees'].alias('arrival_latitude'),
                    airport_geocode_gold['longitude_decimal_degrees'].alias('arrival_longitude')
                ) 

# COMMAND ----------

display(depart_geocode)

# COMMAND ----------

# UPPER CASE AIRLINE NAME
depart_geocode = depart_geocode.withColumn("airline_name", upper(col("airline_name")))

# CREATE FLAG FOR INTERNATIONAL ARRIVALS
depart_geocode = depart_geocode.withColumn("arrival_international", when(col("arrival_country") == "USA", "N").otherwise("Y"))

# EDIT FLIGHT STATUS TO BE SPECIFIC
depart_geocode = depart_geocode.withColumn(
    "flight_status",
    when(col("flight_status") == "landed", "LANDED")
    .when((col("flight_status") == "scheduled") & (col("departure_delay") > 0), "DELAYED")
    .when((col("departure_delay").isNotNull()) & (col("departure_delay") > 0), "ENROUTE DELAYED")
    .otherwise(upper(col("flight_status")))
)

# COMMAND ----------

# DEFINE COLUMNS
schema_columns = [
    "airline_iata", "airline_name", "arrival_city", "arrival_country", "arrival_iata", "arrival_international" "arrival_latitude", "arrival_longitude", "departure_actual", "departure_delay", "departure_estimated", "departure_estimated_runway", "departure_iata", "departure_scheduled", "flight_iata", "flight_date", "flight_status", "uploaded_timestamp","uuid"
]

# SORT COLUMNS TO ALPHABETICAL ORDER
sorted_columns = sorted(depart_geocode.columns)

# REORDER DATAFRAME
depart_geocode = depart_geocode.select(sorted_columns)

display(depart_geocode)
depart_geocode.printSchema()


     

# COMMAND ----------

depart_geocode.write.mode("overwrite").saveAsTable("airport_db_silver.departure_silver")


# COMMAND ----------

# MAGIC %md
# MAGIC **LET'S PERFORM THE UNIT TESTS**
# MAGIC
# MAGIC
# MAGIC BY PERFORMING UNIT TESTS, END USERS CAN BE SURE OF THE QUALITY OF THE DATA. THIS WILL AVOID PUTTING INCORRECT OR WRONG DATA INTO PRODUCTION. API OWNERS FREQUENTLY CHANGE THE DATA SCHEMA OR DATA TYPE. THIS WILL FIND ANY CHANGES THAT WILL AFFECT PRODUCTION DATA.

# COMMAND ----------


# QUERY SILVER DEPARTURE DATA
depart_silver = spark.sql("SELECT * FROM airport_db_silver.departure_silver")
display(depart_silver)
depart_silver.printSchema()

# COMMAND ----------


# PRE-DETERMINED COLUMNS LISTS
depart_columns = ["airline_iata", "airline_name", "arrival_city", "arrival_country", "arrival_iata", "arrival_international", "arrival_latitude", "arrival_longitude", "departure_actual", "departure_delay", "departure_estimated", "departure_estimated_runway", "departure_iata", "departure_scheduled", "flight_date", "flight_iata", "flight_status", "uploaded_timestamp"]

# ENSURE EVERY COLUMN EXISTS
for i in depart_columns:
    if i in depart_silver.columns:
        print(f"Column '{i}' exists in DataFrame")
    else:
        raise ValueError(f"Missing column: {i}")

# COMMAND ----------

# CHECK THAT THE DATA ISN'T EMPTY
if depart_silver.count() > 1:
    print("Data found")
else:
    raise ValueError("There is no data!")

# COMMAND ----------

# CHECK THAT CERTAIN COLUMNS DON'T HAVE NULLS
if depart_silver.filter(col("airline_iata").isNull()).limit(1).count() > 0:
    raise ValueError("There is a null in the airline_iata column!")
elif depart_silver.filter(col("airline_name").isNull()).limit(1).count() > 0:
    raise ValueError("There is a null in the airline_name column!")
elif depart_silver.filter(col("arrival_iata").isNull()).limit(1).count() > 0:
    raise ValueError("There is a null in the arrival_iata column!")
elif depart_silver.filter(col("departure_iata").isNull()).limit(1).count() > 0:
    raise ValueError("There is a null in the departure_iata column!")
elif depart_silver.filter(col("departure_scheduled").isNull()).limit(1).count() > 0:
    raise ValueError("There is a null in the departure_scheduled column!")
elif depart_silver.filter(col("flight_iata").isNull()).limit(1).count() > 0:  
    raise ValueError("There is a null in the flight_iata column!")
elif depart_silver.filter(col("flight_date").isNull()).limit(1).count() > 0:  
    raise ValueError("There is a null in the flight_date column!")
elif depart_silver.filter(col("flight_status").isNull()).limit(1).count() > 0: 
    raise ValueError("There is a null in the flight_status column!")
else:
    print("No nulls found in the dataset")

# COMMAND ----------

# MAGIC %md
# MAGIC **DEPARTURE GOLD TABLES**
# MAGIC
# MAGIC IF THE DATA PASSES THE UNIT TESTS, THEN THE DATA CAN BE WRITTEN INTO THE GOLD TABLE.

# COMMAND ----------

depart_silver.write.mode("overwrite").saveAsTable("airport_db_gold.departure_gold")


# COMMAND ----------

# MAGIC %md
# MAGIC **> LET'S CLEAN THE ARRIVAL BRONZE DATA**
# MAGIC
# MAGIC TO GET THE DFW ARRIVAL DATA INTO A USEABLE FORMAT, THE DATA MUST BE IMPROVED BY CHANGING THE DATA TYPES AND THE COLUMN NAMES. THIS STEP WILL EXCLUDE ANY UNNEEDED DATA.

# COMMAND ----------

arrival_bronze=spark.sql('select * from airport_db_raw.aviation_raw')

# COMMAND ----------

display(arrival_bronze)

# COMMAND ----------

arrival_bronze = arrival_bronze.select(
    "airline_iata",
    "airline_name",
    "arrival_actual",
    "arrival_actual_runway",
    "arrival_baggage",
    "arrival_delay",
    "arrival_estimated",
    "arrival_estimated_runway",
    "arrival_gate",
    "arrival_iata",
    "arrival_scheduled",
    "departure_actual",
    "departure_delay",
    "departure_estimated",
    "departure_estimated_runway",
    "departure_iata",
    "departure_scheduled",
    "flight_iata",
    "flight_date",
    "flight_number",
    "flight_status",
    "uploaded_timestamp",
    "uuid"
    )

# COMMAND ----------

# JOIN THE TABLE WITH THE AIRPORT LOCATION TABLE
arrival_geocode = arrival_bronze \
                .join(broadcast(airport_geocode_gold), arrival_bronze.departure_iata == airport_geocode_gold.iata, "left") \
                .select(
                    arrival_bronze['*'], 
                    airport_geocode_gold['city'].alias('depart_city'),
                    airport_geocode_gold['country'].alias('depart_country'),
                    airport_geocode_gold['latitude_decimal_degrees'].alias('depart_latitude'),
                    airport_geocode_gold['longitude_decimal_degrees'].alias('depart_longitude')
                ) 

# COMMAND ----------

display(arrival_geocode)

# COMMAND ----------

# CREATE FLAG FOR INTERNATIONAL FLIGHT
arrival_geocode = arrival_geocode.withColumn("depart_international", when(col("depart_country") == "USA", "N").otherwise("Y"))

# DEFINE COLUMNS AND THEIR TYPES
type_conversions = {
    "flight_date": DateType(),
    "arrival_actual": TimestampType(),
    "arrival_actual_runway": TimestampType(),
    "arrival_delay": IntegerType(),
    "arrival_estimated": TimestampType(),
    "arrival_estimated_runway": TimestampType(),
    "arrival_scheduled": TimestampType(),
    "departure_actual": TimestampType(),
    "departure_delay": IntegerType(),
    "departure_estimated": TimestampType(),
    "departure_estimated_runway": TimestampType(),
    "departure_scheduled": TimestampType(),
    "uploaded_timestamp": TimestampType()
}

# COMMAND ----------

# APPLY TYPES
for col_name, new_type in type_conversions.items():
    arrival_geocode_new = arrival_geocode.withColumn(col_name, col(col_name).cast(new_type))



# COMMAND ----------

display(arrival_geocode_new)

# COMMAND ----------

# EDIT FLIGHT_STATUS FOR SPECIFICITY
arrival_geocode = arrival_geocode.withColumn("flight_status",
    when(col("flight_status") == "landed", "LANDED")
    .when(col("arrival_delay") > 0, "DELAYED")
    .when(col("departure_delay") > 0, "DEPART DELAYED")
    .when((col("flight_status") == "scheduled") & (col("departure_delay") > 0), "DELAYED")
    .when((col("departure_delay").isNotNull()) & (col("departure_delay") > 0), "ENROUTE DELAYED")
    .when(col("flight_status") == "scheduled", "ON TIME")
    .otherwise(upper(col("flight_status")))
)



# COMMAND ----------

# EDIT ARRIVAL BAGGAGE TO SHOW NA IF NONE WAS ASSIGNED
arrival_geocode = arrival_geocode.withColumn(
    "arrival_baggage",
    when(col("arrival_baggage").isNull(), "UNASSIGNED")
    .otherwise(arrival_geocode.arrival_baggage)
)

# EDIT ARRIVAL GATE TO SHOW NA IF NONE WAS ASSIGNED
arrival_geocode = arrival_geocode.withColumn(
    "arrival_gate",
    when(col("arrival_gate").isNull(), "UNASSIGNED")
    .otherwise(arrival_geocode.arrival_gate)
)

# DROP COLUMN
arrival_geocode = arrival_geocode.drop('departure_delay')

# UPPERCASE AIRLINE NAME
arrival_geocode = arrival_geocode.withColumn("airline_name", upper(col("airline_name")))

# SORT COLUMNS
sorted_columns = sorted(arrival_geocode.columns)

# REORDER DATAFRAME
arrival_geocode = arrival_geocode.select(sorted_columns)

display(arrival_geocode)

# COMMAND ----------

# WRITE THE DATA TO THE SILVER TABLE
arrival_geocode.write.mode("overwrite").saveAsTable("airport_db_silver.arrival_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC **LET'S PERFORM THE UNIT TESTS**
# MAGIC
# MAGIC
# MAGIC BY PERFORMING UNIT TESTS, END USERS CAN BE SURE OF THE QUALITY OF THE DATA. THIS WILL AVOID PUTTING INCORRECT OR MISSING DATA INTO PRODUCTION. API OWNERS FREQUENTLY CHANGE THE DATA SCHEMA OR DATA TYPE. THIS WILL FIND ANY CHANGES THAT WILL AFFECT PRODUCTION DATA.

# COMMAND ----------

# QUERY THE SILVER TABLE
arrival_silver = spark.sql("SELECT * FROM airport_db_silver.arrival_silver")
display(arrival_silver)
arrival_silver.printSchema()

# COMMAND ----------

# PRE-DETERMINED COLUMNS
arrival_columns = ["airline_iata", "airline_name", "arrival_actual", "arrival_actual_runway", "arrival_baggage", "arrival_delay", "arrival_estimated", "arrival_estimated_runway", "arrival_gate", "arrival_iata", "arrival_scheduled", "depart_city", "depart_country", "depart_international", "depart_latitude", "depart_longitude", "departure_actual", "departure_estimated", "departure_estimated_runway", "departure_iata", "departure_scheduled", "flight_date", "flight_iata", "flight_number", "flight_status", "uploaded_timestamp"]

# CHECK THAT EVERY COLUMN IS THERE
for i in arrival_columns:
    if i in arrival_silver.columns:
        print(f"Column '{i}' exists in DataFrame")
    else:
        raise ValueError(f"Missing column: {i}")

# CHECK THE DATA ISN'T EMPTY
if arrival_silver.count() > 1:
    print("Data found")
else:
    raise ValueError("There is no data!")

# COLUMNS THAT SHOULDN'T BE NULL
columns_to_check = [
    "airline_name", "arrival_iata",
    "arrival_scheduled", "departure_estimated", "departure_iata", 
    "departure_scheduled", "flight_date", "flight_number", "flight_status"
]

# LOOP THROUGH THE COLUMNS TO CHECK FOR NULL DATA
for col_name in columns_to_check:
    if arrival_silver.filter(col(col_name).isNull()).limit(1).count() > 0:
        raise ValueError(f"There is a null in the {col_name} column!")

print("No nulls found in the dataset")

display(arrival_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC **DEPARTURE GOLD TABLES**
# MAGIC
# MAGIC IF THE DATA PASSES THE UNIT TESTS, THEN THE DATA CAN BE WRITTEN INTO THE GOLD TABLE.

# COMMAND ----------

arrival_silver.write.mode("overwrite").saveAsTable("airport_db_gold.arrival_gold")


# COMMAND ----------

