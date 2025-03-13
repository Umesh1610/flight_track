# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE arrival_gold_slt
# MAGIC COMMENT "Upserts new arrival data from airport_db_gold.arrival_gold"
# MAGIC TBLPROPERTIES ("quality" = "gold")
# MAGIC AS 
# MAGIC MERGE INTO LIVE.arrival_gold_slt AS target
# MAGIC USING STREAM(airport_db_gold.arrival_gold) AS source
# MAGIC ON target.flight_id = source.flight_id  -- Use a unique identifier for upsert
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET *  -- Updates existing records
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT *;  -- Inserts new records

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE departure_gold_slt
# MAGIC COMMENT "Upserts new departure data from airport_db_gold.departure_gold"
# MAGIC TBLPROPERTIES ("quality" = "gold")
# MAGIC AS 
# MAGIC MERGE INTO LIVE.departure_gold_slt AS target
# MAGIC USING STREAM(airport_db_gold.departure_gold) AS source
# MAGIC ON target.flight_id = source.flight_id  -- Use a unique identifier for upsert
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET *  -- Updates existing records
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT *;  -- Inserts new records