**Airport Operations Dashboard - Project Overview**
------------------------------
This project focuses on ensuring the smooth operation of Dallas/Fort Worth International Airport (DFW) by integrating real-time flight and weather data. The system provides airport operators with key insights on flights, baggage, gate assignments, and weather conditions, allowing for improved passenger experience and operational efficiency.

Key Features:
- ✅ Real-Time Flight Tracking: Fetches live arrival and departure updates from the AviationStack API.
- ✅ Weather Integration: Retrieves hourly and weekly weather forecasts from the National Weather Service (NWS) API to assess potential flight delays and disruptions.
- ✅ Automated Data Updates:

Flight data updates every minute.
Weather data refreshes every hour.
- ✅ Interactive Databricks Dashboard: Displays live flight schedules, delays, and weather conditions, allowing DFW airport operators to make data-driven decisions.


**Tech Stack**
----------------------------------------------
- Python (Data fetching, processing, and transformation)
- Apache Spark (PySpark) (Data handling and transformations)
- Databricks 📊 (Data processing, storage, orchestration)


**Data Source**
----------------------------------------------
1. AviationStack API 🛬
The AviationStack API provides real-time global flight tracking data, including flight status, schedules, airline routes, and airport details. It updates every 30-60 seconds and covers 250+ countries and 13,000+ airlines.

Role in the Project
This API serves as the foundation for the project, supplying essential flight data such as:
- ✈ Airline IATA & Name
- ✈ Flight Number
- ✈ Departure & Arrival Details
- ✈ Flight Status & Delays
- ✈ Scheduled & Estimated Times

Challenges
- 🚨 API Limitations: The free version restricts usage to 100 requests per month, impacting real-time data retrieval.


2. National Weather Service API ☀️
The National Weather Service (NWS) is a government agency that provides critical weather forecasts, alerts, and observations. Its API offers public access to a wide range of essential weather data, updating every hour.

Role in the Project
- NWS supplies hourly and weekly weather forecasts for DFW airport managers to assess potential flight delays due to rain, snow, wind, or extreme weather.

Key Data Provided
- 🌡 Temperature
- 🌬 Wind Speed & Direction
- 🌦 Short-Term & Detailed Forecasts

3. Global Airport Database 📍
- The Global Airport Database provides location details for 9,300+ airports worldwide, covering both large and small airports.

Role in the Project
- The AviationStack API lacked location details for flights, requiring an additional data source to map flight origins and destinations accurately.

Key Data Provided
- 📍 ICAO & IATA Codes
- 🛫 Airport Name, City, Country
- 🌍 Latitude, Longitude, Altitude

Challenges
- ⚠ Unclear Data Updates: The database does not specify update frequency or maintenance process, which could impact long-term reliability, especially when new airports are added.




**Architecture and Methodology** 🏗️
----------------------------------------------
Medallion Architecture was adopted to ensure data quality, organization, and reliability across the pipeline.

- 🔹 Bronze Layer – Raw Data Ingestion

Stores data in its original format
Serves as the primary source of truth
- 🔸 Silver Layer – Data Processing & Transformation

Cleansing, deduplication, filtering
Structured & formatted data for analysis
- 🏅 Gold Layer – Optimized & Validated Data


![image](https://github.com/user-attachments/assets/f39b3e6e-5b7f-48e0-bc79-8e250a812056)
**Unit tests performed to ensure data integrity**

Only high-quality, reliable data is promoted for analytics and decision-making
This approach ensures scalable, efficient data management while preventing bad or incomplete data from reaching production-level tables. 🚀
 - No null values were present in critical fields.
 - All expected columns existed in the dataset.
 - Duplicate records were identified and removed.





Data Model 
---------------------------------------------------

![image](https://github.com/user-attachments/assets/b7b936a5-77be-4a48-8674-218af9bac874)

---------------------------------------------------------
**Streaming Processing, Ingestion, & Storage 💾**

- Implemented Databricks Delta Live Tables (SLT) to stream real-time departure and arrival data from the AviationStack API, ensuring continuous ingestion of new flight records.
- Instead of directly appending all records, upsert logic was applied using UUID5(Based on flight_iata, flight_date, flight_status) as a unique identifier to prevent duplicate records and maintain incremental storage in Delta tables.
- Databricks Volumes were utilized for efficient storage and fast access to streaming data, while PySpark handled transformation and enrichment.
- Configured a Databricks workflow to run continuously, leveraging Delta Live Tables (SLT) for seamless data ingestion and incremental updates in real-time.
**Batch Processing 🕒**
- Scheduled hourly batch updates in a Databricks workflow to refresh National Weather Service (NWS) data, ensuring the dashboard reflects the latest weather conditions.
- Since the Global Airport Database lacked clear documentation on update frequency, an automated refresh workflow was not implemented, but manual updates were scheduled as needed.
--------------------------------------------------------
Data Quality 🔢
To ensure data accuracy, consistency, and reliability across the pipeline, robust unit tests and data validation mechanisms were implemented:
- ✅ Ensured incremental data integrity by applying upsert logic with UUID5, preventing data duplication in Streaming Live Delta tables and.
- ✅ Implemented schema validation to confirm that all expected columns exist before promoting data from bronze → silver → gold layers.
- ✅ Applied null-check constraints, replaced null with meaningful data on critical fields to prevent data gaps affecting analytics and reporting.
- ✅ Implemented deduplication logic before writing into gold-layer tables, ensuring only clean, enriched data is used for decision-making.



Dashboards
-----------------------------------------

Departures page:
![image](https://github.com/user-attachments/assets/c9427492-5178-4803-90af-92355e79cd70)

Arrivals Page
![image](https://github.com/user-attachments/assets/1fd82a7f-3ae4-4bea-8355-57dcdfb02ae6)

Delayed Flights
![image](https://github.com/user-attachments/assets/fedd487e-e66c-4497-b6ad-fd1a3f8311c9)

Weather Report
![image](https://github.com/user-attachments/assets/7cf58f80-7259-4e58-82ad-568ebd5ca5d2)





