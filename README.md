**Airport Operations Dashboard - Project Overview**
This project focuses on ensuring the smooth operation of Dallas/Fort Worth International Airport (DFW) by integrating real-time flight and weather data. The system provides airport operators with key insights on flights, baggage, gate assignments, and weather conditions, allowing for improved passenger experience and operational efficiency.

Key Features:
✅ Real-Time Flight Tracking: Fetches live arrival and departure updates from the AviationStack API.
✅ Weather Integration: Retrieves hourly and weekly weather forecasts from the National Weather Service (NWS) API to assess potential flight delays and disruptions.
✅ Automated Data Updates:

Flight data updates every minute.
Weather data refreshes every hour.
✅ Interactive Databricks Dashboard: Displays live flight schedules, delays, and weather conditions, allowing DFW airport operators to make data-driven decisions.


Tech Stack
- Python (Data fetching, processing, and transformation)
- Apache Spark (PySpark) (Data handling and transformations)
- Databricks 📊 (Data processing, storage, orchestration)


**Data Source**
1. AviationStack API 🛬
The AviationStack API provides real-time global flight tracking data, including flight status, schedules, airline routes, and airport details. It updates every 30-60 seconds and covers 250+ countries and 13,000+ airlines.

Role in the Project
This API serves as the foundation for the project, supplying essential flight data such as:
✈ Airline IATA & Name
✈ Flight Number
✈ Departure & Arrival Details
✈ Flight Status & Delays
✈ Scheduled & Estimated Times

Challenges
🚨 API Limitations: The free version restricts usage to 100 requests per month, impacting real-time data retrieval.


2. National Weather Service API ☀️
The National Weather Service (NWS) is a government agency that provides critical weather forecasts, alerts, and observations. Its API offers public access to a wide range of essential weather data, updating every hour.

Role in the Project
NWS supplies hourly and weekly weather forecasts for DFW airport managers to assess potential flight delays due to rain, snow, wind, or extreme weather.

Key Data Provided
🌡 Temperature
🌬 Wind Speed & Direction
🌦 Short-Term & Detailed Forecasts

3. Global Airport Database 📍
The Global Airport Database provides location details for 9,300+ airports worldwide, covering both large and small airports.

Role in the Project
The AviationStack API lacked location details for flights, requiring an additional data source to map flight origins and destinations accurately.

Key Data Provided
📍 ICAO & IATA Codes
🛫 Airport Name, City, Country
🌍 Latitude, Longitude, Altitude

Challenges
⚠ Unclear Data Updates: The database does not specify update frequency or maintenance process, which could impact long-term reliability, especially when new airports are added.




Architecture Diagram

![image](https://github.com/user-attachments/assets/f39b3e6e-5b7f-48e0-bc79-8e250a812056)
