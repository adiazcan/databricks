# Databricks notebook source
# MAGIC %sql
# MAGIC select * from scoredflights

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  OriginAirportCode, Month, DayofMonth, CRSDepHour, Sum(prediction) NumDelays,
# MAGIC     CONCAT(Latitude, ',', Longitude) OriginLatLong
# MAGIC     FROM scoredflights s
# MAGIC     INNER JOIN airport_code_location_lookup_clean_csv a
# MAGIC     ON s.OriginAirportCode = a.Airport
# MAGIC     WHERE Month = 4
# MAGIC     GROUP BY OriginAirportCode, OriginLatLong, Month, DayofMonth, CRSDepHour
# MAGIC     Having Sum(prediction) > 1
# MAGIC     ORDER BY NumDelays DESC

# COMMAND ----------

summary = spark.sql("SELECT  OriginAirportCode, Month, DayofMonth, CRSDepHour, Sum(prediction) NumDelays,     CONCAT(Latitude, ',', Longitude) OriginLatLong FROM scoredflights s INNER JOIN airport_code_location_lookup_clean_csv a ON s.OriginAirportCode = a.Airport WHERE Month = 4 GROUP BY OriginAirportCode, OriginLatLong, Month, DayofMonth, CRSDepHour  Having Sum(prediction) > 1 ORDER BY NumDelays DESC")

# COMMAND ----------

summary.write.mode("overwrite").saveAsTable("flight_delays_summary")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from flight_delays_summary

# COMMAND ----------

