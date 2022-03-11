# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations & Actions Lab (continued)
# MAGIC 
# MAGIC This lab is designed as an optional complement to the preceding lab, and contains additional operations to further solidify the concepts taught in this module.
# MAGIC 
# MAGIC In this lab, students will convert all temperatures to the same unit before calculating aggregates for monthly temperature at each station. A secondary investigation will examine whether or not these data can be used to draw conclusions about statewide temperatures during this period.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview of the Data
# MAGIC 
# MAGIC This lab reuses the weather data from the previous lab.
# MAGIC 
# MAGIC The data include multiple entries from a selection of weather stations, including average temperatures recorded in either Fahrenheit or Celcius. The schema for the table:
# MAGIC 
# MAGIC |ColumnName  | DataType| Description|
# MAGIC |------------|---------|------------|
# MAGIC |NAME        |string   | Station name |
# MAGIC |STATION     |string   | Unique ID |
# MAGIC |LATITUDE    |float    | Latitude |
# MAGIC |LONGITUDE   |float    | Longitude |
# MAGIC |ELEVATION   |float    | Elevation |
# MAGIC |DATE        |date     | YYYY-MM-DD |
# MAGIC |UNIT        |string   | Temperature units |
# MAGIC |TAVG        |float    | Average temperature |
# MAGIC 
# MAGIC While the total number of rows in this dataset would make manual exploration extremely inefficient, many aggregations on these data produce a small enough output for manual review.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confirm `weather` Table Exists and Register DataFrame
# MAGIC 
# MAGIC The `weather` table created in the preceding lab will persists between notebooks. The following logic checks for this table and then re-executes the logic that originally created this table if it does not exist.
# MAGIC 
# MAGIC The final line of the cell registers the DataFrame `weatherDF`. DataFrames do not persist between notebooks, so this is necessary for data manipulation using the DataFrames API.

# COMMAND ----------

try:
  spark.sql("SELECT * FROM weather LIMIT 1")
except:
  spark.sql("DROP TABLE IF EXISTS weather")

  schemaDDL = "NAME STRING, STATION STRING, LATITUDE FLOAT, LONGITUDE FLOAT, ELEVATION FLOAT, DATE DATE, UNIT STRING, TAVG FLOAT"

  sourcePath = "/mnt/training/weather/StationData/stationData.parquet/"

  tablePath = f"{userhome}/weather"

  (spark.read
    .format("parquet")
    .schema(schemaDDL)
    .load(sourcePath)
    .write
    .option("path", tablePath)
    .mode("overwrite")
    .saveAsTable("weather"))

weatherDF = spark.table("weather")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Needed Functions
# MAGIC 
# MAGIC As notebooks are isolated from one another in scope, methods to be used will need to be imported again. The cell below is pre-populated with those methods imported in the solution code for the previous notebook.

# COMMAND ----------

from pyspark.sql.functions import col, min, max, count # add additional methods as a comma-separated list

# Alternatively, you can import all functions by commenting out and executing the following line:

# from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Define a Function to Convert Temperature to Degrees Celcius
# MAGIC Define a function that will accept a column of floats/doubles as an argument and apply the Fahrenheit to Celcius conversion. This will be applied to the `TAVG` column in the next step. (Do not worry about handling conditional logic for units in this function.)
# MAGIC 
# MAGIC The conversion for Fahrenheit to Celcius is:
# MAGIC $$(F\degree - 32) * 5/9 = C\degree$$
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Writing functions that accept a column as an argument and call other functions from the Spark API will ensure the code compiles utilizing the Catalyst optimizer. While [UDFs](https://docs.databricks.com/spark/latest/spark-sql/udf-scala.html) can be registered to execute any desired logic, these will not take advantage of the planning optimization built into the DataFrames API. In Python, there will be an additional cost for serialization to pass data between Python and the JVM.

# COMMAND ----------

# TODO
<FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert Temperature to Degrees Celcius
# MAGIC Use the function defined in the last step to change all temperatures into the same unit.
# MAGIC 
# MAGIC The `UNIT` column indicates some temperatures are in Fahrenheit. Create a DataFrame with a new column with the temperatures for ALL records in Celcius.
# MAGIC 
# MAGIC New columns are created with the built-in `withColumn` method.
# MAGIC 
# MAGIC The [`when` function](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.when) is useful here to use conditionals to insert different values.

# COMMAND ----------

# TODO
<FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Calculate Aggregates for Each Station by Month
# MAGIC Create a DataFrame that reports the following monthy aggregates on `TAVG` in Celcius for each station:
# MAGIC - mean
# MAGIC - standard deviation
# MAGIC - max
# MAGIC - min
# MAGIC   
# MAGIC Order the results by mean `TAVG` descending, and rename each computed column with a descriptive name in snake_case.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> `alias` and `name` allow columns to be renamed.

# COMMAND ----------

# TODO
<FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC Using the interactive `display` functionality in Databricks, it's possible to dynamically change which field is being sorted on, as well as generate various plots.
# MAGIC 
# MAGIC Use the built-in plotting functionality to generate a plot that further aggregates by grouping all stations by month and displays the average minimum, maximum, and average temperature.

# COMMAND ----------

# TODO
<FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Convert Columns to Lower Case
# MAGIC Before saving out the data, it may be useful to convert away from uppercase column names for interoperability with other services.
# MAGIC 
# MAGIC The attribute `.columns` will list the names of all columns in the DataFrame
# MAGIC 
# MAGIC The DataFrame operation `.toDF` accepts a sequence of strings to rename all columns in a single operation. Operations like this can safely be written in native python/scala, as they do not operate in Spark and so won't introduce bottlenecks.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> `withColumnRenamed`, `alias`, and `name` can also be used to renamed columns individually.

# COMMAND ----------

# TODO
<FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save data
# MAGIC 
# MAGIC Using the provided path, write the data using parquet.

# COMMAND ----------

#TODO

monthlyWeatherPath = f"{userhome}/monthly-weather"
# <FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC # Explore Aggregates by State
# MAGIC 
# MAGIC The following operations drive toward calculating statewide aggregates for these data.
# MAGIC 
# MAGIC While aggregating previously aggregated values (in this case, average temperature) is often frowned upon for reducing variance, more important to the present investigation is the skew of the data distribution. While the small number of individual stations allows for manual review of data in this case, this section implements logic that would easily scale to any number of unique entities and allow programmatic checks to prevent publishing misleading reports.
# MAGIC 
# MAGIC Start back with the full data that has all temperatures in Celcius.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Extract the State from the Name Field and Save to a New Column
# MAGIC 
# MAGIC Suitable methods for substring extraction include `substring`, `regex_extract`, and `split`.
# MAGIC 
# MAGIC <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Try avoid using ad-hoc Python or Scala code. Keeping your code in the DataFrames/Spark SQL API will make sure the Catalyst optimizer is used.

# COMMAND ----------

# TODO
<FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Count Number of Records by State
# MAGIC Use this DataFrame to get a count of records in each state.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Also Calculate the Mean Temperature and Standard Deviation for Each State
# MAGIC Display the average and standard deviation of reported `TAVG` for each state alongside the count of records.

# COMMAND ----------

# MAGIC %md
# MAGIC Based on these statistics, is it safe to draw conclusions about statewide weather trends during this period?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
