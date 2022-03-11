// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Transformations & Actions Lab
// MAGIC 
// MAGIC ## Learning Objectives
// MAGIC In this lab, you will:
// MAGIC - Explore data to understand contents
// MAGIC - Validate data consistency
// MAGIC - Derive new fields from current data format
// MAGIC - Convert fields where necessary
// MAGIC - Calculate aggregates
// MAGIC - Save aggregate data
// MAGIC 
// MAGIC ### Skills Explored
// MAGIC * Explore and use DataFrames and SQL transformations
// MAGIC * Gain familiarity with various methods in `spark.sql.functions` by reading API docs
// MAGIC * Review how jobs are triggered in association with actions
// MAGIC * Reinforce concepts of how Spark executes logic against a data source

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

// MAGIC %md
// MAGIC ## Overview of the Data
// MAGIC 
// MAGIC This lab reuses the weather data from the previous lab.
// MAGIC 
// MAGIC The data include multiple entries from a selection of weather stations, including average temperatures recorded in either Fahrenheit or Celcius. The schema for the table:
// MAGIC 
// MAGIC |ColumnName  | DataType| Description|
// MAGIC |------------|---------|------------|
// MAGIC |NAME        |string   | Station name |
// MAGIC |STATION     |string   | Unique ID |
// MAGIC |LATITUDE    |float    | Latitude |
// MAGIC |LONGITUDE   |float    | Longitude |
// MAGIC |ELEVATION   |float    | Elevation |
// MAGIC |DATE        |date     | YYYY-MM-DD |
// MAGIC |UNIT        |string   | Temperature units |
// MAGIC |TAVG        |float    | Average temperature |
// MAGIC 
// MAGIC While the total number of rows in this dataset would make manual exploration extremely inefficient, many aggregations on these data produce a small enough output for manual review.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Register Table and Load Data to DataFrame
// MAGIC 
// MAGIC The following cell re-executes the logic from the last lab and ensures all students will have the same environment.
// MAGIC 
// MAGIC #### A Breakdown of Operations
// MAGIC 1. The first line drops the table `weather` if it exists. This ensures that no conflicts in the metastore will occur.
// MAGIC 1. The schema is defined in SQL DDL. Note that column names are provided in all caps to match the formatting of the source files.
// MAGIC 1. The `sourcePath` specifies the parquet data to be read. The source directory is read-only and was mounted above when running `Includes/Classroom-Setup`.
// MAGIC 1. The `tablePath` variable specifies where the files associated with the unmanaged table will be stored. The `userhome` portion points to a directory created with each student's username on the default object store (root DBFS) associated with the Databricks workspace.
// MAGIC 1. The multiline block of code that includes `spark.read...write...saveAsTable` specifies a source format, schema, and path for reading. The data from the source are copied to the destination `tablePath`, overwriting any data that may already exist in that directory. The table `weather` is registered to the specified files in the destination path using the schema provided. Note that this logic takes advantage of parquet being the default format when writing with Spark (the data written in the `tablePath` will be parquet files).
// MAGIC 1. The final line creates a DataFrame from the `weather` table.
// MAGIC 
// MAGIC The table `weather` and the DataFrame `weatherDF` share the same metadata definitions, meaning that both the schema and the files referenced are identical. This provides analogous access to the data through either the DataFrames API or Spark SQL. Changes to the data saved in the `tablePath` will be immediately reflected in subsequent queries through either of these APIs.

// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS weather")

val schemaDDL = "NAME STRING, STATION STRING, LATITUDE FLOAT, LONGITUDE FLOAT, ELEVATION FLOAT, DATE DATE, UNIT STRING, TAVG FLOAT"

val sourcePath = "/mnt/training/weather/StationData/stationData.parquet/"

val tablePath = s"$userhome/weather"

spark.read
  .format("parquet")
  .schema(schemaDDL)
  .load(sourcePath)
  .write
  .option("path", tablePath)
  .mode("overwrite")
  .saveAsTable("weather")

val weatherDF = spark.table("weather")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Import Needed Functions
// MAGIC 
// MAGIC Based on user preference, this lab can be completed using SQL, Python, or Scala. Remember that SQL queries and the DataFrames API can be bridged by using `spark.sql`, which will return a DataFrame object.
// MAGIC 
// MAGIC Many of the methods used for DataFrame transformations live within the `sql.functions` module. Links to the Scala and Python API docs are provided here:
// MAGIC 
// MAGIC - [pyspark.sql.functions API docs](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions)
// MAGIC - [Scala spark.sql.functions API docs](https://spark.apache.org/docs/2.2.1/api/java/org/apache/spark/sql/functions.html)
// MAGIC 
// MAGIC Note that the methods in each will compile to the same plan on execution. Some individuals report navigation of the Scala docs to be easier, even when coding in Python.
// MAGIC 
// MAGIC If coding in SQL, the built-in functions can be found [here](https://spark.apache.org/docs/2.3.1/api/sql/index.html).
// MAGIC 
// MAGIC Extending on SQL, _most_ of the operations from Hive DML are also supported; full docs [here](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML).
// MAGIC 
// MAGIC **The cell below currently only imports the `col` function. Feel free to append to the import list and re-execute this cell, or import functions as needed later in the lab.**

// COMMAND ----------

import org.apache.spark.sql.functions.{col} // add additional methods as a comma-separated list within the {}

// Alternatively, you can import all functions by commenting out and executing the following line:

// import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Preview 20 Lines of the Data
// MAGIC 
// MAGIC Begin by displaying 20 lines of the data to get an idea of how the data is formatted.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> `limit`, `show`, `head`, and `take` will all accomplish this, but will trigger different numbers of jobs.

// COMMAND ----------

// TODO

<FILL_IN>

// COMMAND ----------

// MAGIC %md
// MAGIC Limiting the view of the data to 20 lines is a transformation, but any time data is returned to display, an action (and **_at least 1_** job) will be triggered.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Define a New DataFrame or View Containing All Distinct Names
// MAGIC 
// MAGIC Because of lazy evaluation, DataFrames and views don't execute until an action is called against them.
// MAGIC 
// MAGIC Registering intermediate temp views or DataFrames essentially allow a set of transformations against a dataset to be given a name. This can be helpful when building up complex logic, as no data will be replicated when an intermediate state will be used multiple times in later queries.
// MAGIC 
// MAGIC Use the `distinct` command on the `NAME` column and save the result to a new DataFame named `uniqueNamesDF`. Note that your definition should not trigger a job.

// COMMAND ----------

// TODO

val uniqueNamesDF = <FILL_IN>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC Now return the count and display the unique names. Each of these is a separate action.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> `display()` will suppress any console output, so do this in 2 cells.

// COMMAND ----------

// MAGIC %md
// MAGIC Again, the lazy evaluation in Spark waits until these results need to be returned to trigger a job.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Confirm Station Information Consistency
// MAGIC The fields `NAME`, `STATION`, `LATITUDE`, `LONGITUDE`, and `ELEVATION` should remain consistent for each unique station throughout the data. 
// MAGIC If this is true, the count of distinct names should be equivalent to the count of the distinct combinations of these five columns in the present data. Write a query that confirms this using the DataFrame or view defined in the previous step as a point of reference.

// COMMAND ----------

// TODO
<FILL_IN>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Examine Date Range for Each Station
// MAGIC 
// MAGIC Create a DataFrame or view containing the earliest and latest date recorded for each station, alongside the total count of records.
// MAGIC 
// MAGIC After a `groupBy`, the `agg` function will allow multiple aggregate calls in a single query.
// MAGIC 
// MAGIC Make sure to `alias` the aggregate columns, as the default outputs will include parentheses (which aren't valid when saving to parquet).

// COMMAND ----------

// TODO
val stationAggDF =
<FILL_IN>

// COMMAND ----------

// MAGIC %md
// MAGIC Displaying the results of this aggregation to the notebook allows the user to manually review an interactive table of the results.

// COMMAND ----------

display(stationAggDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Stations have roughly the same number of records over the same 5 month period.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Save data
// MAGIC 
// MAGIC Using the provided path, write the data using [parquet file format](https://parquet.apache.org/), a columnar storage format that is [drastically better than CSV or JSON](https://databricks.com/session/spark-parquet-in-depth), especially when working in Spark.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> When first creating an unmanaged table with SQL, you can specify the format and location to save to. The main difference between this and using the DataFrameWriter (with `overwrite` mode) is that a table will be registered to the Hive metastore.
// MAGIC 
// MAGIC ```
// MAGIC CREATE TABLE table_name
// MAGIC USING format
// MAGIC LOCATION "/path/to/directory"
// MAGIC AS SELECT * FROM final_view;```

// COMMAND ----------

//TODO

val stationAggPath = s"$userhome/station-agg"
// <FILL_IN>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Wait--What Data is Being Written?
// MAGIC 
// MAGIC Throughout this notebook, many transformations and actions were called. The approach each individual took may vary, but actions were called every time:
// MAGIC - data was displayed
// MAGIC - counts were returned
// MAGIC - results were written to disk
// MAGIC 
// MAGIC These **actions** are easy to spot reviewing the notebook: if a job was triggered, an action occurred.
// MAGIC 
// MAGIC _Everything else_ is a transformation. This includes:
// MAGIC - reading data
// MAGIC - extracting strings
// MAGIC - grouping and aggregation
// MAGIC - creating new columns
// MAGIC - creating views
// MAGIC - defining new DataFrames from other DataFrames
// MAGIC 
// MAGIC When the final write is triggered, Spark looks back to the source (here the files associated with the `weather` table) and creates a plan against those data. **All the computed values, DataFrame definitons, and views of the data returned to the notebook are ignored.** The final DataFrame still indicates a series of transformations against files on disk, just as the first transformation (the `read` operation) referred to these files. While some stages or tasks may be skipped because of implicit caching, Spark will always use your original data source as the single point of truth when building out the physical plan for execution.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Synopsis
// MAGIC 
// MAGIC This notebook explored:
// MAGIC * How to use DataFrames and SQL transformations
// MAGIC * Implementing various methods in `spark.sql.functions` (which likely required reading API docs)
// MAGIC * How **actions** trigger jobs while transformations do not
// MAGIC * Which data is referenced as transformations and actions are executed

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
