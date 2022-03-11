// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Databricks Partner Capstone Project
// MAGIC 
// MAGIC This optional capstone is included in the course to help you solidify key topics related to Databricks, Structured Streaming, and Delta.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Capstone Overview
// MAGIC 
// MAGIC In this project you will build a Delta Lake over incoming Streaming Data by using a series of Bronze, Silver, and Gold Tables. 
// MAGIC 
// MAGIC The Goal of the project is to gain actionable insights from a data lake, using a series of connected tables that: 
// MAGIC * Preserve the raw data
// MAGIC * Enrich the data by joining with additional static table
// MAGIC * Use Structured Streaming along with Delta tables to guarantee a robust solution
// MAGIC 
// MAGIC ### Scenario:
// MAGIC 
// MAGIC A video gaming company stores historical data in a data lake, which is growing exponentially. 
// MAGIC 
// MAGIC The data isn't sorted in any particular way (actually, it's quite a mess).
// MAGIC 
// MAGIC It is proving to be _very_ difficult to query and manage this data because there is so much of it.
// MAGIC 
// MAGIC 
// MAGIC ## Instructions
// MAGIC 1. Read in streaming data into Databricks Delta bronze tables
// MAGIC 2. Create Databricks Delta silver table
// MAGIC 3. Compute aggregate statistics about data i.e. create gold table

// COMMAND ----------

// MAGIC %md
// MAGIC ## Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our environment.

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC ### Set up paths
// MAGIC 
// MAGIC The cell below sets up relevant paths in DBFS.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> It also clears out this directory (to ensure consistent results if re-run). This operation can take several minutes.

// COMMAND ----------

val inputPath = "/mnt/training/gaming_data/mobile_streaming_events"

val basePath = userhome + "/capstone"
val outputPathBronze = basePath + "/gaming/bronze"
val outputPathSilver = basePath + "/gaming/silver"
val outputPathGold   = basePath + "/gaming/gold"

dbutils.fs.rm(basePath, true)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### SQL Table Setup
// MAGIC 
// MAGIC The follow cell drops a table that we'll be creating later in the notebook.
// MAGIC 
// MAGIC (Dropping the table prevents challenges involved if the notebook is run more than once.)

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS mobile_events_delta_gold;

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1: Prepare Schema and Read Streaming Data from input source
// MAGIC 
// MAGIC The input source is a folder containing 20 files of around 50 MB each. 
// MAGIC 
// MAGIC The stream is configured to read one file per trigger. 
// MAGIC 
// MAGIC Run this code to start the streaming read from the file directory. 

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, TimestampType, IntegerType}

lazy val eventSchema = StructType(List(
  StructField("eventName", StringType, true),
  StructField("eventParams", StructType(List(
    StructField("game_keyword", StringType, true),
    StructField("app_name", StringType, true),
    StructField("scoreAdjustment", IntegerType, true),
    StructField("platform", StringType, true),
    StructField("app_version", StringType, true),
    StructField("device_id", StringType, true),
    StructField("client_event_time", TimestampType, true),
    StructField("amount", DoubleType, true)
  )), true)
))

val gamingEventDF = (spark
  .readStream
  .schema(eventSchema) 
  .option("streamName","mobilestreaming_demo") 
  .option("maxFilesPerTrigger", 1)                // treat each file as Trigger event
  .json(inputPath) 
) 

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Step 2: Write Stream to Bronze Table
// MAGIC 
// MAGIC Write some code that performs the following tasks
// MAGIC 
// MAGIC * Write the stream from `gamingEventDF` -- the stream defined above -- to a bronze Delta table in path defined by `outputPathBronze`.
// MAGIC * Convert the input column `client_event_time` to a date format and rename the column to `eventDate`
// MAGIC * Filter out records with a null value in the `eventDate` column
// MAGIC * Make sure you provide a checkpoint directory that is unique to this stream
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Using `append` mode when streaming allows us to insert data indefinitely without rewriting already processed data.

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.functions.to_date

val eventsStream = gamingEventDF
  .withColumn("eventDate", to_date($"eventParams.client_event_time"))      
  .filter(($"eventDate").isNotNull) 
  .writeStream 
  .format("delta") 
  .option("checkpointLocation", outputPathBronze + "/_checkpoint") 
  .outputMode("append") 
  .queryName("bronze_stream")
  .start(outputPathBronze)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 3a: Load static data for enrichment
// MAGIC 
// MAGIC Register a static lookup table to associate `deviceId` with `deviceType` = `{android, ios}`.
// MAGIC 
// MAGIC While we refer to this as a lookup table, here we'll define it as a DataFrame. This will make it easier for us to define a join on our streaming data in the next step.
// MAGIC 
// MAGIC Create `deviceLookupDF` from data in `/mnt/training/gaming_data/dimensionData`.

// COMMAND ----------

// ANSWER
val lookupPath = "/mnt/training/gaming_data/dimensionData"

val deviceLookupDF = spark.read
  .format("delta")
  .load(lookupPath)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Step 3b: Create a streaming silver Delta table
// MAGIC 
// MAGIC A silver table is a table that combines, improves, or enriches bronze data. 
// MAGIC 
// MAGIC In this case we will join the bronze streaming data with some static data to add useful information. 
// MAGIC 
// MAGIC #### Steps to complete
// MAGIC 
// MAGIC Create a new stream by joining `deviceLookupDF` with the bronze table stored at `outputPathBronze` on `deviceId`.
// MAGIC * Make sure you do a streaming read and write
// MAGIC * Your selected fields should be:
// MAGIC   - `eventName`
// MAGIC   - `device_id`
// MAGIC   - `client_event_time`
// MAGIC   - `eventDate`
// MAGIC   - `deviceType`
// MAGIC * **NOTE**: some of these fields are nested; alias them to end up with a flat schema
// MAGIC * Write to `outputPathSilver`
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Don't forget to checkpoint your stream!

// COMMAND ----------

// ANSWER

spark.readStream
  .format("delta")
  .load(outputPathBronze)
  .select($"eventName", 
          ($"eventParams.device_id").as("device_id"), 
          ($"eventParams.client_event_time").as("client_event_time"), 
          $"eventDate")
  .join(deviceLookupDF, Seq("device_id"), "left")
  .writeStream 
  .format("delta") 
  .option("checkpointLocation", outputPathSilver + "/_checkpoint") 
  .outputMode("append") 
  .queryName("silver_stream")
  .start(outputPathSilver)    

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 4a: Batch Process a Gold Table from the Silver Table
// MAGIC 
// MAGIC The company executives want to look at the number of active users by week. They use SQL so our target will be a SQL table backed by a Delta Lake. 
// MAGIC 
// MAGIC The table should have the following columns:
// MAGIC - `WAU`: count of weekly active users (distinct device IDs grouped by week)
// MAGIC - `week`: week of year (the appropriate SQL function has been imported for you)
// MAGIC 
// MAGIC In the first step, calculate these 

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.functions.weekofyear

spark.readStream
  .format("delta")
  .load(outputPathSilver)
  .select($"device_id",
          weekofyear($"client_event_time").as("week")).distinct
  .groupBy($"week")
  .count
  .withColumnRenamed("count", "WAU")
  .writeStream 
  .format("delta") 
  .option("checkpointLocation", outputPathGold + "/_checkpoint") 
  .outputMode("complete") 
  .queryName("gold_stream")
  .start(outputPathGold)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC ### Step 4b: Register Gold SQL Table
// MAGIC 
// MAGIC By linking the Spark SQL table with the Delta Lake file path, we will always get results from the most current valid version of the streaming table.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> It may take some time for the previous streaming operations to start. 
// MAGIC 
// MAGIC Once they have started register a SQL table against the gold Delta Lake path. 
// MAGIC 
// MAGIC * tablename: `mobile_events_delta_gold`
// MAGIC * table Location: `outputPathGold`

// COMMAND ----------

// ANSWER
spark.sql(s"""
    CREATE TABLE IF NOT EXISTS mobile_events_delta_gold
    USING DELTA 
    LOCATION "%s" 
  """.format(outputPathGold))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Step 4c: Visualization
// MAGIC 
// MAGIC The company executives are visual people: they like pretty charts.
// MAGIC 
// MAGIC Create a bar chart out of `mobile_events_delta_gold` where the horizontal axis is month and the vertical axis is WAU.
// MAGIC 
// MAGIC Under <b>Plot Options</b>, use the following:
// MAGIC * <b>Keys:</b> `week`
// MAGIC * <b>Values:</b> `WAU`
// MAGIC 
// MAGIC In <b>Display type</b>, use <b>Bar Chart</b> and click <b>Apply</b>.
// MAGIC 
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/eLearning/Delta/plot-options-bar.png"/>
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> order by `week` to seek time-based patterns.

// COMMAND ----------

// MAGIC %sql
// MAGIC -- ANSWER
// MAGIC 
// MAGIC SELECT * FROM mobile_events_delta_gold
// MAGIC ORDER BY week

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 5: Wrap-up
// MAGIC 
// MAGIC * Stop streams

// COMMAND ----------

for (s <- spark.streams.active)
  s.stop()

// COMMAND ----------

// MAGIC %md
// MAGIC Congratulations: ALL DONE!!

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
