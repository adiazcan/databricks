# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Structured Streaming with Azure EventHubs 
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Establish a connection with Event Hubs in Spark
# MAGIC * Subscribe to and configure an Event Hubs stream
# MAGIC * Parse JSON records from Event Hubs
# MAGIC 
# MAGIC 
# MAGIC ## Datasets Used
# MAGIC This notebook will consumn data being published through an EventHub with the following schema:
# MAGIC 
# MAGIC - `Index`
# MAGIC - `Arrival_Time`
# MAGIC - `Creation_Time`
# MAGIC - `x`
# MAGIC - `y`
# MAGIC - `z`
# MAGIC - `User`
# MAGIC - `Model`
# MAGIC - `Device`
# MAGIC - `gt`
# MAGIC - `id`
# MAGIC - `geolocation`
# MAGIC 
# MAGIC ## Library Requirements
# MAGIC 
# MAGIC 1. the Maven library with coordinate `com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.7`
# MAGIC    - this allows Databricks `spark` session to communicate with an Event Hub
# MAGIC 2. the Python library `azure-eventhub`
# MAGIC    - this is allows the Python kernel to stream content to an Event Hub
# MAGIC 
# MAGIC Documentation on how to install Python libraries:
# MAGIC https://docs.azuredatabricks.net/user-guide/libraries.html#pypi-libraries
# MAGIC 
# MAGIC Documentation on how to install Maven libraries:
# MAGIC https://docs.azuredatabricks.net/user-guide/libraries.html#maven-or-spark-package

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC The following cell sets up a local streaming file read that we'll be writing to Event Hubs.

# COMMAND ----------

# MAGIC %run ./Includes/Streaming-Demo-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Azure Event Hubs</h2>
# MAGIC 
# MAGIC Microsoft Azure Event Hubs is a fully managed, real-time data ingestion service.
# MAGIC You can stream millions of events per second from any source to build dynamic data pipelines and immediately respond to business challenges.
# MAGIC It integrates seamlessly with a host of other Azure services.
# MAGIC 
# MAGIC Event Hubs can be used in a variety of applications such as
# MAGIC * Anomaly detection (fraud/outliers)
# MAGIC * Application logging
# MAGIC * Analytics pipelines, such as clickstreams
# MAGIC * Archiving data
# MAGIC * Transaction processing
# MAGIC * User telemetry processing
# MAGIC * Device telemetry streaming
# MAGIC * <b>Live dashboarding</b>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Define Connection Strings
# MAGIC 
# MAGIC This cell defines the necessary connection strings.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> This is **demo-only**.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC event_hub_name = "activity-data"
# MAGIC connection_string = dbutils.secrets.get(scope="instructor", key="eventhubkey") + ";EntityPath=" + event_hub_name
# MAGIC 
# MAGIC print("Consumer Connection String: {}".format(connection_string))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Stream to Event Hub to Produce Stream

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC ehWriteConf = {
# MAGIC   'eventhubs.connectionString' : connection_string
# MAGIC }
# MAGIC 
# MAGIC checkpointPath = userhome + "/event-hub/write-checkpoint"
# MAGIC dbutils.fs.rm(checkpointPath,True)
# MAGIC 
# MAGIC (activityStreamDF
# MAGIC   .writeStream
# MAGIC   .format("eventhubs")
# MAGIC   .options(**ehWriteConf)
# MAGIC   .option("checkpointLocation", checkpointPath)
# MAGIC   .start())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Event Hubs Configuration</h2>
# MAGIC 
# MAGIC Assemble the following:
# MAGIC * A `startingEventPosition` as a JSON string
# MAGIC * An `EventHubsConf`
# MAGIC   * to include a string with connection credentials
# MAGIC   * to set a starting position for the stream read
# MAGIC   * to throttle Event Hubs' processing of the streams

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import json
# MAGIC 
# MAGIC # Create the starting position Dictionary
# MAGIC startingEventPosition = {
# MAGIC   "offset": "-1",
# MAGIC   "seqNo": -1,            # not in use
# MAGIC   "enqueuedTime": None,   # not in use
# MAGIC   "isInclusive": True
# MAGIC }
# MAGIC 
# MAGIC eventHubsConf = {
# MAGIC   "eventhubs.connectionString" : connection_string,
# MAGIC   "eventhubs.startingPosition" : json.dumps(startingEventPosition),
# MAGIC   "setMaxEventsPerTrigger": 100
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC ### READ Stream using EventHub
# MAGIC 
# MAGIC The `readStream` method is a <b>transformation</b> that outputs a DataFrame with specific schema specified by `.schema()`. 

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.functions import col
# MAGIC 
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
# MAGIC 
# MAGIC eventStreamDF = (spark.readStream
# MAGIC   .format("eventhubs")
# MAGIC   .options(**eventHubsConf)
# MAGIC   .load()
# MAGIC )
# MAGIC 
# MAGIC eventStreamDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Most of the fields in this response are metadata describing the state of the Event Hubs stream. We are specifically interested in the `body` field, which contains our JSON payload.
# MAGIC 
# MAGIC Noting that it's encoded as binary, as we select it, we'll cast it to a string.

# COMMAND ----------

# MAGIC %python
# MAGIC bodyDF = eventStreamDF.select(col("body").cast("STRING"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Each line of the streaming data becomes a row in the DataFrame once an <b>action</b> such as `writeStream` is invoked.
# MAGIC 
# MAGIC Notice that nothing happens until you engage an action, i.e. a `display()` or `writeStream`.

# COMMAND ----------

# MAGIC %python
# MAGIC display(bodyDF, streamName= "bodyDF")

# COMMAND ----------

# MAGIC %md
# MAGIC While we can see our JSON data now that it's cast to string type, we can't directly manipulate it.
# MAGIC 
# MAGIC Before proceeding, stop this stream. We'll continue building up transformations against this streaming DataFrame, and a new action will trigger an additional stream.

# COMMAND ----------

# MAGIC %python
# MAGIC for s in spark.streams.active:
# MAGIC   if s.name == "bodyDF":
# MAGIC     s.stop()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## <img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Parse the JSON payload
# MAGIC 
# MAGIC The EventHub acts as a sort of "firehose" (or asynchronous buffer) and displays raw data in the JSON format.
# MAGIC 
# MAGIC If desired, we could save this as raw bytes or strings and parse these records further downstream in our processing.
# MAGIC 
# MAGIC Here, we'll directly parse our data so we can interact with the fields.
# MAGIC 
# MAGIC The first step is to define the schema for the JSON payload.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Both time fields are encoded as `LongType` here because of non-standard formatting.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType
# MAGIC 
# MAGIC schema = StructType([
# MAGIC   StructField("Arrival_Time", LongType(), True),
# MAGIC   StructField("Creation_Time", LongType(), True),
# MAGIC   StructField("Device", StringType(), True),
# MAGIC   StructField("Index", LongType(), True),
# MAGIC   StructField("Model", StringType(), True),
# MAGIC   StructField("User", StringType(), True),
# MAGIC   StructField("gt", StringType(), True),
# MAGIC   StructField("x", DoubleType(), True),
# MAGIC   StructField("y", DoubleType(), True),
# MAGIC   StructField("z", DoubleType(), True),
# MAGIC   StructField("geolocation", StructType([
# MAGIC     StructField("PostalCode", StringType(), True),
# MAGIC     StructField("StateProvince", StringType(), True),
# MAGIC     StructField("city", StringType(), True),
# MAGIC     StructField("country", StringType(), True)
# MAGIC   ]), True),
# MAGIC   StructField("id", StringType(), True)
# MAGIC ])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Parse the data
# MAGIC 
# MAGIC Next we can use the function `from_json` to parse out the full message with the schema specified above.
# MAGIC 
# MAGIC When parsing a value from JSON, we end up with a single column containing a complex object.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.functions import col, from_json
# MAGIC 
# MAGIC parsedEventsDF = bodyDF.select(
# MAGIC   from_json(col("body"), schema).alias("json"))
# MAGIC 
# MAGIC parsedEventsDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Note that we can further parse this to flatten the schema entirely and properly cast our time fields.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.functions import from_unixtime
# MAGIC 
# MAGIC flatSchemaDF = (parsedEventsDF
# MAGIC   .select(from_unixtime(col("json.Arrival_Time")/1000).alias("Arrival_Time").cast("timestamp"),
# MAGIC           (col("json.Creation_Time")/1E9).alias("Creation_Time").cast("timestamp"),
# MAGIC           col("json.Device").alias("Device"),
# MAGIC           col("json.Index").alias("Index"),
# MAGIC           col("json.Model").alias("Model"),
# MAGIC           col("json.User").alias("User"),
# MAGIC           col("json.gt").alias("gt"),
# MAGIC           col("json.x").alias("x"),
# MAGIC           col("json.y").alias("y"),
# MAGIC           col("json.z").alias("z"),
# MAGIC           col("json.id").alias("id"),
# MAGIC           col("json.geolocation.country").alias("country"),
# MAGIC           col("json.geolocation.city").alias("city"),
# MAGIC           col("json.geolocation.PostalCode").alias("PostalCode"),
# MAGIC           col("json.geolocation.StateProvince").alias("StateProvince"))
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC This flat schema provides us the ability to view each nested field as a column.

# COMMAND ----------

# MAGIC %python
# MAGIC display(flatSchemaDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Stop all active streams

# COMMAND ----------

# MAGIC %python
# MAGIC for s in spark.streams.active:
# MAGIC   s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Event Hubs FAQ
# MAGIC 
# MAGIC This [FAQ](https://github.com/Azure/azure-event-hubs-spark/blob/master/FAQ.md) can be an invaluable reference for occasional Spark-EventHub debugging.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
