// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # <img src="https://files.training.databricks.com/images/DeltaLake-logo.png" width=80px> Delta Lake Architecture
// MAGIC 
// MAGIC ## Learning Objectives
// MAGIC By the end of this lesson, you should be able to:
// MAGIC * Discuss the advantages of Delta over a traditional Lambda architecture
// MAGIC * Describe Bronze, Gold, and Silver tables
// MAGIC * Unify batch jobs and streaming jobs using Delta Lake
// MAGIC * Create a Delta Lake pipeline
// MAGIC * Describe how Delta ensures ACID compliant transactions
// MAGIC 
// MAGIC This notebook demonstrates using Delta Lakes as an optimization layer on top of blob storage to ensure reliability (i.e. ACID compliance) and low latency within unified Streaming + Batch data pipelines.
// MAGIC 
// MAGIC We will discuss this relative to a traditional Lamdba architecture.
// MAGIC 
// MAGIC <img src=https://files.training.databricks.com/images/adbcore/delta_azure.png width=800px>
// MAGIC 
// MAGIC An example of a Delta Lake Architecture might be the above.
// MAGIC 
// MAGIC 1. many **devices** generate data across different ingestion paths.
// MAGIC 2. streaming data can be ingested from **IOT Hub** or **Event Hub**
// MAGIC 3. batch data can be ingested by **Azure Data Factory** or **Databricks**
// MAGIC 4. Extracted, Transformed data is loaded into a **Delta Lake**

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Lambda Architecture
// MAGIC 
// MAGIC The Lambda architecture is a big data processing architecture that combines both batch- and real-time processing methods.
// MAGIC It features an append-only immutable data source that serves as system of record. Timestamped events are appended to
// MAGIC existing events (nothing is overwritten). Data is implicitly ordered by time of arrival.
// MAGIC 
// MAGIC Notice how there are really two pipelines here, one batch and one streaming, hence the name <i>lambda</i> architecture.
// MAGIC 
// MAGIC It is very difficult to combine processing of batch and real-time data as is evidenced by the diagram below.
// MAGIC 
// MAGIC 
// MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/lambda.png" style="height: 400px"/></div><br/>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Delta Lake Architecture
// MAGIC 
// MAGIC The Delta Lake Architecture is a vast improvmemt upon the traditional Lambda architecture. At each stage, we enrich our data through a unified pipeline that allows us to combine batch and streaming workflows through a shared filestore with ACID compliant transactions.
// MAGIC 
// MAGIC **Bronze** tables contain raw data ingested from various sources (JSON files, RDBMS data,  IoT data, etc.).
// MAGIC 
// MAGIC **Silver** tables will provide a more refined view of our data. We can join fields from various bronze tables to enrich streaming records, or update account statuses based on recent activity.
// MAGIC 
// MAGIC **Gold** tables provide business level aggregates often used for reporting and dashboarding. This would include aggregations such as daily active website users, weekly sales per store, or gross revenue per quarter by department. 
// MAGIC 
// MAGIC The end outputs are actionable insights, dashboards and reports of business metrics.
// MAGIC 
// MAGIC <img src=https://files.training.databricks.com/images/adbcore/delta/bronze_silver_gold.png width=800px>
// MAGIC 
// MAGIC 
// MAGIC By considering our business logic at all steps of the ETL pipeline, we can ensure that storage and compute costs are optimized by reducing unnecessary duplication of data and limiting ad hoc querying against full historic data.
// MAGIC 
// MAGIC Each stage can be configured as a batch or streaming job, and ACID transactions ensure that we succeed or fail completely.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/DeltaLake-logo.png" width="80px"/>
// MAGIC 
// MAGIC # Unifying Structured Streaming with Batch Jobs with Delta Lake
// MAGIC 
// MAGIC In this notebook, we will explore combining streaming and batch processing with a single pipeline. We will begin by defining the following logic:
// MAGIC 
// MAGIC - ingest streaming JSON data from disk and write it to a Delta Lake Table `/activity/Bronze`
// MAGIC - perform a Stream-Static Join on the streamed data to add additional geographic data
// MAGIC - transform and load the data, saving it out to our Delta Lake Table `/activity/Silver`
// MAGIC - summarize the data through aggregation into the Delta Lake Table `/activity/Gold/groupedCounts`
// MAGIC - materialize views of our gold table through streaming plots and static queries
// MAGIC 
// MAGIC We will then demonstrate that by writing batches of data back to our bronze table, we can trigger the same logic on newly loaded data and propagate our changes automatically.

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC Set shuffle partitions to match the number of executor cores in the cluster.

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC sqlContext.setConf("spark.sql.shuffle.partitions", "8")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Set up relevant Delta Lake paths
// MAGIC 
// MAGIC These paths will serve as the file locations for our Delta Lake tables.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Each streaming write has its own checkpoint directory.
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> You cannot write out new Delta files within a repository that contains Delta files. Note that our hierarchy here isolates each Delta table into its own directory.

// COMMAND ----------

lazy val activityPath = userhome + "/activity"

lazy val activityBronzePath = activityPath + "/Bronze"
lazy val activityBronzeCheckpoint = activityBronzePath + "/checkpoint"

lazy val activitySilverPath = activityPath + "/Silver"
lazy val activitySilverCheckpoint = activitySilverPath + "/checkpoint"

lazy val activityGoldPath = activityPath + "/Gold"
lazy val groupedCountPath = activityGoldPath + "/groupedCount"
lazy val groupedCountCheckpoint = groupedCountPath + "/checkpoint"

// COMMAND ----------

// MAGIC %md
// MAGIC ## Reset Pipeline
// MAGIC 
// MAGIC To reset the pipeline, run the following:

// COMMAND ----------

dbutils.fs.rm(activityPath, true)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Datasets Used
// MAGIC This notebook will consume cell phone accelerometer data. Records have been downsampled so that the streaming data represents less than 3% of the total data being produced. The remainder will be processed as batches.
// MAGIC 
// MAGIC The following fields are present:
// MAGIC 
// MAGIC - `Index`
// MAGIC - `Arrival_Time`
// MAGIC - `Creation_Time`
// MAGIC - `x`
// MAGIC - `y`
// MAGIC - `z`
// MAGIC - `User`
// MAGIC - `Model`
// MAGIC - `Device`
// MAGIC - `gt`
// MAGIC - `geolocation`
// MAGIC 
// MAGIC ## Define Schema
// MAGIC 
// MAGIC For streaming jobs, we need to define our schema before we start.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We'll reuse this same schema later in the notebook to define our batch processing, which will eliminate the jobs triggered by eliminating a file scan AND enforce the schema that we've defined here.

// COMMAND ----------

import org.apache.spark.sql.types.{StructField, StructType, LongType, StringType, DoubleType}

val schema = 
StructType(List(
  StructField(s"Arrival_Time",LongType,true),
  StructField(s"Creation_Time",LongType,true),
  StructField(s"Device",StringType,true),
  StructField(s"Index",LongType,true),
  StructField(s"Model",StringType,true),
  StructField(s"User",StringType,true),
  StructField(s"geolocation",StructType(List(
    StructField(s"city",StringType,true),
    StructField(s"country",StringType,true)
  )),true),
  StructField(s"gt",StringType,true),
  StructField(s"id",LongType,true),
  StructField(s"x",DoubleType,true),
  StructField(s"y",DoubleType,true),
  StructField(s"z",DoubleType,true)))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC ### Define Streaming Load from Files in Blob
// MAGIC 
// MAGIC Our streaming source directory has 36 JSON files of 5k records each saved in a repository. Here, we'll trigger processing on files one at a time. 
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> In a production setting, this same logic would allow us to only read new files written to our source directory. We could define `maxFilesPerTrigger` to control the amount of data we consume with each load, or omit this option to consume all new data on disk since the last time the stream has processed.

// COMMAND ----------

val rawEventsDF = spark
  .readStream
  .format("json")
  .schema(schema)
  .option("maxFilesPerTrigger", 1)
  .load("/mnt/training/definitive-guide/data/activity-json/streaming")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### WRITE Stream using Delta Lake
// MAGIC 
// MAGIC #### General Notation
// MAGIC Use this format to write a streaming job to a Delta Lake table.
// MAGIC 
// MAGIC <pre>
// MAGIC (myDF
// MAGIC   .writeStream
// MAGIC   .format("delta")
// MAGIC   .option("checkpointLocation", checkpointPath)
// MAGIC   .outputMode("append")
// MAGIC   .start(path)
// MAGIC )
// MAGIC </pre>
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> While we _can_ write directly to tables using the `.table()` notation, this will create fully managed tables by writing output to a default location on DBFS. This is not best practice for production jobs.
// MAGIC 
// MAGIC #### Output Modes
// MAGIC Notice, besides the "obvious" parameters, specify `outputMode`, which can take on these values
// MAGIC * `append`: add only new records to output sink
// MAGIC * `complete`: rewrite full output - applicable to aggregations operations
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> At present, `update` mode is **not** supported for streaming Delta jobs.
// MAGIC 
// MAGIC #### Checkpointing
// MAGIC 
// MAGIC When defining a Delta Lake streaming query, one of the options that you need to specify is the location of a checkpoint directory.
// MAGIC 
// MAGIC `.writeStream.format("delta").option("checkpointLocation", <path-to-checkpoint-directory>) ...`
// MAGIC 
// MAGIC This is actually a structured streaming feature. It stores the current state of your streaming job.
// MAGIC 
// MAGIC Should your streaming job stop for some reason and you restart it, it will continue from where it left off.
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> If you do not have a checkpoint directory, when the streaming job stops, you lose all state around your streaming job and upon restart, you start from scratch.
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Also note that every streaming job should have its own checkpoint directory: no sharing.

// COMMAND ----------

rawEventsDF
  .writeStream
  .format("delta")
  .option("checkpointLocation", activityBronzeCheckpoint)
  .outputMode("append")
  .start(activityBronzePath)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Load Static Lookup Table
// MAGIC 
// MAGIC Before enriching our bronze data, we will load a static lookup table for our country codes.
// MAGIC 
// MAGIC Here, we'll use a parquet file that contains countries and their associated codes and abbreviations.
// MAGIC 
// MAGIC While we can load this as a table (which will copy all files to the workspace and make it available to all users), here we'll manipulate it as a DataFrame.

// COMMAND ----------

lazy val geoForLookupDF = spark
  .read
  .format("parquet")
  .load("/mnt/training/countries/ISOCountryCodes/ISOCountryLookup.parquet/")
  .select($"EnglishShortName".alias(s"country"), $"alpha3Code".alias(s"countryCode3"))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC ## Create QUERY tables (aka "silver tables")
// MAGIC 
// MAGIC Our current bronze table contains nested fields, as well as time data that has been encoded in non-standard unix time (`Arrival_Time` is encoded as milliseconds from epoch, while `Creation_Time` records nanoseconds between record creation and receipt). 
// MAGIC 
// MAGIC We also wish to enrich our data with 3 letter country codes for mapping purposes, which we'll obtain from a join with our `geoForLookupDF`.
// MAGIC 
// MAGIC In order to parse the data in human-readable form, we create query/silver tables out of the raw data.
// MAGIC 
// MAGIC We will stream from our previous file write, define transformations, and rewrite our data to disk.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Notice how we do not need to specify a schema when loading Delta files: it is inferred from the metadata!
// MAGIC 
// MAGIC The fields of a complex object can be referenced with a "dot" notation as in:
// MAGIC 
// MAGIC 
// MAGIC `$"geolocation.country"`
// MAGIC 
// MAGIC A large number of these fields/columns can become unwieldy.
// MAGIC 
// MAGIC For that reason, it is common to extract the sub-fields and represent them as first-level columns as seen below:

// COMMAND ----------

import org.apache.spark.sql.functions.from_unixtime

val parsedEventsDF = spark.readStream
  .format("delta")
  .load(activityBronzePath)
  .select(from_unixtime($"Arrival_Time"/1000).as("Arrival_Time").cast("timestamp"),
          ($"Creation_Time"/1E9).as("Creation_Time").cast("timestamp"),
          $"Device",
          $"Index",
          $"Model",
          $"User",
          $"gt",
          $"x",
          $"y",
          $"z",
          $"geolocation.country".as("country"),
          $"geolocation.city".as("city"))
  .join(geoForLookupDF, Seq("country"), "left")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Write to QUERY Tables (aka "silver tables")

// COMMAND ----------

parsedEventsDF
  .writeStream
  .format("delta")
  .option("checkpointLocation", activitySilverCheckpoint)
  .outputMode("append")
  .start(activitySilverPath)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC #### See list of active streams.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You should currently see two active streams, one for each streaming write that you've triggered. If you have called `display` on either of your streaming DataFrames, you will see an additional stream, as `display` writes the stream to memory.

// COMMAND ----------

spark.streams.active.map(_.id).foreach(println)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Gold Table: Grouped Count of Events
// MAGIC 
// MAGIC Here we read a stream of data from `activitySilverPath` and write another stream to `activityGoldPath/groupedCount`.
// MAGIC 
// MAGIC The data consists of a total counts of all event, grouped by `hour`, `gt`, and `countryCode3`.
// MAGIC 
// MAGIC Performing this aggregation allows us to reduce the total number of rows in our table from hundreds of thousands (or millions, once we've loaded our batch data) to dozens.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Notice that we're writing to a named directory within our gold path. If we wish to define additional aggregations, we would organize these parallel to thie directory to avoid metadata write conflicts.

// COMMAND ----------

import org.apache.spark.sql.functions.{window,hour}


spark.readStream
  .format("delta")
  .load(activitySilverPath)
  .groupBy(window($"Arrival_Time", "60 minute"),$"gt", $"countryCode3")
  .count()
  .withColumn("hour",hour($"window.start"))
  .drop($"window")
  .writeStream
  .format("delta")
  .option("checkpointLocation", groupedCountCheckpoint)
  .outputMode("complete")
  .start(groupedCountPath)

// COMMAND ----------

// MAGIC %md
// MAGIC ### CREATE A Table Using Delta Lake
// MAGIC 
// MAGIC Create a table called `gt_count` using `DELTA` out of the above data.
// MAGIC 
// MAGIC NOTE: You will not be able to run this command until the `activityCountsQuery` has initialized.

// COMMAND ----------

spark.sql(s"""
  DROP TABLE IF EXISTS grouped_count
""")
spark.sql(s"""
  CREATE TABLE grouped_count
  USING DELTA
  LOCATION "$groupedCountPath"
""")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC #### Important Considerations for `complete` Output with Delta
// MAGIC 
// MAGIC When using `complete` output mode, we rewrite the entire state of our table each time our logic runs. While this is ideal for calculating aggregates, we **cannot** read a stream from this directory, as Structured Streaming assumes data is only being appended in the upstream logic.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Certain options can be set to change this behavior, but have other limitations attached. For more details, refer to [Delta Streaming: Ignoring Updates and Deletes](https://docs.databricks.com/delta/delta-streaming.html#ignoring-updates-and-deletes).
// MAGIC 
// MAGIC The gold Delta table we have just registered will perform a static read of the current state of the data each time we run the following query.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM grouped_count

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Materialized View: Windowed Count of Hourly `gt` Events
// MAGIC 
// MAGIC Plot the occurrence of all events grouped by `gt`.
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Because we're using `complete` output mode for our gold table write, we cannot define a streaming plot on these files.
// MAGIC 
// MAGIC Instead, we'll define a temp table based on the files written to our silver table. We will them use this table to execute our streaming queries.
// MAGIC 
// MAGIC In order to create a LIVE bar chart of the data, you'll need to fill out the <b>Plot Options</b> as shown:
// MAGIC 
// MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/ch5-plot-options.png"/></div><br/>
// MAGIC 
// MAGIC ### Note on Gold Tables & Materialized Views
// MAGIC 
// MAGIC When we call `display` on a streaming DataFrame or execute a SQL query on a streaming view, we are using memory as our sink. 
// MAGIC 
// MAGIC In this case, we have already calculated all the values necessary to materialize our streaming view above in the gold table we've written to disk. 
// MAGIC 
// MAGIC **However**, we re-execute this logic on our silver table to generate streaming views, as structured streaming will not support reads from upstream files that have beem overwritten.

// COMMAND ----------

spark.readStream
  .format("delta")
  .load(activitySilverPath)
  .createOrReplaceTempView("query_table")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT gt, HOUR(Arrival_Time) hour, COUNT(*) total_events
// MAGIC FROM query_table
// MAGIC GROUP BY gt, HOUR(Arrival_Time)
// MAGIC ORDER BY hour

// COMMAND ----------

// MAGIC %md
// MAGIC ## Batch Load Data into Bronze Table
// MAGIC 
// MAGIC We can use the same pipeline to process batch data.
// MAGIC 
// MAGIC By loading our raw data into our bronze table, we will push it through our already running streaming logic.
// MAGIC 
// MAGIC Here, we'll run 4 batches of around 170k records. We can track each batch through our streaming plots above.

// COMMAND ----------

for (batch <- List.range(0, 4))
  spark
    .read
    .format("json")
    .schema(schema)
    .load("/mnt/training/definitive-guide/data/activity-json/batch-%s".format(batch))
    .write
    .format("delta")
    .mode("append")
    .save(activityBronzePath)

// COMMAND ----------

// MAGIC %md
// MAGIC Note that even on our small cluster, we can pass a batch of over 5 million records through our logic above without problems. 

// COMMAND ----------

spark
  .read
  .format("json")
  .schema(schema)
  .load("/mnt/training/definitive-guide/data/activity-json/batch")
  .write
  .format("delta")
  .mode("append")
  .save(activityBronzePath)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> While our streaming materialized view above updates as data flows in, we can also easily generate this view from our `grouped_count` table. 
// MAGIC 
// MAGIC We will need to re-run this query each time we wish to update the data. Run the below query now, and then after your batch has finished processing.
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> The state reflected in a query on a registered Delta table will always reflect the most recent valid state of the files.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM grouped_count

// COMMAND ----------

// MAGIC %md
// MAGIC ## Wrapping Up
// MAGIC 
// MAGIC Finally, make sure all streams are stopped.

// COMMAND ----------

import org.apache.spark.sql.streaming.StreamingQuery
spark.streams.active.foreach((s: StreamingQuery) => s.stop())

// COMMAND ----------

// MAGIC %md
// MAGIC ## Summary
// MAGIC 
// MAGIC Delta Lake is ideally suited for use in streaming data lake contexts.
// MAGIC 
// MAGIC Use the Delta Lake architecture to craft raw, query and summary tables to produce beautiful visualizations of key business metrics.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional Topics & Resources
// MAGIC 
// MAGIC * <a href="https://docs.databricks.com/delta/delta-streaming.html#as-a-sink" target="_blank">Delta Streaming Write Notation</a>
// MAGIC * <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#" target="_blank">Structured Streaming Programming Guide</a>
// MAGIC * <a href="https://www.youtube.com/watch?v=rl8dIzTpxrI" target="_blank">A Deep Dive into Structured Streaming</a> by Tagatha Das. This is an excellent video describing how Structured Streaming works.
// MAGIC * <a href="http://lambda-architecture.net/#" target="_blank">Lambda Architecture</a>
// MAGIC * <a href="https://bennyaustin.wordpress.com/2010/05/02/kimball-and-inmon-dw-models/#" target="_blank">Data Warehouse Models</a>
// MAGIC * <a href="https://people.apache.org//~pwendell/spark-nightly/spark-branch-2.1-docs/latest/structured-streaming-kafka-integration.html#" target="_blank">Reading structured streams from Kafka</a>
// MAGIC * <a href="http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#creating-a-kafka-source-stream#" target="_blank">Create a Kafka Source Stream</a>
// MAGIC * <a href="https://docs.databricks.com/delta/delta-intro.html#case-study-multi-hop-pipelines#" target="_blank">Multi Hop Pipelines</a>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>