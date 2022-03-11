// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img src="https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png" style="float: left: margin: 20px"/>
// MAGIC 
// MAGIC # Structured Streaming
// MAGIC 
// MAGIC ## In this lesson you:
// MAGIC * Learn about Structured Streaming at a high level
// MAGIC 
// MAGIC ## Audience
// MAGIC * Primary Audience: Data Engineers
// MAGIC * Secondary Audience: Data Scientists, Software Engineers

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> The Problem</h2>
// MAGIC 
// MAGIC We have a stream of data coming in from a TCP-IP socket, Kafka, Kinesis or other sources...
// MAGIC 
// MAGIC The data is coming in faster than it can be consumed
// MAGIC 
// MAGIC How do we solve this problem?
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/drinking-from-the-fire-hose.png"/>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> The Micro-Batch Model</h2>
// MAGIC 
// MAGIC Many APIs solve this problem by employing a Micro-Batch model.
// MAGIC 
// MAGIC In this model, we take our firehose of data and collect data for a set interval of time (the **Trigger Interval**).
// MAGIC 
// MAGIC In our example, the **Trigger Interval** is two seconds.
// MAGIC 
// MAGIC <img style="width:100%" src="https://files.training.databricks.com/images/streaming-timeline.png"/>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Processing the Micro-Batch</h2>
// MAGIC 
// MAGIC For each interval, our job is to process the data from the previous [two-second] interval.
// MAGIC 
// MAGIC As we are processing data, the next batch of data is being collected for us.
// MAGIC 
// MAGIC In our example, we are processing two seconds worth of data in about one second.
// MAGIC 
// MAGIC <img style="width:100%" src="https://files.training.databricks.com/images/streaming-timeline-1-sec.png">

// COMMAND ----------

// MAGIC %md
// MAGIC ### What happens if we don't process the data fast enough when reading from a TCP/IP Stream?
// MAGIC 
// MAGIC In the case of a TCP/IP stream, we will most likely drop packets.
// MAGIC 
// MAGIC In other words, we would be losing data.
// MAGIC 
// MAGIC If this is an IoT device measuring the outside temperature every 15 seconds, this might be OK.
// MAGIC 
// MAGIC If this is a critical shift in stock prices, you could be out thousands of dollars.

// COMMAND ----------

// MAGIC %md
// MAGIC ### What happens if we don't process the data fast enough when reading from a pub/sub system like Kafka?
// MAGIC 
// MAGIC In the case of a pub/sub system, it simply means we fall further behind.
// MAGIC 
// MAGIC Eventually, the pub/sub system would reach resource limits inducing other problems.
// MAGIC 
// MAGIC However, we can always re-launch the cluster with enough cores to catch up and stay current.

// COMMAND ----------

// MAGIC %md
// MAGIC Our goal is simply to process the data for the previous interval before data from the next interval arrives.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> From Micro-Batch to Table</h2>
// MAGIC 
// MAGIC In Apache Spark, we treat such a stream of **micro-batches** as continuous updates to a table.
// MAGIC 
// MAGIC The developer then defines a query on this **input table**, as if it were a static table.
// MAGIC 
// MAGIC The computation on the input table is then pushed to a **results table**.
// MAGIC 
// MAGIC And finally, the results table is written to an output **sink**.
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/eLearning/Delta/stream2rows.png" style="height: 300px"/>

// COMMAND ----------

// MAGIC %md
// MAGIC In general, Spark Structured Streams consist of two parts:
// MAGIC * The **Input source** such as
// MAGIC   * Kafka
// MAGIC   * Azure Event Hub
// MAGIC   * Files on a distributed system
// MAGIC   * TCP-IP sockets
// MAGIC * And the **Sinks** such as
// MAGIC   * Kafka
// MAGIC   * Azure Event Hub
// MAGIC   * Various file formats
// MAGIC   * The system console
// MAGIC   * Apache Spark tables (memory sinks)
// MAGIC   * The completely custom `foreach()` iterator

// COMMAND ----------

// MAGIC %md
// MAGIC ### Update Triggers
// MAGIC Developers define **triggers** to control how frequently the **input table** is updated.
// MAGIC 
// MAGIC Each time a trigger fires, Spark checks for new data (new rows for the input table), and updates the result.
// MAGIC 
// MAGIC From the <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=trigger#pyspark.sql.streaming.DataStreamWriter.trigger" target="_blank">docs</a> for `DataStreamWriter.trigger(Trigger)`:
// MAGIC > The default value is ProcessingTime(0) and it will run the query as fast as possible.
// MAGIC 
// MAGIC And the process repeats in perpetuity.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Summary</h2>
// MAGIC 
// MAGIC A stream is a sequence of data that is made available over time. In Structured Streaming we treat a <b>stream</b> of data as a table to which data is continously appended.
// MAGIC 
// MAGIC The developer then defines a query on this input table, as if it were a static table, to compute a final result table that will be written to an output <b>sink</b>.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Additional Topics &amp; Resources</h2>
// MAGIC 
// MAGIC **Q:** Where can I find documentation on Structured Streaming?
// MAGIC **A:** See <a href="https://docs.databricks.com/spark/latest/structured-streaming/index.html" target="_blank">Structured Streaming Guide</a>.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
