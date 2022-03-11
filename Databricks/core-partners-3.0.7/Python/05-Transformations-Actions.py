# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations & Actions
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lessons, you should be able to:
# MAGIC * Describe the difference between eager and lazy execution
# MAGIC * Define and identify transformations
# MAGIC * Define and identify actions
# MAGIC * Describe the fundamentals of how the Catalyst Optimizer works
# MAGIC * Discriminate between wide and narrow transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Laziness By Design
# MAGIC 
# MAGIC Fundamental to Apache Spark are the notions that
# MAGIC * Transformations are **LAZY**
# MAGIC * Actions are **EAGER**
# MAGIC 
# MAGIC The following code condenses the logic from the preceding lab and uses the DataFrames API to:
# MAGIC - Specify a schema, format, and file source for the data to be loaded
# MAGIC - Select columns to `GROUP BY`
# MAGIC - Aggregate with a `COUNT`
# MAGIC - Provide an alias name for the aggregate output
# MAGIC - Specify a column to sort on
# MAGIC 
# MAGIC This cell defines a series of **transformations**. By definition, this logic will result in a DataFrame and will not trigger any jobs.

# COMMAND ----------

schemaDDL = "NAME STRING, STATION STRING, LATITUDE FLOAT, LONGITUDE FLOAT, ELEVATION FLOAT, DATE DATE, UNIT STRING, TAVG FLOAT"

sourcePath = "/mnt/training/weather/StationData/stationData.parquet/"

countsDF = (spark.read
  .format("parquet")
  .schema(schemaDDL)
  .load(sourcePath)
  .groupBy("NAME", "UNIT").count()
  .withColumnRenamed("count", "counts")
  .orderBy("NAME")
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Because `display` is an **action**, a job _will_ be triggered, as logic is executed against the specified data to return a result.

# COMMAND ----------

display(countsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why is Laziness So Important?
# MAGIC 
# MAGIC Laziness is at the core of Scala and Spark.
# MAGIC 
# MAGIC It has a number of benefits:
# MAGIC * Not forced to load all data at step #1
# MAGIC   * Technically impossible with **REALLY** large datasets.
# MAGIC * Easier to parallelize operations
# MAGIC   * N different transformations can be processed on a single data element, on a single thread, on a single machine.
# MAGIC * Optimizations can be applied prior to code compilation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catalyst Optimizer
# MAGIC 
# MAGIC Because our API is declarative a large number of optimizations are available to us.
# MAGIC 
# MAGIC Some of the examples include:
# MAGIC   * Optimizing data type for storage
# MAGIC   * Rewriting queries for performance
# MAGIC   * Predicate push downs
# MAGIC 
# MAGIC ![Catalyst](https://files.training.databricks.com/images/105/catalyst-diagram.png)

# COMMAND ----------

# MAGIC %md
# MAGIC Additional Articles:
# MAGIC * <a href="https://databricks.com/session/deep-dive-into-catalyst-apache-spark-2-0s-optimizer" target="_blank">Deep Dive into Catalyst: Apache Spark 2.0's Optimizer</a>, Yin Huai's Spark Summit 2016 presentation.
# MAGIC * <a href="https://www.youtube.com/watch?v=6bCpISym_0w" target="_blank">Catalyst: A Functional Query Optimizer for Spark and Shark</a>, Michael Armbrust's presentation at ScalaDays 2016.
# MAGIC * <a href="https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html" target="_blank">Deep Dive into Spark SQL's Catalyst Optimizer</a>, Databricks Blog, April 13, 2015.
# MAGIC * <a href="http://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf" target="_blank">Spark SQL: Relational Data Processing in Spark</a>, Michael Armbrust, Reynold S. Xin, Cheng Lian, Yin Huai, Davies Liu, Joseph K. Bradley, Xiangrui Meng, Tomer Kaftan, Michael J. Franklin, Ali Ghodsi, Matei Zaharia,<br/>_Proceedings of the 2015 ACM SIGMOD International Conference on Management of Data_.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Actions
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC In production code, actions will generally **write data to persistent storage** using the DataFrameWriter discussed in the preceding notebooks.
# MAGIC 
# MAGIC During interactive code development in Databricks notebooks, the `display` method will frequently be used to **materialize a view of the data** after logic has been applied.
# MAGIC 
# MAGIC A number of other actions provide the ability to return previews or specify physical execution plans for how logic will map to data. For the complete list, review the [API docs](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset).
# MAGIC 
# MAGIC | Method | Return | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | `collect()` | Collection | Returns an array that contains all of Rows in this Dataset. |
# MAGIC | `count()` | Long | Returns the number of rows in the Dataset. |
# MAGIC | `first()` | Row | Returns the first row. |
# MAGIC | `foreach(f)` | - | Applies a function f to all rows. |
# MAGIC | `foreachPartition(f)` | - | Applies a function f to each partition of this Dataset. |
# MAGIC | `head()` | Row | Returns the first row. |
# MAGIC | `reduce(f)` | Row | Reduces the elements of this Dataset using the specified binary function. |
# MAGIC | `show(..)` | - | Displays the top 20 rows of Dataset in a tabular form. |
# MAGIC | `take(n)` | Collection | Returns the first n rows in the Dataset. |
# MAGIC | `toLocalIterator()` | Iterator | Return an iterator that contains all of Rows in this Dataset. |
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Actions such as `collect` can lead to out of memory errors by forcing the collection of all data.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Transformations
# MAGIC 
# MAGIC Transformations have the following key characteristics:
# MAGIC * They eventually return another `DataFrame`.
# MAGIC * They are immutable - that is each instance of a `DataFrame` cannot be altered once it's instantiated.
# MAGIC   * This means other optimizations are possible - such as the use of shuffle files (to be discussed in detail later)
# MAGIC * Are classified as either a Wide or Narrow operation
# MAGIC 
# MAGIC Most operations in Spark are **transformations**. While many transformations are [DataFrame operations](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset), writing efficient Spark code will require importing methods from the `sql.functions` module, which contains [transformations corresponding to SQL built-in operations](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$).

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Narrow Transformations
# MAGIC 
# MAGIC The data required to compute the records in a single partition reside in at most one partition of the parent RDD.
# MAGIC 
# MAGIC Examples include:
# MAGIC * `filter(..)`
# MAGIC * `drop(..)`
# MAGIC * `coalesce()`
# MAGIC 
# MAGIC ![](https://databricks.com/wp-content/uploads/2018/05/Narrow-Transformation.png)

# COMMAND ----------

from pyspark.sql.functions import col

display(countsDF.filter(col("NAME").like("%TX%")))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Wide Transformations
# MAGIC 
# MAGIC The data required to compute the records in a single partition may reside in many partitions of the parent RDD. These operations require that data is **shuffled** between executors.
# MAGIC 
# MAGIC Examples include:
# MAGIC * `distinct()`
# MAGIC * `groupBy(..).sum()`
# MAGIC * `repartition(n)`
# MAGIC 
# MAGIC ![](https://databricks.com/wp-content/uploads/2018/05/Wide-Transformation.png)

# COMMAND ----------

display(countsDF.groupBy("UNIT").sum("counts"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Pipelining
# MAGIC 
# MAGIC 
# MAGIC - Pipelining is the idea of executing as many operations as possible on a single partition of data.
# MAGIC - Once a single partition of data is read into RAM, Spark will combine as many narrow operations as it can into a single **Task**
# MAGIC - Wide operations force a shuffle, conclude a stage, and end a pipeline.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Shuffles
# MAGIC 
# MAGIC A shuffle operation is triggered when data needs to move between executors.
# MAGIC 
# MAGIC To carry out the shuffle operation Spark needs to
# MAGIC * Convert the data to the UnsafeRow, commonly refered to as Tungsten Binary Format.
# MAGIC * Write that data to disk on the local node - at this point the slot is free for the next task.
# MAGIC * Send that data across the wire to another executor
# MAGIC   * Technically the Driver decides which executor gets which piece of data.
# MAGIC   * Then the executor pulls the data it needs from the other executor's shuffle files.
# MAGIC * Copy the data back into RAM on the new executor
# MAGIC   * The concept, if not the action, is just like the initial read "every" `DataFrame` starts with.
# MAGIC   * The main difference being it's the 2nd+ stage.
# MAGIC 
# MAGIC As we will see in a moment, this amounts to a free cache from what is effectively temp files.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Some actions induce in a shuffle. Good examples would include the operations `count()` and `reduce(..)`.
# MAGIC 
# MAGIC For more details on shuffling, refer to the [RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### UnsafeRow (aka Tungsten Binary Format)
# MAGIC 
# MAGIC As a quick side note, the data that is "shuffled" is in a format known as `UnsafeRow`, or more commonly, the Tungsten Binary Format.
# MAGIC 
# MAGIC `UnsafeRow` is the in-memory storage format for Spark SQL, DataFrames & Datasets.
# MAGIC 
# MAGIC Advantages include:
# MAGIC * Compactness:
# MAGIC   * Column values are encoded using custom encoders, not as JVM objects (as with RDDs).
# MAGIC   * The benefit of using Spark 2.x's custom encoders is that you get almost the same compactness as Java serialization, but significantly faster encoding/decoding speeds.
# MAGIC   * Also, for custom data types, it is possible to write custom encoders from scratch.
# MAGIC * Efficiency: Spark can operate _directly out of Tungsten_, without first deserializing Tungsten data into JVM objects.
# MAGIC 
# MAGIC ### How UnsafeRow works
# MAGIC * The first field, "123", is stored in place as its primitive.
# MAGIC * The next 2 fields, "data" and "bricks", are strings and are of variable length.
# MAGIC * An offset for these two strings is stored in place (32L and 48L respectively shown in the picture below).
# MAGIC * The data stored in these two offset’s are of format “length + data”.
# MAGIC * At offset 32L, we store 4 + "data" and likewise at offset 48L we store 6 + "bricks".
# MAGIC 
# MAGIC <div style="text-align: center"><img src="https://files.training.databricks.com/images/unsafe-row-format.png"></div>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Stages
# MAGIC * When we shuffle data, it creates what is known as a stage boundary.
# MAGIC * Stage boundaries represent a process bottleneck.
# MAGIC 
# MAGIC Take for example the following transformations:
# MAGIC 
# MAGIC |Step |Transformation|
# MAGIC |----:|--------------|
# MAGIC | 1   | Read    |
# MAGIC | 2   | Select  |
# MAGIC | 3   | Filter  |
# MAGIC | 4   | GroupBy |
# MAGIC | 5   | Select  |
# MAGIC | 6   | Filter  |
# MAGIC | 7   | Write   |

# COMMAND ----------

# MAGIC %md
# MAGIC Spark will break this one job into two stages (steps 1-4b and steps 4c-8):
# MAGIC 
# MAGIC **Stage #1**
# MAGIC 
# MAGIC |Step |Transformation|
# MAGIC |----:|--------------|
# MAGIC | 1   | Read |
# MAGIC | 2   | Select |
# MAGIC | 3   | Filter |
# MAGIC | 4a | GroupBy 1/2 |
# MAGIC | 4b | shuffle write |
# MAGIC 
# MAGIC **Stage #2**
# MAGIC 
# MAGIC |Step |Transformation|
# MAGIC |----:|--------------|
# MAGIC | 4c | shuffle read |
# MAGIC | 4d | GroupBy  2/2 |
# MAGIC | 5   | Select |
# MAGIC | 6   | Filter |
# MAGIC | 7   | Write |

# COMMAND ----------

# MAGIC %md
# MAGIC In **Stage #1**, Spark will create a pipeline of transformations in which the data is read into RAM (Step #1), and then perform steps #2, #3, #4a & #4b
# MAGIC 
# MAGIC All partitions must complete **Stage #1** before continuing to **Stage #2**
# MAGIC * It's not possible to group all records across all partitions until every task is completed.
# MAGIC * This is the point at which all the tasks must synchronize.
# MAGIC * This creates our bottleneck.
# MAGIC * Besides the bottleneck, this is also a significant performance hit: disk IO, network IO and more disk IO.
# MAGIC 
# MAGIC Once the data is shuffled, we can resume execution...
# MAGIC 
# MAGIC For **Stage #2**, Spark will again create a pipeline of transformations in which the shuffle data is read into RAM (Step #4c) and then perform transformations #4d, #5, #6 and finally the write action, step #7.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Lineage
# MAGIC From the developer's perspective, we start with a read and conclude (in this case) with a write
# MAGIC 
# MAGIC |Step |Transformation|
# MAGIC |----:|--------------|
# MAGIC | 1   | Read    |
# MAGIC | 2   | Select  |
# MAGIC | 3   | Filter  |
# MAGIC | 4   | GroupBy |
# MAGIC | 5   | Select  |
# MAGIC | 6   | Filter  |
# MAGIC | 7   | Write   |
# MAGIC 
# MAGIC However, Spark starts with the action (`write(..)` in this case).
# MAGIC 
# MAGIC Next, it asks the question, what do I need to do first?
# MAGIC 
# MAGIC It then proceeds to determine which transformation precedes this step until it identifies the first transformation.
# MAGIC 
# MAGIC |Step |Transformation| |
# MAGIC |----:|--------------|-|
# MAGIC | 7   | Write   | Depends on #6 |
# MAGIC | 6   | Filter  | Depends on #5 |
# MAGIC | 5   | Select  | Depends on #4 |
# MAGIC | 4   | GroupBy | Depends on #3 |
# MAGIC | 3   | Filter  | Depends on #2 |
# MAGIC | 2   | Select  | Depends on #1 |
# MAGIC | 1   | Read    | First |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why Work Backwards?
# MAGIC **Question:** So what is the benefit of working backward through your action's lineage?<br/>
# MAGIC **Answer:** It allows Spark to determine if it is necessary to execute every transformation.
# MAGIC 
# MAGIC Take another look at our example:
# MAGIC * Say we've executed this once already
# MAGIC * On the first execution, step #4 resulted in a shuffle
# MAGIC * Those shuffle files are on the various executors (src & dst)
# MAGIC * Because the transformations are immutable, no aspect of our lineage can change.
# MAGIC * That means the results of our last shuffle (if still available) can be reused.
# MAGIC 
# MAGIC |Step |Transformation| |
# MAGIC |----:|--------------|-|
# MAGIC | 7   | Write   | Depends on #6 |
# MAGIC | 6   | Filter  | Depends on #5 |
# MAGIC | 5   | Select  | Depends on #4 |
# MAGIC | 4   | GroupBy | <<< shuffle |
# MAGIC | 3   | Filter  | *don't care* |
# MAGIC | 2   | Select  | *don't care* |
# MAGIC | 1   | Read    | *don't care* |
# MAGIC 
# MAGIC In this case, what we end up executing is only the operations from **Stage #2**.
# MAGIC 
# MAGIC This saves us the initial network read and all the transformations in **Stage #1**
# MAGIC 
# MAGIC |Step |Transformation|   |
# MAGIC |----:|---------------|:-:|
# MAGIC | 1   | Read          | *skipped* |
# MAGIC | 2   | Select        | *skipped* |
# MAGIC | 3   | Filter        | *skipped* |
# MAGIC | 4a  | GroupBy 1/2   | *skipped* |
# MAGIC | 4b  | shuffle write | *skipped* |
# MAGIC | 4c  | shuffle read  | - |
# MAGIC | 4d  | GroupBy  2/2  | - |
# MAGIC | 5   | Select        | - |
# MAGIC | 6   | Filter        | - |
# MAGIC | 7   | Write         | - |

# COMMAND ----------

# MAGIC %md
# MAGIC ### And Caching...
# MAGIC 
# MAGIC The reuse of shuffle files (aka our temp files) is just one example of Spark optimizing queries anywhere it can.
# MAGIC 
# MAGIC We cannot assume this will be available to us.
# MAGIC 
# MAGIC Shuffle files are by definition temporary files and will eventually be removed.
# MAGIC 
# MAGIC However, we cache data to explicitly accomplish the same thing that happens inadvertently with shuffle files.
# MAGIC 
# MAGIC In this case, the lineage plays the same role. Take for example:
# MAGIC 
# MAGIC |Step |Transformation| |
# MAGIC |----:|--------------|-|
# MAGIC | 7   | Write   | Depends on #6 |
# MAGIC | 6   | Filter  | Depends on #5 |
# MAGIC | 5   | Select  | <<< cache |
# MAGIC | 4   | GroupBy | <<< shuffle files |
# MAGIC | 3   | Filter  | ? |
# MAGIC | 2   | Select  | ? |
# MAGIC | 1   | Read    | ? |
# MAGIC 
# MAGIC In this case we cached the result of the `select(..)`.
# MAGIC 
# MAGIC We never even get to the part of the lineage that involves the shuffle, let alone **Stage #1**.
# MAGIC 
# MAGIC Instead, we pick up with the cache and resume execution from there:
# MAGIC 
# MAGIC |Step |Transformation|   |
# MAGIC |----:|---------------|:-:|
# MAGIC | 1   | Read          | *skipped* |
# MAGIC | 2   | Select        | *skipped* |
# MAGIC | 3   | Filter        | *skipped* |
# MAGIC | 4a  | GroupBy 1/2   | *skipped* |
# MAGIC | 4b  | shuffle write | *skipped* |
# MAGIC | 4c  | shuffle read  | *skipped* |
# MAGIC | 4d  | GroupBy  2/2  | *skipped* |
# MAGIC | 5a  | cache read    | - |
# MAGIC | 5b  | Select        | - |
# MAGIC | 6   | Filter        | - |
# MAGIC | 7   | Write         | - |

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
