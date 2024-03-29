# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # DataFrame & Column
# MAGIC 1. Construct columns
# MAGIC 1. Subset columns
# MAGIC 1. Add or replace columns
# MAGIC 1. Subset rows
# MAGIC 1. Sort rows
# MAGIC 
# MAGIC ##### Methods
# MAGIC - DataFrame (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html" target="_blank">Scala</a>): `select`, `selectExpr`, `drop`, `withColumn`, `withColumnRenamed`, `filter`, `distinct`, `limit`, `sort`
# MAGIC - Column (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.html#pyspark.sql.Column" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Column.html" target="_blank">Scala</a>): `alias`, `isin`, `cast`, `isNotNull`, `desc`, operators

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC Let's use the BedBricks events dataset.

# COMMAND ----------

eventsDF = spark.read.parquet(eventsPath)
display(eventsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Construct columns
# MAGIC 
# MAGIC A **column** is a logical construction that will be computed based on the data in a DataFrame using an expression
# MAGIC 
# MAGIC Construct a new column based on the input columns existing in a DataFrame

# COMMAND ----------

from pyspark.sql.functions import col

col("device")
eventsDF.device
eventsDF["device"]

# COMMAND ----------

# MAGIC %md
# MAGIC Use column objects to form complex expressions

# COMMAND ----------

col("ecommerce.purchase_revenue_in_usd") + col("ecommerce.total_item_quantity")
col("event_timestamp").desc()
(col("ecommerce.purchase_revenue_in_usd") * 100).cast("int")

# COMMAND ----------

# MAGIC %md
# MAGIC Here's an example of using these column expressions in the context of a DataFrame

# COMMAND ----------

revDF = (eventsDF.filter(col("ecommerce.purchase_revenue_in_usd").isNotNull())
  .withColumn("purchase_revenue", (col("ecommerce.purchase_revenue_in_usd") * 100).cast("int"))
  .withColumn("avg_purchase_revenue", col("ecommerce.purchase_revenue_in_usd") / col("ecommerce.total_item_quantity"))
  .sort(col("avg_purchase_revenue").desc()))

display(revDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Subset columns
# MAGIC Use DataFrame transformations to subset columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### **`select()`**
# MAGIC Selects a set of columns or column based expressions

# COMMAND ----------

devicesDF = eventsDF.select("user_id", "device")
display(devicesDF)

# COMMAND ----------

from pyspark.sql.functions import col

locationsDF = eventsDF.select("user_id",
  col("geo.city").alias("city"),
  col("geo.state").alias("state"))

display(locationsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **`selectExpr()`**
# MAGIC Selects a set of SQL expressions

# COMMAND ----------

appleDF = eventsDF.selectExpr("user_id", "device in ('macOS', 'iOS') as apple_user")
display(appleDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### `drop()`
# MAGIC Returns a new DataFrame after dropping the given column, specified as a string or column object
# MAGIC 
# MAGIC Use strings to specify multiple columns

# COMMAND ----------

anonymousDF = eventsDF.drop("user_id", "geo", "device")
display(anonymousDF)

# COMMAND ----------

noSalesDF = eventsDF.drop(col("ecommerce"))
display(noSalesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Add or replace columns
# MAGIC Use DataFrame transformations to add or replace columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### `withColumn`
# MAGIC Returns a new DataFrame by adding a column or replacing the existing column that has the same name.

# COMMAND ----------

mobileDF = eventsDF.withColumn("mobile", col("device").isin("iOS", "Android"))
display(mobileDF)

# COMMAND ----------

purchaseQuantityDF = eventsDF.withColumn("purchase_quantity", col("ecommerce.total_item_quantity").cast("int"))
purchaseQuantityDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### `withColumnRenamed()`
# MAGIC Returns a new DataFrame with a column renamed.

# COMMAND ----------

locationDF = eventsDF.withColumnRenamed("geo", "location")
display(locationDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Subset Rows
# MAGIC Use DataFrame transformations to subset rows

# COMMAND ----------

# MAGIC %md
# MAGIC #### `filter()`
# MAGIC Filters rows using the given SQL expression or column based condition.

# COMMAND ----------

purchasesDF = eventsDF.filter("ecommerce.total_item_quantity > 0")
display(purchasesDF)

# COMMAND ----------

revenueDF = eventsDF.filter(col("ecommerce.purchase_revenue_in_usd").isNotNull())
display(revenueDF)

# COMMAND ----------

androidDF = eventsDF.filter((col("traffic_source") != "direct") & (col("device") == "Android"))
display(androidDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### `dropDuplicates()`
# MAGIC Returns a new DataFrame with duplicate rows removed, optionally considering only a subset of columns.
# MAGIC 
# MAGIC ##### Alias: `distinct`

# COMMAND ----------

eventsDF.distinct()

# COMMAND ----------

distinctUsersDF = eventsDF.dropDuplicates(["user_id"])
display(distinctUsersDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### `limit()`
# MAGIC Returns a new DataFrame by taking the first n rows.

# COMMAND ----------

limitDF = eventsDF.limit(100)
display(limitDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Sort rows
# MAGIC Use DataFrame transformations to sort rows

# COMMAND ----------

# MAGIC %md
# MAGIC #### `sort()`
# MAGIC Returns a new DataFrame sorted by the given columns or expressions.
# MAGIC 
# MAGIC ##### Alias: `orderBy`

# COMMAND ----------

increaseTimestampsDF = eventsDF.sort("event_timestamp")
display(increaseTimestampsDF)

# COMMAND ----------

decreaseTimestampsDF = eventsDF.sort(col("event_timestamp").desc())
display(decreaseTimestampsDF)

# COMMAND ----------

increaseSessionsDF = eventsDF.orderBy(["user_first_touch_timestamp", "event_timestamp"])
display(increaseSessionsDF)

# COMMAND ----------

decreaseSessionsDF = eventsDF.sort(col("user_first_touch_timestamp").desc(), col("event_timestamp"))
display(decreaseSessionsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Purchase Revenues Lab
# MAGIC 
# MAGIC Prepare dataset of events with purchase revenue.
# MAGIC 1. Extract purchase revenue for each event
# MAGIC 2. Filter events where revenue is not null
# MAGIC 3. Check what types of events have revenue
# MAGIC 4. Drop unneeded column
# MAGIC 
# MAGIC ##### Methods
# MAGIC - DataFrame: `select`, `drop`, `withColumn`, `filter`, `dropDuplicates`
# MAGIC - Column: `isNotNull`

# COMMAND ----------

eventsDF = spark.read.parquet(eventsPath)
display(eventsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Extract purchase revenue for each event
# MAGIC Add new column **`revenue`** by extracting **`ecommerce.purchase_revenue_in_usd`**

# COMMAND ----------

# TODO
revenueDF = (eventsDF
  .withColumn("revenue", col("ecommerce.purchase_revenue_in_usd")))
display(revenueDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

# COMMAND ----------

expected1 = [5830.0, 5485.0, 5289.0, 5219.1, 5180.0, 5175.0, 5125.0, 5030.0, 4985.0, 4985.0]
result1 = [row.revenue for row in revenueDF.sort(col("revenue").desc_nulls_last()).limit(10).collect()]

assert(expected1 == result1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Filter events where revenue is not null
# MAGIC Filter for records where **`revenue`** is not **`null`**

# COMMAND ----------

# TODO
purchasesDF = revenueDF.filter(col("revenue").isNotNull())
display(purchasesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Check what types of events have revenue
# MAGIC Find unique **`event_name`** values in **`purchasesDF`** in one of two ways:
# MAGIC - Select "event_name" and get distinct records
# MAGIC - Drop duplicate records based on the "event_name" only
# MAGIC 
# MAGIC Hint: There's only one event associated with revenues

# COMMAND ----------

# TODO
distinctDF = purchasesDF.dropDuplicates(["event_name"])
display(distinctDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Drop unneeded column
# MAGIC Since there's only one event type, drop **`event_name`** from **`purchasesDF`**.

# COMMAND ----------

# TODO
finalDF = purchasesDF.drop("event_name")
display(finalDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Chain all the steps above excluding step 3

# COMMAND ----------

# TODO
finalDF = (eventsDF
  .FILL_IN
)

display(finalDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

# COMMAND ----------

assert(finalDF.count() == 180678)

# COMMAND ----------

expected_columns = {'device', 'ecommerce', 'event_previous_timestamp', 'event_timestamp',
                    'geo', 'items', 'revenue', 'traffic_source',
                    'user_first_touch_timestamp', 'user_id'}
assert(set(finalDF.columns) == expected_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up classroom

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Cleanup

# COMMAND ----------


