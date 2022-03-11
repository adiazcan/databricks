// Databricks notebook source
// MAGIC 
// MAGIC %run "./Classroom-Setup"

// COMMAND ----------

// MAGIC %run "./Delta-Lab-1-Prep"

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC sqlContext.setConf("spark.sql.shuffle.partitions", "8")

// COMMAND ----------

val DeltaPath = userhome + "/delta/customer-data/"
val CustomerCountsPath = userhome + "/delta/customer_counts/"
dbutils.fs.rm(CustomerCountsPath, true)

spark.read
  .format("delta")
  .load(DeltaPath)
  .groupBy("CustomerID", "Country")
  .count()
  .withColumnRenamed("count", "total_orders")
  .write
  .mode("overwrite")
  .format("delta")
  .partitionBy("Country")
  .save(CustomerCountsPath)

// COMMAND ----------

spark.sql("""
  DROP TABLE IF EXISTS customer_counts
""")

spark.sql(s"""
  CREATE TABLE customer_counts
  USING DELTA
  LOCATION '$CustomerCountsPath'
""")

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, DoubleType, IntegerType, StringType}

lazy val inputSchema = StructType(List(
  StructField("InvoiceNo", IntegerType, true),
  StructField("StockCode", StringType, true),
  StructField("Description", StringType, true),
  StructField("Quantity", IntegerType, true),
  StructField("InvoiceDate", StringType, true),
  StructField("UnitPrice", DoubleType, true),
  StructField("CustomerID", IntegerType, true),
  StructField("Country", StringType, true)
))

// COMMAND ----------

val newDataPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-small.csv"

spark.sql("DROP TABLE IF EXISTS new_customer_counts")
val newDataDF = spark
  .read
  .option("header", "true")
  .schema(inputSchema)
  .csv(newDataPath)

newDataDF.groupBy("CustomerID", "Country")
  .count()
  .withColumnRenamed("count", "total_orders")
  .write
  .saveAsTable("new_customer_counts")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC MERGE INTO customer_counts
// MAGIC USING new_customer_counts
// MAGIC ON customer_counts.Country = new_customer_counts.Country
// MAGIC AND customer_counts.CustomerID = new_customer_counts.CustomerID
// MAGIC WHEN MATCHED THEN
// MAGIC   UPDATE SET total_orders = customer_counts.total_orders + new_customer_counts.total_orders
// MAGIC WHEN NOT MATCHED THEN
// MAGIC   INSERT *

// COMMAND ----------

newDataDF.write
  .format("delta")
  .mode("append")
  .save(DeltaPath)

