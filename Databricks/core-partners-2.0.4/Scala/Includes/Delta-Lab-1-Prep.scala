// Databricks notebook source
// MAGIC 
// MAGIC %run "./Classroom-Setup"

// COMMAND ----------

val inputPath = "/mnt/training/online_retail/data-001/data.csv"
val DataPath = userhome + "/delta/customer-data/"

//remove directory if it exists
dbutils.fs.rm(DataPath, true)

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

spark.read
  .option("header", "true")
  .schema(inputSchema)
  .csv(inputPath)
  .write
  .mode("overwrite")
  .format("delta")
  .partitionBy("Country")
  .save(DataPath)

// COMMAND ----------

spark.sql(s"""
  DROP TABLE IF EXISTS customer_data_delta
""")
spark.sql(s"""
  CREATE TABLE customer_data_delta
  USING DELTA
  LOCATION "$DataPath"
""")

// COMMAND ----------

val miniDataInputPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv"

spark
  .read
  .option("header", "true")
  .schema(inputSchema)
  .csv(miniDataInputPath)
  .write
  .format("delta")
  .partitionBy("Country")
  .mode("append")
  .save(DataPath)

// COMMAND ----------

spark.read.format("json").load("/mnt/training/enb/commonfiles/upsert-data.json").createOrReplaceTempView("upsert_data")

// COMMAND ----------

// MAGIC %sql
// MAGIC MERGE INTO customer_data_delta
// MAGIC USING upsert_data
// MAGIC ON customer_data_delta.InvoiceNo = upsert_data.InvoiceNo
// MAGIC   AND customer_data_delta.StockCode = upsert_data.StockCode
// MAGIC WHEN MATCHED THEN
// MAGIC   UPDATE SET *
// MAGIC WHEN NOT MATCHED
// MAGIC   THEN INSERT *
