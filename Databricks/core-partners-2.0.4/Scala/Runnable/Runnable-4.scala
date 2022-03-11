// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %scala
// MAGIC dbutils.widgets.text("ranBy", "")

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions.{lit, unix_timestamp}
// MAGIC import org.apache.spark.sql.types.TimestampType
// MAGIC 
// MAGIC val tags = com.databricks.logging.AttributionContext.current.tags
// MAGIC val username = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
// MAGIC val ranBy = dbutils.widgets.get("ranBy")
// MAGIC val path = username+"/academy/runLog.parquet"
// MAGIC 
// MAGIC spark
// MAGIC   .range(1)
// MAGIC   .select(unix_timestamp.alias("runtime").cast(TimestampType), lit(ranBy).alias("ranBy"))
// MAGIC   .write
// MAGIC   .mode("APPEND")
// MAGIC   .parquet(path)
// MAGIC 
// MAGIC display(spark.read.parquet(path))

// COMMAND ----------

// MAGIC %scala
// MAGIC dbutils.notebook.exit(path)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
