# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations & Actions Lab
# MAGIC 
# MAGIC **Objectives**
# MAGIC * Review what triggers a job and what does not
# MAGIC * Review catalyst

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Import needed functions
# MAGIC 
# MAGIC Here we're importing all from `sql.functions` & `sql.types`.
# MAGIC - Dataframes are the main tool for manipulating data in spark
# MAGIC - Dataframes are structured with columns and rows similar to a SQL table
# MAGIC - To manipulate the records of a data frame SQL functions are needed
# MAGIC - To define the contents of a dataframe sql.types are needed

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Overview of the data
# MAGIC 
# MAGIC The data is a small collection of rows with the following schema:
# MAGIC 
# MAGIC 
# MAGIC |ColumnName    | DataType|
# MAGIC |--------------|---------|
# MAGIC |city	       |string   |
# MAGIC |country	   |string	 |
# MAGIC |countrycode3  |string   |
# MAGIC |StateProvince |string   |
# MAGIC |PostalCode    |string   |
# MAGIC 
# MAGIC We can use SQL to look at this table:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM geo_for_lookup_csv

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> If the previous cell failed, execute the following cell to create the necessary table to complete this lab.

# COMMAND ----------

# (spark.read
#   .format("csv")
#   .option("header", "true")
#   .load("/mnt/training/enb/commonfiles/geo_for_lookup.csv")
#   .write
#   .mode("overwrite")
#   .saveAsTable("geo_for_lookup_csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC Run the code provided to define a DataFrame from the existing table and display the DataFrame.

# COMMAND ----------

df = spark.read.table("geo_for_lookup_csv")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Count Number of Entries per Country
# MAGIC 
# MAGIC In this exercise you will create a dataframe that has one column for each unique country, and another column with the count of the records for that country.
# MAGIC 
# MAGIC Dataframes have a `groupBy` operation that we can use for this.
# MAGIC 
# MAGIC See the scala documentation [here](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset) or the python documentation [here](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=groupby#pyspark.sql.DataFrame.groupby).
# MAGIC 
# MAGIC **Hint**: `groupBy` should be combined with an aggregate function like min, max, avg, or sum. In order to get a count of values use `count()`.
# MAGIC 
# MAGIC Assign your new DataFrame to the variable `countryCounts`.

# COMMAND ----------

#ANSWER
countryCounts = df.groupBy("country").count()
display(countryCounts)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Save data
# MAGIC 
# MAGIC Write the data using [parquet file format](https://parquet.apache.org/), a columnar storage format that is [drastically better than CSV or JSON](https://databricks.com/session/spark-parquet-in-depth), especially when working in Spark.
# MAGIC 
# MAGIC ### Steps to complete:
# MAGIC 1. Create a filepath variable by adding the string `"/data.parquet"` to the `userhome` variable
# MAGIC 2. Using that path use `dataframe.write.parquet` to write to that path

# COMMAND ----------

#ANSWER
countsPath = userhome + "/country-counts"
countryCounts.write.format("parquet").mode("overwrite").save(countsPath)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Schemas for DataFrames
# MAGIC 
# MAGIC When reading and writing data, Spark _may_ need the schema of the data.
# MAGIC 
# MAGIC For some data the schema is available.
# MAGIC * Tables store the schema in Spark's catalog
# MAGIC * Parquet files include a schema definition
# MAGIC * CSV and JSON files may have column names, but do not have datatypes
# MAGIC 
# MAGIC In order to read data from a format without the schema you may have to define it.
# MAGIC 
# MAGIC (**Note**: While you _can_ infer the schema from CSV and JSON, this is not best practice for big data or production jobs.)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Print Schema from `df` and `countryCounts`
# MAGIC This is simple with the `printSchema` method.

# COMMAND ----------

#ANSWER

df.printSchema()
countryCounts.printSchema()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Read the Parquet data and provide a Schema
# MAGIC 
# MAGIC If no schema is provided then Spark will infer the schema by reading the metadata associated with the parquet file.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> With CSV and JSON format, inferring schema can be an expensive process as the entire file must be scanned.
# MAGIC 
# MAGIC Write some code to read the Parquet data as a DataFrame. First provide no schema and use `printSchema` to see the schema that Spark inferred.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We'll be using the Parquet data saved in the Reading & Write Data lab.

# COMMAND ----------

parquetPath = userhome + '/geo_for_lookup'

# COMMAND ----------

# ANSWER
parquetDF = spark.read.format("parquet").load(parquetPath)
parquetDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Then add the schema.
# MAGIC 
# MAGIC To add the schema you have two options.
# MAGIC 
# MAGIC ### Schema as StructType
# MAGIC 
# MAGIC An example of using StructType to define a schema:
# MAGIC 
# MAGIC ```
# MAGIC schemaDefinition = StructType(
# MAGIC   [
# MAGIC     StructField("ColumnName1", StringType(), False),
# MAGIC     StructField("ColumnName2", IntegerType(), False),
# MAGIC     StructField("ColumnName3", StringType(), False),
# MAGIC     StructField("ColumnName4", IntegerType(), False),
# MAGIC     StructField("ColumnName5", StringType(), False)
# MAGIC   ]
# MAGIC )
# MAGIC ```
# MAGIC 
# MAGIC And then pass the schema:
# MAGIC 
# MAGIC `df = spark.read.schema(schemaDefinition)........`
# MAGIC 
# MAGIC 
# MAGIC ### Schema as String
# MAGIC 
# MAGIC You can provide the schema as a SQL DDL(Data Definition Language) string
# MAGIC 
# MAGIC `spark.read.schema("column1 STRING, column2 STRING ").format("parquet").load(path_to_file)`

# COMMAND ----------

# ANSWER

schema = StructType(
  [
    StructField("city", StringType(), False),
    StructField("country", StringType(), False),
    StructField("countrycode3", StringType(), False),
    StructField("StateProvince", StringType(), False),
    StructField("PostalCode", StringType(), False)

  ]
)

parquetDF = spark.read.schema(schema).format("parquet").load(parquetPath)
parquetDF.printSchema()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Because we provided a schema, no job was triggered as we loaded our data.
# MAGIC 
# MAGIC A job will be triggered when we display our DataFrame.

# COMMAND ----------

display(parquetDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  A Job is also triggered when we run any action on our loaded data.

# COMMAND ----------

parquetDF.count()


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
