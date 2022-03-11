// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Reading & Writing Data Lab
// MAGIC 
// MAGIC **Objectives**
// MAGIC * Read data into a DataFrame
// MAGIC * Write the results out to dbfs
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC * Read Data into a Dataframe
// MAGIC * Write a DataFrame to DBFS

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> If you did not run the previous notebook, execute the following cell to create the necessary table to complete this lab.

// COMMAND ----------

// spark.read
//   .format("csv")
//   .option("header", "true")
//   .load("/mnt/training/enb/commonfiles/geo_for_lookup.csv")
//   .write
//   .mode("overwrite")
//   .saveAsTable("geo_for_lookup_csv")

// COMMAND ----------

// MAGIC %md
// MAGIC Here we're importing all from `sql.functions` & `sql.types`.
// MAGIC - Dataframes are the main tool for manipulating data in spark
// MAGIC - Dataframes are structured with columns and rows similar to a SQL table
// MAGIC - To manipulate the records of a data frame SQL functions are needed
// MAGIC - To define the contents of a dataframe sql.types are needed

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Lab Overview
// MAGIC 
// MAGIC In this lab you will write code to:
// MAGIC * Read data from a file and a table into a dataframe
// MAGIC * Write that file back to storage in a different format
// MAGIC * Transform the data and write to storage

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Overview of the data
// MAGIC 
// MAGIC The data is a small collection of rows with the following schema
// MAGIC 
// MAGIC 
// MAGIC |ColumnName    | DataType|
// MAGIC |--------------|---------|
// MAGIC |city	       |string   |
// MAGIC |country	   |string	 |
// MAGIC |countrycode3  |string   |
// MAGIC |StateProvince |string   |
// MAGIC |PostalCode    |string   |

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Creating a Dataframe from a table.
// MAGIC 
// MAGIC In this exercise you will define a Dataframe by reading a table from your workspace.
// MAGIC 
// MAGIC Once you have defined the dataframe you will save it in JSON format.
// MAGIC 
// MAGIC Steps to complete:
// MAGIC 1. View available tables
// MAGIC 2. View Table Details
// MAGIC 3. Define a Dataframe from a table
// MAGIC 4. Save the data in Parquet format
// MAGIC 5. Verify the data has been written
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC **Notes**
// MAGIC 
// MAGIC What is a "table" in databricks/spark?
// MAGIC 
// MAGIC > A table is a combination of a data location and a description of the schema or structure of the data, databricks maintains this catalog of avaialable tables.

// COMMAND ----------

// MAGIC %md
// MAGIC ##  Step One: Viewing available tables
// MAGIC 
// MAGIC Since the first step will be reading from a table, here are the tools available to view tables in your workspace.
// MAGIC 
// MAGIC Viewing tables can be done using a SQL cell in your notebook, or using a sql command in python or scala.
// MAGIC 
// MAGIC SQL method:
// MAGIC ```
// MAGIC SHOW TABLES
// MAGIC ```
// MAGIC 
// MAGIC Python\scala Method:
// MAGIC 
// MAGIC ```
// MAGIC display(spark.sql("SHOW TABLES"))
// MAGIC ```
// MAGIC 
// MAGIC You can use any of these APIs to complete this task.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SHOW TABLES

// COMMAND ----------

// ANSWER
display(spark.sql("show tables"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Step Two: Viewing Table Details
// MAGIC 
// MAGIC Like the previous step, "Viewing Available Tables" this step can also be done using the SQL, Scala, or Python APIs.
// MAGIC 
// MAGIC The **DESCRIBE EXTENDED** command will show table details including the location of that directory that contains the files with the data in them.
// MAGIC 
// MAGIC In this step run the "DESCRIBE EXTENDED" command to get information on the location and format of the "geo_for_lookup_csv" table.
// MAGIC 
// MAGIC SQL method:
// MAGIC 
// MAGIC ```
// MAGIC DESCRIBE EXTENDED geo_for_lookup_csv
// MAGIC ```
// MAGIC 
// MAGIC Python\scala Method:
// MAGIC 
// MAGIC ```
// MAGIC display(spark.sql('DESCRIBE EXTENDED geo_for_lookup_csv'))
// MAGIC ```

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC DESCRIBE EXTENDED geo_for_lookup_csv;

// COMMAND ----------

//ANSWER
display(spark.sql("DESCRIBE EXTENDED geo_for_lookup_csv"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Step 3: Defining a dataframe from the table
// MAGIC 
// MAGIC In this step you will create a dataframe from the table.
// MAGIC 
// MAGIC To create a dataframe spark needs to know the format and location of the data.
// MAGIC 
// MAGIC If the data has already been defined in the workspace as a table, then this information is available to spark, and creating the dataframe is as easy as using spark.read.table()
// MAGIC 
// MAGIC The API docs for python are [here](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=dataframereader#pyspark.sql.DataFrameReader.table) and scala are [here](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader).
// MAGIC 
// MAGIC Steps to complete
// MAGIC 
// MAGIC 1. In one cell, define the dataframe
// MAGIC 2. In a second cell display the dataframe using display()

// COMMAND ----------

// ANSWER

val df = spark.read.table("geo_for_lookup_csv")

// COMMAND ----------

//ANSWER

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Questions for Review on Defining and Displaying a Dataframe
// MAGIC 
// MAGIC When a notebook cell executes, output from spark, if present, is appended to the bottom of the cell. Take a look at the two cells you created and the output from spark at the bottom of each cell.
// MAGIC 
// MAGIC **Note:**
// MAGIC - the dataframe definition did *NOT* trigger a spark job.
// MAGIC - the `display()` command triggered a spark job.
// MAGIC 
// MAGIC ### Why the difference?
// MAGIC 
// MAGIC In the first instance, defining the dataframe, spark just created the definition and did not physically touch the data. We are defining a transformation.
// MAGIC 
// MAGIC A spark job is triggered any time the data has to be physically accessed by spark executors. In this case defining the table does not require reading the file because it is a defined table.
// MAGIC 
// MAGIC Displaying the data requires reading the data so the `display()` command triggers a spark job.
// MAGIC 
// MAGIC A **Transformation** is an operation that defines a new Dataframe, an **Action** is an operation that triggers computation and returns results.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Step 4: Save the data in Parquet format
// MAGIC 
// MAGIC In this step you will save the data in Partquet format.
// MAGIC 
// MAGIC Spark dataframes have a write function, using that write function save the data to dbfs (databricks file system).
// MAGIC 
// MAGIC Transforming data from one format to another is a common operation.
// MAGIC 
// MAGIC In this case we will be reading the data from the table, where it is stored as CSV (comma separated values) and storing it as Parquet.
// MAGIC 
// MAGIC We will write the data into the "userhome" directory created by the classroom setup script.
// MAGIC 
// MAGIC ### Steps to complete:
// MAGIC 1. Create a filepath variable by adding the string `"/geo_for_lookup"` to the `userhome` variable
// MAGIC 2. Using that path use `dataframe.write.format("parquet")` to write to that path

// COMMAND ----------

//ANSWER

val filepath = userhome + "/geo_for_lookup"
//You may want to verify the filepath with a print statement
//print(filepath)

df.write.format("parquet").mode("overwrite").save(filepath)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Step 5: Verify the data has been written
// MAGIC 
// MAGIC Look at the contents of the directory that the dataframe was written to.
// MAGIC 
// MAGIC For this step you can use a python cell a scala cell or an `%fs` cell.
// MAGIC 
// MAGIC If you use an %fs cell then you would ls filepath (replace filepath with the full path, no command substitution is available in fs cells)
// MAGIC 
// MAGIC If using a python or scala cell then  `dbutils.fs.ls(filepath)` will list the contents of the directory.

// COMMAND ----------

display(dbutils.fs.ls(filepath))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Questions
// MAGIC 
// MAGIC **Q:** Why was the content written to a directory instead of a single file?
// MAGIC 
// MAGIC **A:** _Spark is a distributed system, Multiple workers would not be able to share writing to a single file. Each worker writes a file to this directory._
// MAGIC 
// MAGIC **Q:** What are the other files in the directory?
// MAGIC 
// MAGIC **A:** _Spark writes a few files to the output directory to manage the job. These files are the ones that start with an underscore. These files are used internally by spark and they can be ignored._

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Congratulations!
// MAGIC You have read a table into a dataframe and transformed the data into Parquet format and saved to DBFS.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
