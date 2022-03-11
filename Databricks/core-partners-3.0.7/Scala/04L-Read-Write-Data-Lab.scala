// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Reading & Writing Data and Tables Lab
// MAGIC 
// MAGIC ## Learning Objectives
// MAGIC **In this lab, you will:**
// MAGIC - Use DataFrames to:
// MAGIC   - Read data from parquet format
// MAGIC   - Define and use schemas while loading data
// MAGIC   - Write data to Parquet
// MAGIC   - Save data to tables
// MAGIC - Register views
// MAGIC - Read data from tables and views back to DataFrames
// MAGIC - (**OPTIONAL**) Explore differences between managed and unmanaged table
// MAGIC 
// MAGIC **Resources**
// MAGIC * [Spark SQL, DataFrames and Datasets Guide](https://spark.apache.org/docs/2.2.0/sql-programming-guide.html)
// MAGIC * [Spark Load and Save Functions](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html)
// MAGIC * [Databases and Tables - Databricks Docs](https://docs.databricks.com/user-guide/tables.html)
// MAGIC * [Managed and Unmanaged Tables](https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables)
// MAGIC * [Creating a Table with the UI](https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui)
// MAGIC * [Create a Local Table](https://docs.databricks.com/user-guide/tables.html#create-a-local-table)
// MAGIC * [Saving to Persistent Tables](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables)
// MAGIC * [DataFrame Reader Docs](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader)
// MAGIC * [DataFrame Writer Docs](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Read Parquet into a DataFrame
// MAGIC 
// MAGIC Load the data in the `sourcePath` variable into a DataFrame named `tempDF`. No options need to be set beyond specifying the format.

// COMMAND ----------

// TODO

sourcePath = "/mnt/training/weather/StationData/stationData.parquet/"

// val tempDF = <FILL_IN>

// COMMAND ----------

// MAGIC %md
// MAGIC ### Review and Define the Schema
// MAGIC 
// MAGIC Note that a single job was triggered. With parquet files, the schema for each column is recorded, but Spark must still peek at the file to read this information (hence the job).
// MAGIC 
// MAGIC To avoid triggering this job, a schema can be passed as an argument. Define the schema here using SQL DDL or Spark types and fields (as demonstrated in the previous lesson).

// COMMAND ----------

// TODO

<FILL_IN> // Import types and define schema OR use SQL DDL.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Load Data with Defined Schema
// MAGIC 
// MAGIC No job will be triggered when a schema definition is provided. Load the data from `sourcePath` into a DataFrame named `weatherDF`.

// COMMAND ----------

// TODO

val weatherDF = <FILL_IN>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Save an Unmanaged Table
// MAGIC The DataFrame method `saveAsTable` registers the data currently referenced in the DataFrame to the metastore and saves a copy of the data.
// MAGIC 
// MAGIC If a `"path"` is not provided, Spark will create a managed table, meaning that both the metadata AND data are copied and stored in the root storage DBFS associated with the workspace. This means that dropping or modifying the table will modify the data in the DBFS. An unmanaged table allows decoupling of the data and the metadata, so a table can easily be renamed or removed from the workspace without deleting or migrating the underlying data.
// MAGIC 
// MAGIC Save `weatherDF` as an unmanaged table. Use the `tablePath` provided with the `"path"` option. Set the mode to `"overwrite"` (which will drop the table if it currently exists). Pass the table name `"weather"` to the `saveAsTable` method.

// COMMAND ----------

// TODO

val tablePath = s"$userhome/weather"

// weatherDF
//   .write
//   <FILL_IN>

// COMMAND ----------

// MAGIC %md
// MAGIC ### Query Table
// MAGIC This table contains the same data as the `weatherDF`. Remember that tables **persist between sessions** and (by default) are **available to all users in the workspace**.
// MAGIC 
// MAGIC Let's preview our data.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM weather

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Overview of the Data
// MAGIC 
// MAGIC The data include multiple entries from a selection of weather stations, including average temperatures recorded in either Fahrenheit or Celcius. The schema for the table:
// MAGIC 
// MAGIC |ColumnName  | DataType| Description|
// MAGIC |------------|---------|------------|
// MAGIC |NAME        |string   | Station name |
// MAGIC |STATION     |string   | Unique ID |
// MAGIC |LATITUDE    |float    | Latitude |
// MAGIC |LONGITUDE   |float    | Longitude |
// MAGIC |ELEVATION   |float    | Elevation |
// MAGIC |DATE        |date     | YYYY-MM-DD |
// MAGIC |UNIT        |string   | Temperature units |
// MAGIC |TAVG        |float    | Average temperature |

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Creating a Temp View with SQL
// MAGIC It's easy to register temp views using SQL queries. Temp views essentially allow a set of SQL transformations against a dataset to be given a name. This can be helpful when building up complex logic, or when an intermediate state will be used multiple times in later queries. Note that no job is triggered on view definition.
// MAGIC 
// MAGIC Use SQL to create a temp view named `station_counts` that returns the count of average temperatures recorded in both F and C for each station, ordered by station name.
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> `COUNT` will create the column name `count(1)` be default. Alias a descriptive column name that won't require escaping this column name in further SQL queries. (Parentheses are also not valid in column names when writing to parquet format.)

// COMMAND ----------

// MAGIC %sql
// MAGIC -- TODO
// MAGIC 
// MAGIC CREATE OR REPLACE TEMP VIEW station_counts
// MAGIC AS <FILL_IN>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC This aggregate view is small enough for manual examination. `SELECT *` will return all records as an interactive tabular view. Make mental note of whether or not any stations report temperature in both units, and the approximate number of records for each station.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> While no job was triggered when defining the view, a job is triggered _each time_ a query is executed against the view.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM station_counts

// COMMAND ----------

// MAGIC %md
// MAGIC ## Define a DataFrame from a View
// MAGIC Transforming a table or view back to a DataFrame is simple, and will not trigger a job. The metadata associated with the table is just reassigned to the DataFrame, so the same access permissions and schema are now accessible through both the DataFrame and SQL APIs.
// MAGIC 
// MAGIC Use `spark.table()` to create a DataFrame named `countsDF` from the view `"station_counts"`.

// COMMAND ----------

// TODO

val countsDF = <FILL_IN>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Write to Parquet
// MAGIC Writing this DataFrame back to disk will persist the computed aggregates for later.
// MAGIC 
// MAGIC Save `countsDF` to the provided `countsPath` with in parquet format.

// COMMAND ----------

// TODO

val countsPath = s"$userhome/stationCounts"

// countsDF.write
//   <FILL_IN>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Synopsis
// MAGIC 
// MAGIC In this lab we:
// MAGIC * Read Parquet files to Dataframes both with and without defining a schema
// MAGIC * Saved a managed and unmanaged table
// MAGIC * Created an aggregate temp view of our data
// MAGIC * Created a dataframe from that temp view
// MAGIC * Wrote the new dataframe back to Parquet files 

// COMMAND ----------

// MAGIC %md
// MAGIC ## **OPTIONAL**: Exploring Managed and Unmanaged Tables
// MAGIC 
// MAGIC In this section, we'll explore Databricks default behavior on managed vs. unmanaged tables. The differences in syntax for defining these are small, but the performance, cost, and security implications can be significant. **In almost all use cases, UNmanaged tables are preferred.**

// COMMAND ----------

// MAGIC %md
// MAGIC ## Save a Managed Table
// MAGIC To explore the differences between managed and unmanaged tables, save the DataFrame `weatherDF` from earlier in the lesson without the `"path"` option. Use the table name `"weather_managed"`.

// COMMAND ----------

// TODO

<FILL_IN> // Reuse the code block above without the "path" option.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Review Spark Catalog
// MAGIC 
// MAGIC Note the `tableType` field for our tables and views:
// MAGIC - The unmanaged table `weather` is `EXTERNAL`
// MAGIC - The managed table `weather_managed` is `MANAGED`
// MAGIC - The temp view `station_counts` is `TEMPORARY`

// COMMAND ----------

display(spark.catalog.listTables)

// COMMAND ----------

// MAGIC %md
// MAGIC Using SQL `SHOW TABLES` provides most of the same information, but does not indicate whether or not a table is managed.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SHOW TABLES

// COMMAND ----------

// MAGIC %md
// MAGIC ## Examine Table Details
// MAGIC Use the SQL command `DESCRIBE EXTENDED table_name` to examine the two weather tables.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC DESCRIBE EXTENDED weather

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC DESCRIBE EXTENDED weather_managed

// COMMAND ----------

// MAGIC %md
// MAGIC Run the following cell to assign the `managedTablePath` variable and confirm that both paths are correct with the information printed above.

// COMMAND ----------

lazy val managedTablePath = s"dbfs:/user/hive/warehouse/${spark.catalog.currentDatabase}.db/weather_managed"

println("The weather table is saved at:") 
println(s"    $tablePath")
println("The weather_managed table is saved at:")
println(s"    $managedTablePath")
println()

// COMMAND ----------

// MAGIC %md
// MAGIC The same DataFrame was used to create both of these directories, and therefore the content is identical (noting that file names are unique hashes).

// COMMAND ----------

dbutils.fs.ls(tablePath)

// COMMAND ----------

dbutils.fs.ls(managedTablePath)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Check Directory Contents after Dropping Tables
// MAGIC Now drop both tables and again list the contents of these directories.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC DROP TABLE weather;
// MAGIC DROP TABLE weather_managed;

// COMMAND ----------

dbutils.fs.ls(tablePath)

// COMMAND ----------

dbutils.fs.ls(managedTablePath)

// COMMAND ----------

// MAGIC %md
// MAGIC **This highlights the main differences between managed and unmanaged tables.** The files associated with managed tables will always be stored to this default location on the root DBFS storage linked to the workspace, and will be deleted when a table is dropped.
// MAGIC 
// MAGIC Files for unmanaged tables will be persisted in the `path` provided at table creation, preventing users from inadvertently deleting underlying files. **Unmanaged tables can easily be migrated to other databases or renamed, but these operations with managed tables will require rewriting ALL underlying files.**

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
