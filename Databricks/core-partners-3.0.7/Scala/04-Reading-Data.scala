// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Reading & Writing Data and Tables
// MAGIC 
// MAGIC ## Learning Objectives
// MAGIC By the end of this lessons, you should be able to:
// MAGIC * Use DataFrames to:
// MAGIC   * Read data from CSV format
// MAGIC   * Define and use schemas while loading data
// MAGIC   * Write data to Parquet
// MAGIC   * Save data to tables
// MAGIC * Register views
// MAGIC * Read data from tables and views back to DataFrames
// MAGIC * Describe similarities and difference between DataFrames, views, and tables with regards to persistence and scope
// MAGIC 
// MAGIC 
// MAGIC **Additional Resources**
// MAGIC * [Spark SQL, DataFrames and Datasets Guide](https://spark.apache.org/docs/2.2.0/sql-programming-guide.html)
// MAGIC * [Spark Load and Save Functions](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html)
// MAGIC * [Databases and Tables - Databricks Docs](https://docs.databricks.com/user-guide/tables.html)
// MAGIC * [Managed and Unmanaged Tables](https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables)
// MAGIC * [Creating a Table with the UI](https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui)
// MAGIC * [Create a Local Table](https://docs.databricks.com/user-guide/tables.html#create-a-local-table)
// MAGIC * [Saving to Persistent Tables](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Spark SQL and DataFrames
// MAGIC 
// MAGIC Since Spark 2.0, Spark SQL and the DataFrames API have provided analogous entry points to easily manipulate data. DataFrames, tables, and temp views register the necessary metadata to execute Spark logic. The SparkSession uses these metadata to optimize computation while abstracting away many of the considerations about how data will be accessed and distributed during processing.
// MAGIC 
// MAGIC While there are slight differences between how users will interact with DataFrames, tables, and temp views, the following holds true for all:
// MAGIC - They are built on top of Spark SQL, which simplifies and optimizes Spark execution.
// MAGIC - They are collections of rows and provide easy access to columnar transformations.
// MAGIC - Rows will compile down to RDDs at execution.
// MAGIC - They provide fault-tolerant access to data stored externally.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> It is possible to create DataFrames/tables/views directly from RDDs. This course will focus on manipulating files stored in external cloud storage or data being loaded from sources such as relational databases and pub/sub services.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Creating DataFrames by Reading from CSV
// MAGIC 
// MAGIC ### The Data Source
// MAGIC * For this exercise, use a file called **iris.csv**.
// MAGIC * The is the canonical [iris dataset](https://archive.ics.uci.edu/ml/datasets/iris).
// MAGIC * Use **&percnt;fs head ...** to view the first few lines of the file.

// COMMAND ----------

// MAGIC %fs head /mnt/training/iris/iris.csv

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Read the CSV File
// MAGIC The following is standard syntax for loading data to DataFrames, here with a few options specifically set for CSV files.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The default delimiter for reading CSV is a comma(`,`), but this is possible to change with the option `"delimiter"` to extend this method to cover files using pipes, semi-colons, tabs, or other custom separators.

// COMMAND ----------

val csvFilePath = "/mnt/training/iris/iris.csv"

val tempDF = spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .load(csvFilePath)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Note the options being used
// MAGIC 
// MAGIC #### `format`
// MAGIC - The `format` method specifies what type of data is being loaded.
// MAGIC - The default format in Spark is Parquet.
// MAGIC - This method provides access to dozens of file formats, as well as connections to a multitude of connected services; a fairly complete list is available [here](https://docs.databricks.com/data/data-sources/index.html).
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Some formats have named methods (`json`, `csv`, `parquet`) that can be used in place of `load(filePath)`, e.g., `spark.read.csv(filePath)`. These methods are analogous to specifying the format using the `format` method, but less extensible.
// MAGIC 
// MAGIC #### `"header"`
// MAGIC - The `"header"` option tells the DataFrame to use the first row for column names.
// MAGIC - Without this option, columns will be assigned anonymous names `_c0`, `_c1`, ... `_cN`.
// MAGIC - The `DataFrameReader` peeks at the first line of the file to grab this information, triggering a job.
// MAGIC 
// MAGIC #### `"inferSchema"`
// MAGIC - The `"inferSchema"` option will scan the file to assign types to each column.
// MAGIC - Without this option, all columns will be assigned the `StringType`.
// MAGIC - The `DataFrameReader` will scan the entire contents of the file to determine which type to infer, triggering a job.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Print the Schema
// MAGIC 
// MAGIC Execute the command `printSchema()`to see the structure of the `DataFrame`.
// MAGIC 
// MAGIC The name of each column and its type are printed to the notebook.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Other `DataFrame` functions are covered in following lessons.

// COMMAND ----------

tempDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC From the schema notice:
// MAGIC * There are 6 columns in the DataFrame.
// MAGIC * The correct types were inferred for each column.
// MAGIC * The first column was assigned the anonymous name `_c0` because a valid field name was not present in the CSV.
// MAGIC * Our other column names contain capital letters and periods.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Providing a schema when reading from a CSV
// MAGIC 
// MAGIC A *Job* is triggered anytime we are "physically" __required to touch the data__. Our previous read triggered 2 jobs, one to peek at the header and the second to infer the types through a full file scan.
// MAGIC 
// MAGIC Defining and specifying a schema will prevent any jobs from triggering when defining a read operation.
// MAGIC 
// MAGIC ### Declare the schema
// MAGIC 
// MAGIC A schema is a list of field names and data types. Note that this example overrides the names provided by the file; when working with CSVs, the schema is applied to columns in the order they appear.
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Many file types will compare user-defined field names and types to those encoded in the file and fail if an incorrect schema is passed.

// COMMAND ----------

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

lazy val csvSchema = StructType(
  StructField("index", IntegerType) ::
  StructField("sepal_length", DoubleType) ::
  StructField("sepal_width", DoubleType) ::
  StructField("petal_length", DoubleType) ::
  StructField("petal_width", DoubleType) ::
  StructField("species", StringType) :: Nil)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Read the data
// MAGIC 
// MAGIC Pass the schema object defined above using the `schema()` method.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We still need to specify the option `"header"` as `true`.  We have already given the correct column names in our schema definition, but we need to let Spark know that the first row in the file should not be read in as actual data.

// COMMAND ----------

val irisDF = spark.read
 .option("header", "true")
 .schema(csvSchema)          // Use the specified schema
 .csv(csvFilePath)

// COMMAND ----------

// MAGIC %md
// MAGIC No jobs were triggered.
// MAGIC 
// MAGIC ![](https://files.training.databricks.com/images/adbcore/schema_preview.png)
// MAGIC 
// MAGIC Click the arrow next to the DataFrame name to see how the schema was assigned. Note that the previously anonymous column name is now "index", the other column names are formatted in snake_case, and all of data types correspond to those defined above.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Register a Temp View
// MAGIC 
// MAGIC Temporary views in Spark are essentially the equivalent to DataFrames for SQL. Neither temporary views nor DataFrames will persist between notebooks/job runs. Temporary views are only added to the Spark catalog rather than the metastore.
// MAGIC 
// MAGIC It is easy to create a temporary view from a DataFrame. This provides the ability to easily switch between the DataFrames API and SQL.

// COMMAND ----------

irisDF.createOrReplaceTempView("iris_temp")

// COMMAND ----------

// MAGIC %md
// MAGIC Next, take a peek at the data with a simple SQL SELECT statement:

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM iris_temp

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Writing to Files
// MAGIC 
// MAGIC In many cases the changes applied through DataFrame or SQL actions will need to be persisted. By writing files to disk, this data can easily be passed between sessions and shared with other users.
// MAGIC 
// MAGIC ### The Parquet File Format
// MAGIC 
// MAGIC Parquet is the default file format when working with Spark. Parquet is a columnar format that is supported by many data processing systems. Spark is optimized to perform operations on parquet files (note that the Delta Lake format is built on top of parquet). Spark SQL provides support for both reading and writing Parquet files that automatically preserves the schema of the original data. When writing Parquet files, **all columns are automatically converted to be nullable for compatibility reasons.**
// MAGIC 
// MAGIC More discussion on <a href="http://parquet.apache.org/documentation/latest/" target="_blank">Parquet</a>
// MAGIC 
// MAGIC ### Write Options
// MAGIC 
// MAGIC There are [many write options](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameWriter.html), and writing to some integrated services will require specific esoteric options to be passed. The syntax in the cell below is the most minimal but explicit example of using DataFrames to save data. Here, the data and schema currently associated with `irisDF` will be persisted in the directory specified by the `outputFilePath`.
// MAGIC 
// MAGIC #### `format`
// MAGIC Much like the DataFrameReader, the DataFrameWriter accepts a wide range of formats. It also supports use of a few file-specific methods (`json`, `parquet`, `csv`) in place of the `.save` syntax.
// MAGIC 
// MAGIC #### `mode`
// MAGIC Spark has several [save modes](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#save-modes).
// MAGIC 
// MAGIC | mode | details |
// MAGIC | --- | --- |
// MAGIC | `"error"` | **DEFAULT**; will raise an error message if data already exists at the specified path. |
// MAGIC | `"overwrite"` | If data exists in the target path, it will be deleted before the new data is saved. (This is used heavily throughout the course so that lessons or individual cells may be re-run without conflict.) |
// MAGIC | `"append"` | If data exists in the target path, new data to be saved will be appended to existant data. |
// MAGIC | `"ignore"` | If data exists in the target path, new data will **NOT** be saved. |

// COMMAND ----------

val outputFilePath = s"$userhome/iris"

irisDF
  .write
  .format("parquet")
  .mode("overwrite")
  .save(outputFilePath)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> When writing with Spark, the destination write path will _always_ be a directory of files. Those files that specifically contain data will have the extension matching the specified format, while other files in this directory are required for concurrency and fault-tolerance.

// COMMAND ----------

display(dbutils.fs.ls(outputFilePath))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Registering Tables in Databricks
// MAGIC 
// MAGIC Databricks allows us to "register" the equivalent of "tables" so that they can be easily accessed by all users.
// MAGIC 
// MAGIC The benefits of registering a table in a workspace include:
// MAGIC * Tables persist between notebooks and sessions
// MAGIC * It is available for any user on the platform (permissions permitting)
// MAGIC * Minimizes exposure of credentials
// MAGIC * Easier to advertise available datasets to other users
// MAGIC * ACLs can be set on tables to control user access throughout a workspace
// MAGIC 
// MAGIC ## Register a Table with the Databricks UI
// MAGIC Databrick's UI also has built-in support for working with a number of different data sources and registering tables.
// MAGIC 
// MAGIC We can upload the CSV file [here](https://files.training.databricks.com/courses/adbcore/commonfiles/iris.csv) and [register it as a table using the UI](https://docs.databricks.com/data/tables.html#create-a-table-using-the-ui).
// MAGIC 
// MAGIC ## Register a Table Programmatically
// MAGIC The following cell registers a table using the Parquet data written out in the previous step. No data is copied during this process.
// MAGIC 
// MAGIC The [Databricks docs on creating tables programmatically](https://docs.databricks.com/data/tables.html#create-a-table-programmatically) details this method as well as the `dataFrame.write.saveAsTable("table-name")` method. Note that the latter method will _always_ write data to the destination directory (regardless of whether the table create is [managed or unmanaged](https://docs.databricks.com/data/tables.html#managed-and-unmanaged-tables)). These concepts will be explored in the following lab.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Both scala and python have a `spark.sql()` method that allows arbitrary SQL commands to be run in their respective kernels. This is especially useful in that it allows strings stored as variables to be passed programmatically into SQL commands.

// COMMAND ----------

val tableName = "iris"

spark.sql(s"DROP TABLE IF EXISTS $tableName")

spark.sql(s"""
  CREATE TABLE $tableName
  USING PARQUET
  LOCATION "$outputFilePath"
""")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Query Table
// MAGIC Within the notebook in which they're defined, interacting with tables and views is identical--we just query them with SQL. The difference to remember is that tables **persist between sessions** and (by default) are **available to all users in the workspace**.
// MAGIC 
// MAGIC Click the `Data` icon on the left sidebar to view all tables in your database.
// MAGIC 
// MAGIC Let's preview our data.

// COMMAND ----------

display(spark.sql(s"SELECT * FROM $tableName"))

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Reading from a Table/View
// MAGIC 
// MAGIC Transforming a table or view back to a DataFrame is simple, and will not trigger a job. The metadata associated with the table is just reassigned to the DataFrame, so the same access permissions and schema are now accessible through both the DataFrames and SQL APIs.

// COMMAND ----------

lazy val newIrisDF = spark.table("iris")
newIrisDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Creating a Temp View with SQL
// MAGIC 
// MAGIC Earlier, we created a register a temp view using the DataFrame API and it’s just as easy to register temp views using SQL queries. Note that no job is triggered on view definition.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE OR REPLACE TEMP VIEW setosas
// MAGIC AS (SELECT *
// MAGIC   FROM iris
// MAGIC   WHERE species = "setosa")

// COMMAND ----------

// MAGIC %md
// MAGIC Note that `SELECT * FROM table` yields the same result as `display(df)` would if working with a DataFrame. The next lesson will discuss why displaying the contents of this view takes more time than creating it.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM setosas

// COMMAND ----------

// MAGIC %md
// MAGIC ### Review Spark Catalog
// MAGIC 
// MAGIC Listing the tables within the Spark catalog demonstrates that temp views are treated much the same as tables, but are not associated with a database. The temp views will not persist between Spark Sessions (e.g., notebooks), while the table will persist as part of the database to which it was registered.

// COMMAND ----------

display(spark.catalog.listTables())

// COMMAND ----------

// MAGIC %md
// MAGIC ### Key Takeaways
// MAGIC * DataFrames, tables, and views provide robust access to optimized Spark computations.
// MAGIC * DataFrames and views provide similar temporary access to data; tables persist these access patterns to the workspace.
// MAGIC * No job is executed when creating a DataFrame from a table - the schema is stored in the table definition on Databricks.
// MAGIC * Data types inferred or set in the schema are persisted when writing to files or saving to tables.
// MAGIC * ACLs can be used to control access to tables within the workspace.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
