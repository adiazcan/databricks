# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Data - Tables
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC * Demonstrate how to pre-register data sources in Databricks.
# MAGIC * Introduce temporary views over files.
# MAGIC * Read data from tables/views.
# MAGIC 
# MAGIC **Additional Resources**
# MAGIC * [Databases and Tables - Databricks Docs](https://docs.databricks.com/user-guide/tables.html)
# MAGIC * [Managed and Unmanaged Tables](https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables)
# MAGIC * [Creating a Table with the UI](https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui)
# MAGIC * [Create a Local Table](https://docs.databricks.com/user-guide/tables.html#create-a-local-table)
# MAGIC * [Saving to Persistent Tables](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Registering Tables in Databricks
# MAGIC 
# MAGIC Databricks allows us to "register" the equivalent of "tables" so that they can be easily accessed by all users.
# MAGIC 
# MAGIC The benefits of registering a table in a workspace include:
# MAGIC * Once setup, it never has to be done again
# MAGIC * It is available for any user on the platform (permissions permitting)
# MAGIC * Minimizes exposure of credentials
# MAGIC * No real overhead to reading the schema (no infer-schema)
# MAGIC * Easier to advertise available datasets to other users
# MAGIC 
# MAGIC ## Register a table with the Databricks UI
# MAGIC While the following cells can be run to programmatically register data for a table, Databrick's UI also has built-in support for working with a number of different data sources and registering tables.
# MAGIC 
# MAGIC We can upload the CSV file [here](https://files.training.databricks.com/courses/adbcore/commonfiles/geo_for_lookup.csv) and register it as a table using the UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Temporary View
# MAGIC 
# MAGIC A DataFrame can be registered as a temporary table or "view"
# MAGIC 
# MAGIC This is similar to a table describe above, the main difference is it is only available to the notebook that created it. 
# MAGIC 
# MAGIC The information regarding the temporary view is stored in the Spark catalog, not the metastore. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read a CSV file into a table

# COMMAND ----------

(spark.read
  .format("csv")
  .option("header", "true")
  .load("/mnt/training/enb/commonfiles/geo_for_lookup.csv")
  .write
  .mode("overwrite")
  .saveAsTable("geo_for_lookup_csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Table
# MAGIC 
# MAGIC We can query the table directly using SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM geo_for_lookup_csv

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Reading from a Table/View
# MAGIC 
# MAGIC We can now create a `DataFrame` for the "table" **geo_for_lookup_csv** with one simple command.
# MAGIC 
# MAGIC Here, we'll print the schema. We now have access to all DataFrame functionality to manipulate and query the data from our table.

# COMMAND ----------

geoForLookupDF = spark.read.table("geo_for_lookup_csv")

geoForLookupDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Takeaways
# MAGIC * No job is executed when creating a Dataframe from a table - the schema is stored in the table definition on Databricks.
# MAGIC * Data types shown here are those defined when the table was registered.
# MAGIC * This is a managed table, meaning that both the metadata AND data are stored in the DBFS. This means that dropping or modifying the table will modify the data in the DBFS (but not the source file, which we will no longer need access to).
# MAGIC * ACLs can be used to control access to this table within the workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Temporary Views
# MAGIC 
# MAGIC You can also take an existing `DataFrame` and register it as a view exposing it as a table to the SQL API.
# MAGIC 
# MAGIC Here, we'll create a temporary view of just our unique countries and their country codes.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW countrycodes AS (
# MAGIC   SELECT DISTINCT(country) country, countrycode3
# MAGIC   FROM geo_for_lookup_csv
# MAGIC )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC While we can query this as a table, it does not persist outside of our Spark session.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> While our SQL syntax uses the word `VIEW`, this is analogous to a temporary local table. More info in the docs [here](https://docs.databricks.com/user-guide/tables.html#create-a-local-table
# MAGIC ).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM countrycodes

# COMMAND ----------

# MAGIC %md
# MAGIC This can also be accomplished from a DataFrame. Here, we'll just register a view to look at postal code values for entries that have state or province specified.

# COMMAND ----------

(geoForLookupDF.select("StateProvince", "PostalCode")
  .where("StateProvince != 'None'")
  .createOrReplaceTempView("postalcodes"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM postalcodes

# COMMAND ----------

# MAGIC %md
# MAGIC Throughout the course, you'll see how persisted tables can be shared between notebooks whereas temp views are notebook specific (and don't persist between sessions).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
