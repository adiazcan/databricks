# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC This notebook exists just to demonstrate that we can use Azure Data Factory to run a Databricks notebook.
# MAGIC 
# MAGIC Here, we'll just be creating a table using a quick SQL query.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS swedish_customers

# COMMAND ----------

# MAGIC %md
# MAGIC The cell below will fail, as you defined this table outside the default database.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE swedish_customers
# MAGIC AS (
# MAGIC   SELECT *
# MAGIC   FROM customer_data_delta
# MAGIC   WHERE Country = 'Sweden')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM swedish_customers

# COMMAND ----------


