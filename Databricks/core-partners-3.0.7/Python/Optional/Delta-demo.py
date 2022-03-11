# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark Delta Tables
# MAGIC 
# MAGIC  This Notebook Demonstrates some of the features of a Spark Delta Table
# MAGIC 
# MAGIC The purpose of this notebook is to run it as an introduction to the features of Delta

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Key Features of Delta Tables
# MAGIC 
# MAGIC In this demo I will show a few features of Delta
# MAGIC 
# MAGIC * Transactions
# MAGIC * Schema enforcement
# MAGIC * Time Travel
# MAGIC * Data Skipping

# COMMAND ----------

# MAGIC %run "../../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Setup 
# MAGIC 
# MAGIC The cell below verifies that classroom setup worked and configured our home directory and sets some variables that are used later

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC print(userhome)
# MAGIC # Set up a directory for files
# MAGIC filedir = userhome + "/delta_demo"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Write some data to a file
# MAGIC 
# MAGIC The cell below just writes some data in JSON format that will be written to a file in dbfs

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # create a string
# MAGIC 
# MAGIC datastring = """{"id":3,"name":"Dorothy"}
# MAGIC {"id":1,"name":"Charles"}
# MAGIC {"id":4,"name":"Jacob"}
# MAGIC {"id":2,"name":"Evan"}"""
# MAGIC 
# MAGIC print(datastring)
# MAGIC 
# MAGIC filename = filedir + "/data.json"
# MAGIC # reformat string for python
# MAGIC pythonfilename = filename.replace("dbfs:", "/dbfs")
# MAGIC print(pythonfilename)
# MAGIC print(filename)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Write the data to dbfs
# MAGIC 
# MAGIC This cell writes the data to dbfs.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC #dbutils.fs.put(filename, datastring, true)
# MAGIC try:
# MAGIC   dbutils.fs.put(filename,"" )
# MAGIC except:
# MAGIC   print("File Already Exists")
# MAGIC   
# MAGIC # Write data to file  
# MAGIC file1 = open(pythonfilename,"w+")
# MAGIC file1.write(datastring)
# MAGIC file1.close()
# MAGIC 
# MAGIC # verify data is written into file
# MAGIC 
# MAGIC dbutils.fs.head(filename)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Review of the data
# MAGIC 
# MAGIC This is a simple dataset, tracking employee id's and names. 
# MAGIC 
# MAGIC 
# MAGIC The data is stored as JSON
# MAGIC 
# MAGIC ```
# MAGIC {"id":3,"name":"Dorothy"}
# MAGIC {"id":1,"name":"Charles"}
# MAGIC {"id":4,"name":"Jacob"}
# MAGIC {"id":2,"name":"Evan"}
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Viewing the data
# MAGIC 
# MAGIC Run this cell to view the data

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC dbutils.fs.head(filename)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Create a Dataframe from the original data
# MAGIC 
# MAGIC A Dataframe is the format used to process data in spark. 
# MAGIC 
# MAGIC This cell reads the data into a Dataframe

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC df = spark.read.format("json").load(filename)
# MAGIC display(df)
# MAGIC # The data is now in a dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Write the dataframe to a Delta Table
# MAGIC 
# MAGIC Once we have read the data into a Dataframe we can write it into a Delta Table. 

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # set variable for writing the delta table
# MAGIC deltaDirectory =  filedir + "/delta_table"
# MAGIC 
# MAGIC # reformat the file string for use in python
# MAGIC pythondirectory = deltaDirectory.replace("dbfs:", "")
# MAGIC 
# MAGIC # Write the data to the delta table
# MAGIC df.write.format("delta").mode("append").save(deltaDirectory)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Delta table Write Path
# MAGIC 
# MAGIC When writing the initial write, and all subsequent writes the following process is followed
# MAGIC 
# MAGIC ![alt text](https://raw.githubusercontent.com/tomthetrainerDB/images/master/1.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Key Features
# MAGIC 
# MAGIC * The write was atomic
# MAGIC * The data is stored as Parquet
# MAGIC 
# MAGIC 
# MAGIC The write was atomic (all or nothing), any files that are written are not committed until the delta log is updated. 
# MAGIC 
# MAGIC If the write had included dozens of files from dozens of tasks, they would all be committed or none would be committed. 
# MAGIC 
# MAGIC Parquet is an efficient column oriented storage format. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Viewing the data directory
# MAGIC 
# MAGIC Run the cell below to see the log directory and the parquet files tat hold data.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC display(dbutils.fs.ls(deltaDirectory))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Run a SQL query against the delta table
# MAGIC 
# MAGIC The same code could be run in a SQL cell, 
# MAGIC 
# MAGIC 
# MAGIC "Select * from delta.\`filepath\`"

# COMMAND ----------

# MAGIC %python
# MAGIC #print(deltaDirectory)
# MAGIC 
# MAGIC print(pythondirectory)
# MAGIC querystring = "SELECT * FROM delta.`" + pythondirectory +"`"
# MAGIC display(spark.sql(querystring))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Delta History command
# MAGIC 
# MAGIC Delta maintains a log file of changes to the table
# MAGIC 
# MAGIC * Enables Time Travel
# MAGIC * Enables Data Skipping
# MAGIC 
# MAGIC Run the cell below to see the history. 
# MAGIC 
# MAGIC This could also be run in a SQL cell as
# MAGIC 
# MAGIC "DESCRIBE HISTORY delta.\`directorypath\`"
# MAGIC 
# MAGIC When ran the first time, you should see the table has only one version.
# MAGIC 
# MAGIC Changes to the table will result in additional versions. 

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC querystring = "DESCRIBE HISTORY delta.`" + pythondirectory + "`"
# MAGIC display(spark.sql(querystring))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Table History
# MAGIC 
# MAGIC The table history command is executed by reading the log file
# MAGIC 
# MAGIC ![alt text](https://raw.githubusercontent.com/tomthetrainerDB/images/master/2.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### The Delta Log
# MAGIC 
# MAGIC * Is the source of truth for a succesful write
# MAGIC   * On success the file is updated, any files in the directory and not in the Log will be ignored by delta
# MAGIC * Also used to allow transactional reads / time travel
# MAGIC * Enables data skipping feature

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Additional Features of Delta
# MAGIC 
# MAGIC ### A Delta table can be deleted from
# MAGIC 
# MAGIC Try that with a traditional Data Lake !!
# MAGIC 
# MAGIC Most Big Data stores are write once, a delete in those environments requires re-writing the whole table.
# MAGIC 
# MAGIC Typically a delete in a big data system involves an expensive read of all data followed by a rewrite. 
# MAGIC 
# MAGIC Run the following cell to delete user 1

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC querystring = "Delete FROM delta.`" + pythondirectory + "` where id = 1"
# MAGIC #print(querystring)
# MAGIC display(spark.sql(querystring))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Verify Delete
# MAGIC 
# MAGIC Run this cell to verify the delete

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC querystring = "SELECT * FROM delta.`" + pythondirectory +"`"
# MAGIC display(spark.sql(querystring))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Overview of Delete Path
# MAGIC ![alt text](https://raw.githubusercontent.com/tomthetrainerDB/images/master/delete.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Review the history after the delete
# MAGIC 
# MAGIC Review the history of the table by running the cell below

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC querystring = "DESCRIBE HISTORY delta.`" + pythondirectory + "`"
# MAGIC display(spark.sql(querystring))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Delta Data Guarantees
# MAGIC 
# MAGIC If the data does not have the correct schema, it is not written
# MAGIC 
# MAGIC The cells below demonstrate attempts to write properly formatted data, and improperly formatted data to the delta table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Data review
# MAGIC 
# MAGIC The good data fits, it has an id and a name field.
# MAGIC 
# MAGIC The bad data does not have an id or a name field.
# MAGIC 
# MAGIC good.json 
# MAGIC 
# MAGIC ```
# MAGIC {"id":22,"name":"New_person"}
# MAGIC ```
# MAGIC 
# MAGIC bad.json
# MAGIC 
# MAGIC ```
# MAGIC {"badid":22,"badname":"bad_person"}
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Good data
# MAGIC 
# MAGIC Write the good data to dbfs

# COMMAND ----------

# MAGIC %python
# MAGIC goodjsonpath = filedir + "/good.json"
# MAGIC 
# MAGIC goodjsonpath = goodjsonpath.replace("dbfs:", "")
# MAGIC 
# MAGIC try:
# MAGIC   dbutils.fs.put(goodjsonpath, """{"id":22,"name":"New_person"}""")
# MAGIC except:
# MAGIC   print("File Already Exists")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Bad data 
# MAGIC 
# MAGIC Write the bad data to dbfs

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC badjsonpath = filedir + "/bad.json"
# MAGIC badjsonpath = badjsonpath.replace("dbfs:", "")
# MAGIC try:
# MAGIC   dbutils.fs.put(badjsonpath, """{"badid":22,"badname":"bad_person"}""")
# MAGIC except:
# MAGIC   print("File Already Exists")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Read the good and bad data and save to delta 
# MAGIC 
# MAGIC Read the data into a DF and write to Delta Table

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC goodDF = spark.read.json(goodjsonpath)
# MAGIC goodDF.write.format("delta").mode("append").save(deltaDirectory)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Verify the good data is written

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC querystring = "SELECT * FROM delta.`" + pythondirectory +"`"
# MAGIC display(spark.sql(querystring))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Overview of successful write to Delta Table
# MAGIC 
# MAGIC ![alt text](https://raw.githubusercontent.com/tomthetrainerDB/images/master/goodwrite.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Read the bad data and attempt to write
# MAGIC 
# MAGIC The following cells attempt to write the improperly formatted data to the delta table

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC badDF = spark.read.json(badjsonpath)
# MAGIC try:
# MAGIC   badDF.write.format("delta").mode("append").save(deltaDirectory)
# MAGIC except:
# MAGIC   print("Data was not written to delta table")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Uncomment the cell below to see the informative error generated by the failed write

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC #Uncomment this cell to see the error
# MAGIC 
# MAGIC 
# MAGIC #badDF.write.format("delta").mode("append").save(deltaDirectory)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Failed write overview
# MAGIC 
# MAGIC ![alt text](https://raw.githubusercontent.com/tomthetrainerDB/images/master/badwrite.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Time Travel
# MAGIC 
# MAGIC The Delta log has a list of what files are valid for each delete and insert operation.
# MAGIC 
# MAGIC By referencing this list, a request can be made for the data at a specific point in time. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Read path for a query specifying a specific time, or version of the Delta Table
# MAGIC 
# MAGIC ![alt text](https://raw.githubusercontent.com/tomthetrainerDB/images/master/timetravel.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Data Skipping
# MAGIC 
# MAGIC When Data is written to a Delta Table, the delta log notes statistics on the parquet files that have been written. 
# MAGIC 
# MAGIC Information such as minimum value and max value for a file can be used to skip files that do could not possible match a point or range lookup.
# MAGIC 
# MAGIC A query such as "where date = today" would allow delta to skip files where max(date)< today. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Read Path for a job that had a filter useful for data skipping
# MAGIC 
# MAGIC ![alt text](https://raw.githubusercontent.com/tomthetrainerDB/images/master/dataskipping.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Data Skipping Demonstrated
# MAGIC 
# MAGIC The following cells will demonstrate how data skipping works

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Query with filter that matches many records
# MAGIC 
# MAGIC The first query will look for records where id < 100. 
# MAGIC 
# MAGIC All records have an id < 100
# MAGIC 
# MAGIC Note the number of jobs and the time to execute.
# MAGIC 
# MAGIC You should see 2 jobs

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC querystring = "SELECT * FROM delta.`" + pythondirectory +"` where id < 100"
# MAGIC display(spark.sql(querystring))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Query with filter that matches 0 records
# MAGIC 
# MAGIC The following cell executes a query with a filter id > 100. 
# MAGIC 
# MAGIC The max id for this table is 22. 
# MAGIC 
# MAGIC No records can match this filter. 
# MAGIC 
# MAGIC The first -- and only -- phase of the job generated by this query will be to parse the delta log for statistic information on the parquet files that hold data for this table. 
# MAGIC 
# MAGIC When no parquet files can possibly match, the job is done

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC querystring = "SELECT * FROM delta.`" + pythondirectory +"` where id > 100"
# MAGIC display(spark.sql(querystring))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### A view of the delta log statistics
# MAGIC 
# MAGIC The delta log contains lines for each commit. 
# MAGIC 
# MAGIC The lines are json formatted and include statistics of min/max values within that file
# MAGIC 
# MAGIC The following cells explore the log files
# MAGIC 
# MAGIC **NOTE**
# MAGIC 
# MAGIC You should never hand edit or write to the delta log, this notebook parses it for demonstration purposes only !!

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### List the contents of the delta log directory

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC dbutils.fs.ls(pythondirectory + "/_delta_log")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC dbutils.fs.ls(pythondirectory + "/_delta_log").__class__.__name__

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Find a log file and view statistics
# MAGIC 
# MAGIC The log directory contains
# MAGIC * log entries with extension ".json"
# MAGIC * .crc files to verify the .json files
# MAGIC 
# MAGIC Using dbutils we can pull the json content from the file into a variable. 
# MAGIC 
# MAGIC The cell below takes the first json file it finds and extracts the data into a string.

# COMMAND ----------

# MAGIC %python
# MAGIC # Loop through the files
# MAGIC for l  in dbutils.fs.ls(pythondirectory + "/_delta_log"):
# MAGIC   # Extract a single json example
# MAGIC   if l.path.endswith("json"):
# MAGIC     myjsondata = dbutils.fs.head(l.path)
# MAGIC     print(l)
# MAGIC     break

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Log file format
# MAGIC 
# MAGIC 
# MAGIC The log file has a json formatted string per line. 
# MAGIC 
# MAGIC The line with the statistics will start with {"add"
# MAGIC 
# MAGIC The first cell below prints the complete file.
# MAGIC 
# MAGIC The second cell extracts the line with file statistics. 

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC print(myjsondata)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # extract the line with statistics
# MAGIC for l in myjsondata.splitlines():
# MAGIC   if l.startswith('{"add"'):
# MAGIC     myjsondata2 = l
# MAGIC     print(l)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### View the Statistics

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC print(myjsondata2)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Extract specific fields used for Data Skipping
# MAGIC 
# MAGIC The path and the min and max values are the fields used for data skipping. 
# MAGIC 
# MAGIC The following cells extract and print those fields for this specific file. 
# MAGIC 
# MAGIC Note that the operation for the whole table would parse all the JSON files and the statistics for each parquet file that is relevent for the query at that point in the history of the table. 

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import json
# MAGIC loaded_json = json.loads(myjsondata2)
# MAGIC 
# MAGIC print("The File referenced is: " + loaded_json['add']['path'])

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC print("The Statistics for that file are: " + loaded_json['add']['stats'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup
# MAGIC To remove all files used in this demo, run the cell below:

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC dbutils.fs.rm(userhome + "/delta_demo", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### End of Demo
# MAGIC 
# MAGIC You should now have a basic understanding of
# MAGIC * Delta History
# MAGIC * Delta Data Integrity Checks
# MAGIC * Delta Data Skipping
# MAGIC 
# MAGIC 
# MAGIC **A Reminder**
# MAGIC 
# MAGIC You should never have to read the delta logs manually as this notebook does. 
# MAGIC 
# MAGIC The reading of the delta logs performed in this notebook was to help you to understand delta operations. 
# MAGIC 
# MAGIC Reading the files is harmless, but writing to the delta log directory, or manually writing data files to the delta directory is not supported and may corrupt your delta table.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
