// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC #Azure Databricks Notebooks As Part Of Azure Data Factory Pipelines
// MAGIC 
// MAGIC ## Learning Objectives
// MAGIC By the end of this lesson, you should be able to:
// MAGIC * Create an Azure Data Factory
// MAGIC * Create a ADF Pipeline Containing an Azure Databricks linked service
// MAGIC * Trigger the Pipeline
// MAGIC * Monitor the Pipeline
// MAGIC * Use widgets to pass variables to notebooks
// MAGIC * Define notebook exit messages
// MAGIC 
// MAGIC Azure Data Factory is a cloud-based ETL and data integration service that allows you to create data-driven workflows for orchestrating data movement and transforming data at scale. Using Azure Data Factory, you can create and schedule data-driven workflows (called pipelines) that can ingest data from disparate data stores.
// MAGIC 
// MAGIC A data factory can have one or more pipelines. A pipeline is a logical grouping of activities that together perform a task. For example, a pipeline could contain a set of activities that ingest and clean log data, and then kick off a mapping data flow to analyze the log data. The pipeline allows you to manage the activities as a set instead of each one individually. You deploy and schedule the pipeline instead of the activities independently.
// MAGIC 
// MAGIC While here we will show scheduling a notebook with the Data Factory UI, you can also schedule .jar and .py files, taking advantage of the much lower cost of Data Engineering vs. interactive clusters. [Find detailed pricing details here](https://azure.microsoft.com/en-us/pricing/details/databricks/).
// MAGIC 
// MAGIC **Additional Resources**
// MAGIC - [Run a Databricks notebook with the Databricks Notebook Activity in Azure Data Factory](https://docs.microsoft.com/en-us/azure/data-factory/transform-data-using-databricks-notebook)
// MAGIC - [Passing parameters between notebooks and Data Factory](https://docs.microsoft.com/en-us/azure/data-factory/transform-data-databricks-notebook#passing-parameters-between-notebooks-and-data-factory)
// MAGIC - [Branching and chaining activities in a Data Factory pipeline](https://docs.microsoft.com/en-us/azure/data-factory/tutorial-control-flow-portal)
// MAGIC - [Expressions and functions in Azure Data Factory](https://docs.microsoft.com/en-us/azure/data-factory/control-flow-expression-language-functions)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC If a Data Factory does not exist in your account, follow instructions here:
// MAGIC [Create a Data Factory](https://docs.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory-portal#create-a-data-factory)
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> The `Author & Monitor` button launches the Azure Data Factory UI.

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Navigate to Azure Databricks Linked Service
// MAGIC 1.  On the `Let's get started` page, select the Pencil icon in the left pane to switch to the Author tab.
// MAGIC 1.  Select `Connections` at the lower left corner of the Factory Resources pane.
// MAGIC 1.  Select `+ New` in the Connections tab under `Linked Services`.
// MAGIC 1.  In the `New Linked Service` window, select the `Compute` tab, then select the `Azure Databricks` tile, and select `Continue`.

// COMMAND ----------

// MAGIC %md
// MAGIC <div><img src="https://files.training.databricks.com/courses/azure-databricks/images/ADF-Setup-5.png"/></div><br/>

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2. Configure Linked Service
// MAGIC **IMPORTANT**: To properly populate, this form should be completed from top to bottom. All fields not mentioned can be left with defaults.
// MAGIC 
// MAGIC 1. Select your current subscription from the drop down for `Azure subscription`
// MAGIC 1. Select the `Databricks workspace` you are currently in
// MAGIC 1. For `Select cluster`, select `Existing interactive cluster`. **Normally `New job cluster` is prefered for triggered pipelines as they use a lower cost engineering tier cluster.** 
// MAGIC 1. For `Access token` follow the instructions [here](https://docs.databricks.com/dev-tools/api/latest/authentication.html#generate-a-token) to generate an Access Token in the Databricks UI.
// MAGIC 1. Select the name of your cluster from the drop down list under `Choose from existing clusters`
// MAGIC 1. Select `Create`

// COMMAND ----------

// MAGIC %md
// MAGIC ## 3. Create an ADF Pipeline & Add a Databricks Notebook Activity
// MAGIC 1. Hover over the number to the right of `Pipelines` and click on the elipses that appear.
// MAGIC 1. Select `New pipeline`.
// MAGIC 1. In the `Activities` panel to the right of the `Factory Resources` panel, click `Databricks` to expand this section.
// MAGIC 1. Drag the `Notebook` option into the tableau to the right.

// COMMAND ----------

// MAGIC %md
// MAGIC ## 4. Configure Databricks Notebook Activity
// MAGIC 1. With the notebook activity still selected, click the `Azure Databricks` tab near the bottom of the screen.
// MAGIC 1. Select the `Databricks linked service` you created above in section 2 from the drop down list.
// MAGIC 1. Under the `Settings` tab, click the `Browse` button to enter an interactive file explorer for the directory of your linked Databricks workspace. 
// MAGIC 1. Navigate to the directory that contains the lesson notebooks for this course. Select the `Runnable` directory, and pick the notebook `Record-Run`. Click `OK`.
// MAGIC 1. Click `Base parameters` to expand a drop down, and then click `New`.
// MAGIC 1. Under `Name` enter `ranBy`. For `Value`, enter `ADF`.

// COMMAND ----------

// MAGIC %md
// MAGIC ## 5. Publish and Trigger the Pipeline
// MAGIC 1. At the top left, you should see a `Publish all` button highlighted in blue with a yellow **1** on it. Click this to save your configurations (this is required to trigger the pipeline).
// MAGIC 1. Click `Add trigger` and then choose `Trigger now` from the drop-down. Click `Finish` at the bottom of the blade that appears.

// COMMAND ----------

// MAGIC %md
// MAGIC ## 6. Monitor the Run
// MAGIC 1. On the left most pane select the `Monitor` icon below the pencil icon. This will pull up a list of all recent pipeline runs.
// MAGIC 1. In the `Actions` column, click on the left icon to `View activity runs`. This will allow you to see the current progress of your pipeline.
// MAGIC 1. Your scheduled notebook will appear in the list at the bottom of the window. Click the glasses icon to bring up the `Details`.
// MAGIC 1. In the window the appears, click the `Run page url`. The page that loads will be a live view of the notebook as it runs in Azure Databricks.
// MAGIC 1. Once the notebook has finished running, you'll be able to view the `Output` of the notebook by clicking the middle icon in the `Actions` column. Note that the `"runOutput"` here is the value that was passed to `dbutils.notebook.exit()` in the scheduled notebook.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
