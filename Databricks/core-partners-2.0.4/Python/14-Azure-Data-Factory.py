# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #Azure Databricks Notebooks As Part Of Azure Data Factory Pipelines
# MAGIC 
# MAGIC **In this lesson:**
# MAGIC * Create an Azure Data Factory.
# MAGIC * Create a ADF Pipeline Containing an Azure Databricks linked service.
# MAGIC * Trigger the Pipeline.
# MAGIC * Monitor the Pipeline.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> While here we will show scheduling a notebook with Data Factory, you can also schedule .jar and .py files, taking advantage of the much lower cost of Data Engineering Light vs. interactive clusters. [Find detailed pricing details here](https://azure.microsoft.com/en-us/pricing/details/databricks/).

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 1: Create an Azure Data Factory<br>
# MAGIC 
# MAGIC 1.  In the Azure Portal click `+ Create a resource`, select `Analytics`, and then select `Data Factory`
# MAGIC 2.  In the `New data factory` pane, enter a globally unique name for your Data Factory
# MAGIC 3.  Select your Azure `Subscription`
# MAGIC 4.  For `Resource Group`, select `Use existing` and select the resource group you are using for this training
# MAGIC 5.  For `Version` select `V2 (Preview)`
# MAGIC 6.  For `Location` **select the region you are using for this training**
# MAGIC 7.  Select `Create`
# MAGIC 8.  Select `Pin to dashboard`
# MAGIC 9.  Once the Data Factory is deployed you will see your Data Factory page.  Select the `Author & Monitor` tile to launch the UI

# COMMAND ----------

# MAGIC %md
# MAGIC <div><img src="https://files.training.databricks.com/images/adbcore/azure_class/datafactory1.png"/></div><br/>
# MAGIC <div><img src="https://files.training.databricks.com/images/adbcore/azure_class/datafactory2.png"/></div><br/>
# MAGIC <div><img src="https://files.training.databricks.com/images/adbcore/azure_class/datafactory3.png"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create an Azure Databricks Linked Service
# MAGIC 1.  On the `Let's get started` page, select the Pencil icon in the left pane to switch to the Author tab.
# MAGIC 2.  Select `Connections` at the lower left corner of the Factory Resources pane.
# MAGIC 3.  Select `+ New` in the Connctions tab under `Linked Services`.
# MAGIC 4.  In the `New Linked Service` window, select the `Compute` tab, then select the `Azure Databricks` tile, and select `Continue`.
# MAGIC 5.  Complete the following steps in the `New Linked Service` window.<br>
# MAGIC   a. For `Name`, enter `AzureDatabricks_LinkedService`.<br>
# MAGIC   b. For `Cluster`, select `Existing Cluster`.  Normally `New Cluster` is prefered for triggered pipelines as they use a lower cost engineering tier cluster.<br>
# MAGIC   c. For `Domain/Region`, select the region where your Databricks workspace is located.<br>
# MAGIC   d. For `Access token` follow the instructions <a href="https://docs.databricks.com/api/latest/authentication.html#generate-token" target="blank">here</a> to generate an Access Token in the Databricks UI.<br>
# MAGIC   e. Copy the `Existing cluster id` from the Databricks UI.  It is on the bottom of the `Tags` tab of the `Clusters` UI for the cluster you are using for this training.<br>
# MAGIC   f. Select `Finish`.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC <div><img src="https://files.training.databricks.com/courses/azure-databricks/images/ADF-Setup-5.png"/></div><br/>
# MAGIC <div><img src="https://files.training.databricks.com/courses/azure-databricks/images/ADF-Setup-6.png"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create an ADF Pipeline
# MAGIC 1.  Select the `+` icon next to the next to the `Search Resourses` text box in the `Factory Resources` pane and choose `Pipeline`.
# MAGIC 2.  Enter `ETL_DWFactResellerSales` in the `Name` field under the `General` tab for the pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC <div><img src="https://files.training.databricks.com/courses/azure-databricks/images/ADF-Setup-22.png"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create a Databricks Notebook activity in the Pipeline
# MAGIC 1.  In the `Activities` toolbox, expand `Databricks`. Drag the `Notebook` activity to the pipeline designer surface.
# MAGIC 2.  Under the `General` tab of the properteis for the notebook activity, enter `09b-Populate-Warehouse-ADF` for `Name`.
# MAGIC 3.  Under the `Settings` tab select `AzureDatabricks_LinkedService` for the `Linked service`.
# MAGIC 4.  For the `Notebook path`, enter the relative path for the notebook ... `/Users/<userhome>/09b-Populate-Warehouse-ADF`.  Replace `<userhome>` with your Azure username and verify the relative path to this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC <div><img src="https://files.training.databricks.com/courses/azure-databricks/images/ADF-Setup-23.png"/></div><br/>
# MAGIC <div><img src="https://files.training.databricks.com/courses/azure-databricks/images/ADF-Setup-24.png"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC ###Trigger and Monitor The Pipeline
# MAGIC 1.  Select `Trigger` and then choose `Trigger Now` in the top menu of the new pipeline window.
# MAGIC 2.  On the left most pane select the `Monitor` icon below the pencil icon.
# MAGIC 3.  In the `Monitor` window you can see the current and past runs of your pipelines.  Your pipeline should succeed in about 1 minute.

# COMMAND ----------

# MAGIC %md
# MAGIC <div><img src="https://files.training.databricks.com/courses/azure-databricks/images/ADF-Setup-29.png"/></div><br/>
# MAGIC <div><img src="https://files.training.databricks.com/courses/azure-databricks/images/ADF-Setup-30.png"/></div><br/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
