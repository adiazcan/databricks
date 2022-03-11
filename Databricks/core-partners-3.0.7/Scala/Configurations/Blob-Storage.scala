// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC #![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Create Blob Stores
// MAGIC 
// MAGIC This notebook is focused on configuring the blob storage required for the ADB Core partner training, but should provide general enough instructions to be useful in other settings.
// MAGIC  
// MAGIC  
// MAGIC  ### Learning Objectives
// MAGIC  By the end of this walkthrough, you'll have:
// MAGIC  
// MAGIC  - Created two blob storage containers
// MAGIC  - Loaded data into a container
// MAGIC  - Created a read/list SAS token
// MAGIC  - Created a SAS token with full privileges
// MAGIC  - Defined access requirements as Secrets in Azure Key Vault

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC First, access a Storage Account in the Azure Portal.
// MAGIC 
// MAGIC One has been provided for you, but you can follow instruction here to [create a new Storage Account in your Resource Group](https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account?tabs=azure-portal).
// MAGIC 
// MAGIC 1. Click on "All resources"
// MAGIC 2. Click on the storage account starting with `g1`
// MAGIC 
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/resources.png" width=800px />

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Next, access the Blobs associated with this storage account.
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/storage.png" width=800px />

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Currently, we have no containers defined in our blob. Click the indicated button to add a container.
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/blobs-empty.png" width=800px />

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC We'll name our first container `commonfiles`.
// MAGIC 
// MAGIC Click "OK" to continue.
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/new-blob.png" width=800px />

// COMMAND ----------

// MAGIC %md
// MAGIC Now we'll click back into this container...
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/blobs-1.png" width=800px />
// MAGIC 
// MAGIC ... so we can upload data.
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/blob-empty.png" width=800px />

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Download [this file](https://files.training.databricks.com/courses/adbcore/commonfiles/sales.csv) to your local machine. 
// MAGIC 
// MAGIC 1. Select the downloaded file from the file picker.
// MAGIC 2. Click "Upload"
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/file-upload.png"/>
// MAGIC 
// MAGIC Once you see the file successfully uploaded, click on the the name of the storage account to return to your blobs list:
// MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/blob-1.png" width=800px/>

// COMMAND ----------

// MAGIC %md
// MAGIC Create another container named `myblob`. We will not upload anything to this location at this time.
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/blobs-2.png" width=800px/>

// COMMAND ----------

// MAGIC %md
// MAGIC #### CAUTION
// MAGIC Before continuing, make sure that you have completed the instructions for [Configuring a Key Vault]($./Key-Vault).
// MAGIC 
// MAGIC For ease of access, make sure that you have your Key Vault open to the "Secrets" blade in a separate tab of your browser.
// MAGIC 
// MAGIC (We will be defining a few secrets from our storage account.)

// COMMAND ----------

// MAGIC %md
// MAGIC Back in your storage container...
// MAGIC 
// MAGIC 1. Click "Shared access signature"
// MAGIC 2. Deselect the appropriate permissions to create a "Read-Only" Token. 
// MAGIC 3. Click "Generate SAS and connection string" to generate the SAS Token.
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/sas-read.png" width=800px />

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Retrieve the SAS Token generated.
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/sas-write-secrets.png" />

// COMMAND ----------

// MAGIC %md
// MAGIC In your Key Vault Secrets tab:
// MAGIC 
// MAGIC 1. Enter `storageread` as the Name
// MAGIC 2. Paste your SAS token as the Value
// MAGIC 3. Click "Create"
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/storageread.png" />

// COMMAND ----------

// MAGIC %md
// MAGIC You should see one secret now in your vault.
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/secrets-1.png" width=800px />
// MAGIC 
// MAGIC  You'll want to "Generate/Import" another secret.
// MAGIC 
// MAGIC This one will be named `storagewrite`. Back in your blob storage SAS token tab:
// MAGIC 
// MAGIC 1. Select all the permissions
// MAGIC 1. Click "Generate SAS and connection string"
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/sas-write.png"/>
// MAGIC 
// MAGIC Copy and paste the newly generated SAS token to your Secrets.

// COMMAND ----------

// MAGIC %md
// MAGIC Finally, you'll create one more secret.
// MAGIC 
// MAGIC 1. Name: `storageaccount`
// MAGIC 2. Value: copy/paste the name of your storage account
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/account-name.png"/>

// COMMAND ----------

// MAGIC %md
// MAGIC When you're done, you should see the following keys:
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/secrets-all.png" width=800px/>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
