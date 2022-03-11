# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Key Vault
# MAGIC 
# MAGIC [Azure Key Vault](https://docs.microsoft.com/en-us/azure/key-vault/key-vault-whatis) provides us with a number of options for storing and sharing secrets and keys between Azure applications, and has direct integration with Azure Databricks. In this notebook, we'll focus on setting proper permissions and syncing with Databricks. These instructions are based around configurations and settings for the ADB Core partner training, but should be adaptable to production requirements.
# MAGIC 
# MAGIC ### Learning Objectives
# MAGIC By the end of this walkthrough, you'll understand how to:
# MAGIC - Configure Key Vault Access Policies
# MAGIC - Access the Databricks Secret Scopes UI
# MAGIC - Link Key Vault to Azure Databricks
# MAGIC 
# MAGIC **NOTE**: Instructions for loading secrets into the Key Vault are present in the [Blob Storage Configuration]($./Blob-Storage) notebook.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> **PLEASE** open a new browser tab and navigate to <https://portal.azure.com>.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Key Vault Access Policies
# MAGIC 
# MAGIC 1. Go to "All resources"
# MAGIC 2. Click on the Key Vault resource
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/resources-kv.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC First, we'll need to set the proper "Access policies"
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/keyvault-home.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC While our user is a "Contributor" on this resource, we must add an access policy to add/list/use secrets.
# MAGIC 
# MAGIC Click "Add access policy"
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/access-none.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Select "Key, Secret, & Certificate Mangement" from the dropdown
# MAGIC 2. Click to select a principal
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/access-template.png" />

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Search for your user ID
# MAGIC 2. Click on the matching result to select
# MAGIC 3. Click "Select"
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/access-principal.png" />

# COMMAND ----------

# MAGIC %md
# MAGIC Now you'll need to click "Add" and then...
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/access-not-added.png" />

# COMMAND ----------

# MAGIC %md
# MAGIC ... you'll click "Save" to finalize the configurations.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/access-not-saved.png" />

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Access Azure Databricks Secrets UI
# MAGIC 
# MAGIC Now that you have an instance of Azure Key Vault up and running, it is time to let Azure Databricks know how to connect to it.
# MAGIC 
# MAGIC The first step is to open a new web browser tab and navigate to `https://<your_azure_databricks_url>#secrets/createScope` 
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The number after the `?o=` is the unique workspace identifier; append `#secrets/createScope` to this.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/db-secrets.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC ## Link Azure Databricks to Key Vault
# MAGIC We'll be copy/pasting some values from the Azure Portal to this UI.
# MAGIC 
# MAGIC In the Azure Portal on your Key Vault tab:
# MAGIC 1. Go to properties
# MAGIC 2. Copy and paste the DNS Name
# MAGIC 3. Copy and paste the Resource ID
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/properties.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC In the Databricks Secrets UI:
# MAGIC 
# MAGIC 1. Enter the name of the secret scope; here, we'll use `students`.
# MAGIC 2. Paste the DNS Name
# MAGIC 3. Paste the Resource ID
# MAGIC 4. Click "Create"
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/db-secrets-complete.png" />
# MAGIC 
# MAGIC   > MANAGE permission allows users to read and write to this secret scope, and, in the case of accounts on the Azure Databricks Premium Plan, to change permissions for the scope.
# MAGIC 
# MAGIC   > Your account must have the Azure Databricks Premium Plan for you to be able to select Creator. This is the recommended approach: grant MANAGE permission to the Creator when you create the secret scope, and then assign more granular access permissions after you have tested the scope.

# COMMAND ----------

# MAGIC %md
# MAGIC After a moment, you will see a dialog verifying that the secret scope has been created. Click "Ok" to close the box.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/db-secrets-confirm.png" />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create secrets in Key Vault
# MAGIC 
# MAGIC To create secrets in Key Vault that can be accessed from your new secret scope in Databricks, you need to either use the Azure portal or the Key Vault CLI. For simplicity's sake, we will use the Azure portal:
# MAGIC 
# MAGIC 1. Select **Secrets** in the left-hand menu.
# MAGIC 2. Select **+ Generate/Import** in the Secrets toolbar.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/secrets-none.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC In the next blade:
# MAGIC 
# MAGIC 1. Enter the name of the secret (this will be the key to access the secret value; this will be visible in plain text)
# MAGIC 2. Paste/enter the value for the secret (this will be the value that is stored as a secret; this will be `[REDACTED]`).
# MAGIC 3. Click "Create"
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/storageread.png" />

# COMMAND ----------

# MAGIC %md
# MAGIC You can immediately use this secret in your Azure Databricks environment.
# MAGIC 
# MAGIC For specific instructions on adding keys for the ADB Core Blob Storage demo, make sure you've completed all steps in [this notebook]($./Blob-Storage).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
