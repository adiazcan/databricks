# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Databricks CLI and Secret Scopes
# MAGIC 
# MAGIC Using the Databricks command line interface (CLI), we can configure secret scopes in the workspace. These secret scopes allow you to store secrets, such as database connection strings, securely. If someone tries to output a secret to a notebook, it is replaced by `[REDACTED]`. This helps prevent someone from viewing the secret or accidentally leaking it when displaying or sharing the notebook.
# MAGIC 
# MAGIC Databricks secrets rely on using the CLI. Full docs are [here](https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC In this lesson, we will demonstrate using secrets to store sensitive connection information for an encrypted S3 bucket. In this notebook, we'll demonstrate:
# MAGIC 
# MAGIC 1. Basic CLI usage
# MAGIC   1. How to install
# MAGIC   1. Accessing the docs from the command line with `-h`
# MAGIC   1. Generating and configuring a token
# MAGIC   1. Reviewing the `.databrickscfg` file to add additional workspaces
# MAGIC 1. Secrets Management
# MAGIC   1. Creating scopes (CLI)
# MAGIC   1. Adding secrets to a scope (CLI)
# MAGIC   1. Listing scopes (notebook and CLI)
# MAGIC   1. Listing secrets (notebook and CLI)
# MAGIC   1. Using secrets (notebook)
# MAGIC 
# MAGIC ### Online Resources
# MAGIC 
# MAGIC - [Databricks CLI](https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html)
# MAGIC - [Databricks Secrets CLI](https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html#secrets-cli)
# MAGIC - [Databricks Secrets](https://docs.databricks.com/user-guide/secrets/index.html)
# MAGIC - [Databricks Secret Access Control](https://docs.databricks.com/user-guide/secrets/secret-acl.html)
# MAGIC - [Managing Databricks Groups](https://docs.databricks.com/administration-guide/admin-settings/groups.html#managing-groups)
# MAGIC - [Using Secrets in a Notebook](https://docs.databricks.com/user-guide/secrets/example-secret-workflow.html#use-the-secrets-in-a-notebook)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Configuring the Databricks CLI
# MAGIC 
# MAGIC ### Installation
# MAGIC 
# MAGIC To install the Databricks CLI, run `pip install databricks-cli` from the command line.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Use `pip3 install databricks-cli` if Python 3 is not your default Python.
# MAGIC 
# MAGIC ### Configuring Access with User-Generated Token
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The full Databricks CLI docs are available for each command using the `-h` flag.
# MAGIC 
# MAGIC First, generate a token from the Databricks UI.
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/awscore/s3bucket-secrets/user-settings.png)<br><hr>
# MAGIC ![](https://files.training.databricks.com/images/awscore/s3bucket-secrets/generate-token.png)<br><hr>
# MAGIC ![](https://files.training.databricks.com/images/awscore/s3bucket-secrets/new-token.png)<br>
# MAGIC 
# MAGIC Copy this value to your clipboard.
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/awscore/s3bucket-secrets/copy-token.png)<br>
# MAGIC 
# MAGIC In the command line, run `databricks configure --token`
# MAGIC 
# MAGIC As prompted, enter the URL for the Databricks workspace.
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/awscore/s3bucket-secrets/cli-token.png)<br>
# MAGIC 
# MAGIC As prompted, paste the token generated from the Databricks UI.
# MAGIC 
# MAGIC To confirm success, run `databricks workspace ls`. You should see the directories in the top level of your workspace.
# MAGIC 
# MAGIC ### Reviewing Tokens and Adding Additional Workspaces
# MAGIC 
# MAGIC Token configurations are saved to `~/.databrickscfg`.
# MAGIC 
# MAGIC To review and edit this file in vim, run `vi ~/.databrickscfg`.
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/awscore/s3bucket-secrets/databrickscfg.png)<br>
# MAGIC 
# MAGIC The recently added token will be listed under `[DEFAULT]`.
# MAGIC 
# MAGIC You can add additional profiles by adding the option `--profile <profile_name>` to your `databricks configure --token` command, or by directly editing this file. Each entry will have the form:
# MAGIC 
# MAGIC ```
# MAGIC [PROFILE-NAME]
# MAGIC host = <host-url>
# MAGIC token = <token>
# MAGIC ```
# MAGIC All Databricks CLI commands will accept the `--profile` option, but will use the `DEFAULT` profile unless otherwise specified.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Configuring Secrets with the Databricks CLI
# MAGIC 
# MAGIC ### Create a Scope
# MAGIC 
# MAGIC To create a new scope, execute `databricks secrets create-scope --scope <scope-name>`.
# MAGIC 
# MAGIC You can confirm success by running `databricks secrets list-scopes`.
# MAGIC 
# MAGIC ### Add a Secret
# MAGIC 
# MAGIC Secrets are added as key/value pairs. The key will be visible and should describe the value it corresponds to.
# MAGIC 
# MAGIC To add a new secret, execute `databricks secrets put --scope <scope-name> --key <key>`.
# MAGIC 
# MAGIC This will open a vim editor in which you can enter the secret value.
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/awscore/s3bucket-secrets/put-secret-key.png)<br>
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You can pass secret values directly as strings in the command line using the `--string-value <TEXT>` option, or load the contents of a file using `--binary-file <PATH>`.
# MAGIC 
# MAGIC To confirm your secret has loaded, run `databricks secrets list --scope <scope-name>`.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> While secrets provide a great way to manage tokens and other credentials you don't wish to display in plain text, users in the Databricks workspace with access to these secrets will be able to read and print out the bytes. Secrets should therefore be conceived of as a more secure way to manage access to resources than distributing credentials in files or plaintext, but **you should only grant access to individuals that can be trusted with these secrets**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### List Secret Scopes
# MAGIC 
# MAGIC To list the existing secret scopes the `dbutils.secrets` utility can be used.
# MAGIC 
# MAGIC You can list all scopes currently available in your workspace with:

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.secrets.listScopes()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### List Secrets within a specific scope
# MAGIC 
# MAGIC 
# MAGIC To list the secrets within a specific scope, you can supply that scope name.

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.secrets.list("awscore")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Using your Secrets
# MAGIC 
# MAGIC To use your secrets, you supply the scope and key to the `get` method.
# MAGIC 
# MAGIC Run the following cell to retrieve and print a secret.

# COMMAND ----------

# MAGIC %python
# MAGIC print(dbutils.secrets.get(scope="awscore", key="readAccess"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Secrets are not displayed in clear text
# MAGIC 
# MAGIC Notice that the value when printed out is `[REDACTED]`. This is to prevent your secrets from being exposed.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
