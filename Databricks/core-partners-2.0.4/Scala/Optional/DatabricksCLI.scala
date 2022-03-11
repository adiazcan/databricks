// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC #Introduction to Databricks CLI (Command Line Interface)
// MAGIC 
// MAGIC Instead of navigating through the UI to create clusters, jobs, etc. we can use the CLI to programmatically accomplish these tasks. This notebook is intended to be a reference guide - you should use a terminal to execute the commands below.
// MAGIC 
// MAGIC Documentation: [https://github.com/databricks/databricks-cli](https://github.com/databricks/databricks-cli)
// MAGIC 
// MAGIC **NOTE**: You must use Python Version > 2.7.9. Python 3 is not supported.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Install Databricks CLI
// MAGIC 
// MAGIC Start by installing the Databricks CLI using pip: `pip install --upgrade databricks-cli`
// MAGIC 
// MAGIC If you are encountering error messages installing the Databricks CLI, go through and execute the commands below (assumes running Linux or macOS):
// MAGIC 
// MAGIC 0. [Install Homebrew](https://brew.sh/): `/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"`
// MAGIC 0. Install Python 2 via Homebrew: `brew install python2`
// MAGIC 0. [Install pip via Homebrew](https://docs.brew.sh/Homebrew-and-Python.html): `python2 -m pip install --upgrade pip`
// MAGIC 0. Install virtualenv: `/usr/local/bin/python2 -m pip install virtualenv`
// MAGIC 0. Navigate to the directory you want to create the virtualenv, and run: `/usr/local/bin/python2 -m virtualenv dbenv`
// MAGIC   * You can change the name to be something other than `dbenv`
// MAGIC 0. From the directory you created the virtualenv, activate it: `. $HOME/dbenv/bin/activate`
// MAGIC   * Substitute `$HOME` with your path to the virtual environment.
// MAGIC 
// MAGIC Now try `pip install --upgrade databricks-cli`

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Authentication
// MAGIC 
// MAGIC There are two ways to configure the command line interface (choose one or the other):
// MAGIC * Username and password
// MAGIC   * `databricks configure` (enter hostname/username/password at prompt)
// MAGIC * [Authentication token](https://docs.databricks.com/api/latest/authentication.html#token-management)
// MAGIC   * `databricks configure --token` (enter hostname/auth-token at prompt)
// MAGIC   * Databricks Admins can enable REST API Tokens, so users can use tokens instead of username/passwords for REST API Authentication. These tokens have expiration and can be revoked. Personal access tokens are disabled by default.
// MAGIC 
// MAGIC Once that is configured, you can start using Databricks CLI!

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting help
// MAGIC 
// MAGIC Run `databricks workspace -h` to see a list of commands.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Importing/Exporting files
// MAGIC 
// MAGIC To view the files in your workspace: `databricks workspace ls /Users/example@databricks.com`
// MAGIC 
// MAGIC To copy a file from your local machine to DBFS: `dbfs cp test.txt dbfs:/test.txt`
// MAGIC  * Or recursively upload a directory: `dbfs cp -r test-dir dbfs:/test-dir`
// MAGIC 
// MAGIC You can also download files from DBFS to your local machine: `dbfs cp dbfs:/test.txt ./test.txt`
// MAGIC  * Or recursively download a directory: `dbfs cp -r dbfs:/test-dir ./test-dir`

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Launch a Python 3 cluster
// MAGIC 
// MAGIC To see all of the options to interact with clusters: `databricks clusters -h`
// MAGIC 
// MAGIC To see all clusters: `databricks clusters list`
// MAGIC 
// MAGIC To create a cluster using Python 3 (available only via REST API):
// MAGIC 
// MAGIC 0. Make a file called `launchPython3.json` containing:
// MAGIC ```
// MAGIC {"cluster_name": "python-3-demo",
// MAGIC     "node_type_id": "class-node",
// MAGIC     "num_workers": 1,
// MAGIC     "spark_version": "3.4.x-scala2.11",
// MAGIC     "spark_env_vars": {
// MAGIC       "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
// MAGIC 	}
// MAGIC }```
// MAGIC 0. Run `databricks clusters create --json-file launchPython3.json`.
// MAGIC  * This will return the cluster id and launch the cluster if successful.

// COMMAND ----------

// MAGIC %md
// MAGIC You can also use the CLI to create jobs, view job/cluster status, etc.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
