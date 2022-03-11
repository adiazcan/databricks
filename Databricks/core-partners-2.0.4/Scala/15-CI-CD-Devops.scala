// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # CI/CD with Azure DevOps
// MAGIC 
// MAGIC While we won't be demonstrating [all of the features of Azure DevOps](https://docs.microsoft.com/en-us/azure/devops/user-guide/alm-devops-features?view=azure-devops) today, here are some of the features that make it well-suited to CI/CD with Azure Databricks.
// MAGIC 
// MAGIC 1. Integrated Git repositories
// MAGIC 1. Integration with other Azure services
// MAGIC 1. Automatic virtual machine management for testing builds
// MAGIC 1. Secure deployment
// MAGIC 1. Friendly GUI that generates (and accepts) various scripted files
// MAGIC 
// MAGIC ## But what is CI/CD?
// MAGIC 
// MAGIC (The below is taken from [Atlassian](https://www.atlassian.com/continuous-delivery/ci-vs-ci-vs-cd).)
// MAGIC 
// MAGIC ### Continuous Integration
// MAGIC 
// MAGIC Developers practicing continuous integration merge their changes back to the main branch as often as possible. The developer's changes are validated by creating a build and running automated tests against the build. By doing so, you avoid the integration hell that usually happens when people wait for release day to merge their changes into the release branch.
// MAGIC 
// MAGIC ### Continuous Delivery
// MAGIC 
// MAGIC Continuous delivery is an extension of continuous integration to make sure that you can release new changes to your customers quickly in a sustainable way. This means that on top of having automated your testing, you also have automated your release process and you can deploy your application at any point of time by clicking on a button.
// MAGIC 
// MAGIC ### Continuous Deployment
// MAGIC 
// MAGIC Continuous deployment goes one step further than continuous delivery. With this practice, every change that passes all stages of your production pipeline is released to your customers. There's no human intervention, and only a failed test will prevent a new change to be deployed to production.
// MAGIC 
// MAGIC ## Who benefits?
// MAGIC 
// MAGIC _Everyone_. Once properly configured, automated testing and deployment can free up your engineering team and enable your data team to push their changes into production. For example:
// MAGIC 
// MAGIC - Data engineers can easily deploy changes to generate new tables for BI analysts.
// MAGIC - Data scientists can update models being used in production.
// MAGIC - Data analysts can modify scripts being used to generate dashboards.
// MAGIC 
// MAGIC In short, changes made to a Databricks notebook can be pushed to production with a simple mouse click (and then any amount of oversight that your DevOps team feels is appropriate).

// COMMAND ----------

// MAGIC %md
// MAGIC ## Barebones Databricks CI/CD with Azure DevOps
// MAGIC 
// MAGIC The below are the most basic instructions for connecting a Databricks workspace to Azure DevOps for CI/CD.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Requirements
// MAGIC The following services are needed
// MAGIC 
// MAGIC - Azure DevOps
// MAGIC - Azure Databricks Workspaces (eg. QA, DEV, PROD)

// COMMAND ----------

// MAGIC %md
// MAGIC ### End Goal Scenario
// MAGIC 1. Develop a notebook in DEV workspace
// MAGIC 1. Commit this to Azure DevOps (Master branch of the repo)
// MAGIC 1. Once the commit is successful, this notebook will automatically be deployed into PROD workspace
// MAGIC Note: Ideally, a user wants to create a feature branch first and work there. Once it's reviewed by peers using 'pull request', this feature branch can be committed to master branch. Then, it'll be automatically deployed into higher environment like Staging for production testing or PROD directly.

// COMMAND ----------

// MAGIC %md
// MAGIC ### High Level Steps
// MAGIC At a high level, setting up CI/CD on Azure Databrick with Azure DevOps consists of 4 steps:
// MAGIC 
// MAGIC 1. Setting up Azure DevOps Repo
// MAGIC 1. Have your Azure Workspace and notebook configured to use Azure DevOps
// MAGIC 1. Azure DevOps - Create a build pipeline (CI)
// MAGIC 1. Azure DevOps - Create a release pipeline (CD)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 1: Setting up Azure DevOps Repo
// MAGIC 1. Go to [https://aex.dev.azure.com](https://aex.dev.azure.com). Make sure that you are logged in under the correct account.
// MAGIC 1. If your account has access to multiple Azure subscriptions, make sure that you're in the same directory as your Databricks workspaces.
// MAGIC 1. Create a project.
// MAGIC 1. Fill the form provided. Choose the visibility, initial source control type, and work item process.
// MAGIC 
// MAGIC <img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/cicd_demo/image2019-3-15_17-10-42.png width=800px>
// MAGIC 1. Once Project is created, go to **Repos**, then **Initialize** your repo.
// MAGIC 
// MAGIC <img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/cicd_demo/image2019-3-15_17-23-2.png width=800px>

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 2: Setting up Azure Workspace and your notebook
// MAGIC 
// MAGIC 1. Create 2 workspaces (here named: DEV & PROD) for development/testing purposes.
// MAGIC 1. Go to DEV workspace and setup your Git integration provider to Azure DevOps Services<br>
// MAGIC <img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/cicd_demo/image2019-3-15_17-28-14.png width=350px><img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/cicd_demo/image2019-3-15_17-29-48.png width=400px>
// MAGIC 1. Sync your notebook to the repo created above<br>
// MAGIC <img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/cicd_demo/image2019-3-15_17-57-37.png width=600px>
// MAGIC >**Note**: format of URL is <br>`http://dev.azure.com/<myOrg>/<myproject>/_git/<myRepo>` <br>which differs from the URL of the repo displayed in Azure DevOps <br>`http://dev.azure.com/<myOrg>/_git/<myRepo>`
// MAGIC 1. Commit some code to test if repo is setup correctly.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 3: Azure DevOps - Create a build pipeline (CI)
// MAGIC Build pipeline provides **CI** portion of CI/CD.
// MAGIC 1. Create a new build pipeline.<br>
// MAGIC <img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/cicd_demo/image2019-3-15_18-12-10.png width=600px>
// MAGIC 1. Use the visual designer to create pipeline<br>
// MAGIC <img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/cicd_demo/image2019-3-15_18-17-9.png width=600px>
// MAGIC 1. Select project, repo, and default branch to build, then Continue<br>
// MAGIC <img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/cicd_demo/image2019-3-15_18-19-57.png width=600px>
// MAGIC 1. Start with an empty job<br>
// MAGIC <img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/cicd_demo/image2019-3-15_18-23-59.png width=600px>
// MAGIC 1. Search "**Publish Build Artifacts**" task and add it to a Agent job<br>
// MAGIC <img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/cicd_demo/image2019-3-15_18-28-22.png width=1000px>
// MAGIC 1. Select added task, and fill in some parameters such as "Path to publish" and "Artifact name"<br>
// MAGIC <img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/cicd_demo/image2019-3-15_22-35-58.png width=1000px>
// MAGIC 1. Go to **Triggers** tab and enable continuous integration. This will automatically trighger a build whenever you commit your code to the repo.<br>
// MAGIC <img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/cicd_demo/image2019-3-15_18-57-55.png width=1000px>
// MAGIC 1. Save & queue
// MAGIC 1. Go to Builds page and you will see your pipeline and the build is created<br>
// MAGIC <img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/cicd_demo/image2019-3-15_18-54-32.png width=1000px>

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 4: Azure DevOps - Create a release pipeline (CD)
// MAGIC Release pipeline provides **CD** portion of CI/CD.
// MAGIC 1. Similar to build pipeline, create a new release pipeline<br>
// MAGIC <img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/cicd_demo/image2019-3-15_19-4-25.png width=600px>
// MAGIC 1. Again, start with an "Empty job"
// MAGIC 1. "Add an artifact" which we just created in a build pipeline above<br>
// MAGIC <img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/cicd_demo/image2019-3-15_19-10-18.png width=600px>
// MAGIC 1. Then add a task in a stage<br>
// MAGIC <img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/cicd_demo/image2019-3-15_19-18-21.png width=600px>
// MAGIC 1. Search "Databricks", then add **Databrick Deploy Notebooks**<br>(Note: If this is the first time adding this task, you first have to install “**Databricks Script Deployment Task by Data Thirst**”, then those other databricks tasks will become available.)<br>Important Note: Package provided by 3rd party<br>
// MAGIC <img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/cicd_demo/image2019-3-15_19-22-6.png width=1000px>
// MAGIC 1. Once the task is added, fill the required parameters then Save<br>(Note: Databricks bearer token has to be obtained from PROD workspace for access permission)
// MAGIC <img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/cicd_demo/image2019-3-15_22-42-10.png width=1000px>
// MAGIC 1. Also, enable Continuous deployment trigger, then Save<br>
// MAGIC <img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/cicd_demo/image2019-3-15_22-46-13.png width=1000px>
// MAGIC 1. Finally, create a release<br>
// MAGIC <img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/cicd_demo/image2019-3-15_22-47-41.png width=400px>
// MAGIC <br>CI/CD setup is now completed. If you commit your code from DEV workspace to the repo (master branch), the same notebook should be available in PROD.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
