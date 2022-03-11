// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Azure Databricks Core
// MAGIC 
// MAGIC ### COURSE DESCRIPTION
// MAGIC As a first party service on Azure, Azure Databricks is tightly integrated with many services in the Azure ecosystem.  This makes Azure Databricks the ideal processing engine at the heart of many data workflows including Batch, Streaming, Lambda/Delta, and multi-hop ETL, as well as, ML and other Pipelines.  This class dives into how to configure and connect Azure Databricks with some key tools in the Azure ecosystem to build scalable, robust data workflows.
// MAGIC 
// MAGIC ### PREREQUISITES
// MAGIC - Basic Proficiency in either Scala or Python
// MAGIC - Basic knowledge of Spark DataFrames helpful (recommended)
// MAGIC 
// MAGIC ### TOPICS COVERED INCLUDE
// MAGIC - Azure Portal Basics (For Databricks and Other Relevant Services)
// MAGIC - Reading/Writing to Azure SQL DW and SQL DB w/ Azure Databricks
// MAGIC - Reading/Writing to Cosmos DB
// MAGIC - Blob Store Containers (Creating and Mounting in DBFS)
// MAGIC - Structured Streaming and Databricks Delta Architecture

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC 
// MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
// MAGIC |:---:|---|---|
// MAGIC | | **Introduction** | |
// MAGIC | |  [Introduction to Apache Spark]($./01-Spark-Overview) | Students Create a Cluster |
// MAGIC | |  [Azure Databricks Platform Architecture](https://files.training.databricks.com/courses/adbcore/slides/Azure-Databricks-Architecture-Security.pdf) | Slides 1 - 7 |
// MAGIC | |  [The Databricks Environment]($./02-The-Databricks-Environment) | Codealong |
// MAGIC | |  Security Discovery & Compliance | Slides 8 - 30 |
// MAGIC | |  [Configuring Blob Storage]($./03a-Blob-Storage) | Codealong |
// MAGIC | |  [Configuring Key Vault]($./03b-Key-Vault) | Codealong |
// MAGIC | |  [Key Vault-Backed Secret Scopes]($./03c-Key-Vault-Backed-Secret-Scopes) | Codealong |
// MAGIC | | **The DataFrames API** | |
// MAGIC | |  [Reading Data]($./04-Reading-Data) | Codealong |
// MAGIC | |  [Reading & Writing Data Lab]($./04L-Read-Write-Data-Lab) | Lab |
// MAGIC | |  [Transformations Actions]($./05-Transformations-Actions) | Codealong |
// MAGIC | |  [Transformations Actions Lab-1]($./05L.a-Transformations-Actions-Lab-1) | Lab |
// MAGIC | |  [Transformations Actions Lab-2]($./05L.b-Transformations-Actions-Lab-2) | Lab (optional) |
// MAGIC | | **Building and Querying a Delta Lake** | |
// MAGIC | |  [Managed Delta Lake Overview and Architecture](https://files.training.databricks.com/courses/adbcore/slides/Delta-Lake-Technical-Sales-Pitch.pdf) | Presentation Discussion |
// MAGIC | |  [Open Source Delta Lake]($./06-Open-Source-Delta-Lake) | Codealong |
// MAGIC | |  [Delta Lake Basics Lab]($./06L-Delta-Lake-Lab-1) | Lab |
// MAGIC | |  [Managed Delta Lake]($./07-Managed-Delta-Lake) | Codealong |
// MAGIC | |  [Delta Lake Time Machine and Optimization Lab]($./07L-Delta-Lake-Lab-2) | Lab |
// MAGIC | | **Structured Streaming** | |
// MAGIC | |  [MSFT Logical Reference Architectures and ADB](https://files.training.databricks.com/courses/adbcore/slides/Azure-Logical-Archs.pdf) | Presentation Discussion |
// MAGIC | |  [Structured Streaming Concepts]($./08-Structured-Streaming-Concepts) | Codealong |
// MAGIC | |  [Time Windows]($./09-Time-Windows) | Codealong |
// MAGIC | |  [Streaming with Event Hubs]($./10-Streaming-With-Event-Hubs-Demo) | Demo |
// MAGIC | | **Delta Lake Architecture** | |
// MAGIC | |  [Delta Lake Architecture]($./11-Delta-Architecture) | Codealong |
// MAGIC | | **Production Workloads on Azure Databricks** | | |
// MAGIC | |  [Production Workloads on Azure Databricks with Azure Data Factory]($./12-Azure-Data-Factory) | Demo |
// MAGIC | | [CI/CD w/ Azure DevOps]($./13-CI-CD-Devops) | Demo |
// MAGIC | | **Integrating with Other Azure Services** |  |  |
// MAGIC | | [Azure SQL Data Warehouse]($./14-SQL-Data-Warehouse) | Demo |
// MAGIC | | [Azure Cosmos DB]($./15-Cosmos-DB) | Demo |
// MAGIC | | [Azure Databricks Best Practices & Learnings](https://files.training.databricks.com/courses/adbcore/slides/Azure-Databricks-Best-Practices.pdf) | Presentation Discussion |
// MAGIC | | **Capstone** |  |  |
// MAGIC | | [Capstone]($./99-Capstone) | Capstone |

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
