// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Databricks Overview
// MAGIC * Who is Databricks?
// MAGIC * What does Databricks offer that is not Open-Source Spark?
// MAGIC * What is Apache Spark?
// MAGIC * A Unifying Engine
// MAGIC * The RDD
// MAGIC * DataFrames, Datasets & SQL
// MAGIC * Scala, Python, Java, R & SQL
// MAGIC * The Cluster: Drivers, Executors, Slots & Tasks
// MAGIC * Quick Note on Jobs & Stages
// MAGIC * Quick Note on Cluster Management

// COMMAND ----------

// MAGIC %md
// MAGIC ## <img src="https://files.training.databricks.com/images/databricks-logo.png" width=30px> Who is Databricks?
// MAGIC 
// MAGIC Databricks was founded by the creators of Apache Spark, Delta Lake and MLflow.
// MAGIC 
// MAGIC Over 2000 global companies use our platform across big data & machine learning lifecycle.
// MAGIC 
// MAGIC **Our Vision**: Accelerate innovation by unifying data science, data engineering and business.
// MAGIC 
// MAGIC **Our Solution**: Unified Analytics Platform
// MAGIC 
// MAGIC <img src=https://files.training.databricks.com/images/adbcore/AAHBu7dlapBDiq6Qf5bUOhIpDwU7YasCBHkB.png width=600px>

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## <img src="https://files.training.databricks.com/images/databricks-logo.png" width=30px> What does Databricks offer that is not Open-Source Spark?
// MAGIC 
// MAGIC * Databricks Workspace - Interactive Data Science & Collaboration.
// MAGIC * Databricks Workflows - Production Jobs & Workflow Automation.
// MAGIC * Databricks Runtime
// MAGIC * Databricks I/O (DBIO) - Optimized Data Access Layer
// MAGIC * Databricks Serverless - Fully Managed Auto-Tuning Platform
// MAGIC * Databricks Enterprise Security (DBES) - End-To-End Security & Compliance

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) What is Apache Spark?
// MAGIC 
// MAGIC Spark is a unified processing engine that can analyze big data using SQL, machine learning, graph processing or real-time stream analysis:
// MAGIC 
// MAGIC ![Spark Engines](https://files.training.databricks.com/images/wiki-book/book_intro/spark_4engines.png)
// MAGIC <br/>
// MAGIC <br/>
// MAGIC * At its core is the Spark Engine.
// MAGIC * The DataFrames API provides an abstraction above RDDs while simultaneously improving performance 5-20x over traditional RDDs with its Catalyst Optimizer.
// MAGIC * Spark ML provides high quality and finely tuned machine learning algorithms for processing big data.
// MAGIC * The Graph processing API gives us an easily approachable API for modeling pairwise relationships between people, objects, or nodes in a network.
// MAGIC * The Streaming APIs give us End-to-End Fault Tolerance, with Exactly-Once semantics, and the possibility for sub-millisecond latency.
// MAGIC 
// MAGIC And it all works together seamlessly!

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##<img src="https://files.training.databricks.com/images/databricks-logo.png" width=30px>  Azure Databricks
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/adbcore/AAEm3c7c2OxHX59IeFUy5cF3z72YP1arKIUB.png" width=400px>
// MAGIC 
// MAGIC As a compute engine, Azure Databricks sits at the center of your Azure-based software platform and provides native integration with Azure Active Directory (Azure AD) and other Azure services.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Scala, Python, Java, R & SQL
// MAGIC * Besides being able to run in many environments...
// MAGIC * Apache Spark makes the platform even more approachable by supporting multiple languages:
// MAGIC   * Scala - Apache Spark's primary language.
// MAGIC   * Python - More commonly referred to as PySpark
// MAGIC   * R - <a href="https://spark.apache.org/docs/latest/sparkr.html" target="_blank">SparkR</a> (R on Spark)
// MAGIC   * Java
// MAGIC   * SQL - Closer to ANSI SQL 2003 compliance
// MAGIC     * Now running all 99 TPC-DS queries
// MAGIC     * New standards-compliant parser (with good error messages!)
// MAGIC     * Subqueries (correlated & uncorrelated)
// MAGIC     * Approximate aggregate stats
// MAGIC * With the older RDD API, there are significant differences with each language's implementation, namely in performance.
// MAGIC * With the newer DataFrames API, the performance differences between languages are nearly nonexistence (especially for Scala, Java & Python).
// MAGIC * With that, not all languages get the same amount of love - just the same, that API gap for each language is rapidly closing, especially between Spark 1.x and 2.x.
// MAGIC 
// MAGIC ![RDD vs DataFrames](https://files.training.databricks.com/images/105/rdd-vs-dataframes.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) RDDs
// MAGIC * The primary data abstraction of Spark engine is the RDD: Resilient Distributed Dataset
// MAGIC   * Resilient, i.e., fault-tolerant with the help of RDD lineage graph and so able to recompute missing or damaged partitions due to node failures.
// MAGIC   * Distributed with data residing on multiple nodes in a cluster.
// MAGIC   * Dataset is a collection of partitioned data with primitive values or values of values, e.g., tuples or other objects.
// MAGIC * The original paper that gave birth to the concept of RDD is <a href="https://cs.stanford.edu/~matei/papers/2012/nsdi_spark.pdf" target="_blank">Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing</a> by Matei Zaharia et al.
// MAGIC * Today, with Spark 2.x, we treat RDDs as the assembly language of the Spark ecosystem.
// MAGIC * DataFrames, Datasets & SQL provide the higher level abstraction over RDDs.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) The Cluster: Drivers, Executors, Slots & Tasks
// MAGIC ![Spark Physical Cluster, slots](https://files.training.databricks.com/images/105/spark_cluster_slots.png)

// COMMAND ----------

// MAGIC %md
// MAGIC * The **Driver** is the JVM in which our application runs.
// MAGIC * The secret to Spark's awesome performance is parallelism.
// MAGIC   * Scaling vertically is limited to a finite amount of RAM, Threads and CPU speeds.
// MAGIC   * Scaling horizontally means we can simply add new "nodes" to the cluster almost endlessly.
// MAGIC * We parallelize at two levels:
// MAGIC   * The first level of parallelization is the **Executor** - a Java virtual machine running on a node, typically, one instance per node.
// MAGIC   * The second level of parallelization is the **Slot** - the number of which is determined by the number of cores and CPUs of each node.
// MAGIC * Each **Executor** has a number of **Slots** to which parallelized **Tasks** can be assigned to it by the **Driver**.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ![Spark Physical Cluster, tasks](https://files.training.databricks.com/images/105/spark_cluster_tasks.png)
// MAGIC <br/>
// MAGIC <br/>
// MAGIC * The JVM is naturally multithreaded, but a single JVM, such as our **Driver**, has a finite upper limit.
// MAGIC * By creating **Tasks**, the **Driver** can assign units of work to **Slots** for parallel execution.
// MAGIC * Additionally, the **Driver** must also decide how to partition the data so that it can be distributed for parallel processing (not shown here).
// MAGIC * Consequently, the **Driver** is assigning a **Partition** of data to each task - in this way each **Task** knows which piece of data it is to process.
// MAGIC * Once started, each **Task** will fetch from the original data source the **Partition** of data assigned to it.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Quick Note on Jobs & Stages
// MAGIC * Each parallelized action is referred to as a **Job**.
// MAGIC * The results of each **Job** (parallelized/distributed action) is returned to the **Driver**.
// MAGIC * Depending on the work required, multiple **Jobs** will be required.
// MAGIC * Each **Job** is broken down into **Stages**.
// MAGIC * This would be analogous to building a house (the job)
// MAGIC   * The first stage would be to lay the foundation.
// MAGIC   * The second stage would be to erect the walls.
// MAGIC   * The third stage would be to add the room.
// MAGIC   * Attempting to do any of these steps out of order just won't make sense, if not just impossible.
// MAGIC 
// MAGIC ** *Note:* ** *We will be going much deeper into Jobs & Stages and the *<br/>
// MAGIC *effect they have on our software as we progress through this class.*

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Quick Note on Cluster Management
// MAGIC 
// MAGIC * At a much lower level, Spark Core employs a **Cluster Manager** that is responsible for provisioning nodes in our cluster.
// MAGIC   * Databricks provides a robust, high-performing **Cluster Manager** as part of its overall offerings.
// MAGIC * In each of these scenarios, the **Driver** is [presumably] running on one node, with each **Executors** running on N different nodes.
// MAGIC * For the sake of this class, we don't need to concern ourselves with cluster management.
// MAGIC   * Ya Databricks!
// MAGIC * From a developer's and student's perspective my primary focus is on...
// MAGIC   * The number of **Partitions** my data is divided into.
// MAGIC   * The number of **Slots** I have for parallel execution.
// MAGIC   * How many **Jobs** am I triggering?
// MAGIC   * And lastly the **Stages** those jobs are divided into.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
