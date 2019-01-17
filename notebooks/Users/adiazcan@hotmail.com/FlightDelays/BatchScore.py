# Databricks notebook source
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, Bucketizer
from pyspark.sql.functions import array, col, lit
from pyspark.sql.types import *

# COMMAND ----------

accountName = "encbigdatastorage"
containerName = "sparkcontainer"

# COMMAND ----------

data_schema = StructType([
        StructField('OriginAirportCode',StringType()),
        StructField('Month', IntegerType()),
        StructField('DayofMonth', IntegerType()),
        StructField('CRSDepHour', IntegerType()),
        StructField('DayOfWeek', IntegerType()),
        StructField('Carrier', StringType()),
        StructField('DestAirportCode', StringType()),
        StructField('DepDel15', IntegerType()),
        StructField('WindSpeed', DoubleType()),
        StructField('SeaLevelPressure', DoubleType()),  
        StructField('HourlyPrecip', DoubleType())])

# COMMAND ----------

dfDelays = spark.read.csv("wasbs://" + containerName + "@" + accountName + ".blob.core.windows.net/FlightsAndWeather/*/*/FlightsAndWeather.csv",
                    schema=data_schema,
                    sep=",",
                    header=True)

# COMMAND ----------

# Load the saved pipeline model
model = PipelineModel.load("/dbfs/FileStore/models/pipelineModel")

# COMMAND ----------

# Make a prediction against the dataset
prediction = model.transform(dfDelays)

# COMMAND ----------

prediction.write.mode("overwrite").saveAsTable("scoredflights")

# COMMAND ----------

