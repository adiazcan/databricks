# Databricks notebook source
# MAGIC %md
# MAGIC ## Install Great Expectations

# COMMAND ----------

# MAGIC   %pip install great-expectations

# COMMAND ----------

import datetime

import pandas as pd
from ruamel import yaml

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up Great Expectations

# COMMAND ----------

root_directory = "/dbfs/great_expectations/"

data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=root_directory
    ),
)
context = BaseDataContext(project_config=data_context_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare your data

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect your data

# COMMAND ----------

datasource_name = "yellow_trip"
dataconnector_name = "yellowcs"
pipeline_stage = "yellowpipeline"
pipeline_run_id = "yellowid"

my_spark_datasource_config = {
    "name": datasource_name,
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        dataconnector_name: {
            "module_name": "great_expectations.datasource.data_connector",
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": [
                pipeline_stage,
                pipeline_run_id,
            ],
        }
    },
}
context.test_yaml_config(yaml.dump(my_spark_datasource_config))
context.add_datasource(**my_spark_datasource_config)

# COMMAND ----------

batch_request = RuntimeBatchRequest(
    datasource_name=datasource_name,
    data_connector_name=dataconnector_name,
    data_asset_name="Yellow Trips",  
    batch_identifiers={
        pipeline_stage: "prod",
        pipeline_run_id: f"my_run_name_{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={"batch_data": df},  
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Expectations

# COMMAND ----------

expectation_suite_name = "yellow_trip_DQ"
context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

print(validator.head())

# COMMAND ----------

validator.expect_column_values_to_not_be_null(column="passenger_count")
validator.expect_column_values_to_be_between(column="passenger_count", min_value=1)
validator.expect_column_values_to_be_between(
    column="congestion_surcharge", min_value=0, max_value=1000
)

validator.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate your data

# COMMAND ----------

my_checkpoint_name = "yellow_trip_check"
checkpoint_config = {
    "name": my_checkpoint_name,
    "config_version": 1.0,
    "class_name": "SimpleCheckpoint",
    "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
}

my_checkpoint = context.test_yaml_config(yaml.dump(checkpoint_config))
context.add_checkpoint(**checkpoint_config)

checkpoint_result = context.run_checkpoint(
    checkpoint_name=my_checkpoint_name,
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
)

# COMMAND ----------


