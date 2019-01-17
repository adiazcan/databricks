# Databricks notebook source
# MAGIC %md # Deploy the model

# COMMAND ----------

from azureml.core import Workspace
import azureml.core

# Check core SDK version number
print("SDK version:", azureml.core.VERSION)

#'''
ws = Workspace.from_config()
print('Workspace name: ' + ws.name, 
      'Azure region: ' + ws.location, 
      'Subscription id: ' + ws.subscription_id, 
      'Resource group: ' + ws.resource_group, sep = '\n')
#'''

# COMMAND ----------

##NOTE: service deployment always gets the model from the current working dir.
import os

model_name = "FlightDelays.mml"
model_dbfs = os.path.join("/dbfs", model_name)

print("copy model from dbfs to local")
model_local = "file:" + os.getcwd() + "/" + model_name
dbutils.fs.cp(model_name, model_local, True)

# COMMAND ----------

#Register the model
from azureml.core.model import Model
mymodel = Model.register(model_path = model_name, # this points to a local file
                       model_name = model_name, # this is the name the model is registered as, am using same name for both path and name.                 
                       description = "Flight model demo",
                       workspace = ws)

print(mymodel.name, mymodel.description, mymodel.version)

# COMMAND ----------

#%%writefile score_sparkml.py
score_sparkml = """

import json

def init():
    try:
        # One-time initialization of PySpark and predictive model
        import pyspark
        from azureml.core.model import Model
        from pyspark.ml import PipelineModel
        
        global trainedModel
        global spark
        
        spark = pyspark.sql.SparkSession.builder.appName("ADB and AML notebook by Parashar").getOrCreate()
        model_name = "{model_name}" #interpolated
        model_path = Model.get_model_path(model_name)
        trainedModel = PipelineModel.load(model_path)
    except Exception as e:
        trainedModel = e

def run(input_json):
    if isinstance(trainedModel, Exception):
        return json.dumps({{"trainedModel":str(trainedModel)}})
      
    try:
        sc = spark.sparkContext
        input_list = json.loads(input_json)
        input_rdd = sc.parallelize(input_list)
        input_df = spark.read.json(input_rdd)
    
        # Compute prediction
        prediction = trainedModel.transform(input_df)
        #result = prediction.first().prediction
        predictions = prediction.collect()

        #Get each scored result
        preds = [str(x['prediction']) for x in predictions]
        result = ",".join(preds)
    except Exception as e:
        result = str(e)
    return json.dumps({{"result":result}})

""".format(model_name=model_name)

exec(score_sparkml)

with open("score_sparkml.py", "w") as file:
    file.write(score_sparkml)

# COMMAND ----------

from azureml.core.conda_dependencies import CondaDependencies 

myacienv = CondaDependencies.create(conda_packages=['scikit-learn','numpy','pandas'])

with open("mydeployenv.yml","w") as f:
    f.write(myacienv.serialize_to_string())

# COMMAND ----------

#deploy to ACI
from azureml.core.webservice import AciWebservice, Webservice

myaci_config = AciWebservice.deploy_configuration(
    cpu_cores = 1, 
    memory_gb = 1, 
    tags = {'name':'Flight Delays'}, 
    description = 'A web service to predict Flight Delays.')

# COMMAND ----------

# this will take 10-15 minutes to finish

service_name = "flightdelaysws"
runtime = "spark-py" 
driver_file = "score_sparkml.py"
my_conda_file = "mydeployenv.yml"

# image creation
from azureml.core.image import ContainerImage
myimage_config = ContainerImage.image_configuration(execution_script = driver_file, 
                                    runtime = runtime, 
                                    conda_file = my_conda_file)

# Webservice creation
myservice = Webservice.deploy_from_model(
  workspace=ws, 
  name=service_name,
  deployment_config = myaci_config,
  models = [mymodel],
  image_config = myimage_config
    )

myservice.wait_for_deployment(show_output=True)

# COMMAND ----------

#for using the Web HTTP API 
print(myservice.scoring_uri)

# COMMAND ----------

testInput = "{\"OriginAirportCode\":\"SAT\",\"Month\":5,\"DayofMonth\":5,\"CRSDepHour\":13,\"DayOfWeek\":7,\"Carrier\":\"MQ\",\"DestAirportCode\":\"ORD\",\"WindSpeed\":9,\"SeaLevelPressure\":30.03,\"HourlyPrecip\":0}"
testInput2 = "{\"OriginAirportCode\":\"ATL\",\"Month\":2,\"DayofMonth\":5,\"CRSDepHour\":8,\"DayOfWeek\":4,\"Carrier\":\"MQ\",\"DestAirportCode\":\"MCO\",\"WindSpeed\":3,\"SeaLevelPressure\":31.03,\"HourlyPrecip\":0}"

test_json = "[" + testInput + "," + testInput2 + "]"

# COMMAND ----------

print(test_json)

# COMMAND ----------

#using data defined above predict if income is >50K (1) or <=50K (0)
myservice.run(input_data=test_json)

# COMMAND ----------

#comment to not delete the web service
myservice.delete()

# COMMAND ----------

