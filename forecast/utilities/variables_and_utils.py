import sys
import os
import boto3
import sys
import time
from awsglue.utils import getResolvedOptions

sys.path.insert(0, os.path.abspath("./utilities") )

glue_client = boto3.client("glue", region_name='eu-west-1')
args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
workflow_name = args['WORKFLOW_NAME']
workflow_run_id = args['WORKFLOW_RUN_ID']
workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name,
                                                          RunId=workflow_run_id)["RunProperties"]

# Common variables
project = str(workflow_name)
role_arn = "arn:aws:iam::<accountID>:role/service-role/AmazonForecast-ExecutionRole-1709790591616"
region = 'eu-west-1'
tts_input_file_uri = str(workflow_params['SELLIN_TTS_DIRECTORY'])
rts_input_file_uri = str(workflow_params['SELLIN_RTS_DIRECTORY'])
sleep_duration = 60   #frequency of poll event from API to check status of tasks


# # Common variables
# project = 'iac_demo'
# #(role_name = 'glue-role-for-testing'
# role_arn = "arn:aws:iam::<account_id>:role/service-role/AmazonForecast-ExecutionRole-1709790591616"
# region = 'eu-west-1'
# tts_input_file_uri= 's3://iac-demo/processed/tts/target_timeseries.csv'
# rts_input_file_uri= 's3://iac-demo/processed/rts/related_timeseries.csv'
# sleep_duration=60   #frequency of poll event from API to check status of tasks

# Dataset Group variables
domain = "RETAIL"

# Dataset variables
tts_schema = {
   "Attributes":[
      {
         "AttributeName":"item_id",
         "AttributeType":"string"
      },
      {
         "AttributeName":"timestamp",
         "AttributeType":"timestamp"
      },
      {
         "AttributeName":"demand",
         "AttributeType":"float"
      },
      {
         "AttributeName":"location_id",
         "AttributeType":"string"
      }
   ]
}

rts_schema = {
   "Attributes":[
      {
         "AttributeName":"item_id",
         "AttributeType":"string"
      },
      {
         "AttributeName":"timestamp",
         "AttributeType":"timestamp"
      },
      {
         "AttributeName":"location_id",
         "AttributeType":"string"
      },
      {
         "AttributeName":"price",
         "AttributeType":"float"
      }
   ]
}


# Dataset import job variables
timezone = "EST"
TIMESTAMP_FORMAT = "yyyy-MM-dd"

# Predictor variables
FORECAST_HORIZON = 38
DATASET_FREQUENCY = "1W"
FORECAST_DIMENSIONS = ['location_id']

# Forecast variables
FORECAST_TYPES = ['0.50']
FORECAST_FREQUENCY = "1W"

# Forecast Export variables
export_path = 's3://iac-demo/baseline-output'
export_name = project + "_export"

# What if forecast variables

timeseriesTransformationsScenario = [
            {
                "Action": {
                    "AttributeName": "price",
                    "Operation": "MULTIPLY",
                    "Value": 0.90
                },
                "TimeSeriesConditions": [
                    {
                        "AttributeName": "timestamp",
                        "AttributeValue": "2023-12-31 00:00:00",
                        "Condition": "GREATER_THAN"
                    }
                ]
            }
        ]

what_if_export_path = 's3://iac-demo/what-if-output/'

workflow_params['SELLIN_WHAT_IF_OUTPUT_DIRECTORY'] = what_if_export_path

combined_view_target_s3_path = 's3://iac-demo/combined-output/'


######################################## Functions used in the main script ############################################3

def wait(callback, time_interval = 10):

    while True:
        status = callback()['Status']
        if status in ('ACTIVE', 'CREATE_FAILED'): break
        time.sleep(time_interval)
    
    return (status=="ACTIVE")


# importing forecast notebook utility from notebooks/common directory
sys.path.insert( 0, os.path.abspath(".") )

session = boto3.Session(region_name=region) 
forecast = session.client(service_name='forecast') 
forecastquery = session.client(service_name='forecastquery')

#() assert forecast.list_what_if_analyses()

# Function to create a dataset group

def create_dataset_group(project):
    response = forecast.create_dataset_group(
    DatasetGroupName = project + "_dsg",
    Domain="RETAIL",
    )
    return response['DatasetGroupArn']
    

# Function to create a dataset
def create_dataset(project, dataset_frequency, dataset_type, schema, jobname_suffix):
    response = forecast.create_dataset(
        Domain = "RETAIL",
        DatasetType = dataset_type,
        DatasetName = project + jobname_suffix,
        DataFrequency = dataset_frequency, 
        Schema = schema
    )
    return response['DatasetArn']

    

def update_dataset_group(dataset_group_arn, tts_dataset_arn, rts_dataset_arn):
    forecast.update_dataset_group( 
        DatasetGroupArn = dataset_group_arn, 
        DatasetArns = [
            tts_dataset_arn,
            rts_dataset_arn,
        ]
    )

def create_dataset_import_job(project, dataset_arn, s3_data_path, jobname_suffix):
    response = forecast.create_dataset_import_job(
        DatasetImportJobName = project + jobname_suffix,
        DatasetArn = dataset_arn,
        DataSource = {
            "S3Config" : {
                "Path" : s3_data_path,
                "RoleArn" : role_arn
            }
        },
        TimestampFormat = TIMESTAMP_FORMAT
    )
    return response['DatasetImportJobArn']

def create_predictor(dataset_group_arn):
    response = forecast.create_auto_predictor(
        PredictorName = project + "_new_predictor",
        ForecastHorizon = FORECAST_HORIZON,
        ForecastFrequency = DATASET_FREQUENCY,
        ForecastDimensions = FORECAST_DIMENSIONS,
        ExplainPredictor = True,
        OptimizationMetric="WAPE",
        DataConfig = {
            "DatasetGroupArn" : dataset_group_arn,
            "AdditionalDatasets" : [
                {
                    "Name": "holiday",
                    "Configuration": {
                        "CountryCode": ["IN"]
                    }
                }
            ]
        }
    )
    return response['PredictorArn']

def create_forecast(project, predictor_arn):
    response = forecast.create_forecast(
        ForecastName = project + "forecast_baseline",
        ForecastTypes=FORECAST_TYPES,
        PredictorArn = predictor_arn
    )
    return response['ForecastArn']

def create_whatif_analysis(whatifanalysisname, forecast_arn):
    response = forecast.create_what_if_analysis(
        WhatIfAnalysisName=whatifanalysisname,
        ForecastArn=forecast_arn,
    )
    return response['WhatIfAnalysisArn']

def create_whatif_forecast(whatifforecastname, whatifanalysisarn, timeseriestransformation):
    response = forecast.create_what_if_forecast(
        WhatIfForecastName=whatifforecastname,
        WhatIfAnalysisArn=whatifanalysisarn,
        TimeSeriesTransformations=timeseriestransformation
    )
    return response['WhatIfForecastArn']

def create_what_if_forecast_export(scenario, whatif_forecast_arn, export_path):
    response = forecast.create_what_if_forecast_export(
        WhatIfForecastExportName=scenario,
        WhatIfForecastArns=[whatif_forecast_arn],
        Destination={
            "S3Config":  {
                "Path": export_path,
                "RoleArn": role_arn,
            }
        }
    )
    return response['WhatIfForecastExportArn']

def wait_till_active(job_type, resource_arn):
    status = None
    
    if job_type == 'create_import_job':
        status = wait(lambda: forecast.describe_dataset_import_job(DatasetImportJobArn=resource_arn))
    elif job_type == 'create_predictor':
        status = wait(lambda: forecast.describe_auto_predictor(PredictorArn=resource_arn))
    elif job_type == 'create_forecast':
        status = wait(lambda: forecast.describe_forecast(ForecastArn=resource_arn))
    elif job_type == 'create_whatif_analysis':
        status = wait(lambda: forecast.describe_what_if_analysis(WhatIfAnalysisArn=resource_arn))
    elif job_type == 'create_whatif_forecast':
        status = wait(lambda: forecast.describe_what_if_forecast(WhatIfForecastArn=resource_arn))
    elif job_type == 'create_whatif_export_job':
        status = wait(lambda: forecast.describe_what_if_forecast_export(WhatIfForecastExportArn=resource_arn))
    
    return status

def describe_auto_predictor(predictor_arn):
    return forecast.describe_auto_predictor(PredictorArn=predictor_arn)
