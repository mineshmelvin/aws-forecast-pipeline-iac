import sys
import os
sys.path.insert( 0, os.path.abspath(".") )
from utilities.variables_and_utils import variables_and_utils as utilities
import pandas as pd

########################## Function to supress printing account numbers ############################

import re

def mask_arn(input_string):

    mask_regex = re.compile(':\d{12}:')
    mask = mask_regex.search(input_string)
        
    while mask:
        input_string = input_string.replace(mask.group(),'X'*12)
        mask = mask_regex.search(input_string) 
        
    return input_string

####################### Derive the instance of AWS SDK client for Amazon Forecast ##################

forecast = utilities.forecast
# Checking to make sure we can communicate with Amazon Forecast
assert forecast.list_forecasts()


######################################## Create dataset group ######################################

dataset_arns = []

dataset_group_response = None
dataset_group_arn = None

dataset_group_arn = utilities.create_dataset_group(utilities.project)
    
describe_dataset_group_response = forecast.describe_dataset_group(DatasetGroupArn=dataset_group_arn)
print(f"The DatasetGroup with ARN {mask_arn(dataset_group_arn)} is {describe_dataset_group_response['Status']}.")

######################################## Create tts dataset ########################################
tts_dataset_arn = None

tts_dataset_arn = utilities.create_dataset(
                                                    utilities.project, 
                                                    utilities.DATASET_FREQUENCY, 
                                                    'TARGET_TIME_SERIES', 
                                                    utilities.tts_schema, "_tts"
                                                )
    
describe_dataset_response = forecast.describe_dataset(DatasetArn=tts_dataset_arn)
print(f"Dataset ARN {mask_arn(tts_dataset_arn)} is now {describe_dataset_response['Status']}.") 

######################################## Create rts dataset ########################################
rts_dataset_arn = None

rts_dataset_arn = utilities.create_dataset(
                                                    utilities.project, 
                                                    utilities.DATASET_FREQUENCY, 
                                                    'RELATED_TIME_SERIES', 
                                                    utilities.rts_schema, "_rts"
                                                )
   
describe_dataset_response = forecast.describe_dataset(DatasetArn=rts_dataset_arn)
print(f"Dataset ARN {mask_arn(rts_dataset_arn)} is now {describe_dataset_response['Status']}.") 

############################# Update the datasets to the dataset group #############################

utilities.update_dataset_group(dataset_group_arn, tts_dataset_arn, rts_dataset_arn)

############################### Create tts dataset import job ######################################

tts_dataset_import_job_arn = None

tts_dataset_import_job_arn = utilities.create_dataset_import_job(
                                                                        utilities.project, 
                                                                        utilities.TIMESTAMP_FORMAT, 
                                                                        tts_dataset_arn, 
                                                                        utilities.tts_input_file_uri, "_tts_import"
                                                                        )

############################### Create rts dataset import job ##########################################

rts_dataset_import_job_arn = None

rts_dataset_import_job_arn = utilities.create_dataset_import_job(
                                                                        utilities.project, 
                                                                        utilities.TIMESTAMP_FORMAT, 
                                                                        rts_dataset_arn, 
                                                                        utilities.rts_input_file_uri, "_rts_import"
                                                                        )


# Wait for import jobs become active
    
print("Waiting for TTS dataset import job to become ACTIVE.\nStatus:")
utilities.wait_till_active('create_import_job', tts_dataset_import_job_arn)

print("Waiting for RTS dataset import job to become ACTIVE.\nStatus:")
utilities.wait_till_active('create_import_job', rts_dataset_import_job_arn)


## Stop the Resources
# stop_ts_dataset_import_job_arn = forecast.stop_resource(ResourceArn=ts_dataset_import_job_arn)

####################################### Create predictor ###############################################

predictor_arn = None

predictor_arn = utilities.create_predictor(dataset_group_arn)

# Wait for predictor to become active
print("Waiting for Predictor to become ACTIVE. Depending on data size and predictor setting，it can take several hours to be ACTIVE.\n\nCurrent Status:")
status = utilities.wait_till_active('create_predictor', predictor_arn)
describe_auto_predictor_response = utilities.describe_auto_predictor(predictor_arn)
print(f"\n\nThe Predictor is now {describe_auto_predictor_response['Status']}.")

######################################## Create forecast ##############################################

forecast_arn = utilities.create_forecast(utilities.project, predictor_arn)

# Wait for forecast to become active
print("Waiting for Baseline Forecast to become ACTIVE.\n\nCurrent Status:")
status = utilities.wait_till_active('create_forecast', forecast_arn)

###################################### Create forecast export ##########################################
exportname = utilities.project + "_forecast_export_job"
forecast_export_response = forecast.create_forecast_export_job(
                                                                ForecastExportJobName = exportname,
                                                                ForecastArn=forecast_arn, 
                                                                Destination = {
                                                                   "S3Config" : {
                                                                       "Path":utilities.export_path,
                                                                       "RoleArn": utilities.role_arn
                                                                   } 
                                                                }
                                                              )
forecastExportJobArn = forecast_export_response['ForecastExportJobArn']

# Wait for forecast export to become active
print("Waiting for Forecast export to become ACTIVE.\n\nCurrent Status:")
status = utilities.wait_till_active('create_forecast', forecastExportJobArn)

#################################### Create the what if analyses #######################################

WhatIfAnalysisArn = utilities.create_whatif_analysis("PricePromotionAnalysis", forecast_arn)

# Wait for what if analysis to become active
print("Waiting for What-if Analysis to become ACTIVE.\n\nCurrent Status:")
status = utilities.wait_till_active('create_whatif_analysis', WhatIfAnalysisArn)

##################### Create the What-if forecast inside the above What-if Analysis ####################

WhatIfForecastArn_scene1_arn = utilities.create_whatif_forecast("decrease_10_pct_p50", WhatIfAnalysisArn, utilities.timeseriesTransformationsScenario)

# Wait for what if forecasts to become active
print("Waiting for What-if Forecast to become ACTIVE.\n\nCurrent Status:")
utilities.wait_till_active('create_whatif_forecast', WhatIfForecastArn_scene1_arn)

#################################### Export the What-if forecast #######################################

WhatIfForecastExportArn_scene1 = utilities.create_what_if_forecast_export("decrease_10_pct_p50", WhatIfForecastArn_scene1_arn, utilities.what_if_export_path)

# Wait for what if forecast exports to become active
print("Waiting for What-if Forecast Export to become ACTIVE.\n\nCurrent Status:")
utilities.wait_till_active('create_whatif_export_job', WhatIfForecastExportArn_scene1)

#################################### Clean up #######################################

# forecast.delete_resource_tree(ResourceArn=dataset_group_arn)

## (response = forecast.list_dataset_import_jobs("dd")

# for i in response['DatasetImportJobs']:

#     try:
#         if i['DatasetImportJobArn'].index('TAXI_TIME_FORECAST_SUBSET_DEMO'):
#             print('Deleting',i['DatasetImportJobName'])
#             forecast.delete_dataset_import_job(DatasetImportJobArn=i['DatasetImportJobArn'])
#     except:
#         pass

# forecast.delete_dataset(DatasetArn=ts_dataset_arn)

## List the resources
# for dataset_group in forecast.list_dataset_groups()['DatasetGroups']: print('dg: '+dataset_group['DatasetGroupArn'])
# for dataset in forecast.list_datasets()['Datasets']: print('ds: '+dataset['DatasetArn'])
# for job in forecast.list_dataset_import_jobs()['DatasetImportJobs']: print('ij: '+job['DatasetImportJobArn'])
# for predictor in forecast.list_predictors()['Predictors']: print('pd: '+predictor['PredictorArn'])

## Delete the resources
#forecast.delete_resource_tree(ResourceArn=get_dataset_group_arn("pipeline_processed_sdk_dg"))
#forecast.delete_dataset_import_job(DatasetImportJobArn="arn:aws:forecast:eu-west-1:<accountID>:dataset-import-job/forecast_dataset/forecast_import_job")
#forecast.delete_dataset(DatasetArn="arn:aws:forecast:eu-west-1:<accountID>:dataset/forecast_sdk_")
