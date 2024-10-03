import sys
import polars as pl
import boto3
import s3fs
import awsglue.utils import getResolvedOptions

FUTURE_HORIZON = 3
CLEANED_DATA_DIRECTORY = "s3://iac-demo/preprocessed/cleaned_data.csv"
RTS_DIRECTORY = "s3://iac-demo/processed/rts/related_timeseries.csv"
TTS_DIRECTORY = "s3://iac-demo/processed/tts/target_timeseries.csv"

glue_client = boto3.client("glue", region_name='eu-west-1')
args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
workflow_name = args['WORKFLOW_NAME']
workflow_run_id = args['WORKFLOW_RUN_ID']
workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name,
                                                          RunId=workflow_run_id)["RunProperties"]
directories = str(workflow_params['TARGET_FILES']).strip().split(",")
print(directories)

for directory in directories:
    if "demo" in directory:
        print(directory)
        raw_data = pl.read_csv(directory.strip(), infer_schema_length=100000, separator='|')
    else: 
        continue

raw_data = pl.read_csv(directory.strip(), infer_schema_length=100000, separator='|')

raw_data = raw_data.drop("description")    

raw_data = raw_data.with_columns(
        (pl.col("VolumeTonnes")/1000).alias("VolumeKgs")
    ).drop("VolumeTonnes")

raw_data = raw_data.with_columns(
    pl.col("ProductCode").cast(pl.Int32).alias("item_id"),
    pl.col("DayOfSale").cast(pl.Date).alias("timestamp"),
    pl.col("Price").cast(pl.Float64).alias("price"),
    pl.col("VolumeKgs").cast(pl.Float64).alias("demand"),
    pl.col("StoreName").cast(pl.String).alias("location_id")
)

related_time_series = raw_data.select([
    pl.col("item_id"),
    pl.col("timestamp"),
    pl.col("location_id"),
    pl.col("price")
])

target_time_series = raw_data.select([
    pl.col("item_id"),
    pl.col("timestamp"),
    pl.col("demand"),
    pl.col("location_id")
])

# Save the target time series to a csv file
with fs.open(TTS_DIRECTORY, mode='wb') as f:
    target_time_series.write_csv(f)
print("Target time series saved")

# Save the related time series to a csv file
with fs.open(RTS_DIRECTORY, mode='wb') as f:
    related_time_series.write_csv(f)
print("Related time series saved")