# Workflow orchestration
/* start_workflow -> preprocessing_trigger -> preprocessing_job -> forecast_trigger -> forecast_job */

# Glue catalog for crawlers
resource "aws_glue_catalog_database" "iac-demo-catalog" {
  name = "iac_demo_catalog"
  tags = var.vpc_tags
}

# Glue workflow orchestration
resource "aws_glue_workflow" "iac-demo-workflow" {
    name = "iac_demo_workflow"
    description = "Workflow to orchestrate glue pipeline from source data to final output files. This workflow includes finding the newest file from an s3 bucket, cleaning up and preparing the data, running forecast training, and finally export it to data source."
    max_concurrent_runs = 1
    tags = var.vpc_tags
}

# Glue trigger to start workflow pipeline
resource "aws_glue_trigger" "start-preprocessing-trigger" {
    name = "start_preprocessing_trigger"
    type = "ON_DEMAND"
    workflow_name = aws_glue_workflow.iac-demo-workflow.name
    enabled = true

    actions {
        job_name = aws_glue_job.start-preprocessing-job.name
    }
    tags = var.vpc_tags
}

# Glue job for preprocessing data
resource "aws_glue_job" "start-preprocessing-job" {
    name = "preprocess_raw_data"
    role_arn = var.glue_service_role
    glue_version = "3.0"
    max_capacity = 0.0625
    
    default_arguments = {
      "--additional-python-modules": "polars"
    }

    command {
        name = "pythonshell"
        script_location = "s3://${aws_s3_object.preprocessing-script.bucket}/${aws_s3_object.preprocessing-script.key}"
        python_version = "3.9"
    }
    tags = var.vpc_tags
}

# Glue trigger to start forecasting pipeline
resource "aws_glue_trigger" "start-forecast-trigger" {
    name = "start_forecast_trigger"
    type = "CONDITIONAL"
    workflow_name = aws_glue_workflow.iac-demo-workflow.name

    predicate {
      conditions {
        job_name = aws_glue_job.start-preprocessing-job.name
        state = "SUCCEEDED"
      }
    }

    actions {
      job_name = aws_glue_job.start-forecast-job.name
    }
}

# Glue job for running AWS Forecast using python
resource "aws_glue_job" "start-forecast-job" {
    name = "start_forecast_job"
    role_arn = var.glue_service_role
    glue_version = "3.0"
    max_capacity = 0.0625

    default_arguments = {
      "--extra-py-files" : "s3://${aws_s3_object.forecast-utilities.bucket}/${aws_s3_object.forecast-utilities.key},s3://${aws_s3_object.forecast-utils.bucket}/${aws_s3_object.forecast-utils.key}"
    }

    command {
        name = "pythonshell"
        script_location = "s3://${aws_s3_object.forecast-script.bucket}/${aws_s3_object.forecast-script.key}"
        python_version = "3.9"
    }

    tags = var.vpc_tags
}


# Glue crawler for forecast output
resource "aws_glue_crawler" "forecast-output-crawler" {
  name = "forecast_output_crawler"
  database_name = "iac-demo-catalog"
  provider = aws.ct_region
  role = var.glue_service_role
  table_prefix = "forecast_output_"
  
  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }

  s3_targets {
    path = "s3://${aws_s3_bucket.iac-demo.bucket}/forecast-output/"
    recursive = true
  }
  configuration = jsonencode({
    "Version": 1.0,
    "Grouping": {
        TableLevelConfiguration = 3 }
    }
  )

 tags = var.tags
}