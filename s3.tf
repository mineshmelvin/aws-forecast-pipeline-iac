resource "aws_s3_bucket" "iac-demo" {
    bucket = "iac_demo"
    provider = aws.ct_region
    tags = var.vpc_tags
}

# Bucket containing all glue job scripts
resource "aws_s3_bucket" "iac-demo-glue-scripts" {
    bucket = "iac_demo_glue_scripts"
}

# Upload preprocessing script
resource "aws_s3_object" "preprocessing-script" {
    bucket = aws_s3_bucket.iac-demo-glue-scripts.id
    key   = "preprocess/preprocess_data.py"
    source = "preprocess/preprocess_data.py"
    etag = filemd5("preprocess/preprocess_data.py")
}

# Upload of forecasting script
resource "aws_s3_object" "forecasting-script" {

    bucket = aws_s3_bucket.iac-demo-glue-scripts.id
    key    = "forecast/run_forecast.py"
    source = "forecast/run_forecast.py"
    etag   = filemd5("forecast/run_forecast.py")

    tags = var.vpc_tags
}

# Upload of sellout forecasting utilities zip file
resource "aws_s3_object" "sellout-forecasting-utilities" {

    bucket = aws_s3_bucket.iac-demo-glue-scripts.id
    key    = "forecast/utils.zip"
    source = data.archive_file.preprocess-utils.output_path
    etag   = filemd5(data.archive_file.preprocess-utils.output_path)

    tags = var.vpc_tags
}
