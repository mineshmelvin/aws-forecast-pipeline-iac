data "archive_file" "preprocess-utils" {
    type = "zip"
    output_path = "${path.root}/forecast/zip/utilities.zip"
    source_dir = "forecast/utilities/"
}