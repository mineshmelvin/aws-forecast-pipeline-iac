# AWS Global

variable "region" {
    type = string
    description = "region to deploy the infrastructure"
    default = "us-west-1"
}

# VPC

variable "cidr" {
    type = string
    default = "10.6.0.0/26"
}

variable "vpc_name" {
    type = string
    default = "iac-demo"
}

variable "private_cidr" {
    type = list(string)
    default = [ "10.6.0.0/30", "10.6.0.0/30" ]
}

variable "public_cidr" {
    type = list(string)
    default = [ "10.6.2.0/30", "10.6.2.0/30" ]
}

variable "vpc_tags" {
    type = map(string)
    default = {
        "Name" : "iac-demo-vpc",
        "Environment" : "Dev"
    }
}

# Glue

variable "glue_job_worker_type" {
    default = "Standard"
}

variable "glue_max_capacity" {
    default = 1.0
}

variable "glue_service_role" {
    default = "arn:aws:iam::<accountID>:role/service-role/AWSGlueServiceRole-Crawler"
}

# S3