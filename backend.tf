terraform {
  required_providers {
    aws = {
        source = "hashicorp/aws"
    }
    awscc = {
        source = "hashicorp/awscc"
        version = "~> 0.1"
    }
  }
}

provider "aws" {
    region = var.region
}

provider "awscc" {
    region = var.region
}

module "remote_state" {
    source = "terraform-aws-modules/remote-state/aws"
    version = "1.0.4"
    state_bucket_name = "iac-demo-terraform-state"
    lock_table_name = "iac-demo-terraform-lock"
}

terraform {
  backend "s3" {
    bucket = "iac-demo-terraform-state"
    key    = "terraform-state.iac-demo.tfstate"
    encrypt = true
    dynomodb_table = "iac-demo-terraform-lock"
    region = var.region
  }
}