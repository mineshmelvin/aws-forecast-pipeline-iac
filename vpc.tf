module "vpc" {
    source = "terraform-aws-modules/vpc/aws"
    version = "4.0.2"
    providers = {
      aws = aws.ct_region
    }

    name = var.vpc_name
    cidr = var.cidr

    private_subnets = var.private_cidr
    public_subnets = var.public_cidr
    enable_dns_hostnames = true

    tags = var.vpc_tags

}

module "flow_logs" {
    source = "terraform-aws-modules/flow-logs/aws"
    providers = {
      aws = aws.ct_region
    }
    vpc_id           = module.vpc.vpc_id
    region           = var.region
    log_retention    = 7
    tags             = var.vpc_tags 
}