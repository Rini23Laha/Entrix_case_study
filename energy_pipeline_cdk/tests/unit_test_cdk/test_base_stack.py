import pytest
from aws_cdk import App
from aws_cdk.assertions import Template
from energy_pipeline_cdk.energy_pipeline_cdk.base_stack import EnergyPipelineBaseStack


@pytest.fixture
def template():
    app = App()
    stack = EnergyPipelineBaseStack(app, "TestEnergyPipelineBaseStack")
    return Template.from_stack(stack)


def test_s3_buckets_created(template):
    template.resource_count_is("AWS::S3::Bucket", 3)


def test_glue_role_created(template):
    template.has_resource_properties("AWS::IAM::Role", {
        "AssumeRolePolicyDocument": {
            "Statement": [{
                "Action": "sts:AssumeRole",
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"}
            }]
        }
    })


def test_quicksight_role_created(template):
    template.has_resource_properties("AWS::IAM::Role", {
        "AssumeRolePolicyDocument": {
            "Statement": [{
                "Principal": {"Service": "quicksight.amazonaws.com"}
            }]
        }
    })


def test_glue_databases_created(template):
    template.resource_count_is("AWS::Glue::Database", 3)
    template.has_resource_properties("AWS::Glue::Database", {
        "DatabaseInput": {"Name": "energy_raw_db"}
    })


def test_glue_tables_created(template):
    template.resource_count_is("AWS::Glue::Table", 3)
    template.has_resource_properties("AWS::Glue::Table", {
        "DatabaseName": "energy_gold_db",
        "TableInput": {
            "Name": "tb_mkt_price_gold"
        }
    })


def test_outputs_exist(template):
    outputs = [
        "LandingBucketNameOutput",
        "ApiLandingBucketNameOutput",
        "DomainBucketNameOutput",
        "GlueRoleArnOutput",
        "QuickSightRoleArnOutput"
    ]
    for output in outputs:
        assert template.find_outputs(output) is not None
