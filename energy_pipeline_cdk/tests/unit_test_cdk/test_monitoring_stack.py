import pytest
from aws_cdk import App
from aws_cdk.assertions import Template, Match
from energy_pipeline_cdk.energy_pipeline_cdk.monitoring_stack import EnergyPipelineMonitoringStack  # adjust path


@pytest.fixture
def template():
    app = App()
    stack = EnergyPipelineMonitoringStack(
        app,
        "TestMonitoringStack",
        glue_jobs=["job1", "job2"],
        email="rinilaha@gmail.com"
    )
    return Template.from_stack(stack)


def test_sns_topic_created(template):
    template.has_resource_properties("AWS::SNS::Topic", {})


def test_email_subscription_created(template):
    template.has_resource_properties("AWS::SNS::Subscription", {
        "Protocol": "email",
        "Endpoint": "rinilaha@gmail.com"
    })


def test_cloudwatch_rules_created(template):
    template.resource_count_is("AWS::Events::Rule", 2)

    for job_name in ["job1", "job2"]:
        template.has_resource_properties("AWS::Events::Rule", {
            "EventPattern": Match.object_like({
                "source": ["aws.glue"],
                "detail-type": ["Glue Job State Change"],
                "detail": {
                    "state": ["FAILED"],
                    "jobName": [job_name]
                }
            }),
            "State": "ENABLED"
        })





def test_output_sns_topic_arn(template):
    template.has_output("FailureSNSTopic", Match.any_value())
