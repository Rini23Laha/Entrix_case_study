import pytest
from aws_cdk import App
from aws_cdk.assertions import Template, Match
from energy_pipeline_cdk.energy_pipeline_cdk.EnergyPipelineMonitoringStack import EnergyPipelineMonitoringStack


@pytest.fixture
def template():
    app = App()
    stack = EnergyPipelineMonitoringStack(app, "TestMonitoringStack", glue_job_names=["rawjob", "refinedjob"])
    return Template.from_stack(stack)


def test_event_rules_created(template):
    template.resource_count_is("AWS::Events::Rule", 2)
    template.has_resource_properties("AWS::Events::Rule", {
        "EventPattern": {
            "source": ["aws.glue"],
            "detail-type": ["Glue Job State Change"],
            "detail": {
                "state": ["FAILED"],
                "jobName": Match.array_with(["rawjob"])
            }
        }
    })
