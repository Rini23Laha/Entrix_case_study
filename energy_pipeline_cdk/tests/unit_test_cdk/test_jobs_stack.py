import pytest
from aws_cdk import App
from aws_cdk.assertions import Template, Match
from energy_pipeline_cdk.energy_pipeline_cdk.jobs_stack import EnergyPipelineJobsStack  # adjust path if needed


@pytest.fixture
def template():
    app = App()
    stack = EnergyPipelineJobsStack(
        app,
        "TestJobsStack",
        glue_role_arn="arn:aws:iam::123456789012:role/GlueRole",
        domain_bucket_name="my-domain-bucket"
    )
    return Template.from_stack(stack)


def test_glue_workflow_created(template):
    template.has_resource_properties("AWS::Glue::Workflow", {
        "Name": "EnergyDataPipelineWorkflow"
    })


def test_glue_jobs_created(template):
    template.resource_count_is("AWS::Glue::Job", 4)

    for job_name in [
        "market_price_data_raw_to_refined_batch",
        "market_price_data_refined_to_gold_batch",
        "market_price_data_gold_transform_batch",
        "data_quality_validation"
    ]:
        template.has_resource_properties("AWS::Glue::Job", {
            "Name": job_name
        })


def test_triggers_created(template):
    # Check all 4 triggers created with correct type
    expected_triggers = {
        "TriggerRawToRefined": "ON_DEMAND",
        "TriggerRefinedToGold": "CONDITIONAL",
        "TriggerGoldTransform": "CONDITIONAL",
        "TriggerDQJob": "CONDITIONAL"
    }

    for trigger_name, trigger_type in expected_triggers.items():
        template.has_resource_properties("AWS::Glue::Trigger", {
            "Name": trigger_name,
            "Type": trigger_type
        })


def test_eventbridge_rule_created(template):
    template.has_resource_properties("AWS::Events::Rule", {
        "ScheduleExpression": "cron(0 3 * * ? *)"
    })


