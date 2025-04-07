from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_events as events,
    aws_events_targets as targets
)
from constructs import Construct


class EnergyPipelineJobsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, glue_role_arn: str, domain_bucket_name: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # === Glue Workflow ===
        workflow = glue.CfnWorkflow(self, "GlueWorkflow", name="EnergyDataPipelineWorkflow")

        # === Helper to create jobs ===
        def create_glue_job(name: str, script_path: str, temp_prefix: str, extra_args=None):
            default_args = {
                "--TempDir": f"s3://{domain_bucket_name}/{temp_prefix}/temp/",
                "--job-language": "python",
                "--config-bucket": domain_bucket_name,
                "--config-key": "config/common_config.json",
                "--metadata-key": f"checkpoints/{temp_prefix}_last_processed.json"
            }
            if extra_args:
                default_args.update(extra_args)

            return glue.CfnJob(self, f"{name}Job",
                name=name,
                role=glue_role_arn,
                command={
                    "name": "glueetl",
                    "scriptLocation": f"s3://{domain_bucket_name}/{script_path}",
                    "pythonVersion": "3"
                },
                glue_version="3.0",
                default_arguments=default_args,
                number_of_workers=2,
                worker_type="G.1X"
            )

        # === Glue Jobs ===
        raw_job = create_glue_job(
            "market_price_data_raw_to_refined_batch",
            "raw/glue/energy_pipeline_batch_raw.py",
            "raw"
        )

        refined_job = create_glue_job(
            "market_price_data_refined_to_gold_batch",
            "refined/glue/energy_pipeline_batch_refined.py",
            "refined",
            extra_args={
                "--schema-key": "refined/config/schema/refined_schema.yaml"
            }
        )

        gold_job = create_glue_job(
            "market_price_data_gold_transform_batch",
            "gold/glue/energy_pipeline_batch_gold.py",
            "gold"
        )

        dq_job = create_glue_job(
            "data_quality_validation",
            "data_quality/glue/dq_validation_gold.py",
            "dq"
        )

        # === Trigger Chain ===
        glue.CfnTrigger(self, "TriggerRawToRefined",
            name="TriggerRawToRefined",
            type="ON_DEMAND",
            actions=[{"jobName": raw_job.name}],
            workflow_name=workflow.name
        )

        glue.CfnTrigger(self, "TriggerRefinedToGold",
            name="TriggerRefinedToGold",
            type="CONDITIONAL",
            actions=[{"jobName": refined_job.name}],
            workflow_name=workflow.name,
            predicate={
                "conditions": [{
                    "jobName": raw_job.name,
                    "state": "SUCCEEDED",
                    "logicalOperator": "EQUALS"
                }]
            }
        )

        glue.CfnTrigger(self, "TriggerGoldTransform",
            name="TriggerGoldTransform",
            type="CONDITIONAL",
            actions=[{"jobName": gold_job.name}],
            workflow_name=workflow.name,
            predicate={
                "conditions": [{
                    "jobName": refined_job.name,
                    "state": "SUCCEEDED",
                    "logicalOperator": "EQUALS"
                }]
            }
        )

        glue.CfnTrigger(self, "TriggerDQJob",
            name="TriggerDQJob",
            type="CONDITIONAL",
            actions=[{"jobName": dq_job.name}],
            workflow_name=workflow.name,
            predicate={
                "conditions": [{
                    "jobName": gold_job.name,
                    "state": "SUCCEEDED",
                    "logicalOperator": "EQUALS"
                }]
            }
        )

        # === EventBridge Scheduled Trigger: 3 AM UTC Daily ===
        rule = events.Rule(self, "DailyWorkflowTrigger",
            schedule=events.Schedule.cron(minute="0", hour="3")
        )

        rule.add_target(targets.AwsApi(
            service="Glue",
            action="startWorkflowRun",
            parameters={"Name": workflow.name}
        ))
