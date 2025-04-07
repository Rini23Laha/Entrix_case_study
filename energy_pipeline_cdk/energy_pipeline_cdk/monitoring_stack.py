from aws_cdk import (
    Stack,
    aws_events as events,
    aws_events_targets as targets,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    CfnOutput
)
from constructs import Construct

class EnergyPipelineMonitoringStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, glue_jobs: list[str], email: str, topic_name="GlueJobFailureAlerts", **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # Create SNS topic
        failure_topic = sns.Topic(self, topic_name)
        failure_topic.add_subscription(subs.EmailSubscription(email))

        # Create CloudWatch event rules for each Glue job failure
        for job_name in glue_jobs:
            events.Rule(self, f"{job_name}FailureRule",
                event_pattern=events.EventPattern(
                    source=["aws.glue"],
                    detail_type=["Glue Job State Change"],
                    detail={
                        "state": ["FAILED"],
                        "jobName": [job_name]
                    }
                ),
                targets=[targets.SnsTopic(failure_topic)]
            )

        # Output
        CfnOutput(self, "FailureSNSTopic", value=failure_topic.topic_arn)
