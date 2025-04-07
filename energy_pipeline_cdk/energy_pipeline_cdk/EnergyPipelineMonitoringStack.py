from aws_cdk import (
    Stack,
    aws_events as events,
    aws_events_targets as targets,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_iam as iam,
    CfnOutput
)
from constructs import Construct


class EnergyPipelineMonitoringStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, glue_job_names, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # Create SNS topic for Glue job failures
        failure_topic = sns.Topic(self, "PipelineFailureTopic")
        failure_topic.add_subscription(subs.EmailSubscription("rinilaha@gmail.com"))

        # Create CloudWatch rules for each Glue job failure
        for job_name in glue_job_names:
            rule = events.Rule(self, f"{job_name}FailureRule",
                event_pattern=events.EventPattern(
                    source=["aws.glue"],
                    detail_type=["Glue Job State Change"],
                    detail={
                        "state": ["FAILED"],
                        "jobName": [job_name]
                    }
                )
            )
            rule.add_target(targets.SnsTopic(failure_topic))

        # Output
        CfnOutput(self, "FailureAlertsTopicArn", value=failure_topic.topic_arn)
