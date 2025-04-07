from aws_cdk import App, Environment
from energy_pipeline_cdk.base_stack import EnergyPipelineBaseStack
from energy_pipeline_cdk.jobs_stack import EnergyPipelineJobsStack
from energy_pipeline_cdk.monitoring_stack import EnergyPipelineMonitoringStack

app = App()

env = Environment(account="230908437522", region="us-east-1")

base_stack = EnergyPipelineBaseStack(app, "EnergyPipelineBaseStack")

jobs_stack = EnergyPipelineJobsStack(app, "EnergyPipelineJobsStack",
    glue_role_arn=base_stack.glue_role.role_arn,
    domain_bucket_name=base_stack.domain_bucket.bucket_name
)

monitoring_stack = EnergyPipelineMonitoringStack(app, "EnergyPipelineMonitoringStack",
    glue_jobs=[
        "market_price_data_raw_to_refined_batch",
        "market_price_data_refined_to_gold_batch",
        "market_price_data_gold_transform_batch"
    ],
    email="rinilaha@gmail.com",
    env=env
)



app.synth()
